//! Memory-mapped file for sharing a queue across processes
//! 
//! This is a backing that both producer or consumer need to use to speak
//! over shared memory.
//! 
//! We introduce another concept, called create vs attach. This is independent
//! of the producer/consumer roles. Create means that someone is setting up
//! the rings for the first time with some config options. Attach means that
//! someone is connecting to an existing ring and loads the configuration from
//! the memory mapped registers.
//! 
//! Although there is no limit to the number of times new_attach can be
//! called, it is undefined behavior for there to be more than one producer
//! and more than one consumer (i.e., subsequent attaches should be in a
//! read-only manner, and in this case the act of dequeueing is NOT considered
//! a read-only operation as it must manipulate the registers)

use std::cell::UnsafeCell;
use std::fmt;
use std::io::Error as IoError;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicUsize, Ordering};

use memmap::{MmapMut, MmapOptions};

use super::buffer_pool::BufferPool;

/// Size of cache line, to prevent false sharing
const CACHE_LINE_SIZE: usize = 64;

/// Size of a page. Since we're currently hardcoded, support for hugepages
/// is not yet implemented
static PAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub enum Error {
    /// Size of buffer must be cache-aligned to 64 bytes
    BufferSize,
    
    /// Number of elements must be a power of 2, and must be large enough to
    /// store buffer headroom data. Size of headroom is governed by
    /// [`BufferLength`]
    BufferPoolSize,

    /// An I/O error occurred dealing with the mmap backend
    Io(IoError),

    /// Not enough buffers available in pool
    NoBuffers,
}

type BufferIndex = u32;

/// Registers that are stored in beginning of the memory-mapped file.
/// 
/// This struct should not exceed the size of the cache line. There are no
/// checks for this scenario, and it will result in the buffer pool clobbering
/// the registers.
#[repr(C)]
pub struct Registers {
    /// Head of the queue, what producer writes to
    head: AtomicUsize,

    /// Tail of the queue, what consumer reads from
    tail: AtomicUsize,

    /// Size of individual buffer, in bytes
    buffer_size: usize,

    /// Number of buffers in the queue
    buffer_pool_size: usize,

    /// Number of slots in the ring buffer
    slots: usize,
}

/// Mapping of a queue to memory that can be shared across processes
/// 
/// Although it's marked as Send+Sync, caller should never allow more than one
/// producer or more than one consumer per mapping.
pub struct Mapping {
    /// Memory-mapped file we store to prevent dropping and unmapping. This
    /// would cause other pointers in memory to become dangling
    _mmap: MmapMut,

    /// Points to the registers in the memory-mapped file
    registers: *mut Registers,

    /// Number of slots in the ring buffer
    slots: usize,

    /// Ring buffer of indices 
    ring: Vec<*mut BufferIndex>,
    
    /// Pool of buffer chains
    buffer_pool: UnsafeCell<BufferPool>,
}

// Safety: Mapping is Send because *mut Registers points to memory-mapped
// memory.
unsafe impl Send for Mapping {}

// Safety: Mapping is Sync because *mut Registers points to memory-mapped
// memory that we use atomics to read/write from
unsafe impl Sync for Mapping {}

impl Mapping {
    /// Creates the memory-mapped files needed for a ring buffer. Accepts
    /// ring configuration options
    /// 
    /// This is independent of whether or not the caller intends to use the
    /// mapping in a producer or consumer role
    /// 
    /// Returns an [`Error`] if unsuccesful
    pub fn new_create<S>(
        path: S,
        buffer_size: usize,
        buffer_pool_size: usize,
        slots: usize,
    ) -> Result<Self, Error>
    where
        S: Into<String>,
    {
        // verify buffer size is cache-aligned
        if buffer_size % CACHE_LINE_SIZE != 0 {
            return Err(Error::BufferSize);
        }

        // verify buffer pool size is a power of 2 and greater than 0
        if buffer_pool_size.count_ones() != 1 || buffer_pool_size == 0 {
            return Err(Error::BufferPoolSize);
        }

        // create a file at path (typically should be /dev/shm) with the
        // calculated file size
        let file_size = calculate_mapping_size(
            buffer_size,
            buffer_pool_size,
            slots,
            PAGE_SIZE,
        );
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.into())?;
        file.set_len(file_size as u64)?;

        // map the file into memory
        let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        // get the pointer to the registers
        let buf = mmap.as_mut_ptr();
        let registers = buf as *mut Registers;

        // write to the registers with the config options we've been given
        unsafe {
            (*registers) = Registers {
                head: AtomicUsize::new(0),
                tail: AtomicUsize::new(0),
                buffer_size,
                buffer_pool_size,
                slots,
            };
        }

        // set up the ring buffer
        let ring_start = unsafe { mmap.as_mut_ptr().add(CACHE_LINE_SIZE) };
        let mut ring = Vec::with_capacity(slots);
        for i in 0..slots {
            ring.push(unsafe { 
                ring_start.add(i * size_of::<BufferIndex>()) as *mut BufferIndex 
            });
        }

        // set up the buffer pool
        let pool_start = unsafe { ring_start.add(slots * size_of::<BufferIndex>()) };
        let buffer_pool = UnsafeCell::new(BufferPool::new(
            buffer_size,
            buffer_pool_size,
            pool_start,
        ));
            
        Ok(Mapping {
            _mmap: mmap,
            registers,
            slots,
            ring,
            buffer_pool,
        })
    }

    /// Attaches to an existing memory-mapped file that contains the ring
    /// buffer configuration from another call to [`new_create`].
    /// 
    /// This is independent of whether or not the caller intends to use the
    /// mapping in a producer or consumer role
    /// 
    /// Returns an [`Error`] if unsuccesful
    /// 
    /// [`new_create`]: Self::new_create
    pub fn new_attach<S>(path: S) -> Result<Self, Error>
    where
        S: Into<String>,
    {
        // open the file at path
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.into())?;

        // map the file into memory
        let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        // get the pointer to the registers
        let buf = mmap.as_mut_ptr();
        let registers = buf as *mut Registers;

        // load the buffer size, buffer pool size and slots
        let (
            buffer_size,
            buffer_pool_size,
            slots
        ) = unsafe {(
            (*registers).buffer_size,
            (*registers).buffer_pool_size,
            (*registers).slots,
        )};

        // set up the ring buffer
        let ring_start = unsafe { mmap.as_mut_ptr().add(CACHE_LINE_SIZE) };
        let mut ring = Vec::with_capacity(slots);
        for i in 0..slots {
            ring.push(unsafe { 
                ring_start.add(i * size_of::<BufferIndex>()) as *mut BufferIndex 
            });
        }

        // set up the buffer pool
        let pool_start = unsafe { ring_start.add(slots * size_of::<BufferIndex>()) };
        let buffer_pool = UnsafeCell::new(BufferPool::new(
            buffer_size,
            buffer_pool_size,
            pool_start,
        ));
            
        Ok(Mapping {
            _mmap: mmap,
            registers,
            slots,
            ring,
            buffer_pool,
        })
    }

    /// Number of pending entries in the queue that have not been dequeued. In
    /// other words, how many items the consumer can dequeue before running out
    /// of items
    pub fn pending(&self) -> usize {
        // load head and tail registers
        let (head, tail) = unsafe {
            (
                (*self.registers).head.load(Ordering::Acquire),
                (*self.registers).tail.load(Ordering::Acquire)
            )
        };

        // panic if the head has wrapped around the tail somehow
        assert!(head >= tail);

        head - tail
    }

    /// Capacity of ring buffer. In other words, how many items the producer
    /// can enqueue before running out of space
    pub fn capacity(&self) -> usize {
        self.slots - self.pending()
    }

    /// Bulk enqueue operation. Returns number of entries succesfully enqueued.
    /// 
    /// Buffer pool is responsible for allocating multiple buffers if needed.
    pub fn enqueue_bulk_bytes(&mut self, data: &[impl AsRef<[u8]>]) -> usize {
        let buffer_pool = unsafe { &mut *self.buffer_pool.get() };
        // trim data to the smaller of the two (capacity or data length)
        let enqueued = data.len().min(self.capacity());
        let data = &data[..enqueued];

        // get the head pointer as an AtomicUsize
        let head = unsafe { &(*self.registers).head };
        // get the current head value
        let head_start = head.load(Ordering::Acquire);

        for (data_idx, input) in data.iter().enumerate() {
            let input = input.as_ref();

            // allocate buffer chain for this input
            if let Some((buffer, buffer_idx)) = buffer_pool.alloc_chain(input.len()) {
                // write data to buffer chain
                if unsafe { buffer_pool.write_chain(&buffer, input) }.is_none() {
                    println!("buffer pool write failed, no available buffer space");
                    // write failed, release chain and skip this input
                    buffer_pool.release_chain(&buffer);
                    continue;
                }
                
                // store buffer index in ring
                let ring_idx = (head_start + data_idx) % self.slots;
                unsafe {
                    *self.ring[ring_idx] = buffer_idx as u32;
                }
            } else {
                // failed to allocate chain, stop here
                head.store(head_start + data_idx, Ordering::Release);
                return data_idx;
            }
        }

        // write the new head value as a single store after all the writes
        head.store(head_start + enqueued, Ordering::Release);

        // since length is trimmed, we simply return length of data
        enqueued
    }

    /// Bulk dequeue operation. Returns number of entries succesfully dequeued
    pub fn dequeue_bulk_bytes(&mut self, data: &mut [Vec<u8>]) -> usize {
        let buffer_pool = unsafe { &mut *self.buffer_pool.get() };
        // trim data to the smaller of the two (pending or data length)
        let dequeued = data.len().min(self.pending());
        let data = &mut data[..dequeued];

        // get the tail pointer as an AtomicUsize
        let tail = unsafe { &(*self.registers).tail };
        // get the current tail value
        let tail_start = tail.load(Ordering::Acquire);

        for (data_idx, output) in data.iter_mut().enumerate() {
            // get the buffer to read from
            let ring_idx = (tail_start + data_idx) % self.slots;
            let buffer_idx = unsafe { *self.ring[ring_idx] };
            let buffer = buffer_pool.get_buffer(buffer_idx);
            
            unsafe {
                // copy the data out of the buffer
                let bytes_read = buffer_pool.read_chain(&buffer, output);
                output.truncate(bytes_read);
            }
            
            // release buffer chain
            buffer_pool.release_chain(&buffer);
        }

        // write the new tail value as a single store after all the reads
        tail.store(tail_start + dequeued, Ordering::Release);

        // since length is trimmed, we simply return length of data
        dequeued
    }
}

/// How many bytes should be allocated for the memory-mapped file
fn calculate_mapping_size(
    buffer_size: usize,
    buffer_pool_size: usize,
    slots: usize,
    page_size: usize,
) -> usize {
    // registers are at least size of cache line
    let mut size = CACHE_LINE_SIZE;

    // ring buffer is num slots * size of buffer index
    size += slots * size_of::<BufferIndex>();

    // buffer pool size
    size += BufferPool::calculate_mapping_size(
        buffer_size, 
        buffer_pool_size, 
        PAGE_SIZE, 
        CACHE_LINE_SIZE
    );

    // round up to nearest page size
    size += page_size - (size % page_size);

    size
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Self {
        Error::Io(e)
    }
}

// implement display for Error

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::BufferSize => write!(f, "buffer size invalid"),
            Error::BufferPoolSize => write!(f, "buffer pool size invalid"),
            Error::NoBuffers => write!(f, "no buffers available in pool"),
            Error::Io(e) => write!(f, "I/O error: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Using a single mapping, enqueue and dequeue some data and be sure that
    /// the data is the same.
    /// 
    /// Note that in production, we won't be using the same mapping for both
    /// producer and consumer, but this is more a test to ensure that the
    /// producer and consumer logic is working as expected.
    #[test]
    fn test_single_mapping_enqueue_dequeue() {
        let mut mapping = Mapping::new_create(
            "/dev/shm/test_single_mapping_enqueue_dequeue",
            2048,
            16,
            8,
        ).unwrap();

        let data = vec![
            [0u8; 16],
            [1u8; 16],
            [2u8; 16],
            [3u8; 16],
        ];
        println!("enqueue: {}", mapping.enqueue_bulk_bytes(&data));

        let mut output = vec![vec![0u8; 1024]; 16];
        let dequeued = mapping.dequeue_bulk_bytes(&mut output);
        println!("dequeue: {}", dequeued);

        for idx in 0..dequeued {
            assert_eq!(&output[idx], &data[idx]);
        }
    }

    /// Using two different mappings, enqueue and dequeue some data and be
    /// sure that the data is the same.
    #[test]
    fn test_dual_mapping_enqueue_dequeue() {
        // create producer + attach consumer
        let mut producer = Mapping::new_create(
            "/dev/shm/test_dual_mapping_enqueue_dequeue_producer",
            2048,
            16,
            8,
        ).unwrap();
        let mut consumer = Mapping::new_attach(
            "/dev/shm/test_dual_mapping_enqueue_dequeue_producer",
        ).unwrap();

        let data = vec![
            [0u8; 16],
            [1u8; 16],
            [2u8; 16],
            [3u8; 16],
        ];
        println!("enqueue: {}", producer.enqueue_bulk_bytes(&data));

        let mut output = vec![vec![0u8; 1024]; 16];
        let dequeued = consumer.dequeue_bulk_bytes(&mut output);
        println!("dequeue: {}", dequeued);

        for idx in 0..dequeued {
            assert_eq!(&output[idx], &data[idx]);
        }
    }

    #[test]
    fn test_large_message() {
        let mut mapping = Mapping::new_create(
            "/dev/shm/test_large_message",
            64,  
            16,
            8,
        ).unwrap();

        // create data larger than single buffer (64 bytes)
        let data = vec![
            vec![1u8; 100],
            vec![2u8; 150],
        ];
        
        println!("enqueue: {}", mapping.enqueue_bulk_bytes(&data));

        let mut output = vec![vec![0u8; 1024]; 16];
        let dequeued = mapping.dequeue_bulk_bytes(&mut output);
        println!("dequeue: {}", dequeued);

        for idx in 0..dequeued {
            assert_eq!(&output[idx], &data[idx]);
        }
    }

    #[test]
    fn test_buffer_pool_exhaustion() {
        let mut mapping = Mapping::new_create(
            "/dev/shm/test_pool_exhaustion",
            64,
            4,
            8,
        ).unwrap();

        // try to enqueue messages that would require more buffers than available
        let data = vec![
            // needs 2 buffers
            vec![1u8; 100],  
            // needs 3 buffers
            vec![2u8; 150],  
            // total would need 5 buffers, but pool only has 4
        ];
        
        let enqueued = mapping.enqueue_bulk_bytes(&data);
        println!("enqueued: {}", enqueued);
        
        // should only enqueue first message as second would exceed pool
        assert_eq!(enqueued, 1); 

        let mut output = vec![vec![0u8; 1024]; 16];
        let dequeued = mapping.dequeue_bulk_bytes(&mut output);
        println!("dequeue: {}", dequeued);

        assert_eq!(dequeued, 1);
        assert_eq!(&output[0], &data[0]);
    }
}
