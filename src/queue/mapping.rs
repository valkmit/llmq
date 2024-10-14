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

use std::fmt;
use std::io::Error as IoError;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicUsize, Ordering};

use memmap::{MmapMut, MmapOptions};

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
    Io(IoError)
}

/// Registers that are stored in beginning of the memory-mapped file.
/// 
/// This struct should not exceed the size of the cache line. There are no
/// checks for this scenario, and it will result in the buffer pool clobbering
/// the registers.
#[repr(C)]
pub struct Registers {
    /// Head of the queue, what producer writes to
    head: usize,

    /// Tail of the queue, what consumer reads from
    tail: usize,

    /// Size of individual buffer, in bytes
    buffer_size: usize,

    /// Number of buffers in the queue
    buffer_pool_size: usize,
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
    pub registers: *mut Registers,
    
    /// List of all buffers in the buffer pool. Used as a nice wrapper to store
    /// headroom data in the buffer itself
    buffer_pool: Vec<Buffer>,
}

// Safety: Mapping is Send because *mut Registers points to memory-mapped
// memory.
unsafe impl Send for Mapping {}

// Safety: Mapping is Sync because *mut Registers points to memory-mapped
// memory that we use atomics to read/write from
unsafe impl Sync for Mapping {}

type BufferLength = usize;

/// A single buffer in the buffer pool
struct Buffer {
    length: *mut BufferLength,
    data: *mut u8,
}

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
    ) -> Result<Self, Error>
    where
        S: Into<String>,
    {
        // verify buffer size is cache-aligned
        if buffer_size % CACHE_LINE_SIZE != 0 {
            return Err(Error::BufferSize);
        }

        // verify buffer size is greater than size of BufferLength
        if buffer_size <= std::mem::size_of::<BufferLength>() {
            return Err(Error::BufferSize);
        }

        // verify buffer pool size is a power of 2
        if buffer_pool_size.count_ones() != 1 {
            return Err(Error::BufferPoolSize);
        }

        // verify buffer pool size is greater than 0
        if buffer_pool_size == 0 {
            return Err(Error::BufferPoolSize);
        }

        // create a file at path (typically should be /dev/shm) with the
        // calculated file size
        let file_size = calculate_mapping_size(
            buffer_size,
            buffer_pool_size,
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
            (*registers).head = 0;
            (*registers).tail = 0;
            (*registers).buffer_size = buffer_size;
            (*registers).buffer_pool_size = buffer_pool_size;
        }

        // set up the buffer pool
        let mut ring_buffer_ptr = unsafe { buf.add(CACHE_LINE_SIZE) };
        let mut buffer_pool = Vec::with_capacity(buffer_pool_size);
        for _ in 0..buffer_pool_size {
            let length = ring_buffer_ptr as *mut usize;
            ring_buffer_ptr = unsafe {
                ring_buffer_ptr.add(std::mem::size_of::<BufferLength>()) 
            };
            let data = ring_buffer_ptr;
            ring_buffer_ptr = unsafe {
                ring_buffer_ptr.add(buffer_size)
            };

            buffer_pool.push(Buffer { length, data });
        }

        Ok(Mapping {
            _mmap: mmap,
            registers,
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
        unimplemented!()
    }

    /// Number of pending entries in the queue that have not been dequeued. In
    /// other words, how many items the consumer can dequeue before running out
    /// of items
    pub fn pending(&self) -> usize {
        // load head and tail registers
        let (head, tail) = unsafe {
            (
                AtomicUsize::from_ptr(&mut (*self.registers).head)
                    .load(Ordering::Acquire),
                AtomicUsize::from_ptr(&mut (*self.registers).tail)
                    .load(Ordering::Acquire)
            )
        };

        // panic if the head has wrapped around the tail somehow
        assert!(head >= tail);

        head - tail
    }

    /// Capacity of ring buffer. In other words, how many items the producer
    /// can enqueue before running out of space
    pub fn capacity(&self) -> usize {
        // number of buffers in the pool
        let buffer_pool_size = unsafe {
            (*self.registers).buffer_pool_size
        };

        // number of buffers that are "used" (produced but not consumed)
        let pending = self.pending();

        // capacity is buffer pool size minus pending
        buffer_pool_size - pending
    }

    /// Bulk enqueue operation. Returns number of entries succesfully enqueued.
    /// 
    /// If any of the entries is larger than buffer_size, it will be silently
    /// truncated to the pool's buffer size.
    pub fn enqueue_bulk_bytes(&self, data: &[impl AsRef<[u8]>]) -> usize {
        // trim data to the smaller of the two (capacity or data length)
        let enqueued = data.len().min(self.capacity());
        let data = &data[..enqueued];

        // get the head pointer as an AtomicUsize, buffer_pool_size, and
        // buffer_size
        let head = unsafe {
            AtomicUsize::from_ptr(&mut (*self.registers).head)
        };
        let (buffer_size, buffer_pool_size) = unsafe {
            (
                (*self.registers).buffer_size,
                (*self.registers).buffer_pool_size
            )
        };

        // get the current head value
        let head_start = head.load(Ordering::Acquire);

        for (data_idx, input) in data.iter().enumerate() {
            let input = input.as_ref();

            // truncate input data if it's longer than the buffer size
            let input_len = buffer_size.min(input.len());
            let input = &input[..input_len];

            // get the buffer to write to
            let ring_idx = (head_start + data_idx) % buffer_pool_size;
            let buffer = &self.buffer_pool[ring_idx];
            
            unsafe {
                // write the length of the data into the buffer
                buffer.length.write_volatile(input_len);

                // copy the data into the buffer
                std::ptr::copy_nonoverlapping(
                    input.as_ptr(),
                    buffer.data,
                    input_len
                );
            }
        }

        // write the new head value as a single store after all the writes
        head.store(head_start + enqueued, Ordering::Release);

        // since length is trimmed, we simply return length of data
        enqueued
    }

    /// Bulk dequeue operation. Returns number of entries succesfully dequeued
    pub fn dequeue_bulk_bytes(&self, data: &mut [Vec<u8>]) -> usize {
        // trim data to the smaller of the two (pending or data length)
        let dequeued = data.len().min(self.pending());

        // get the tail pointer as an AtomicUsize and buffer_pool_size
        let tail = unsafe {
            AtomicUsize::from_ptr(&mut (*self.registers).tail)
        };
        let buffer_pool_size = unsafe {
            (*self.registers).buffer_pool_size
        };

        // get the current tail value
        let tail_start = tail.load(Ordering::Acquire);

        for (data_idx, output) in data.iter_mut().enumerate() {
            // get the buffer to read from
            let ring_idx = (tail_start + data_idx) % buffer_pool_size;
            let buffer = &self.buffer_pool[ring_idx];

            // copy the data out of the buffer
            unsafe {
                // read the length of the data from the buffer (and clear it)
                let length = buffer.length.read_volatile();
                buffer.length.write_volatile(0);

                // copy the data into the output
                output.clear();
                output.extend_from_slice(
                    std::slice::from_raw_parts(buffer.data, length)
                );
            }
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
    page_size: usize,
) -> usize {
    // registers are at least size of cache line
    let mut size = CACHE_LINE_SIZE;

    // buffer pool is size of buffer times number of buffers
    size += buffer_size * buffer_pool_size;

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
        let mapping = Mapping::new_create(
            "/dev/shm/test_single_mapping_enqueue_dequeue",
            2048,
            16,
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
}
