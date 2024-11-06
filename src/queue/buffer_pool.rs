use std::mem::size_of;
use std::sync::atomic::{
    AtomicBool, 
    AtomicU16, 
    AtomicU32,
    Ordering,
};


/// Metadata at the start of every buffer
#[repr(C)]
struct BufferHeader {
    /// If there are more buffers in the message. If true, [`next`] refers to
    /// the next message
    ///
    /// [`next`]: Self::next
    next: AtomicU32,

    /// Length of the data in the current buffer (NOT the total length!)
    length: AtomicU16,

    /// Whether or not this buffer is available for use. If true, all other
    /// fields are in an undefined state
    free: AtomicBool,

    /// If [`more`]` is true, the index of the next buffer is part of a multipart
    /// message
    ///
    /// [`more`]: Self::more
    more: AtomicBool,
}

/// Cheap wrapper around a memory region pointing to some shared memory.
/// The point is to allow for chained bufferse
#[derive(Clone)]
pub struct Buffer {
    header: *mut BufferHeader,
    data: *mut u8,
}

/// A pool of memory buffers sliced up from a larger memory region
pub struct BufferPool {
    /// Next index in pool to check for a free buffer
    next: usize,

    /// List of buffers in the pool
    pool: Vec<Buffer>,

    /// Size of an individual buffer
    buffer_size: usize,
}

impl BufferPool {
    /// Instantiates a buffer pool backed by memory passed to it. We chunk
    /// out memory provided by backing into buffers of size `buffer_size`.
    ///
    /// Note that we assume that backing is large enough for all buffers.
    /// Caller is responsible for ensuring backing is at least
    /// [`BufferPool::calculate_mapping_size`] bytes.
    ///
    /// [`calculate_mapping_size`]: Self::calculate_mapping_size
    pub fn new(buffer_size: usize, buffer_pool_size: usize, mut backing: *mut u8) -> Self {
        let pool = (0..buffer_pool_size)
            .map(|_| {
                let header = backing as *mut BufferHeader;
                unsafe {
                    (*header) = BufferHeader {
                        next: AtomicU32::new(0),
                        length: AtomicU16::new(0),
                        free: AtomicBool::new(true),
                        more: AtomicBool::new(false),
                    };
                }
                
                let data = unsafe { backing.add(size_of::<BufferHeader>()) };
                let buffer = Buffer { header, data };
                backing = unsafe { data.add(buffer_size) };
                buffer
            })
            .collect();

        Self {
            next: 0,
            pool,
            buffer_size,
        }
    }

    /// Requests a chain of buffers to accomodate a payload of the given length
    /// Assumes length is non-zero
    ///
    /// Returns None if there is no chain of buffers that can accomodate the
    /// payload, otherwise returns tuple of (Buffer, index)
    pub fn alloc_chain(&mut self, length: usize) -> Option<(Buffer, usize)> {
        let buffers_needed = (length + self.buffer_size - 1) / self.buffer_size;
        if buffers_needed == 1 {
            return self.alloc_single();
        }

        // allocate first buffer and keep track of it as our head
        let (head, head_idx) = self.alloc_single()?;
        let mut current = head.clone();

        // create and link remaining buffers
        for _ in 1..buffers_needed {
            if let Some((next_buffer, next_idx)) = self.alloc_single() {
                unsafe {
                    (*current.header).more.store(true, Ordering::Release);
                    (*current.header).next.store(next_idx as u32, Ordering::Release);
                }
                current = next_buffer;
            } else {
                // failed to allocate enough buffers, release what we had
                println!("failed to allocate, not enough available buffers.");
                self.release_chain(&head);
                return None;
            }
        }

        // tidy the last buffer
        unsafe {
            (*current.header).more.store(false, Ordering::Release);
            (*current.header).next.store(0, Ordering::Release);
        }

        Some((head, head_idx))
    }

    /// Allocate a single buffer
    /// Returns tuple of (Buffer, index) if successful
    fn alloc_single(&mut self) -> Option<(Buffer, usize)> {
        let buffer_pool_size = self.pool.len();
        let start = self.next;

        // try to find a free buffer starting from next
        for i in 0..buffer_pool_size {
            let idx = (start + i) % buffer_pool_size;
            unsafe {
                if (*self.pool[idx].header).free.compare_exchange(
                    true,
                    false,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ).is_ok() {
                    (*self.pool[idx].header).length.store(0, Ordering::Release);
                    (*self.pool[idx].header).more.store(false, Ordering::Release);
                    (*self.pool[idx].header).next.store(0, Ordering::Release);

                    self.next = (idx + 1) % buffer_pool_size;
                    return Some((self.pool[idx].clone(), idx));
                }
            }
        }

        None
    }

    /// Writes an array of input buffers across a chain of destination buffers
    /// Returns Some(bytes_written) if all data was written successfully,
    /// or None if the provided chain didn't have enough capacity
    pub unsafe fn write_chain(&self, head: &Buffer, bufs: &[&[u8]]) -> Option<usize> {
        let mut current: &Buffer = head;
        let mut bytes_remaining: usize = bufs.iter().map(|b| b.len()).sum();
        let mut bytes_written = 0;

        // which buf we are dealing with
        let mut buf_idx = 0;
        // read position in current buf
        let mut buf_offset = 0;
        
        while bytes_remaining > 0 {
            // if data is already present in this buffer
            // for some reason, unsuccessful write
            assert!((*current.header).length.load(Ordering::Acquire) == 0, "Buffer should be empty");

            // calculate how much we can write to this buffer
            let write_size = bytes_remaining.min(self.buffer_size);
            // current write position in dest buffer
            let mut buffer_written = 0;

            while buffer_written < write_size && buf_idx < bufs.len() {
                let current_buf = bufs[buf_idx];
                let bytes_left_in_buf = current_buf.len() - buf_offset;
                let can_write = (write_size - buffer_written).min(bytes_left_in_buf);

                // copy data into current buffer
                std::ptr::copy_nonoverlapping(
                    current_buf.as_ptr().add(buf_offset),
                    current.data.add(buffer_written),
                    can_write
                );

                buffer_written += can_write;
                buf_offset += can_write;

                // move to next buf if we've consumed all of current one
                if buf_offset == current_buf.len() {
                    buf_idx += 1;
                    buf_offset = 0;
                }
            }

            // update the length in the buffer header
            (*current.header).length.store(buffer_written as u16, Ordering::Release);
            
            bytes_written += buffer_written;
            bytes_remaining -= buffer_written;
            
            // move to next buffer if there's more data and more buffers
            if bytes_remaining > 0 {
                if !(*current.header).more.load(Ordering::Acquire) {
                    println!("no more buffers available");
                    // chain ended before we could write all data
                    return None;
                }
                current = &self.pool[(*current.header).next.load(Ordering::Acquire) as usize];
            }
        }
        
        Some(bytes_written)
    }

    /// Reads data from a chain of buffers into the provided slice
    /// Returns number of bytes read
    pub unsafe fn read_chain(&self, head: &Buffer, data: &mut Vec<u8>) -> usize {
        let mut current = head;
        let mut bytes_read = 0;
        
        loop {
            let buffer_length = (*current.header).length.load(Ordering::Acquire) as usize;

            // grow vec if necessary
            if bytes_read + buffer_length > data.len() {
                data.resize(bytes_read + buffer_length, 0);
            }
            
            // copy data from the current buffer
            if buffer_length > 0 {
                std::ptr::copy_nonoverlapping(
                    current.data,
                    data.as_mut_ptr().add(bytes_read),
                    buffer_length
                );
                bytes_read += buffer_length;
            }
            
            // move to next buffer if there is one
            if !(*current.header).more.load(Ordering::Acquire) {
                break;
            }
            current = &self.pool[(*current.header).next.load(Ordering::Acquire) as usize];
        }

        bytes_read
    }

    /// Retrieves a buffer at a given index from the pool.
    pub fn get_buffer(&self, index: u32) -> Buffer {
        self.pool[index as usize].clone()
    }

    /// Releases a chain of buffers starting at the given buffer. Only the
    /// "root" buffer may be given.
    ///
    /// Note that this does not follow Rust's typical semantics of Drop
    /// releasing resources, because we need to make our own decision about
    /// when to communicate that this underlying memory is free.
    pub fn release_chain(&self, head: &Buffer) {
        unsafe {
            let mut current = head;
            while (*current.header).more.load(Ordering::Acquire) {
                let next = (*current.header).next.load(Ordering::Acquire);
                (*current.header).length.store(0, Ordering::Release);
                (*current.header).free.store(true, Ordering::Release);
                current = &self.pool[next as usize];
            }

            // release the tail
            (*current.header).free.store(true, Ordering::Release);
        }
    }

    /// How many bytes are we required to map to store the buffer pool to
    /// some memory-mapped file?
    ///
    /// Includes padding to align size to page size
    ///
    /// ## Parameters
    /// * `buffer_size`: Size of each buffer in the pool
    /// * `buffer_pool_size`: Number of buffers in the pool
    /// * `page_size`: Size of a page in the system (must be a power of 2)
    /// * `cache_line_size`: Size of a cache line in the system (must be a power
    ///    of 2)
    pub fn calculate_mapping_size(
        buffer_size: usize,
        buffer_pool_size: usize,
        page_size: usize,
        cache_line_size: usize,
    ) -> usize {
        // assert that page_size is a power of 2
        assert!(page_size.is_power_of_two());

        // set pagesz to the mask of the page size
        let pgsz = page_size - 1;

        // size of every buffer will be buffer_size + size of header, and
        // rounded up to the closest size that is a multiple of CACHE_LINE_SIZE
        let bufsz = Self::calculate_buffer_size(buffer_size, cache_line_size);

        // the pool size will be the bufsz * number of items in the pool, and
        // rounded up to the closest size that is a multiple of PAGE_SIZE
        let mut poolsz = bufsz * buffer_pool_size;
        poolsz = (poolsz + pgsz) & !pgsz;

        poolsz
    }

    /// Rounds up the buffer size to the closest multiple of cache line size
    fn calculate_buffer_size(buffer_size: usize, cache_line_size: usize) -> usize {
        // assert that cache_line_size is a power of 2
        assert!(cache_line_size.is_power_of_two());

        // set cachesz to the mask of the cache line size
        let cachesz = cache_line_size - 1;

        // round up to the closest cache line size
        let mut bufsz = buffer_size + size_of::<BufferHeader>();
        bufsz = (bufsz + cachesz) & !cachesz;
        bufsz
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_pool(buffer_size: usize, pool_size: usize) -> (BufferPool, Vec<u8>) {
        let cache_line_size = 64;
        let page_size = 4096;
        let mapping_size = BufferPool::calculate_mapping_size(
            buffer_size,
            pool_size,
            page_size,
            cache_line_size,
        );
        
        let mut backing = vec![0u8; mapping_size];
        let pool = BufferPool::new(
            buffer_size,
            pool_size,
            backing.as_mut_ptr(),
        );
        (pool, backing)
    }

    #[test]
    fn test_alloc_single_buffer() {
        let buffer_size = 1024;
        let pool_size = 4;
        let (mut pool, _backing) = create_test_pool(buffer_size, pool_size);

        // allocate a buffer for data that fits in a single buffer
        let (buffer, _head_idx) = pool.alloc_chain(500).expect("Should allocate successfully");
        
        unsafe {
            assert!(!(*buffer.header).free.load(Ordering::Acquire), "Buffer should not be marked as free");
            assert!(!(*buffer.header).more.load(Ordering::Acquire), "Single buffer should not have more buffers");
            assert_eq!((*buffer.header).next.load(Ordering::Acquire), 0, "Single buffer should have next set to 0");
        }

        pool.release_chain(&buffer);
        unsafe {
            assert!((*buffer.header).free.load(Ordering::Acquire), "Buffer should be marked as free after release");
        }
    }

    #[test]
    fn test_alloc_multiple_buffers() {
        let buffer_size = 1024;
        let pool_size = 4;
        let (mut pool, _backing) = create_test_pool(buffer_size, pool_size);

        // allocate a chain for data that spans multiple buffers
        let (buffer, _head_idx) = pool.alloc_chain(1500).expect("Should allocate successfully");
        
        unsafe {
            // check first buffer
            assert!(!(*buffer.header).free.load(Ordering::Acquire), "First buffer should not be marked as free");
            assert!((*buffer.header).more.load(Ordering::Acquire), "First buffer should have more buffers");
            let next_idx = (*buffer.header).next.load(Ordering::Acquire) as usize;
            
            // check second buffer
            let next_buffer = &pool.pool[next_idx];
            assert!(!(*next_buffer.header).free.load(Ordering::Acquire), "Second buffer should not be marked as free");
            assert!(!(*next_buffer.header).more.load(Ordering::Acquire), "Second buffer should not have more buffers");
        }

        pool.release_chain(&buffer);
        unsafe {
            assert!((*buffer.header).free.load(Ordering::Acquire), "First buffer should be marked as free after release");
            let next_idx = (*buffer.header).next.load(Ordering::Acquire) as usize;
            let next_buffer = &pool.pool[next_idx];
            assert!((*next_buffer.header).free.load(Ordering::Acquire), "Second buffer should be marked as free after release");
        }
    }

    #[test]
    fn test_alloc_chain_out_of_buffers() {
        let buffer_size = 1024;
        let pool_size = 2;
        let (mut pool, _backing) = create_test_pool(buffer_size, pool_size);

        // first allocation should succeed
        let _first = pool.alloc_chain(1500).expect("First allocation should succeed");
        
        // second allocation requiring two buffers should fail
        let second = pool.alloc_chain(1500);
        assert!(second.is_none(), "Should fail when out of buffers");
    }

    #[test]
    fn test_alloc_chain_sequential() {
        let buffer_size = 1024;
        let pool_size = 4;
        let (mut pool, _backing) = create_test_pool(buffer_size, pool_size);

        // allocate and verify the chain
        let (first, _head_idx) = pool.alloc_chain(500).expect("First allocation should succeed");
        let (second, _head_idx) = pool.alloc_chain(500).expect("Second allocation should succeed");
        
        unsafe {
            assert!(!(*first.header).free.load(Ordering::Acquire));
            assert!(!(*second.header).free.load(Ordering::Acquire));
        }
    }

    #[test]
    fn test_write_and_read_single_buffer() {
        let buffer_size = 1024;
        let pool_size = 4;
        let (mut pool, _backing) = create_test_pool(buffer_size, pool_size);
        
        let test_data = b"Hello, World!";
        let (buffer,_head_idx) = pool.alloc_chain(test_data.len()).expect("Should allocate successfully");
        
        unsafe {
            let written = pool.write_chain(&buffer, &[test_data]);
            assert_eq!(written.expect("Should successfully write bytes"), test_data.len());
            
            let mut output = vec![0u8; test_data.len()];
            let read = pool.read_chain(&buffer, &mut output);
            
            assert_eq!(read, test_data.len());
            assert_eq!(&output, test_data);
        }
    }

    #[test]
    fn test_write_and_read_multiple_buffers() {
        let buffer_size = 64;
        let pool_size = 4;
        let (mut pool, _backing) = create_test_pool(buffer_size, pool_size);
        
        // create test data that's longer than a single buffer (>64 bytes)
        let test_data = b"This is a much longer string that will definitely span multiple buffers because it needs to be longer than 64 bytes to ensure proper testing of the multi-buffer functionality in our implementation";
        let (buffer, _head_idx) = pool.alloc_chain(test_data.len()).expect("Should allocate successfully");
        
        unsafe {
            let written = pool.write_chain(&buffer, &[test_data]);
            assert_eq!(written.expect("Should successfully write bytes"), test_data.len());

            let mut output = vec![0u8; test_data.len()];
            let read = pool.read_chain(&buffer, &mut output);
            
            assert_eq!(read, test_data.len());
            assert_eq!(&output, test_data);
        }
    }

    #[test]
    fn test_write_multiple_bufs_multi_buffer() {
        // small buffer size to force spanning across multiple buffers
        let buffer_size = 8;
        let pool_size = 5;
        let (mut pool, _backing) = create_test_pool(buffer_size, pool_size);
        
        // three buffers that will require multiple chain buffers to store
        let buf1 = b"Hello Hello";       
        let buf2 = b", testing ";
        let buf3 = b"multiple buffers!";
        let total_len = buf1.len() + buf2.len() + buf3.len();
        
        let (buffer, _) = pool.alloc_chain(total_len).expect("Should allocate buffer");
        
        unsafe {
            let written = pool.write_chain(&buffer, &[buf1, buf2, buf3]);
            assert_eq!(written.expect("Should write successfully"), total_len);
            
            let mut output = Vec::new();
            let read = pool.read_chain(&buffer, &mut output);
            
            assert_eq!(read, total_len);
            assert_eq!(&output, b"Hello Hello, testing multiple buffers!");

            // confirm we needed multiple buffers
            assert!((*buffer.header).more.load(Ordering::Acquire), 
                "Should require multiple buffers");
        }
    }
}