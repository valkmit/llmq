use std::mem::size_of;

/// Metadata at the start of every buffer
#[repr(C)]
struct BufferHeader {
    /// If there are more buffers in the message. If true, [`next`] refers to
    /// the next message
    ///
    /// [`next`]: Self::next
    next: u32,

    /// Length of the data in the current buffer (NOT the total length!)
    length: u16,

    /// Whether or not this buffer is available for use. If true, all other
    /// fields are in an undefined state
    free: bool,

    /// If [`more`]` is true, the index of the next buffer is part of a multipart
    /// message
    ///
    /// [`more`]: Self::more
    more: bool,
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
                        next: 0,
                        length: 0,
                        free: true,
                        more: false,
                    };
                }
                
                let data = unsafe { backing.add(size_of::<BufferHeader>()) };
                let buffer = Buffer { header, data };
                backing = unsafe { backing.add(buffer_size) };
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
    /// payload
    pub fn alloc_chain(&mut self, length: usize) -> Option<Buffer> {
        let buffers_needed = (length + self.buffer_size - 1) / self.buffer_size;
        if buffers_needed == 1 {
            return self.alloc_single();
        }

        let mut allocated = Vec::with_capacity(buffers_needed);
        let mut remaining_length = length;

        // continue to allocate until length is satisfied
        while remaining_length > 0 {
            if let Some(buffer) = self.alloc_single() {
                allocated.push(buffer);
                remaining_length = remaining_length.saturating_sub(self.buffer_size);
            } else {
                // failed to allocate enough buffers, release what we had up to this point
                println!("Failed to allocate, not enough available buffers.");
                if let Some(head) = allocated.get(0) {
                    self.release_chain(head);
                }
                return None;
            }
        }

        // link the buffers together
        for i in 0..allocated.len() - 1 {
            unsafe {
                let current = &allocated[i];
                let next = &allocated[i + 1];
                (*current.header).more = true;
                (*current.header).next = self.get_buffer_index(next) as u32;
            }
        }

        // tidy the last buffer
        unsafe {
            let last = allocated.last().unwrap();
            (*last.header).more = false;
            (*last.header).next = 0;
        }

        Some(allocated.remove(0))
    }

    /// Allocate a single buffer
    fn alloc_single(&mut self) -> Option<Buffer> {
        let buffer_pool_size = self.pool.len();
        let start = self.next;

        // try to find a free buffer starting from next
        for i in 0..buffer_pool_size {
            let idx = (start + i) % buffer_pool_size;
            unsafe {
                if (*self.pool[idx].header).free {
                    // found a free buffer
                    (*self.pool[idx].header).free = false;
                    (*self.pool[idx].header).length = 0;
                    (*self.pool[idx].header).more = false;
                    (*self.pool[idx].header).next = 0;

                    // update next to start searching from the next position
                    self.next = (idx + 1) % buffer_pool_size;

                    return Some(self.pool[idx].clone());
                }
            }
        }

        None
    }

    /// Get the index of a buffer in the pool
    fn get_buffer_index(&self, buffer: &Buffer) -> usize {
        self.pool
            .iter()
            .position(|b| b.header == buffer.header)
            .expect("Buffer not from this pool")
    }

    /// Releases a chain of buffers starting at the given buffer. Only the
    /// "root" buffer may be given.
    ///
    /// Note that this does not follow Rust's typical semantics of Drop
    /// releasing resources, because we need to make our own decision about
    /// when to communicate that this underlying memory is free.
    pub fn release_chain(&mut self, head: &Buffer) {
        unsafe {
            let mut current = head;
            while (*current.header).more {
                (*current.header).free = true;
                let next = (*current.header).next;
                current = &self.pool[next as usize];
            }

            // release the tail
            (*current.header).free = true;
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
        // rounded up to the closest size that is a mulitple of CACHE_LINE_SIZE
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
        let buffer = pool.alloc_chain(500).expect("Should allocate successfully");
        
        unsafe {
            assert!(!(*buffer.header).free, "Buffer should not be marked as free");
            assert!(!(*buffer.header).more, "Single buffer should not have more buffers");
            assert_eq!((*buffer.header).next, 0, "Single buffer should have next set to 0");
        }
    }

    #[test]
    fn test_alloc_multiple_buffers() {
        let buffer_size = 1024;
        let pool_size = 4;
        let (mut pool, _backing) = create_test_pool(buffer_size, pool_size);

        // allocate a chain for data that spans multiple buffers
        let buffer = pool.alloc_chain(1500).expect("Should allocate successfully");
        
        unsafe {
            // check first buffer
            assert!(!(*buffer.header).free, "First buffer should not be marked as free");
            assert!((*buffer.header).more, "First buffer should have more buffers");
            let next_idx = (*buffer.header).next as usize;
            
            // check second buffer
            let next_buffer = &pool.pool[next_idx];
            assert!(!(*next_buffer.header).free, "Second buffer should not be marked as free");
            assert!(!(*next_buffer.header).more, "Second buffer should not have more buffers");
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
        let first = pool.alloc_chain(500).expect("First allocation should succeed");
        let second = pool.alloc_chain(500).expect("Second allocation should succeed");
        
        unsafe {
            assert!(!(*first.header).free);
            assert!(!(*second.header).free);
            assert_ne!(
                pool.get_buffer_index(&first),
                pool.get_buffer_index(&second),
                "Different allocations should use different buffers"
            );
        }
    }
}