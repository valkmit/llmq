use std::mem::size_of;

/// Metadata at the start of every buffer
#[repr(C)]
struct BufferHeader {
    /// If [`more`]` is true, the index of the next buffer as part of a multipart
    /// message
    /// 
    /// [`more`]: Self::more
    next: u32,

    /// Length of the data in the current buffer (NOT the total length!)
    length: u16,

    /// Whether or not this buffer is available for use. If true, all other
    /// fields are in an undefined state
    free: bool,

    /// If there are more buffers in the message. If true, [`next`] refers to
    /// the nest message
    /// 
    /// [`next`]: Self::next
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
    pub fn new(
        buffer_size: usize,
        buffer_pool_size: usize,
        mut backing: *mut u8,
    ) -> Self {
        let pool = (0..buffer_pool_size)
            .map(|_| {
                let header = backing as *mut BufferHeader;
                let data = unsafe { backing.add(size_of::<BufferHeader>()) };
                let buffer = Buffer { header, data };
                backing = unsafe { backing.add(buffer_size) };
                buffer
            })
            .collect();

        Self {
            next: 0,
            pool,
        }
    }

    /// Requests a chain of buffers to accomodate a payload of the given length
    /// 
    /// Returns None if there is no chain of buffers that can accomodate the
    /// payload
    pub fn alloc_chain(&mut self, length: usize) -> Option<Buffer> {
        None
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
            while !(*current.header).more {
                (*current.header).free = true;
                let next = (*current.header).next;
                current = &self.pool[next as usize];
            }
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
        let bufsz = Self::calculate_buffer_size(
            buffer_size,
            cache_line_size
        );

        // the pool size will be the bufsz * number of items in the pool, and
        // rounded up to the closest size that is a multiple of PAGE_SIZE
        let mut poolsz = bufsz * buffer_pool_size;
        poolsz = (poolsz + pgsz) & !pgsz;

        poolsz
    }

    /// Rounds up the buffer size to the closest multiple of cache line size
    fn calculate_buffer_size(
        buffer_size: usize,
        cache_line_size: usize
    ) -> usize {
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
