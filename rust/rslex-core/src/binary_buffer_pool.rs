use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Condvar, Mutex},
};

struct PoolBuffers {
    buffers: Vec<Vec<u8>>,
    allocated_buffers: usize,
}

struct InnerPool {
    pool_capacity: usize,
    buffer_capacity: usize,
    check_in_condvar: Condvar,
    pool_buffers: Mutex<PoolBuffers>,
}

impl InnerPool {
    pub fn new(pool_capacity: usize, buffer_capacity: usize) -> InnerPool {
        InnerPool {
            pool_capacity,
            buffer_capacity,
            check_in_condvar: Condvar::new(),
            pool_buffers: Mutex::new(PoolBuffers {
                buffers: Vec::with_capacity(pool_capacity),
                allocated_buffers: 0,
            }),
        }
    }

    pub fn check_out(&self) -> Vec<u8> {
        let mut pool_buffers = self
            .pool_buffers
            .lock()
            .expect("[InnerPool::check_out] Failed to acquire pool mutex.");

        if let Some(buffer) = pool_buffers.buffers.pop() {
            buffer
        } else {
            if pool_buffers.allocated_buffers == self.pool_capacity {
                while pool_buffers.buffers.len() == 0 {
                    pool_buffers = self
                        .check_in_condvar
                        .wait(pool_buffers)
                        .expect("[InnerPool::check_out] Failed to wait for buffer to be checked in.");
                }

                pool_buffers
                    .buffers
                    .pop()
                    .expect("[InnerPool::check_out] Expected buffers to be available after Condvar check exits.")
            } else {
                let mut vec = Vec::with_capacity(self.buffer_capacity);
                unsafe { vec.set_len(self.buffer_capacity) }
                pool_buffers.allocated_buffers += 1;
                vec
            }
        }
    }

    pub fn check_in(&self, buffer: Vec<u8>) {
        let mut pool_buffers = self
            .pool_buffers
            .lock()
            .expect("[InnerPool::check_in] Failed to acquire pool mutex.");
        pool_buffers.buffers.push(buffer);
        self.check_in_condvar.notify_one();
    }
}

pub struct PooledBuffer {
    buffer: Option<Vec<u8>>,
    length: usize,
    pool: Arc<InnerPool>,
}

impl PooledBuffer {
    pub(self) fn new(inner_pool: Arc<InnerPool>) -> PooledBuffer {
        PooledBuffer {
            buffer: Some(inner_pool.check_out()),
            length: inner_pool.buffer_capacity,
            pool: inner_pool,
        }
    }

    pub fn truncate(mut self, length: usize) -> PooledBuffer {
        assert!(
            length <= self.buffer.as_ref().unwrap().len(),
            "length must be less than the buffer capacity for this pool."
        );
        PooledBuffer {
            buffer: self.buffer.take(),
            length,
            pool: self.pool.clone(),
        }
    }
}

impl Deref for PooledBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer.as_ref().unwrap()[0..self.length]
    }
}

impl DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer.as_mut().unwrap()[0..self.length]
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.check_in(buffer)
        }
    }
}

#[derive(Clone)]
pub struct BinaryBufferPool {
    inner_pool: Arc<InnerPool>,
}

impl BinaryBufferPool {
    pub fn new(pool_capacity: usize, buffer_capacity: usize) -> BinaryBufferPool {
        BinaryBufferPool {
            inner_pool: Arc::new(InnerPool::new(pool_capacity, buffer_capacity)),
        }
    }

    pub fn check_out(&self) -> PooledBuffer {
        PooledBuffer::new(self.inner_pool.clone())
    }
}
