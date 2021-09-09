use crate::Value;
use std::{
    cell::RefCell,
    cmp::Ordering,
    ops::{Index, IndexMut},
    rc::Rc,
};

/// A pooled buffer of values.
///
/// When dropped, this buffer will be returned to its pool of origin, if there is one. It is
/// possible for an instance of this struct to not have a parent pool, in which case the buffer
/// is freed.
/// The underlying memory might be larger than the size of the buffer.
#[derive(Debug)]
pub struct PooledValuesBuffer {
    vec: Vec<Value>,
    pool: Rc<RefCell<Option<Vec<Value>>>>,
}

impl PooledValuesBuffer {
    /// Creates a new buffer without an associated pool.
    pub fn new_disconnected() -> PooledValuesBuffer {
        PooledValuesBuffer {
            vec: Vec::new(),
            pool: Rc::new(RefCell::new(None)),
        }
    }

    /// Resizes this buffer to the specified size.
    ///
    /// If the new size is larger than the current size, the buffer is filled with [`Value::Null`] values.
    pub fn resize(&mut self, size: usize) {
        self.vec.resize(size, Value::Null);
    }

    /// Returns the length of the buffer.
    pub fn len(&self) -> usize {
        self.vec.len()
    }

    /// Returns whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.vec.is_empty()
    }

    /// Returns a slice to the values in the buffer.
    pub fn as_slice(&self) -> &[Value] {
        &self.vec
    }

    /// Returns a mutable slice to the values in the buffer.
    pub fn as_mut_slice(&mut self) -> &mut [Value] {
        &mut self.vec
    }

    /// Returns an iterator to the values in the buffer.
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.vec.iter()
    }

    /// Clones the values in this buffer into a new Vec.
    pub fn to_vec(&self) -> Vec<Value> {
        self.vec.to_vec()
    }

    /// Returns the value at index i, replacing it with Value::Null.
    pub fn take(&mut self, i: usize) -> Value {
        std::mem::take(&mut self.vec[i])
    }

    /// Swaps two values within the buffer.
    pub fn swap(&mut self, a: usize, b: usize) {
        self.vec.swap(a, b);
    }
}

impl Drop for PooledValuesBuffer {
    fn drop(&mut self) {
        self.pool.replace(Some(std::mem::take(&mut self.vec)));
    }
}

impl PartialEq for PooledValuesBuffer {
    fn eq(&self, other: &Self) -> bool {
        self.vec == other.vec
    }
}

impl PartialOrd for PooledValuesBuffer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_slice().partial_cmp(other.as_slice())
    }
}

impl Clone for PooledValuesBuffer {
    fn clone(&self) -> Self {
        PooledValuesBuffer {
            vec: self.vec.clone(),
            pool: Rc::new(RefCell::new(None)),
        }
    }
}

impl Index<usize> for PooledValuesBuffer {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        &self.vec[index]
    }
}

impl IndexMut<usize> for PooledValuesBuffer {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.vec[index]
    }
}

impl<T: Into<Vec<Value>>> From<T> for PooledValuesBuffer {
    fn from(source: T) -> Self {
        let vec = source.into();
        PooledValuesBuffer {
            vec,
            pool: Rc::new(RefCell::new(None)),
        }
    }
}

/// A pool of vectors of Values.
///
/// Call `get_buffer` to get a buffer. This buffer is guaranteed to be of the requested size.
/// Once the buffer goes out of scope, it is returned to the pool.
#[derive(Clone)]
pub struct ValuesBufferPool {
    // We intentionally allow only one element in the pool to optimize for the main scenario:
    // a single buffer that is re-used sequentially.
    pool: Rc<RefCell<Option<Vec<Value>>>>,
}

impl ValuesBufferPool {
    /// Creates a new pool.
    pub fn new() -> ValuesBufferPool {
        ValuesBufferPool {
            pool: Rc::new(RefCell::new(None)),
        }
    }

    /// Returns a buffer of the requested size from the pool.
    ///
    /// Any values present in the buffer will not be cleared. It is the responsibility of the
    /// caller to set all values.
    pub fn get_buffer(&mut self, required_size: usize) -> PooledValuesBuffer {
        if self.pool.borrow().is_some() {
            let mut vec = self.pool.replace(None).unwrap();
            vec.resize(required_size, Value::Null);

            PooledValuesBuffer {
                vec,
                pool: self.pool.clone(),
            }
        } else {
            PooledValuesBuffer {
                vec: vec![Value::Null; required_size],
                pool: self.pool.clone(),
            }
        }
    }
}

impl Default for ValuesBufferPool {
    fn default() -> Self {
        ValuesBufferPool::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::{PartialEqIterAssertions, Should};
    use itertools::repeat_n;

    #[test]
    fn get_buffer_returns_buffer_of_requested_size() {
        let mut pool = ValuesBufferPool::new();
        let expected_size = 10;
        let buffer = pool.get_buffer(expected_size);
        assert_eq!(buffer.len(), expected_size);
    }

    #[test]
    fn index_into_buffer_returns_correct_value() {
        let mut pool = ValuesBufferPool::new();
        let buffer = pool.get_buffer(1);
        assert_eq!(buffer[0], Value::Null);
    }

    #[test]
    #[should_panic]
    fn index_out_of_range_panics() {
        let mut pool = ValuesBufferPool::new();
        let buffer = pool.get_buffer(1);
        let _ = &buffer[buffer.len()];
    }

    #[test]
    fn index_set_updates_correct_value() {
        let mut pool = ValuesBufferPool::new();
        let mut buffer = pool.get_buffer(1);
        let expected_value = Value::Int64(128);
        buffer[0] = expected_value.clone();
        assert_eq!(buffer[0], expected_value);
    }

    #[test]
    fn as_slice_returns_correct_slice() {
        let mut pool = ValuesBufferPool::new();
        {
            let _ = pool.get_buffer(10);
        }
        let mut second_buffer = pool.get_buffer(1);
        let expected_value = Value::Int64(1);
        second_buffer[0] = expected_value.clone();
        let slice = second_buffer.as_slice();
        assert_eq!(slice, vec![expected_value].as_slice());
    }

    #[test]
    fn as_mut_slice_returns_correct_slice() {
        let mut pool = ValuesBufferPool::new();
        let mut buffer = pool.get_buffer(1);
        let expected_value = Value::Int64(128);
        buffer.as_mut_slice()[0] = expected_value.clone();
        assert_eq!(buffer[0], expected_value);
    }

    #[test]
    fn iter_on_buffer_returns_correct_iterator() {
        let mut pool = ValuesBufferPool::new();
        let expected_values = [Value::Int64(1), Value::Int64(2)];
        let mut buffer = pool.get_buffer(expected_values.len());
        buffer.as_mut_slice().clone_from_slice(&expected_values);
        buffer.iter().should().equal_iterator(expected_values.iter());
    }

    #[test]
    fn buffer_from_into_vec_returns_correct_buffer() {
        let expected_values = vec![Value::Int64(1), Value::Int64(2)];
        let buffer = PooledValuesBuffer::from(expected_values.clone());
        buffer.iter().should().equal_iterator(expected_values.iter());
    }

    #[test]
    fn get_buffer_after_previous_buffer_has_been_freed_reuses_it() {
        let mut pool = ValuesBufferPool::new();
        let expected_value = Value::Int64(5);
        {
            let mut first_buffer = pool.get_buffer(1);
            first_buffer[0] = expected_value.clone();
        }

        let second_buffer = pool.get_buffer(10);
        assert_eq!(second_buffer[0], expected_value);
    }

    #[test]
    fn clone_buffer_returns_correct_buffer() {
        let expected_values = vec![Value::Int64(1), Value::Int64(2)];
        let buffer = PooledValuesBuffer::from(expected_values);
        let cloned_buffer = buffer.clone();
        buffer.iter().should().equal_iterator(cloned_buffer.iter());
    }

    #[test]
    fn to_vec_returns_correct_values() {
        let expected_values = vec![Value::Int64(1), Value::Int64(2)];
        let buffer = PooledValuesBuffer::from(expected_values.clone());
        let actual = buffer.to_vec();
        assert_eq!(actual, expected_values);
    }

    #[test]
    fn resize_changes_size() {
        let original_values = vec![Value::Int64(1), Value::Int64(2)];
        let new_size = original_values.len() / 2;
        let mut buffer = PooledValuesBuffer::from(original_values);

        buffer.resize(new_size);

        assert_eq!(buffer.len(), new_size);
    }

    #[test]
    fn resize_to_smaller_size_preserves_values() {
        let original_values = vec![Value::Int64(1), Value::Int64(2)];
        let new_size = original_values.len() / 2;
        let mut buffer = PooledValuesBuffer::from(original_values.clone());

        buffer.resize(new_size);

        buffer.iter().should().equal_iterator(&original_values[0..new_size]);
    }

    #[test]
    fn resize_to_larger_size_fills_with_null() {
        let original_values = vec![Value::Int64(1), Value::Int64(2)];
        let new_size = original_values.len() * 2;
        let mut buffer = PooledValuesBuffer::from(original_values.clone());

        buffer.resize(new_size);

        buffer.iter().should().equal_iterator(
            original_values
                .iter()
                .chain(repeat_n(&Value::Null, new_size - original_values.len())),
        );
    }

    #[test]
    fn swap_valid_indices_swaps_values() {
        let original_values = vec![Value::Int64(1), Value::Int64(2)];
        let mut buffer = PooledValuesBuffer::from(original_values.clone());

        buffer.swap(0, 1);

        assert_eq!(buffer[0], original_values[1]);
        assert_eq!(buffer[1], original_values[0]);
    }
}
