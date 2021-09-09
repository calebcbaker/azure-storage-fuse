use std::sync::Arc;

#[derive(Clone)]
pub struct SharedVecIter<T: Clone> {
    results: Arc<Vec<T>>,
    current_index: Option<usize>,
}

impl<T: Clone> SharedVecIter<T> {
    pub fn new(vec: Arc<Vec<T>>) -> SharedVecIter<T> {
        SharedVecIter {
            results: vec,
            current_index: None,
        }
    }
}

impl<T: Clone> Iterator for SharedVecIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.current_index = Some(self.current_index.map_or(0, |i| i + 1));
        let idx = self.current_index.unwrap();
        if idx >= self.results.len() {
            None
        } else {
            Some(self.results[idx].clone())
        }
    }
}
