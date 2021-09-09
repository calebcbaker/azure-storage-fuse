use crate::{RecordIteratorRef, RowsPartition};
use std::{
    collections::HashMap,
    mem::drop,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
};
use threadpool::ThreadPool;

pub trait Scheduler {
    fn schedule(&self, prefetch_fn: Box<dyn FnOnce() + Send + Sync>);
}

struct InlineScheduler {}
impl Scheduler for InlineScheduler {
    fn schedule(&self, prefetch_fn: Box<dyn FnOnce() + Send + Sync>) {
        prefetch_fn();
    }
}

struct ThreadScheduler {
    threadpool: Mutex<ThreadPool>,
}

impl ThreadScheduler {
    pub fn new(num_threads: usize) -> ThreadScheduler {
        ThreadScheduler {
            threadpool: Mutex::new(ThreadPool::new(num_threads)),
        }
    }
}

impl Scheduler for ThreadScheduler {
    fn schedule(&self, prefetch_fn: Box<dyn FnOnce() + Send + Sync>) {
        self.threadpool.lock().unwrap().execute(prefetch_fn);
    }
}

struct PrefetchedIter {
    cond_variable: Condvar,
    iter: Mutex<Option<RecordIteratorRef>>,
}

impl PrefetchedIter {
    pub fn new() -> PrefetchedIter {
        PrefetchedIter {
            cond_variable: Condvar::new(),
            iter: Mutex::new(None),
        }
    }

    pub fn set_iterator(&self, iter: RecordIteratorRef) {
        let mut iter_option = self.iter.lock().unwrap();
        assert!(iter_option.is_none());
        iter_option.replace(iter);
        self.cond_variable.notify_one();
    }

    pub fn take_iterator(&self) -> RecordIteratorRef {
        let mut iter_option = self.iter.lock().unwrap();
        while iter_option.is_none() {
            iter_option = self.cond_variable.wait(iter_option).unwrap();
        }

        iter_option.take().unwrap()
    }
}

// The iterators inside PrefetchedIter are not Send+Sync by design. However, the prefetcher ensures
// that the hand-off from one thread to another is safe by limiting access to the underlying
// objects such that no references can be acquired until the object is handed off.
unsafe impl Send for PrefetchedIter {}
unsafe impl Sync for PrefetchedIter {}

pub struct RecordIterPrefetcher<'a> {
    partitions: &'a [Arc<dyn RowsPartition>],
    prefetch_count: usize,
    prefetch_slots: Mutex<HashMap<usize, Arc<PrefetchedIter>>>,
    next_iter_to_prefetch: AtomicUsize,
    scheduler: Box<dyn Scheduler + Send + Sync>,
}

impl<'a> RecordIterPrefetcher<'a> {
    pub fn new(partitions: &'a [Arc<dyn RowsPartition>], prefetch_count: usize, num_threads: usize) -> RecordIterPrefetcher<'a> {
        Self::with_scheduler(
            partitions,
            prefetch_count,
            if num_threads > 0 {
                Box::new(ThreadScheduler::new(num_threads))
            } else {
                Box::new(InlineScheduler {})
            },
        )
    }

    pub fn with_scheduler(
        partitions: &'a [Arc<dyn RowsPartition>],
        prefetch_count: usize,
        scheduler: Box<dyn Scheduler + Send + Sync>,
    ) -> RecordIterPrefetcher<'a> {
        let prefetcher = RecordIterPrefetcher {
            partitions,
            prefetch_count,
            prefetch_slots: Mutex::new(HashMap::new()),
            next_iter_to_prefetch: AtomicUsize::new(0),
            scheduler,
        };
        prefetcher.init();
        prefetcher
    }

    fn init(&self) {
        let mut prefetch_slots = self.prefetch_slots.lock().unwrap();
        for _ in 0..self.prefetch_count {
            self.prefetch_next(&mut prefetch_slots);
        }
    }

    #[tracing::instrument(skip(self, prefetch_slots))]
    fn prefetch_next(&self, prefetch_slots: &mut HashMap<usize, Arc<PrefetchedIter>>) {
        let partition_idx = self.next_iter_to_prefetch.fetch_add(1, Ordering::Relaxed);
        if partition_idx < self.partitions.len() {
            let partition = self.partitions[partition_idx].clone();
            let prefetch_iter = Arc::new(PrefetchedIter::new());
            prefetch_slots.insert(partition_idx, prefetch_iter.clone());
            self.scheduler.schedule(Box::new(move || {
                let iter = partition.iter();
                prefetch_iter.set_iterator(iter);
            }));
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn get_iter(&self, index: usize) -> RecordIteratorRef {
        let mut prefetch_slots = self.prefetch_slots.lock().unwrap();
        if let Some(prefetched_iter) = prefetch_slots.remove(&index) {
            self.prefetch_next(&mut prefetch_slots);
            prefetched_iter.take_iterator()
        } else {
            drop(prefetch_slots);
            self.partitions[index].iter()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{record, IntoRecordIter, Record, SyncRecord};
    use fluent_assertions::{FnAssertions, PartialEqIterAssertions, ResultIterAssertions, Should};
    use std::sync::Mutex;

    #[derive(Debug)]
    struct TestPartition {
        id: usize,
        data: Vec<SyncRecord>,
        iter_create_queue: Arc<Mutex<Vec<usize>>>,
    }

    impl RowsPartition for TestPartition {
        fn iter(&self) -> RecordIteratorRef {
            (*self.iter_create_queue).lock().unwrap().push(self.id);
            let data: Vec<_> = self.data.iter().map(|r| Ok(Record::from(r))).collect();
            Box::new(IntoRecordIter::from(data.into_iter()))
        }
    }

    fn setup(partitions: &[Arc<dyn RowsPartition>], prefetch_count: usize) -> RecordIterPrefetcher {
        RecordIterPrefetcher::with_scheduler(&partitions, prefetch_count, Box::new(InlineScheduler {}))
    }

    #[allow(clippy::type_complexity)]
    fn to_test_partitions(data: Vec<Vec<Record>>) -> (Vec<Arc<dyn RowsPartition>>, Arc<Mutex<Vec<usize>>>) {
        let iter_create_queue = Arc::new(Mutex::new(Vec::new()));
        let partitions = data
            .into_iter()
            .enumerate()
            .map(|(id, records)| {
                Arc::new(TestPartition {
                    id,
                    data: records.into_iter().map(SyncRecord::from).collect(),
                    iter_create_queue: iter_create_queue.clone(),
                }) as Arc<dyn RowsPartition>
            })
            .collect();
        (partitions, iter_create_queue)
    }

    #[test]
    fn get_iter_for_valid_index_returns_correct_iter() {
        let first_partition = vec![record!("A" => 1)];
        let second_partition = vec![record!("A" => 2)];
        let (partitions, _) = to_test_partitions(vec![first_partition.clone(), second_partition.clone()]);
        let prefetcher = setup(&partitions, 2);

        let first_iter = prefetcher.get_iter(0);
        let second_iter = prefetcher.get_iter(1);

        first_iter
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(first_partition);
        second_iter
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(second_partition);
    }

    #[test]
    fn get_iter_for_invalid_index_panics() {
        (|| {
            let partitions = Vec::new();
            let prefetcher = setup(&partitions, 2);
            prefetcher.get_iter(0);
        })
        .should()
        .panic();
    }

    #[test]
    fn prefetches_iters_up_to_prefetch_count() {
        let first_partition = vec![record!("A" => 1)];
        let second_partition = vec![record!("A" => 2)];
        let (partitions, prefetch_queue) = to_test_partitions(vec![first_partition, second_partition]);

        let _ = setup(&partitions, 2);

        let prefetch_queue = prefetch_queue.lock().unwrap();
        assert!(prefetch_queue.contains(&0));
        assert!(prefetch_queue.contains(&1));
    }

    #[test]
    fn does_not_prefetch_iter_beyond_prefetch_count() {
        let first_partition = vec![record!("A" => 1)];
        let second_partition = vec![record!("A" => 2)];
        let (partitions, prefetch_queue) = to_test_partitions(vec![first_partition, second_partition]);

        let _ = setup(&partitions, 1);

        let prefetch_queue = prefetch_queue.lock().unwrap();
        assert!(prefetch_queue.contains(&0));
        assert!(!prefetch_queue.contains(&1));
    }

    #[test]
    fn prefetches_next_iter_once_iter_is_returned_for_full_prefetch_queue() {
        let first_partition = vec![record!("A" => 1)];
        let second_partition = vec![record!("A" => 2)];
        let (partitions, prefetch_queue) = to_test_partitions(vec![first_partition, second_partition]);
        let prefetcher = setup(&partitions, 1);

        let _ = prefetcher.get_iter(0);

        let prefetch_queue = prefetch_queue.lock().unwrap();
        assert!(prefetch_queue.contains(&0));
        assert!(prefetch_queue.contains(&1));
    }

    #[test]
    fn get_iter_not_in_prefetch_queue_when_queue_is_full_returns_iter() {
        let first_partition = vec![record!("A" => 1)];
        let second_partition = vec![record!("A" => 2)];
        let (partitions, prefetch_queue) = to_test_partitions(vec![first_partition, second_partition.clone()]);
        let prefetcher = setup(&partitions, 1);

        let iter = prefetcher.get_iter(1);

        assert!(prefetch_queue.lock().unwrap().contains(&1));
        iter.should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(second_partition);
    }

    #[test]
    fn get_iter_that_was_prefetched_and_retrieved_returns_iter() {
        let partition = vec![record!("A" => 1)];
        let (partitions, _) = to_test_partitions(vec![partition.clone()]);
        let prefetcher = setup(&partitions, 1);
        let _ = prefetcher.get_iter(0);

        let iter = prefetcher.get_iter(0);

        iter.should().all_be_ok().which_inner_values().should().equal_iterator(partition);
    }
}
