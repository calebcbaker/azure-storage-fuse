use crate::{
    downcast_panic_result, prefetching::RecordIterPrefetcher, produce_error, ExecutionResult, RecordIteratorRef, RowsPartition, SyncRecord,
};
use crossbeam_channel::bounded;
use crossbeam_queue::{ArrayQueue, SegQueue};
use crossbeam_utils::{atomic::AtomicCell, thread::scope};
use derivative::Derivative;
use itertools::Itertools;
use std::{
    any::Any,
    cmp::min,
    collections::HashSet,
    fmt::{Debug, Error, Formatter},
    sync::Arc,
};
use tracing::Span;

/// A Dataset represents a logical sequence of records split into [partitions](RowsPartition).
/// Records are produced lazily when they are read, either by enumerating the contents of a partition
/// or by executing a Dataset action.
///
/// A single Dataset can contain partitions from multiple sources. For example, if a Dataset
/// loads data from two different CSV files that have the same schema, each of those files is
/// considered a different source. The partitions can be accessed either as a single flat
/// collection or as a collection of collections, with the partitions from a single source
/// grouped together. Certain operations can have different behavior depending on this grouping.
#[derive(Clone)]
pub struct Dataset {
    partitions_per_source: Vec<Vec<Arc<dyn RowsPartition>>>,
    flattened_partitions: Vec<Arc<dyn RowsPartition>>,
}

impl Debug for Dataset {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.write_fmt(format_args!("Dataset[Partitions: {}]", self.partitions().len()))
    }
}

#[derive(Copy, Clone, Debug)]
pub struct PartitionMetadata {
    pub source_index: usize,
    pub index_in_source: usize,
    pub index_in_dataset: usize,
}

pub trait MapPartitionFn: Fn(RecordIteratorRef, PartitionMetadata) -> ExecutionResult<RecordIteratorRef> + Send + Sync {}

impl<TFn: Fn(RecordIteratorRef, PartitionMetadata) -> ExecutionResult<RecordIteratorRef> + Send + Sync> MapPartitionFn for TFn {}

#[derive(Derivative)]
#[derivative(Debug)]
struct MappedPartition<TFn: MapPartitionFn> {
    source_partition: Arc<dyn RowsPartition>,
    #[derivative(Debug = "ignore")]
    map_fn: Arc<TFn>,
    metadata: PartitionMetadata,
}

impl<TFn: MapPartitionFn> MappedPartition<TFn> {
    pub fn new(source_partition: Arc<dyn RowsPartition>, metadata: PartitionMetadata, map_fn: Arc<TFn>) -> MappedPartition<TFn> {
        MappedPartition {
            source_partition,
            map_fn,
            metadata,
        }
    }
}

impl<TFn: MapPartitionFn> RowsPartition for MappedPartition<TFn> {
    fn iter(&self) -> RecordIteratorRef {
        match (self.map_fn)(self.source_partition.iter(), self.metadata) {
            Ok(iter) => iter,
            Err(e) => produce_error(e),
        }
    }

    fn iter_streaming(&self) -> RecordIteratorRef {
        match (self.map_fn)(self.source_partition.iter_streaming(), self.metadata) {
            Ok(iter) => iter,
            Err(e) => produce_error(e),
        }
    }
}

impl Dataset {
    /// Creates an empty Dataset.
    pub fn empty() -> Dataset {
        Dataset {
            partitions_per_source: vec![],
            flattened_partitions: vec![],
        }
    }

    /// Creates a new Dataset from a collection of partitions.
    ///
    /// All partitions are treated as coming from a single source.
    pub fn from_single_source(partitions: Vec<Arc<dyn RowsPartition>>) -> Dataset {
        Dataset {
            partitions_per_source: vec![partitions.clone()],
            flattened_partitions: partitions,
        }
    }

    /// Creates a new Dataset from a collection of partitions, grouped by source.
    pub fn from_multiple_sources(partitions_per_source: Vec<Vec<Arc<dyn RowsPartition>>>) -> Dataset {
        let flattened_partitions = partitions_per_source.clone().into_iter().flatten().collect_vec();
        Dataset {
            partitions_per_source,
            flattened_partitions,
        }
    }

    /// Whether the Dataset is empty.
    ///
    /// An empty Dataset is a Dataset that contains no partitions. It is possible for a Dataset to
    /// contain partitions which produce no data. In this case, the Dataset won't be considered empty
    /// even though it will produce no records.
    pub fn is_empty(&self) -> bool {
        self.flattened_partitions.is_empty()
    }

    /// The partitions in this Dataset.
    pub fn partitions(&self) -> &[Arc<dyn RowsPartition>] {
        &self.flattened_partitions
    }

    /// The partitions in this Dataset, grouped by source.
    pub fn partition_per_source(&self) -> &[Vec<Arc<dyn RowsPartition>>] {
        &self.partitions_per_source
    }

    /// Keeps only the partitions identified by the specified indices.
    ///
    /// If any of the indices are out of bounds, this method will panic.
    pub fn select_partitions(self, partition_indices: &HashSet<usize>) -> Self {
        for i in partition_indices {
            if *i >= self.partitions().len() {
                tracing::error!(
                    "[Dataset::select_partitions()] out-of-bounds partition selected in select_partitions: {}",
                    i
                );
                panic!("Partition {} is out of bounds.", i);
            }
        }

        let sources_and_offsets =
            self.partitions_per_source
                .into_iter()
                .fold(vec![], |mut sources_and_offsets: Vec<(Vec<_>, usize)>, source| {
                    let offset = sources_and_offsets.last().map(|(s, o)| s.len() + o).unwrap_or(0);
                    sources_and_offsets.push((source, offset));
                    sources_and_offsets
                });
        let new_sources = sources_and_offsets
            .into_iter()
            .map(|(source, offset)| {
                source
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| partition_indices.contains(&(i + offset)))
                    .map(|t| t.1)
                    .collect()
            })
            .collect();
        Dataset::from_multiple_sources(new_sources)
    }

    /// Creates a new Dataset by applying the specified function to the contents of each partition.
    ///
    /// Since Datasets are executed lazily, the function is only invoked once data is pulled from
    /// the Dataset. Each different pull will execute the function.
    pub fn map_partitions<TFn: Fn(RecordIteratorRef, PartitionMetadata) -> ExecutionResult<RecordIteratorRef> + Send + Sync + 'static>(
        &self,
        map_fn: TFn,
    ) -> Dataset {
        let mut index_in_dataset = 0;
        let map_fn = Arc::new(map_fn);
        let new_partitions = self
            .partitions_per_source
            .iter()
            .enumerate()
            .map(|(source_index, source_partitions)| {
                source_partitions
                    .iter()
                    .enumerate()
                    .map::<Arc<dyn RowsPartition>, _>(|(index_in_source, partition)| {
                        let mapped_partition = Arc::new(MappedPartition::new(
                            partition.clone(),
                            PartitionMetadata {
                                source_index,
                                index_in_source,
                                index_in_dataset,
                            },
                            map_fn.clone(),
                        ));
                        index_in_dataset += 1;
                        mapped_partition
                    })
                    .collect_vec()
            })
            .collect_vec();
        Dataset::from_multiple_sources(new_partitions)
    }

    /// Reduces each partition in the Dataset by executing the specified reduce function and then combines
    /// the results by invoking the specified combine function.
    ///
    /// Reduce functions are executed in parallel using up to `parallelization_degree` threads. A separate
    /// thread is used to run the combine function in sequence. The combine function will be executed as
    /// soon as reduce results are available.
    #[tracing::instrument(skip(reduce_fn, combine_fn, initial_value))]
    pub fn reduce_and_combine<
        TResult: Send,
        TIntermediate: Send,
        TError: Send,
        TReduceFn: Fn(RecordIteratorRef, usize) -> Result<TIntermediate, TError> + Send + Sync,
        TCombineFn: Fn(TResult, TIntermediate) -> Result<TResult, TError> + Send + Sync,
    >(
        &self,
        reduce_fn: TReduceFn,
        combine_fn: TCombineFn,
        initial_value: TResult,
        parallelization_degree: usize,
    ) -> Result<TResult, TError> {
        assert_ne!(parallelization_degree, 0);

        let partition_count = self.partitions().len();
        if partition_count == 0 {
            return Ok(initial_value);
        }

        let errors_encountered = SegQueue::new();
        let final_result = AtomicCell::new(None);

        tracing::debug!("[Dataset::reduce_and_combine()] Pushing {} work items", partition_count);
        let work_items = ArrayQueue::new(partition_count);
        for idx in 0..partition_count {
            work_items.push(idx).unwrap_or_else(|e| {
                tracing::error!(error = %e, "[Dataset::reduce_and_combine()] Unexpected failure while creating work items.");
                panic!();
            });
        }

        // If there are fewer partitions than the degree of parallelization, don't bother pre-fetching
        // since it just adds additional overhead.
        let count = if self.partitions().len() <= parallelization_degree {
            0
        } else {
            parallelization_degree
        };
        tracing::debug!(%count, "[Dataset::reduce_and_combine()] Creating Prefetcher");
        let prefetcher = RecordIterPrefetcher::new(self.partitions(), count, count);
        scope(|scope| {
            let (results_sender, results_receiver) = bounded(parallelization_degree);

            // We want to move results_receiver into the combine thread but capture errors_encountered
            // and final_result by reference.
            let errors = &errors_encountered;
            let final_result = &final_result;
            let current_span = Span::current();
            let _combine_thread = scope.spawn(move |_| {
                let span = tracing::info_span!(parent: &current_span, "combine");
                let _span = span.enter();
                // The iterator on the receiver will produce an item per result pushed onto the channel.
                // It will complete and return None when the channel is disconnected, which occurs once all
                // senders have fallen out of scope. We also exit early if any errors have been encountered.
                let mut accumulated = initial_value;

                for result in results_receiver.iter().take_while(|_| errors.is_empty()) {
                    match combine_fn(accumulated, result) {
                        Ok(combine_result) => accumulated = combine_result,
                        Err(e) => {
                            errors.push(e);
                            return;
                        },
                    }
                }

                final_result.store(Some(accumulated));
            });

            let prefetcher_ref = &prefetcher;
            let _reduce_threads = (0..min(parallelization_degree, partition_count))
                .map(|i| {
                    // We capture results_sender by cloning so that each thread owns its own sender and the parent can be released.
                    // This ensures the channel is marked as disconnected once all threads complete which will cause the receiver to
                    // finish, unblocking the main thread.
                    // The other values are captured by reference.
                    let results_sender = results_sender.clone();
                    let errors_encountered = &errors_encountered;
                    let reduce_fn = &reduce_fn;
                    let work_items = &work_items;
                    let current_span = Span::current();
                    scope.spawn(move |_| {
                        let span = tracing::info_span!(parent: &current_span, "reduce", i);
                        let _span = span.enter();
                        while let (0, Ok(partition_idx)) = (errors_encountered.len(), work_items.pop()) {
                            let reduce_result = reduce_fn(prefetcher_ref.get_iter(partition_idx), partition_idx);
                            match reduce_result {
                                Ok(result) => {
                                    if let (Err(e), true) = (results_sender.send(result), errors_encountered.is_empty()) {
                                        tracing::error!(error = %e, "[Dataset::reduce_and_combine()] Channel disconnected unexpectedly with no errors encountered.");
                                        panic!();
                                    }
                                },
                                Err(e) => {
                                    errors_encountered.push(e);
                                },
                            }
                        }
                    })
                })
                .collect_vec();
        })
        .unwrap_or_else(|e| {
            let panic_msgs = if let Some(panics) = e.downcast_ref::<Vec<Box<dyn Any + Send>>>() {
                panics.iter().map(|panic_result| downcast_panic_result(panic_result)).collect()
            } else {
                vec![downcast_panic_result(&e)]
            };
            tracing::error!(errors = ?panic_msgs, "[Dataset::reduce_and_combine()] Unexpected error in reduce/combine threads");
            panic!("{:?}", panic_msgs);
        });

        if let Ok(e) = errors_encountered.pop() {
            Err(e)
        } else {
            Ok(final_result.into_inner().unwrap_or_else(|| {
                tracing::error!("[Dataset::reduce_and_combine()] Expected valid final_results.");
                panic!();
            }))
        }
    }

    /// Reduces each partition in the Dataset by executing the specified function and returns a vector
    /// containing the results for each partition.
    ///
    /// Reduce functions are executed in parallel using up to `parallelization_degree` threads.
    #[tracing::instrument(skip(reduce_fn))]
    pub fn reduce<TResult: Send, TError: Send, TFn: Fn(RecordIteratorRef, usize) -> Result<TResult, TError> + Send + Sync>(
        &self,
        reduce_fn: TFn,
        parallelization_degree: usize,
    ) -> Result<Vec<TResult>, TError> {
        tracing::debug!(%parallelization_degree, "[Dataset::reduce()] reduce");
        self.reduce_and_combine(
            |iter, partition_idx| {
                let result = reduce_fn(iter, partition_idx);
                result.map(|reduced_value| (partition_idx, reduced_value))
            },
            |mut vec, (partition_idx, value)| {
                vec.push((partition_idx, value));
                Ok(vec)
            },
            vec![],
            parallelization_degree,
        )
        .map(|mut reduce_results| {
            reduce_results.sort_by_key(|(partition_idx, _)| *partition_idx);
            reduce_results.into_iter().map(|(_, value)| value).collect_vec()
        })
    }

    /// Returns a collection containing all records in this Dataset.
    ///
    /// Records will be gathered using up to `parallelization_degree` threads.
    #[tracing::instrument]
    pub fn collect(&self, parallelization_degree: usize) -> ExecutionResult<Vec<SyncRecord>> {
        tracing::debug!(%parallelization_degree, "[Dataset::collect()] collect");
        self.reduce(
            |mut partition, _| {
                partition.fold_ok(vec![], |mut records, current| {
                    records.push(SyncRecord::from(current));
                    records
                })
            },
            parallelization_degree,
        )
        .map(|intermediate_results| intermediate_results.into_iter().flatten().collect_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        file_io::StreamError, in_memory_dataset, partition::IntoRecordIter, record, test_helper::*, ExecutionError, Record, RecordSchema,
        SyncRecordSchema, SyncValue, Value,
    };
    use fluent_assertions::{FnAssertions, PartialEqIterAssertions, Should};
    use std::convert::TryFrom;

    #[test]
    fn reduce_and_combine_on_empty_dataset_returns_initial_result() {
        let dataset = Dataset::from_single_source(vec![]);
        let expected_result = 0;
        let result = dataset.reduce_and_combine::<_, _, (), _, _>(|_, _| panic!(), |_, _: i32| panic!(), expected_result, 1);
        assert_eq!(result.unwrap(), expected_result);
    }

    #[test]
    fn reduce_and_combine_on_dataset_with_one_partition_processes_records_in_partition() {
        let dataset = in_memory_dataset::from_single_partition_records(vec![Record::empty(), Record::empty()]);
        let result =
            dataset.reduce_and_combine::<_, _, (), _, _>(|p, _| Ok(p.count()), |total, partition_count| Ok(total + partition_count), 0, 1);
        assert_eq!(result.unwrap(), 2);
    }

    #[test]
    fn reduce_and_combinet_on_dataset_with_one_partition_that_contains_error_returns_error() {
        let expected_error = Box::new(ExecutionError::StreamError(StreamError::NotFound));
        let dataset = in_memory_dataset::from_single_partition_results(vec![Err(expected_error.clone())]);
        let result = dataset.reduce_and_combine(
            |_, _| Err(expected_error.clone()),
            |total: i32, current: i32| Ok(total + current),
            0,
            1,
        );

        assert_eq!(result.err().unwrap(), expected_error);
    }

    #[test]
    fn reduce_and_combine_on_dataset_with_multiple_partitions_processes_all_records() {
        let dataset = in_memory_dataset::from_multiple_partition_records(vec![
            vec![Record::empty(), Record::empty()],
            vec![Record::empty(), Record::empty()],
        ]);
        let result =
            dataset.reduce_and_combine::<_, _, (), _, _>(|p, _| Ok(p.count()), |total, partition_count| Ok(total + partition_count), 0, 1);
        assert_eq!(result.unwrap(), 4);
    }

    #[test]
    fn reduce_and_combine_on_dataset_with_multiple_partitions_where_one_fails_returns_error() {
        let expected_error = Box::new(ExecutionError::StreamError(StreamError::NotFound));
        let dataset = in_memory_dataset::from_multiple_partition_results(vec![
            vec![Ok(Record::empty()), Ok(Record::empty())],
            vec![Ok(Record::empty()), Err(expected_error.clone())],
        ]);
        let result = dataset.reduce_and_combine(
            |_, _| Err(expected_error.clone()),
            |total: i32, current: i32| Ok(total + current),
            0,
            1,
        );
        assert_eq!(result.err().unwrap(), expected_error);
    }

    #[test]
    fn reduce_and_combine_on_dataset_with_multiple_partitions_where_one_fails_stops_processing() {
        let expected_error = Box::new(ExecutionError::StreamError(StreamError::NotFound));
        let dataset = in_memory_dataset::from_multiple_partition_results(vec![
            vec![Ok(Record::empty())],
            vec![Err(expected_error.clone())],
            vec![Ok(Record::empty())],
        ]);
        let result = dataset.reduce_and_combine(|_, _| Err(expected_error.clone()), |_, _: i32| panic!(), 0, 1);
        assert_eq!(result.err().unwrap(), expected_error);
    }

    #[test]
    fn reduce_and_combine_when_combine_fails_returns_error() {
        let expected_error = Box::new(ExecutionError::StreamError(StreamError::NotFound));
        let dataset = in_memory_dataset::from_single_partition_results(vec![Err(expected_error.clone())]);
        let result = dataset.reduce_and_combine(|_, _| Ok(1), |_, _: i32| Err(expected_error.clone()), 0, 1);
        assert_eq!(result.err().unwrap(), expected_error);
    }

    #[test]
    fn reduce_returns_vector_with_results_per_partition() {
        let dataset = in_memory_dataset::from_multiple_partition_records(vec![vec![Record::empty()], vec![Record::empty()]]);
        let result = dataset.reduce::<_, (), _>(|p, _| Ok(p.count()), 1);
        assert_eq!(result.unwrap(), vec![1, 1]);
    }

    #[test]
    fn collect_returns_all_records() {
        let records = vec![Record::empty(), Record::empty()];
        let dataset = in_memory_dataset::from_multiple_partition_records(vec![vec![records[0].clone()], vec![records[1].clone()]]);
        let result = dataset.collect(1);
        assert_eq!(result.unwrap(), records);
    }

    #[test]
    fn map_partitions_returns_partitions_with_mapped_iterators() {
        let original_column = "A";
        let schema = RecordSchema::try_from(vec![original_column]).unwrap();
        let dataset = in_memory_dataset::from_multiple_partition_records(vec![
            vec![Record::new(vec![Value::from(1)], schema.clone())],
            vec![Record::new(vec![Value::from(2)], schema)],
        ]);

        let index_column = "Index";
        let expected_schema = SyncRecordSchema::try_from(vec![original_column, index_column]).unwrap();
        let mapped_results = dataset
            .map_partitions(move |iter, metadata| {
                Ok(Box::new(IntoRecordIter::from(iter.map(move |record| {
                    let (_, mut values) = record?.deconstruct();
                    let schema = RecordSchema::try_from(vec![original_column, index_column]).unwrap();
                    values.resize(2);
                    values[0] = Value::Int64(i64::try_from(values[0].clone()).unwrap() * 2);
                    values[1] = Value::Int64(metadata.index_in_dataset as i64);
                    Ok(Record::new(values, schema))
                }))))
            })
            .collect(1)
            .expect("Expected map_partitions to succeed.");

        mapped_results.iter().should().equal_iterator(
            vec![
                SyncRecord::new(vec![SyncValue::Int64(2), SyncValue::Int64(0)], expected_schema.clone()),
                SyncRecord::new(vec![SyncValue::Int64(4), SyncValue::Int64(1)], expected_schema),
            ]
            .iter(),
        );
    }

    #[test]
    fn select_partitions_with_empty_list_returns_empty_dataset() {
        let dataset = in_memory_dataset::from_multiple_partition_records(vec![vec![record!("A" => 1)], vec![record!("A" => 2)]]);
        let new_dataset = dataset.select_partitions(&vec![].into_iter().collect());
        assert!(new_dataset.is_empty());
    }

    #[test]
    fn select_partitions_with_single_partition_returns_dataset_with_only_that_partition() {
        let dataset = in_memory_dataset::from_multiple_partition_records(vec![vec![record!("A" => 1)], vec![record!("A" => 2)]]);

        dataset
            .select_partitions(&vec![0].into_iter().collect())
            .should()
            .have_num_partitions(1)
            .and()
            .should()
            .contain_results_all_ok_with_records(vec![record!("A" => 1)]);
    }

    #[test]
    fn select_partitions_with_multiple_partitions_in_same_source_return_dataset_with_those_partitions() {
        let dataset = in_memory_dataset::from_multiple_partition_records(vec![
            vec![record!("A" => 1)],
            vec![record!("A" => 2)],
            vec![record!("A" => 3)],
            vec![record!("A" => 4)],
        ]);

        dataset
            .select_partitions(&vec![1, 3].into_iter().collect())
            .should()
            .have_num_partitions(2)
            .and()
            .should()
            .contain_results_all_ok_with_records(&[record!("A" => 2), record!("A" => 4)]);
    }

    #[test]
    fn select_partitions_from_multiple_sources_keeps_partitions_grouped_by_source() {
        let dataset = in_memory_dataset::from_multiple_sources_records(vec![
            vec![vec![record!("A" => 1)], vec![record!("A" => 2)]],
            vec![vec![record!("A" => 3)], vec![record!("A" => 4)], vec![record!("A" => 5)]],
            vec![vec![record!("A" => 6)], vec![record!("A" => 7)]],
        ]);

        dataset
            .select_partitions(&vec![0, 3, 4, 6].into_iter().collect())
            .should()
            .have_num_partitions(4)
            .and()
            .should()
            .pass(|ds| {
                (&ds.partition_per_source()[0][0])
                    .should()
                    .contain_results_all_ok_with_records(vec![record!("A" => 1)]);
                (&ds.partition_per_source()[1][0])
                    .should()
                    .contain_results_all_ok_with_records(vec![record!("A" => 4)]);
                (&ds.partition_per_source()[1][1])
                    .should()
                    .contain_results_all_ok_with_records(vec![record!("A" => 5)]);
                (&ds.partition_per_source()[2][0])
                    .should()
                    .contain_results_all_ok_with_records(vec![record!("A" => 7)]);
            });
    }

    #[test]
    fn select_partitions_with_index_outside_of_bounds_panics() {
        (|| {
            let dataset = in_memory_dataset::from_multiple_partition_records(vec![vec![record!("A" => 1)]]);
            dataset.select_partitions(&vec![4].into_iter().collect())
        })
        .should()
        .panic();
    }
}
