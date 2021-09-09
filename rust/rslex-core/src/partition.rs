use super::{ExecutionError, ExecutionResult};
use crate::{
    arrow::{RecordBatchBuilder, RecordBatchBuilderOptions},
    iterator_extensions::SharedVecIter,
    ExecutionErrorRef, Record, SyncRecord,
};
use arrow::record_batch::RecordBatch;
use std::{cell::RefCell, fmt::Debug, sync::Arc};

/// An iterator over a sequence of [`Records`](crate::Record).
pub trait RecordIterator: Iterator<Item = ExecutionResult<Record>> {
    /// If this iterator produces [`Records`](crate::Record) from columnar data, this returns
    /// the source data as a [`RecordBatch`]. If the source is not columnar, this returns [`None`].
    fn try_as_record_batch(&mut self) -> Option<ExecutionResult<&RecordBatch>> {
        None
    }

    /// Creates a [`RecordBatch`] from this iterator.
    ///
    /// If the underlying data is already a [`RecordBatch`], then a new batch won't be created.
    #[tracing::instrument(skip(self))]
    fn collect_record_batch(&mut self, options: RecordBatchBuilderOptions) -> ExecutionResult<RecordBatch> {
        tracing::debug!(
            ?options.mixed_type_handling,
            ?options.out_of_range_datetime_handling,
            ?options.error_handling,
            ?options.stream_info_handling,
            "[RecordIterator::collect_record_batch()] collect"
        );
        if let Some(result) = self.try_as_record_batch() {
            tracing::debug!("[RecordIterator::collect_record_batch()] RecordIterator was RecordBatch");
            result.map(|x| x.clone())
        } else {
            tracing::debug!("[RecordIterator::collect_record_batch()] Building RecordBatch from RecordIterator");
            let mut builder = RecordBatchBuilder::new(options);
            for record in self {
                builder
                    .append(&record?)
                    .map_err(|e| Box::new(ExecutionError::DataMaterializationError(e)))?;
            }

            builder.finish().map_err(|e| Box::new(ExecutionError::DataMaterializationError(e)))
        }
    }
}

pub struct IntoRecordIter<T: IntoIterator<Item = ExecutionResult<Record>>> {
    iter: T::IntoIter,
}

impl<T: IntoIterator<Item = ExecutionResult<Record>>> RecordIterator for IntoRecordIter<T> {}

impl<T: IntoIterator<Item = ExecutionResult<Record>>> Iterator for IntoRecordIter<T> {
    type Item = ExecutionResult<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<T: IntoIterator<Item = ExecutionResult<Record>>> From<T> for IntoRecordIter<T> {
    fn from(iter: T) -> Self {
        IntoRecordIter { iter: iter.into_iter() }
    }
}

pub type RecordIteratorRef = Box<dyn RecordIterator>;

/// A partition of data.
///
/// Partitions produce sequences of records for part of a [`Dataset`](crate::Dataset). How data is partitioned
/// depends on its source. The partitions themselves capture the information required to produce the
/// records but don't actually pull any data until an iterator is requested. Iterators on a partition
/// are independent from each other. While partitions themselves are thread-safe, the iterators are not
/// and neither is the data produced by them.
pub trait RowsPartition: Send + Sync + Debug {
    /// Default implementation. The iter returned will be optimized for reading the whole partition in sequence,
    /// so will be more aggressively pre-reading. If you only need to read a small portion of records, use
    /// iter_streaming instead.
    fn iter(&self) -> RecordIteratorRef;

    /// Implementation optimized for minimal reading on demand. Call this implementation if the purpose is to only
    /// read a small number of records (like reading headers). Otherwise, prefer iter() over this version.
    fn iter_streaming(&self) -> RecordIteratorRef {
        self.iter()
    }
}

#[derive(Debug)]
pub struct StaticRowsPartition {
    records: Arc<Vec<SyncRecord>>,
}

impl RowsPartition for StaticRowsPartition {
    fn iter(&self) -> RecordIteratorRef {
        Box::new(IntoRecordIter::from(
            SharedVecIter::new(self.records.clone()).map(|r| Ok(Record::from(r))),
        ))
    }
}

impl From<Vec<SyncRecord>> for StaticRowsPartition {
    fn from(records: Vec<SyncRecord>) -> Self {
        StaticRowsPartition {
            records: Arc::new(records),
        }
    }
}

struct ExecutionErrorIterator {
    error: RefCell<Option<Box<ExecutionError>>>,
}

impl RecordIterator for ExecutionErrorIterator {}

impl Iterator for ExecutionErrorIterator {
    type Item = ExecutionResult<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.error.replace(None).map(Err)
    }
}

/// Creates an iterator that will return a single ExecutionError.
pub fn produce_error<T: Into<ExecutionErrorRef>>(error: T) -> RecordIteratorRef {
    Box::new(ExecutionErrorIterator {
        error: RefCell::new(Some(error.into())),
    })
}
