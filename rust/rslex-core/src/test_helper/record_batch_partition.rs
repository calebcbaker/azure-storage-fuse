// Ignoring from code coverage cause this is a test stub only
#![cfg(not(tarpaulin_include))]
use crate::{arrow::RecordBatchIter, RecordIteratorRef, RowsPartition};
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use std::fmt::{Debug, Formatter};

pub struct RecordBatchPartition {
    batch: RecordBatch,
}

impl From<RecordBatch> for RecordBatchPartition {
    fn from(batch: RecordBatch) -> Self {
        RecordBatchPartition { batch }
    }
}

impl Debug for RecordBatchPartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordBatchPartition{{num_columns={}, num_rows={}, schema=({}) }}",
            self.batch.num_columns(),
            self.batch.num_rows(),
            self.batch.schema().fields().iter().map(|f| f.name()).join("|")
        )
    }
}

impl RowsPartition for RecordBatchPartition {
    fn iter(&self) -> RecordIteratorRef {
        Box::new(RecordBatchIter::from(self.batch.clone()))
    }
}
