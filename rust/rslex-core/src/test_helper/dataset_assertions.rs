// Ignoring from code coverage cause this is a test stub only
#![cfg(not(tarpaulin_include))]
use super::CollectExecutionResultsAssertions;
use crate::{Dataset, ExecutionResult, Record, RowsPartition};
use arrow::record_batch::RecordBatch;
use fluent_assertions::{
    utils::{AndConstraint, AndWhichValueConstraint, AssertionFailure, DebugMessage},
    Assertions, IterAssertions, Should,
};
use std::{borrow::Borrow, sync::Arc};

pub struct PartitionConstraint<D> {
    subject: D,
    partition: Arc<dyn RowsPartition>,
}

impl<D: Borrow<Dataset>> PartitionConstraint<D> {
    pub fn and(self) -> D {
        self.subject
    }

    pub fn which_value(self) -> Arc<dyn RowsPartition> {
        self.partition
    }

    pub fn with_records<E>(self, expected: E) -> AndConstraint<D>
    where
        E: IntoIterator,
        <E as IntoIterator>::Item: Borrow<Record>,
    {
        self.partition.clone().should().contain_results_all_ok_with_records(expected);

        AndConstraint::new(self.subject)
    }

    pub fn with_results<E>(self, expected: E) -> AndConstraint<D>
    where
        E: IntoIterator,
        <E as IntoIterator>::Item: Borrow<ExecutionResult<Record>>,
    {
        self.partition.clone().should().contain_results(expected);

        AndConstraint::new(self.subject)
    }
}

pub trait DataAssertions<D> {
    fn have_num_partitions(self, num: usize) -> AndConstraint<D>;

    fn have_partition(self, i: usize) -> PartitionConstraint<D>;

    fn contain_record_batches(self) -> AndWhichValueConstraint<D, Vec<RecordBatch>>;
}

impl<D: Borrow<Dataset>> DataAssertions<D> for Assertions<D> {
    fn have_num_partitions(self, num: usize) -> AndConstraint<D> {
        self.subject().borrow().partitions().should().have_length(num);

        AndConstraint::new(self.into_inner())
    }

    fn have_partition(self, i: usize) -> PartitionConstraint<D> {
        match self.subject().borrow().partitions().get(i) {
            Some(p) => PartitionConstraint {
                partition: p.clone(),
                subject: self.into_inner(),
            },
            None => {
                AssertionFailure::new(format!(
                    "should().have_partition() expecting subject to have partition at index {}",
                    i
                ))
                .subject(self.subject())
                .fail();

                unreachable!()
            },
        }
    }

    fn contain_record_batches(self) -> AndWhichValueConstraint<D, Vec<RecordBatch>> {
        let mut batches = vec![];
        for (i, p) in self.subject().borrow().partitions().iter().enumerate() {
            match p.iter().try_as_record_batch() {
                None => {
                    AssertionFailure::new("should().contain_record_batches() expectation failed")
                        .expecting("for each partition p, p.iter().try_as_record_batch() should return Some(Ok(batch))")
                        .but(format!("{}-th partition p.iter().try_as_record_batch() return None", i))
                        .with_message(&format!("{}-th partition", i), p.debug_message())
                        .subject(self.subject())
                        .fail();
                },
                Some(Err(e)) => {
                    AssertionFailure::new("should().contain_record_batches() expectation failed")
                        .expecting("for each partition p, p.iter().try_as_record_batch() should return Some(Ok(batch))")
                        .but(format!(
                            "{}-th partition p.iter().try_as_record_batch() return Some(Err({}))",
                            i,
                            e.debug_message()
                        ))
                        .with_message(&format!("{}-th partition", i), p.debug_message())
                        .subject(self.subject())
                        .fail();
                },
                Some(Ok(batch)) => {
                    batches.push(batch.clone());
                },
            }
        }

        AndWhichValueConstraint::new(self.into_inner(), batches)
    }
}
