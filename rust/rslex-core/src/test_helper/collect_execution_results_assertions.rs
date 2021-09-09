// Ignoring from code coverage cause this is a test stub only
#![cfg(not(tarpaulin_include))]
use crate::{arrow::RecordBatchIter, Dataset, ExecutionError, ExecutionResult, Record, RowsPartition};
use arrow::record_batch::RecordBatch;
use fluent_assertions::{utils::AndConstraint, Assertions, IterAssertions, PartialEqIterAssertions, ResultAssertions, Should};
use itertools::Itertools;
use std::{borrow::Borrow, sync::Arc};

pub trait CollectExecutionResults {
    fn collect_execution_results(&self) -> Vec<ExecutionResult<Record>>;
}

impl CollectExecutionResults for Dataset {
    fn collect_execution_results(&self) -> Vec<ExecutionResult<Record>> {
        self.partitions().iter().flat_map(|p| p.iter()).collect_vec()
    }
}

impl<'d> CollectExecutionResults for &'d Dataset {
    fn collect_execution_results(&self) -> Vec<ExecutionResult<Record>> {
        self.partitions().iter().flat_map(|p| p.iter()).collect_vec()
    }
}

impl CollectExecutionResults for Arc<dyn RowsPartition> {
    fn collect_execution_results(&self) -> Vec<ExecutionResult<Record>> {
        self.iter().collect_vec()
    }
}

impl<'p> CollectExecutionResults for &'p Arc<dyn RowsPartition> {
    fn collect_execution_results(&self) -> Vec<ExecutionResult<Record>> {
        self.iter().collect_vec()
    }
}

impl CollectExecutionResults for Vec<Arc<dyn RowsPartition>> {
    fn collect_execution_results(&self) -> Vec<ExecutionResult<Record>> {
        self.iter().flat_map(|p| p.iter()).collect_vec()
    }
}

impl CollectExecutionResults for RecordBatch {
    fn collect_execution_results(&self) -> Vec<ExecutionResult<Record>> {
        RecordBatchIter::from(self.clone()).collect_vec()
    }
}

impl CollectExecutionResults for Vec<RecordBatch> {
    fn collect_execution_results(&self) -> Vec<ExecutionResult<Record>> {
        self.iter()
            .map(|b| RecordBatchIter::from(b.clone()).collect_vec())
            .flatten()
            .collect_vec()
    }
}

pub trait CollectExecutionResultsAssertions<I> {
    fn contain_no_results(self) -> AndConstraint<I>;

    fn contain_num_results(self, num: usize) -> AndConstraint<I>;

    fn contain_results<E>(self, expected: E) -> AndConstraint<I>
    where
        E: IntoIterator,
        <E as IntoIterator>::Item: Borrow<ExecutionResult<Record>>;

    fn equal_dataset<D: CollectExecutionResults, E: Borrow<D>>(self, expected: E) -> AndConstraint<I>;

    fn contain_results_all_ok_with_records<E>(self, expected: E) -> AndConstraint<I>
    where
        E: IntoIterator,
        <E as IntoIterator>::Item: Borrow<Record>;

    fn contain_single_error<E: Into<ExecutionError>>(self, error: E) -> AndConstraint<I>;
}

impl<S: CollectExecutionResults> CollectExecutionResultsAssertions<S> for Assertions<S> {
    fn contain_no_results(self) -> AndConstraint<S> {
        let dataset_records = self.subject().collect_execution_results();

        dataset_records.should().be_empty();

        AndConstraint::new(self.into_inner())
    }

    fn contain_num_results(self, num: usize) -> AndConstraint<S> {
        let dataset_records = self.subject().collect_execution_results();

        dataset_records.should().have_length(num);

        AndConstraint::new(self.into_inner())
    }

    fn contain_results<E>(self, expected: E) -> AndConstraint<S>
    where
        E: IntoIterator,
        <E as IntoIterator>::Item: Borrow<ExecutionResult<Record>>,
    {
        let dataset_records = self.subject().collect_execution_results();

        dataset_records.should().equal_iterator(expected);

        AndConstraint::new(self.into_inner())
    }

    fn equal_dataset<D: CollectExecutionResults, E: Borrow<D>>(self, expected: E) -> AndConstraint<S> {
        let dataset_records = self.subject().collect_execution_results();

        dataset_records
            .should()
            .equal_iterator(expected.borrow().collect_execution_results());

        AndConstraint::new(self.into_inner())
    }

    fn contain_results_all_ok_with_records<E>(self, expected: E) -> AndConstraint<S>
    where
        E: IntoIterator,
        <E as IntoIterator>::Item: Borrow<Record>,
    {
        let dataset_records = self
            .subject()
            .collect_execution_results()
            .into_iter()
            .collect::<Result<Vec<_>, _>>();

        dataset_records.should().be_ok().which_value().should().equal_iterator(expected);

        AndConstraint::new(self.into_inner())
    }

    fn contain_single_error<E: Into<ExecutionError>>(self, error: E) -> AndConstraint<S> {
        let dataset_records = self.subject().collect_execution_results();

        dataset_records
            .should()
            .be_single()
            .which_value()
            .should()
            .be_err()
            .with_value(Box::new(error.into()));

        AndConstraint::new(self.into_inner())
    }
}
