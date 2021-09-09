//! Module for interoperability with the Apache Arrow format.
//!
//! [`Datasets`](crate::Dataset) are designed to process data in a streaming, partitioned way. Usually,
//! it is necessary to then deliver the data loaded and processed by the Dataset to some other process.
//! If an in-memory transfer is preferred, then the Apache Arrow format can be used to easily deliver
//! data to many other systems like Pandas or Spark.
//!
//! There are two kinds of conversions provided: convert sequences of [`Records`](crate::Record) in a [`Dataset`](crate::Dataset) into
//! Arrow format and iterate over columns in an Arrow [`RecordBatch`](arrow::record_batch::RecordBatch) as [`Records`](crate::Record).
//!
//! # Examples
//!
//! [`Datasets`](crate::Dataset) can be converted to a Vec of [`RecordBatches`](arrow::record_batch::RecordBatch), with one batch per partition.
//! Partitions can be produced in parallel using the specified degree of parallelization. If one is not provided, the number
//! of cores in the machine will be used.

pub mod ffi;
mod record_batch_builder;
mod record_batch_iter;

// consts
pub mod consts {
    pub const HEADER_STREAM_INFO: &str = "#rslex/streamInfo";
    pub const COLUMN_HANDLER: &str = "handler";
    pub const COLUMN_RESOUCE_ID: &str = "resourceId";
    pub const COLUMN_ARGUMENTS: &str = "arguments";

    pub const HEADER_ERROR_VALUE: &str = "#rslex/errorValue";
    pub const COLUMN_ERROR_CODE: &str = "errorCode";
    pub const COLUMN_SOURCE_VALUE: &str = "sourceValue";
    pub const COLUMN_ERROR_DETAILS: &str = "errorDetails";
}

pub use record_batch_builder::{
    map_arrow_error, ErrorHandling, ExceptionHandling, IntoRecordBatch, RecordBatchBuilder, RecordBatchBuilderOptions, StreamInfoCollector,
    StreamInfoHandling,
};
pub use record_batch_iter::RecordBatchIter;
