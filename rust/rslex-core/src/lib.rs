#![allow(incomplete_features)]
#![feature(specialization)]
#![feature(vec_into_raw_parts)]
#![feature(write_all_vectored)]
#![feature(box_patterns)]

use std::{any::Any, env, error::Error, fmt::Debug, sync::Arc};

use chrono::{DateTime, Duration, TimeZone, Utc};

use crate::file_io::StreamOpener;

pub use self::{
    database_access::{DatabaseAccessor, DatabaseArguments, DatabaseError, DatabaseHandler, DatabaseResult},
    dataset::Dataset,
    error_value::{ErrorValue, SyncErrorValue},
    execution_error::*,
    field_selectors::*,
    file_io::MapErrToUnknown,
    partition::{produce_error, IntoRecordIter, RecordIterator, RecordIteratorRef, RowsPartition, StaticRowsPartition},
    records::{ExpectedFieldMissing, FieldNameConflict, Record, RecordSchema, SyncRecord, SyncRecordSchema},
    stream_info::{SessionProperties, StreamInfo},
    value::{
        create_record_function_with_field_selector, RecordFunction, RecordFunctionBuilder, SyncValue, SyncValueKind, Value, ValueCastError,
        ValueFunction, ValueFunctionBuilder, ValueKind,
    },
    value_with_eq::ValueWithEq,
};

pub mod arrow;
mod database_access;
mod dataset;
mod error_value;
mod execution_error;
mod field_selectors;
#[macro_use]
pub mod file_io;
pub mod binary_buffer_pool;
#[doc(hidden)]
pub mod in_memory_dataset;
pub mod iterator_extensions;
pub mod partition;
mod prefetching;
pub mod records;
pub mod session_properties_ext;
mod stream_info;
#[cfg(any(feature = "test_helper", test))]
pub mod test_helper;
pub(crate) mod value;
pub mod value_with_eq;
pub mod values_buffer_pool;

#[macro_use]
extern crate lazy_static;

pub type ExternalError = Arc<dyn Error + Send + Sync + 'static>;

pub trait PartitionsLoader: Send + Sync + Debug {
    fn load_partitions(
        &self,
        stream_opener: Arc<dyn StreamOpener>,
        prefix: SyncRecord,
        suffix: SyncRecord,
    ) -> ExecutionResult<Vec<Arc<dyn RowsPartition>>>;
}

/// Write records onto a target incrementally.
///
/// Depending on the implementation, a write_record call might or might not modify target. The only
/// guarantee is that target will contain all the data written after ['IncrementalRecordWriter::finish']
/// is called, assuming no errors are returned. If an error is returned, the target must be considered to be
/// in an invalid state and must be cleaned up.
///
/// Since ['IncrementalRecordWriter::finish'] can fail, it should be explicitly called before the writer
/// goes out of scope. Implementations should avoid implicitly calling this during Drop, instead requiring
/// explicit finalization.
pub trait IncrementalRecordWriter<'a> {
    /// Write the specified record to the target.
    fn write_record(&mut self, record: &Record) -> ExecutionResult<()>;

    /// Finish the write operation, ensuring all data is flushed to the target.
    ///
    /// Additional calls after finish result in undefined behavior.
    fn finish(&mut self) -> ExecutionResult<()>;
}

pub trait RecordWriter: Send + Sync + Debug {
    fn get_suffix(&self) -> &str;
    fn get_incremental_writer<'a>(&self, target: &'a mut dyn std::io::Write) -> ExecutionResult<Box<dyn IncrementalRecordWriter<'a> + 'a>>;

    fn write_records(&self, records: RecordIteratorRef, target: &mut dyn std::io::Write) -> ExecutionResult<()> {
        let mut incremental_writer = self.get_incremental_writer(target)?;
        for record_result in records {
            let record = record_result?;
            incremental_writer.write_record(&record)?;
        }

        incremental_writer.finish()
    }
}

/// Determines how to parallelize [`Dataset`] actions.
pub enum ParallelizationDegree {
    Auto,
    Count(usize),
}

impl ParallelizationDegree {
    pub fn to_thread_count(&self) -> usize {
        match self {
            ParallelizationDegree::Auto => env::var("RSLEX_THREAD_COUNT")
                .ok()
                .and_then(|thread_count_str| thread_count_str.parse().ok())
                .unwrap_or_else(num_cpus::get),
            ParallelizationDegree::Count(i) => *i,
        }
    }
}

pub trait DateTimeConversions {
    fn from_csharp_ticks(ticks: i64) -> Self;
    fn to_csharp_ticks(&self) -> i64;
}

impl DateTimeConversions for DateTime<Utc> {
    fn from_csharp_ticks(ticks: i64) -> Self {
        // convert from C# ticks, which is 10e-7 e since 0001-1-1
        let d = Duration::microseconds(ticks / 10); // convert to 10e-6 seconds
        Utc.ymd(1, 1, 1).and_hms(0, 0, 0) + d
    }

    fn to_csharp_ticks(&self) -> i64 {
        let d = *self - Utc.ymd(1, 1, 1).and_hms(0, 0, 0);
        d.num_microseconds().unwrap() * 10
    }
}

/// [`panic::catch_unwind`] returns a Result where the Err variant is a Box<dyn Any + Send> which is usually the message passed to panic!
/// This method attempts to downcast the message to a `String` or `&str`.
pub fn downcast_panic_result(result: &Box<dyn Any + Send>) -> String {
    if let Some(string) = result.downcast_ref::<String>() {
        string.clone()
    } else if let Some(string) = result.downcast_ref::<&str>() {
        string.to_string()
    } else {
        "panic! didn't result in message".to_owned()
    }
}
