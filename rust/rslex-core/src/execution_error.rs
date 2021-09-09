use crate::{
    file_io::{ArgumentError, DestinationError, StreamError},
    DatabaseError, ExpectedFieldMissing, ExternalError, FieldNameConflict, SyncErrorValue, SyncRecord, SyncValue, ValueCastError,
    ValueKind,
};
use derivative::Derivative;
#[cfg(any(feature = "test_helper", test))]
use fluent_assertions::EnumAssertions;
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct UnexpectedTypeError {
    pub value: SyncValue,
    pub column: String,
    pub expected_type: ValueKind,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OutOfRangeValueError {
    pub value: SyncValue,
    pub min: SyncValue,
    pub max: SyncValue,
}

#[cfg_attr(any(feature = "test_helper", test), derive(EnumAssertions))]
#[derive(Clone, Debug, Derivative, Error)]
#[derivative(PartialEq)]
pub enum DataMaterializationError {
    #[error("There is not enough memory to materialize the data.")]
    MemoryError,
    #[error("An unexpected type was found: {0:?}")]
    UnexpectedType(UnexpectedTypeError),
    #[error("An unexpected error value \"{error}\" in record \"{parent_record}\" was encountered while reading the data to materialize.")]
    UnexpectedErrorValue {
        error: Box<SyncErrorValue>,
        parent_record: SyncRecord,
    },
    #[error("A value out of the range supported by the target format was found: {0:?}.")]
    OutOfRangeValue(OutOfRangeValueError),
    #[error("The input data contains no columns.")]
    NoColumns { record_count: usize },
    #[error("An external error was encountered while materializing data: {message}")]
    ExternalError {
        message: String,
        #[derivative(PartialEq = "ignore")]
        #[source]
        source: Option<ExternalError>,
    },
}

#[derive(Clone, Debug, Derivative, Error)]
#[derivative(PartialEq)]
pub struct DataProcessingError {
    pub error_value: Box<SyncErrorValue>,
    pub failing_record: Option<SyncRecord>,
    pub message: String,
}

impl Display for DataProcessingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}. Source record: {}. Error value: {:?}.",
            &self.message,
            match &self.failing_record {
                Some(r) => format!("{:?}", r),
                None => "".to_owned(),
            },
            &self.error_value
        )
    }
}

impl From<std::io::Error> for DataMaterializationError {
    fn from(e: std::io::Error) -> Self {
        DataMaterializationError::ExternalError {
            message: e.to_string(),
            source: Some(Arc::new(e)),
        }
    }
}

/// An error encountered while producing data from a [`Dataset`](crate::Dataset).
#[cfg_attr(any(feature = "test_helper", test), derive(EnumAssertions))]
#[derive(Debug, Clone, Derivative, Error)]
#[derivative(PartialEq)]
pub enum ExecutionError {
    #[error("{0}")]
    ArgumentError(#[from] ArgumentError),
    #[error("error in streaming from input data sources")]
    StreamError(#[from] StreamError),
    #[error("error in data values")]
    ValueError(#[from] ValueCastError),
    #[error("tbd")]
    ExpectedFieldMissing(#[from] ExpectedFieldMissing),
    #[error("A column name conflicts with an existing column. The conflicting name is \"{0}\".")]
    FieldNameConflict(#[from] FieldNameConflict),
    #[error("An error happen when encode/decode context with encoding {0}.")]
    EncodingError(String),
    #[error("tbd")]
    DataMaterializationError(#[from] DataMaterializationError),
    #[error("An error occur when applying data processing.")]
    DataProcessingError(#[from] DataProcessingError),
    #[error("An error occur when streaming data to output target.")]
    DestinationError(#[from] DestinationError),
    #[error("An error occur when executing database query.")]
    DatabaseError(#[from] DatabaseError),
    #[error("{}", message)]
    ExternalError {
        message: String,
        #[derivative(PartialEq = "ignore")]
        #[source]
        source: Option<ExternalError>,
    },
}

pub type ExecutionErrorRef = Box<ExecutionError>;

macro_rules! impl_error_from {
    ($error_type:ty) => {
        impl From<$error_type> for ExecutionErrorRef {
            fn from(e: $error_type) -> Self {
                Box::new(e.into())
            }
        }
    };
}

impl_error_from!(ArgumentError);
impl_error_from!(StreamError);
impl_error_from!(ValueCastError);
impl_error_from!(ExpectedFieldMissing);
impl_error_from!(FieldNameConflict);
impl_error_from!(DataMaterializationError);
impl_error_from!(DataProcessingError);
impl_error_from!(DestinationError);
impl_error_from!(DatabaseError);

pub type ExecutionResult<T> = Result<T, ExecutionErrorRef>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_record;

    #[test]
    fn data_processing_error_display() {
        let dpe = DataProcessingError {
            error_value: Box::new(SyncErrorValue::new("FakeError".into(), SyncValue::Int64(2))),
            failing_record: Some(sync_record! { "Col" => "Value" }),
            message: "Bad record".to_string(),
        };

        let display = &format!("{}", dpe);
        assert_eq!(
            display,
            "Bad record. Source record: {Col: Value}. Error value: SyncErrorValue { error_code: \"FakeError\", source_value: Int64(2), error_details: None }."
        );
    }
}
