use super::{Record, SyncRecord, SyncValue, Value};
use std::{
    borrow::Cow,
    fmt::{Display, Error, Formatter},
    sync::Arc,
};

/// An error value represents an error while applying a transformation to or processing a single
/// value in a Dataset.
///
/// This object is not thread-safe. For a thread-safe version, see [`SyncErrorValue`]. When possible,
/// prefer to use this version for lower overhead.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct ErrorValue {
    /// An error code that identifies the cause of the error.
    pub error_code: Cow<'static, str>,
    /// The value that was being processed when the error was encountered.
    pub source_value: Value,
    /// Additional error-specific information.
    pub error_details: Option<Record>,
}

impl ErrorValue {
    /// Creates a new error value with the specified error code and source value.
    pub fn new<T: Into<Cow<'static, str>>>(error_code: T, source_value: Value) -> ErrorValue {
        ErrorValue {
            error_code: error_code.into(),
            source_value,
            error_details: None,
        }
    }

    /// Creates a new error value with the specified error code, source value, and error details.
    pub fn new_with_details<T: Into<Cow<'static, str>>>(error_code: T, source_value: Value, error_details: Record) -> ErrorValue {
        ErrorValue {
            error_code: error_code.into(),
            source_value,
            error_details: Some(error_details),
        }
    }
}

macro_rules! impl_error_display {
    ($error_type:tt) => {
        impl Display for $error_type {
            fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
                f.write_fmt(format_args!(
                    "{{ErrorCode: \"{}\", SourceValue: {}, Details: ",
                    self.error_code,
                    self.source_value.to_json_like_string()
                ))?;
                if let Some(details) = &self.error_details {
                    f.write_fmt(format_args!("{}}}", details))
                } else {
                    f.write_str("None}")
                }
            }
        }
    };
}

impl_error_display!(ErrorValue);

/// An error value represents an error while applying a transformation to or processing a single
/// value in a Dataset.
///
/// This is the thread-safe version of [`ErrorValue`]. When possible, prefer using the
/// non-thread-safe version for lower overhead.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct SyncErrorValue {
    /// An error code that identifies the cause of the error.
    pub error_code: Arc<str>,
    /// The value that was being processed when the error was encountered.
    pub source_value: SyncValue,
    /// Additional error-specific information.
    pub error_details: Option<SyncRecord>,
}

impl SyncErrorValue {
    /// Creates a new error value with the specified error code and source value.
    pub fn new(error_code: Arc<str>, source_value: SyncValue) -> SyncErrorValue {
        SyncErrorValue {
            error_code,
            source_value,
            error_details: None,
        }
    }

    /// Creates a new error value with the specified error code, source value, and error details.
    pub fn new_with_details(error_code: Arc<str>, source_value: SyncValue, error_details: SyncRecord) -> SyncErrorValue {
        SyncErrorValue {
            error_code,
            source_value,
            error_details: Some(error_details),
        }
    }
}

impl_error_display!(SyncErrorValue);

impl From<SyncErrorValue> for ErrorValue {
    fn from(source: SyncErrorValue) -> Self {
        ErrorValue {
            error_code: source.error_code.to_string().into(),
            source_value: Value::from(source.source_value),
            error_details: source.error_details.map(Record::from),
        }
    }
}

impl From<ErrorValue> for SyncErrorValue {
    fn from(source: ErrorValue) -> Self {
        SyncErrorValue {
            error_code: source.error_code.to_string().into(),
            source_value: SyncValue::from(source.source_value),
            error_details: source.error_details.map(SyncRecord::from),
        }
    }
}

impl PartialEq<SyncErrorValue> for ErrorValue {
    fn eq(&self, other: &SyncErrorValue) -> bool {
        self.error_code.as_ref() == other.error_code.as_ref()
            && self.source_value == other.source_value
            && (self.error_details.is_none() && other.error_details.is_none()
                || self.error_details.is_some()
                    && other.error_details.is_some()
                    && self.error_details.as_ref().unwrap() == other.error_details.as_ref().unwrap())
    }
}

impl PartialEq<ErrorValue> for SyncErrorValue {
    fn eq(&self, other: &ErrorValue) -> bool {
        other == self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RecordSchema, SyncRecordSchema};
    use std::convert::TryFrom;

    #[test]
    fn eq_same_errors_returns_true() {
        assert_eq!(
            ErrorValue::new("ErrorCode", Value::Int64(2)),
            ErrorValue::new("ErrorCode", Value::Int64(2))
        );
    }

    #[test]
    fn eq_different_errors_returns_false() {
        assert_ne!(
            ErrorValue::new("ErrorCode", Value::Int64(2)),
            ErrorValue::new("ErrorCode2", Value::Int64(2))
        );
        assert_ne!(
            ErrorValue::new("ErrorCode", Value::Int64(3)),
            ErrorValue::new("ErrorCode", Value::Int64(2))
        );
    }

    #[test]
    fn eq_same_sync_errors_returns_true() {
        assert_eq!(
            SyncErrorValue::new("ErrorCode".into(), SyncValue::Int64(2)),
            SyncErrorValue::new("ErrorCode".into(), SyncValue::Int64(2))
        );
    }

    #[test]
    fn eq_different_sync_errors_returns_false() {
        assert_ne!(
            SyncErrorValue::new("ErrorCode".into(), SyncValue::Int64(2)),
            SyncErrorValue::new("ErrorCode2".into(), SyncValue::Int64(2))
        );
        assert_ne!(
            SyncErrorValue::new("ErrorCode".into(), SyncValue::Int64(3)),
            SyncErrorValue::new("ErrorCode".into(), SyncValue::Int64(2))
        );
    }

    #[test]
    fn eq_same_error_and_sync_error_returns_true() {
        assert_eq!(
            ErrorValue::new("ErrorCode", Value::Int64(2)),
            SyncErrorValue::new("ErrorCode".into(), SyncValue::Int64(2))
        );
        assert_eq!(
            SyncErrorValue::new("ErrorCode".into(), SyncValue::Int64(2)),
            ErrorValue::new("ErrorCode", Value::Int64(2))
        );
    }

    #[test]
    fn eq_different_error_and_sync_error_returns_false() {
        assert_ne!(
            ErrorValue::new("ErrorCode", Value::Int64(2)),
            SyncErrorValue::new("ErrorCode2".into(), SyncValue::Int64(2))
        );
        assert_ne!(
            SyncErrorValue::new("ErrorCode".into(), SyncValue::Int64(2)),
            ErrorValue::new("ErrorCode2", Value::Int64(2))
        );
    }

    #[test]
    fn error_from_sync_record_returns_correct_value() {
        let source_value = Value::Int64(2);
        let details = Record::new(vec![Value::Int64(2)], RecordSchema::try_from(vec!["A"]).unwrap());
        let error_value = ErrorValue::new_with_details("ErrorCode", source_value, details);
        let sync_error = SyncErrorValue::from(error_value.clone());
        assert_eq!(sync_error, error_value);
    }

    #[test]
    fn sync_error_from_error_returns_correct_value() {
        let source_value = SyncValue::Int64(2);
        let details = SyncRecord::new(vec![SyncValue::Int64(2)], SyncRecordSchema::try_from(vec!["A"]).unwrap());
        let error_value = SyncErrorValue::new_with_details("ErrorCode".into(), source_value, details);
        let sync_error = ErrorValue::from(error_value.clone());
        assert_eq!(error_value, sync_error);
    }
}
