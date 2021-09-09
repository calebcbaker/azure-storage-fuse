use crate::ExternalError;
use derivative::Derivative;
#[cfg(any(feature = "test_helper", test))]
use fluent_assertions::EnumAssertions;
use std::{
    error::Error,
    fmt::Debug,
    io::{Error as IoError, ErrorKind as IoErrorKind},
    sync::{mpsc::RecvError, Arc},
};
use thiserror::Error;

#[cfg_attr(any(feature = "test_helper", test), derive(EnumAssertions))]
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ArgumentError {
    #[error("A required parameter \"{argument:}\" is missing.")]
    MissingArgument { argument: String },
    #[error("Invalid parameter \"{argument:}\", expect {expected:}, but found \"{actual:}\".")]
    InvalidArgument {
        argument: String,
        expected: String,
        actual: String,
    },
}

#[derive(Clone, Debug, Error, Derivative)]
#[derivative(PartialEq)]
pub enum StreamError {
    #[error("stream handler not found for {0}")]
    NoHandler(String),
    #[error("Uri scheme is invalid")]
    InvalidUriScheme,
    #[error("stream not found")]
    NotFound,
    #[error("permission denied when access stream")]
    PermissionDenied,
    #[error("connection failure when access stream")]
    ConnectionFailure {
        #[derivative(PartialEq = "ignore")]
        source: Option<ExternalError>,
    },
    #[error("invalid argument")]
    ArgumentError(#[from] ArgumentError),
    #[error("Invalid input.")]
    InvalidInput {
        message: String,
        #[derivative(PartialEq = "ignore")]
        source: Option<ExternalError>,
    },
    #[error("unexpected error")]
    Unknown(
        String,
        #[derivative(PartialEq = "ignore")]
        #[source]
        Option<ExternalError>,
    ),
}

impl From<RecvError> for StreamError {
    fn from(e: RecvError) -> Self {
        StreamError::Unknown(e.to_string(), Some(Arc::new(e)))
    }
}

impl From<IoError> for StreamError {
    fn from(e: IoError) -> Self {
        match e.kind() {
            IoErrorKind::NotFound => StreamError::NotFound,
            IoErrorKind::PermissionDenied => StreamError::PermissionDenied,
            IoErrorKind::Interrupted => StreamError::ConnectionFailure { source: Some(Arc::new(e)) },
            _ => StreamError::Unknown(e.to_string(), Some(Arc::new(e))),
        }
    }
}

impl From<StreamError> for IoError {
    fn from(e: StreamError) -> Self {
        match e {
            StreamError::NotFound => IoError::from(IoErrorKind::NotFound),
            StreamError::PermissionDenied => IoError::from(IoErrorKind::PermissionDenied),
            e => IoError::new(IoErrorKind::Other, format!("{:?}", e)),
        }
    }
}

pub type StreamResult<T> = std::result::Result<T, StreamError>;

/// a tool to convert any foreign error type to StreamError::Unknown
/// Note use it only with error which you believe will not happen (like debug.assert).
/// Use it instead of .unwrap() or .expect() to avoid panic in very rare case some
/// unexpected error happens.
/// But if we understand the behavior and error message, we should map it to proper
/// StreamError instead of Unknown.
pub trait MapErrToUnknown<T> {
    fn map_err_to_unknown(self) -> StreamResult<T>;
}

impl<E: Error + Debug + Send + Sync + 'static, T> MapErrToUnknown<T> for std::result::Result<T, E> {
    fn map_err_to_unknown(self) -> StreamResult<T> {
        match self {
            Ok(t) => Ok(t),
            Err(e) => Err(StreamError::Unknown(e.to_string(), Some(Arc::new(e)))),
        }
    }
}

impl<E: Debug, T> MapErrToUnknown<T> for std::result::Result<T, E> {
    default fn map_err_to_unknown(self) -> StreamResult<T> {
        match self {
            Ok(t) => Ok(t),
            Err(e) => Err(StreamError::Unknown(format!("{:?}", e), None)),
        }
    }
}
