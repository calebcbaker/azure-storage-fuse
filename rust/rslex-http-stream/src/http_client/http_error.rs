use crate::{DestinationError, ExternalError, StreamError};
use std::{
    fmt::Debug,
    io::{Error as IoError, ErrorKind as IoErrorKind},
};

/// Error type returned by the [`HttpClient`](crate::HttpClient) trait.
/// It serves 3 goals:
/// - Provide enough information for rslex logic to decide what action to take. For now, only information used
///   is ```is_connect```.
/// - Be able to box itself to ExternalError so can be embedded into other Error.
/// - Be able to mock for testing.
#[derive(Debug, Clone)]
pub struct HttpError {
    /// Whether the error represents a HTTP CONNECTION error.
    pub is_connect: bool,
    /// Embedded lower-level error. This usually provides more detailed information.
    pub boxed_error: ExternalError,
}

impl From<HttpError> for StreamError {
    fn from(e: HttpError) -> Self {
        if e.is_connect {
            StreamError::ConnectionFailure {
                source: Some(e.boxed_error),
            }
        } else {
            StreamError::Unknown(format!("{:?}", e), Some(e.boxed_error))
        }
    }
}

impl From<HttpError> for DestinationError {
    fn from(e: HttpError) -> Self {
        DestinationError::from(StreamError::from(e))
    }
}

impl From<HttpError> for IoError {
    fn from(e: HttpError) -> Self {
        if e.is_connect {
            IoError::from(IoErrorKind::ConnectionAborted)
        } else {
            IoError::new(IoErrorKind::Other, format!("{:?}", e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_client::fake_http_client::DummyError;
    use fluent_assertions::*;
    use std::sync::Arc;

    #[test]
    fn from_http_error_to_stream_error() {
        StreamError::from(HttpError::new(true))
            .should()
            .equal(&StreamError::ConnectionFailure {
                source: Some(Arc::new(DummyError {})),
            });

        let dummy_error = DummyError {};

        StreamError::from(HttpError::new(false)).should().equal(&StreamError::Unknown(
            format!("{:?}", HttpError::new(false)),
            Some(Arc::new(dummy_error)),
        ));
    }

    #[test]
    fn from_http_error_to_destination_error() {
        DestinationError::from(HttpError::new(true))
            .should()
            .equal(&DestinationError::from(StreamError::from(HttpError::new(true))));
    }

    #[test]
    fn from_http_error_to_io_error() {
        let io_error = IoError::from(HttpError::new(true));
        io_error.kind().should().be(IoErrorKind::ConnectionAborted);

        let io_error = IoError::from(HttpError::new(false));
        io_error.kind().should().be(IoErrorKind::Other);
    }
}
