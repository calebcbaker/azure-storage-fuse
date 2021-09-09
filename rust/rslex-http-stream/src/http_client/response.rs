use super::{AsyncResponse, HttpError, Response};
use crate::{DestinationError, MapErrToUnknown, StreamError};
use async_trait::async_trait;
use http::StatusCode;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use tracing_sensitive::AsSensitive;

#[derive(Debug)]
pub struct UnsuccessfulResponse {
    pub status_code: StatusCode,
    pub body: String,
}

/// Extension functions for [`Response`].
pub trait ResponseExt {
    /// Check the status code. Wrap un-successful status code into ['UnsuccessfulResponse'],
    /// which has an auto-implementation ```From<UnsuccessfulResponse> for StreamError```.
    /// In many cases it's sufficient to use that directly by the ? syntax.
    ///
    /// ```
    /// use rslex_core::file_io::StreamError;
    /// use rslex_http_stream::{create_http_client, new_request, HttpClient, ResponseExt};
    /// use std::sync::Arc;
    ///
    /// fn pull_url(uri: &str) -> Result<String, StreamError> {
    ///     let http_client = Arc::new(create_http_client()?);
    ///     let request = new_request().uri(uri).body(b"".to_vec()).unwrap();
    ///
    ///     // the success() call here will automatically translate unsuccessful status
    ///     // into Error
    ///     let res = http_client.request(request.into())?.success()?;
    ///     res.into_string()
    /// }
    /// ```
    ///
    /// In some case if you want to do the status mapping by yourself, call map_err:
    /// ```
    /// use rslex_core::file_io::StreamError;
    /// use rslex_http_stream::{create_http_client, new_request, ResponseExt, HttpClient};
    /// use http::StatusCode;
    /// use std::sync::Arc;
    ///
    ///  fn pull_url(uri: &str) -> Result<String, StreamError> {
    ///     let http_client = Arc::new(create_http_client()?);
    ///     let request = new_request().uri(uri).body(b"".to_vec()).unwrap();
    ///
    ///     // the success() call here will automatically translate unsuccessful status
    ///     // into Error
    ///     let res = http_client.request(request.into())?.success().map_err(|e|
    ///         if e.status_code == 404 {
    ///             // when authentication fail, service will return 404 - NotFound. But if it's actually NotFound, it will return an empty list.
    ///             // so we map 404 to permission failure
    ///             StreamError::PermissionDenied
    ///         } else {
    ///             e.into()
    ///         })?;
    ///
    ///     res.into_string()
    /// }
    /// ```
    fn success(self) -> Result<Response, UnsuccessfulResponse>;

    /// A util function to dump the status code and body of the response.
    /// It's just a Debug implementation. Unfortunately the Response type
    /// is defined out of this crate so we cannot implement Debug for it.
    fn debug(&self) -> String;

    /// Retrieve the full body of the response and return as a String.
    /// ```
    /// use rslex_core::file_io::StreamError;
    /// use rslex_http_stream::{create_http_client, new_request, HttpClient, ResponseExt};
    /// use std::sync::Arc;
    ///
    /// fn pull_url(uri: &str) -> Result<String, StreamError> {
    ///     let http_client = Arc::new(create_http_client()?);
    ///     let request = new_request().uri(uri).body(b"".to_vec()).unwrap();
    ///
    ///     let res = http_client.request(request.into())?;
    ///
    ///     res.into_string()
    /// }
    /// ```
    fn into_string(self) -> Result<String, StreamError>;
}

/// Extension functions for [`AsyncResponse`].
#[async_trait]
pub trait AsyncResponseExt {
    /// Check the status code. Wrap un-successful status code into ['UnsuccessfulResponse'],
    /// which has an auto-implementation ```From<UnsuccessfulResponse> for StreamError```.
    /// In many cases it's sufficient to use that directly by the ? syntax.
    ///
    /// ``` no_run
    /// use rslex_core::file_io::StreamError;
    /// use rslex_http_stream::{create_http_client, new_request, AsyncResponseExt, HttpClient};
    /// use std::sync::Arc;
    ///  async fn pull_url(uri: &str) -> Result<String, StreamError> {
    ///     let http_client = Arc::new(create_http_client()?);
    ///     let request = new_request().uri(uri).body(b"".to_vec()).unwrap();
    ///
    ///     // the success() call here will automatically translate unsuccessful status
    ///     // into Error
    ///     let res = http_client.request_async(request.into()).await?.success().await?;
    ///     res.into_string().await
    /// }
    /// ```
    ///
    /// In some case if you want to do the status mapping by yourself, call map_err:
    /// ``` no_run
    /// use rslex_core::file_io::StreamError;
    /// use rslex_http_stream::{create_http_client, new_request, AsyncResponseExt, HttpClient};
    /// use http::StatusCode;
    /// use futures::TryFutureExt;
    /// use std::sync::Arc;
    ///
    ///  async fn pull_url(uri: &str) -> Result<String, StreamError> {
    ///     let http_client = Arc::new(create_http_client()?);
    ///     let request = new_request().uri(uri).body(b"".to_vec()).unwrap();
    ///
    ///     // the success() call here will automatically translate unsuccessful status
    ///     // into Error
    ///     let res = http_client.request_async(request.into()).await?.success().map_err(|e|
    ///         if e.status_code == 404 {
    ///             // when authentication fail, service will return 404 - NotFound. But if it's actually NotFound, it will return an empty list.
    ///             // so we map 404 to permission failure
    ///             StreamError::PermissionDenied
    ///         } else {
    ///             e.into()
    ///         }).await?;
    ///
    ///     res.into_string().await
    /// }
    /// ```
    async fn success(self) -> Result<AsyncResponse, UnsuccessfulResponse>;

    /// A util function to dump the status code and body of the response.
    /// It's just a Debug implementation. Unfortunately the Response type
    /// is defined out of this crate so we cannot implement Debug for it.
    fn debug(&self) -> String;

    /// Retrieve the full body of the response and return as a String.
    /// ```
    /// use rslex_core::file_io::StreamError;
    /// use rslex_http_stream::{create_http_client, new_request, AsyncResponseExt, HttpClient};
    /// use std::sync::Arc;
    /// async fn pull_url(uri: &str) -> Result<String, StreamError> {
    ///     let http_client = Arc::new(create_http_client()?);
    ///     let request = new_request().uri(uri).body(b"".to_vec()).unwrap();
    ///
    ///     let res = http_client.request_async(request.into()).await?;
    ///
    ///     res.into_string().await
    /// }
    /// ```
    async fn into_string(self) -> Result<String, StreamError>;
}

impl ResponseExt for Response {
    fn success(self) -> Result<Response, UnsuccessfulResponse> {
        if self.status().is_success() {
            Ok(self)
        } else {
            tracing::warn!(code = ?self.status(), "[Response::success()] non-successful response\n{}", self.debug().as_sensitive_ref());

            Err(UnsuccessfulResponse {
                status_code: self.status(),
                body: String::from_utf8(self.body().clone()).unwrap_or("Body content failed to convert from UTF8".to_owned()),
            })
        }
    }

    fn debug(&self) -> String {
        format!(
            "Status: {}\nHeaders: {:#?}\nBody: {}",
            self.status(),
            self.headers(),
            String::from_utf8(self.body().clone()).unwrap_or("Body content failed to convert from UTF8".to_owned())
        )
    }

    fn into_string(self) -> Result<String, StreamError> {
        String::from_utf8(self.into_body()).map_err(|e| StreamError::Unknown(format!("convert response from utf8 error: {}", e), None))
    }
}

#[async_trait]
impl AsyncResponseExt for AsyncResponse {
    async fn success(self) -> Result<AsyncResponse, UnsuccessfulResponse> {
        if self.status().is_success() {
            Ok(self)
        } else {
            tracing::warn!(code = ?self.status(), "[AsyncResponse::success()] non-successful response\n{}", self.debug().as_sensitive_ref());

            Err(UnsuccessfulResponse {
                status_code: self.status(),
                body: self
                    .into_string()
                    .await
                    .unwrap_or("Body content failed to convert from UTF8".to_owned()),
            })
        }
    }

    fn debug(&self) -> String {
        format!("Status: {}\nHeaders: {:#?}", self.status(), self.headers())
    }

    async fn into_string(self) -> Result<String, StreamError> {
        let data = hyper::body::to_bytes(self.into_body())
            .await
            .map_err(|e| StreamError::from(HttpError::from(e)))?
            .to_vec();
        String::from_utf8(data).map_err_to_unknown()
    }
}

impl From<UnsuccessfulResponse> for StreamError {
    fn from(s: UnsuccessfulResponse) -> Self {
        match s.status_code.as_u16() {
            401 => StreamError::PermissionDenied,
            403 => StreamError::PermissionDenied,
            404 => StreamError::NotFound,
            _ => StreamError::Unknown(format!("unsuccessful status code {}, body {}", s.status_code, s.body), None),
        }
    }
}

impl From<UnsuccessfulResponse> for DestinationError {
    fn from(e: UnsuccessfulResponse) -> Self {
        DestinationError::from(StreamError::from(e))
    }
}

impl From<UnsuccessfulResponse> for IoError {
    fn from(s: UnsuccessfulResponse) -> Self {
        match s.status_code.as_u16() {
            401 => IoError::from(IoErrorKind::PermissionDenied),
            403 => IoError::from(IoErrorKind::PermissionDenied),
            _ => IoError::new(IoErrorKind::Other, format!("status: {:?}, body: {}", s.status_code, s.body)),
        }
    }
}
