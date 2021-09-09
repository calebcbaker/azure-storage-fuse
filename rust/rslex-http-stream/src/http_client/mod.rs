mod async_body;
mod credential;
mod execution;
#[cfg(any(feature = "fake_http_client", test))]
mod fake_http_client;
mod http_client_builder;
mod http_error;
mod hyper_client;
mod proxy;
mod redirect;
mod request;
mod response;
mod timeout;

use crate::retry::{
    backoff::ExponentialBackoffWithJitter,
    http_client_retry::{INITIAL_BACKOFF, NUMBER_OF_RETRIES},
};
use async_trait::async_trait;
use thiserror::Error;

pub use async_body::AsyncBody;
pub use execution::{Spawn, SpawnBlocking, Wait};
#[cfg(any(feature = "fake_http_client", test))]
pub use fake_http_client::FakeHttpClient;
pub use http_client_builder::HttpClientBuilder;
pub use http_error::HttpError;
pub use hyper::Method;
pub use response::{AsyncResponseExt, ResponseExt};
use std::{sync::Arc, time::Duration};

use crate::retry::backoff::BackoffStrategy;
#[cfg(any(feature = "fake_http_client", test))]
pub use credential::fake_credential::FakeCredential;
pub use credential::ApplyCredential;
pub use request::{AuthenticatedRequest, Request, RequestWithCredential};
use rslex_core::file_io::{DestinationError, StreamError};
use std::ops::Deref;

/// The HTTP Response of the blocking request call in [`HttpClient`].
pub type Response = ::hyper::Response<Vec<u8>>;

/// The HTTP Response of the async request call in [`HttpClient`].
pub type AsyncResponse = ::hyper::Response<hyper::Body>;

// HttpClient trait (later in this file) has two methods for sync and async requests.
// Sync version "request" can be automatically implemented based on the async "request_async".
// The most straightforward way to do that is to add default implementation to the trait itself.
// But part of the implementation requires passing HttpClient instance to the AsyncBody constructor.
// We want to make this Arc<HttpClient> cause copying the http client is expensive and creates a new connection pool.
// To do that, we need to change the request signature on HttpClient to be fn request(self: Arc<Self>...).
// But this way we can't pass it into AsyncBody::new(..., http_client: Arc<dyn HttpClient> inside the default request implementation
// cause rust complains on Arc<Self> size is not know in a compile time.
// SO with this trait, we are splitting HttpClient into two traits. All external users will only deal with HttpClient and defines request and request_async only for
// Arc<dyn HttpClient>. This way, we can pass it into AsyncBody and force all external users to always deal with Arc<HttpClient> to avoid the accidental copy of the actual client.
#[async_trait]
pub trait HttpClientAsync: Sync + Send + 'static {
    async fn request_async(&self, req: AuthenticatedRequest) -> Result<AsyncResponse, HttpError>;
}

/// An interface for HTTP Client.
/// A default implementation will call hyper library.
/// A test double is provided as ['FakeHttpClient'](rslex_http_stream::FakeHttpClient) as well.
///
/// ## Example
/// ``` no_run
/// use rslex_http_stream::{create_http_client, new_request, Method, HttpClient};
/// use std::sync::Arc;
///
/// // this will return a HttpClient implementation wrapping hyper library
/// let http_client = create_http_client().unwrap();
///
/// let request = new_request().method(Method::GET).uri("http://abc.com/").body(b"".to_vec()).unwrap();
/// let response = Arc::new(http_client).request(request.into()).unwrap();
///
/// assert_eq!(response.status(), 200);
/// ```
#[async_trait]
pub trait HttpClient: Send + Sync + 'static {
    /// A blocking version of making a HTTP [`Request`], returning HTTP [`Response`].
    /// Note the blocking version consumes an Arc pointer of HttpClient, so it can send
    /// to a background thread to execute. If you want to keep the HttpClient, clone() it
    /// when calling the function.
    fn request(self: Arc<Self>, req: AuthenticatedRequest) -> Result<Response, HttpError>;

    /// The async version of the http request.
    async fn request_async(self: Arc<Self>, req: AuthenticatedRequest) -> Result<AsyncResponse, HttpError>;
}

#[async_trait]
impl<T: HttpClientAsync> HttpClient for T {
    default fn request(self: Arc<Self>, req: AuthenticatedRequest) -> Result<Response, HttpError> {
        async move {
            let result = self.clone().request_async(req.clone()).await?;
            let (parts, body) = result.into_parts();
            let backoff_strategy = ExponentialBackoffWithJitter::new(INITIAL_BACKOFF, *NUMBER_OF_RETRIES);
            let body = hyper::body::to_bytes(AsyncBody::new(body, Duration::from_secs(30), self, req, backoff_strategy.to_iter()))
                .await?
                .to_vec();
            Ok(Response::from_parts(parts, body))
        }
        .wait()?
    }

    default async fn request_async(self: Arc<Self>, req: AuthenticatedRequest) -> Result<AsyncResponse, HttpError> {
        HttpClientAsync::request_async(self.deref(), req).await
    }
}

#[derive(Error, Debug, Clone)]
pub enum HttpClientCreationError {
    #[error("Reading proxy settings error. {0}")]
    ProxySettings(proxy::ProxySettingsError),
}

impl From<HttpClientCreationError> for StreamError {
    fn from(error: HttpClientCreationError) -> Self {
        StreamError::ConnectionFailure {
            source: Some(Arc::new(error)),
        }
    }
}

impl From<HttpClientCreationError> for DestinationError {
    fn from(error: HttpClientCreationError) -> Self {
        DestinationError::ConnectionFailure {
            source: Some(Arc::new(error)),
        }
    }
}

/// Return builder for [`Request`].
///
/// ## Example
/// ```
/// use rslex_http_stream::{new_request, Method};
///
/// let request = new_request()
///     .uri("http://abc.com")
///     .header("key", "value")
///     .method(Method::HEAD)
///     .body(b"".to_vec());
/// ```
pub fn new_request() -> http::request::Builder {
    hyper::Request::builder()
}

/// Get a default [`HttpClient`] implementation, which calls the hyper library.
pub fn create() -> Result<impl HttpClient, HttpClientCreationError> {
    HttpClientBuilder::with_default_retry().build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_client::proxy::ProxySettingsError;
    use fluent_assertions::Should;

    #[test]
    fn http_client_creation_error_to_stream_error() {
        let error = HttpClientCreationError::ProxySettings(ProxySettingsError::RelativeUrl);
        StreamError::from(error.clone()).should().be(StreamError::ConnectionFailure {
            source: Some(Arc::new(error)),
        });
    }

    #[test]
    fn http_client_creation_error_to_destination_error() {
        let error = HttpClientCreationError::ProxySettings(ProxySettingsError::RelativeUrl);
        DestinationError::from(error.clone())
            .should()
            .be(DestinationError::ConnectionFailure {
                source: Some(Arc::new(error)),
            });
    }
}
