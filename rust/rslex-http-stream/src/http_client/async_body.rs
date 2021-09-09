use super::http_error::HttpError;
use crate::{AsyncResponse, AuthenticatedRequest, ExternalError, HttpClient};
use bytes::Bytes;
use futures::task::{Context, Poll};
use http::{HeaderMap, HeaderValue};
use http_body::Body as HttpBody;
use hyper::Body;
use std::{
    error::Error as ErrorStd,
    fmt::{Display, Formatter},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

pub trait Timeout {
    fn reset(&mut self);
    fn has_timed_out(&self) -> bool;
}

pub struct FixedDurationTimeout {
    duration: Duration,
    timeout_instant: Instant,
}

impl FixedDurationTimeout {
    pub fn new(duration: Duration) -> FixedDurationTimeout {
        let timeout_instant = Instant::now() + duration;
        FixedDurationTimeout { duration, timeout_instant }
    }
}

impl Timeout for FixedDurationTimeout {
    fn reset(&mut self) {
        self.timeout_instant = Instant::now() + self.duration;
    }

    fn has_timed_out(&self) -> bool {
        Instant::now() > self.timeout_instant
    }
}

pub struct AsyncBody<TBackoff: Iterator<Item = Duration>, TTimeout: Timeout> {
    inner_body: State,
    timeout: TTimeout,
    request: AuthenticatedRequest,
    backoff: TBackoff,
    retry_attempt: u64,
    http_client: Arc<dyn HttpClient>,
    bytes_returned: u64,
    bytes_to_skip: u64,
    bytes_skipped: u64,
}

/// The 'Body' state is used to store a pinned pointer of the stream of bytes. The 'Request' state
/// stores the response future and only used when a request needs to be retried.
pub enum State {
    Body(Pin<Box<Body>>),
    Request(Pin<Box<dyn Future<Output = Result<AsyncResponse, HttpError>> + Send>>),
}

#[derive(Debug, PartialEq)]
pub enum ErrorKind {
    Timeout,
    Retry(u64),
    Other,
}

#[derive(Debug)]
pub struct AsyncBodyError {
    kind: ErrorKind,
    inner: Option<ExternalError>,
}

impl AsyncBodyError {
    fn new(kind: ErrorKind, inner: Option<ExternalError>) -> AsyncBodyError {
        AsyncBodyError { kind, inner }
    }
}

impl Display for AsyncBodyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut message = "".to_string();
        match self.kind {
            ErrorKind::Retry(retry_attempt) => {
                message += &format!(
                    "Retry error (attempt number {}) while trying to poll data from the response body",
                    retry_attempt
                );
            },
            ErrorKind::Timeout => message += "Timed out while trying to poll data from the response body",
            ErrorKind::Other => message += "Error while trying to polling trailers",
        }

        if self.inner.is_some() {
            message += &format!(" with inner error {}", &self.inner.as_ref().unwrap().to_string());
        }

        write!(f, "{}", &message)
    }
}

impl ErrorStd for AsyncBodyError {}

impl From<AsyncBodyError> for HttpError {
    fn from(e: AsyncBodyError) -> Self {
        HttpError {
            is_connect: false,
            boxed_error: Arc::new(e),
        }
    }
}

/// Provides overridden methods to pull out the next data buffer of a http stream with delay-enabled retries and timeouts.
///
/// # Remarks
/// The request (with credentials) and the http client to be used for retrying are provided as input. The backoff delay and number
/// of retries is governed by the ['iterator'](crate::retry::backoff::ExponentialBackoffWithJitterIterator) passed to it.
impl<TBackoff: Iterator<Item = Duration>> AsyncBody<TBackoff, FixedDurationTimeout> {
    pub fn new(
        body: Body,
        timeout_duration: Duration,
        http_client: Arc<dyn HttpClient>,
        request: AuthenticatedRequest,
        backoff: TBackoff,
    ) -> AsyncBody<TBackoff, FixedDurationTimeout> {
        AsyncBody {
            inner_body: State::Body(Box::pin(body)),
            timeout: FixedDurationTimeout::new(timeout_duration),
            request,
            backoff,
            retry_attempt: 0,
            http_client,
            bytes_returned: 0,
            bytes_to_skip: 0,
            bytes_skipped: 0,
        }
    }
}

impl<TBackoff: Iterator<Item = Duration>, TTimeout: Timeout> Unpin for AsyncBody<TBackoff, TTimeout> {}

impl<TBackoff: Iterator<Item = Duration>, TTimeout: Timeout> HttpBody for AsyncBody<TBackoff, TTimeout> {
    type Data = Bytes;
    type Error = AsyncBodyError;

    fn poll_data(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        // Outer result will be set to Err() in case we need to retry.
        let poll_result: Result<Poll<Option<Result<Self::Data, Self::Error>>>, AsyncBodyError> = match &mut self.inner_body {
            State::Body(body) => match body.as_mut().poll_data(cx) {
                Poll::Ready(Some(Ok(result))) => {
                    self.timeout.reset();
                    let bytes_to_return = (result.len() as u64 + self.bytes_skipped) as i64 - self.bytes_to_skip as i64;
                    if bytes_to_return > 0 {
                        self.bytes_returned += bytes_to_return as u64;
                        // reset bytes to skip and bytes skipped
                        self.bytes_to_skip = 0;
                        self.bytes_skipped = 0;
                        Ok(Poll::Ready(Some(Ok(Bytes::copy_from_slice(
                            &result[(result.len() as u64 - bytes_to_return as u64) as usize..],
                        )))))
                    } else {
                        self.bytes_skipped += result.len() as u64;
                        cx.waker().clone().wake();
                        Ok(Poll::Pending)
                    }
                },
                Poll::Ready(Some(Err(e))) => Err(AsyncBodyError::new(ErrorKind::Retry(self.retry_attempt), Some(Arc::new(e)))),
                Poll::Pending if self.timeout.has_timed_out() => Err(AsyncBodyError::new(ErrorKind::Timeout, None)),
                res => Ok(res.map_err(|e| AsyncBodyError::new(ErrorKind::Other, Some(Arc::new(e))))),
            },
            State::Request(response_future) => {
                match response_future.as_mut().poll(cx) {
                    Poll::Ready(res) => {
                        match res {
                            Ok(res) => {
                                self.inner_body = State::Body(Box::pin(res.into_body()));
                                // TODO: Check if any delay needs to be added here
                                cx.waker().clone().wake();
                                Ok(Poll::Pending)
                            },
                            Err(e) => Ok(Poll::Ready(Some(Err(AsyncBodyError::new(
                                ErrorKind::Retry(self.retry_attempt),
                                Some(e.boxed_error),
                            ))))),
                        }
                    },
                    Poll::Pending => Ok(Poll::Pending),
                }
            },
        };

        match poll_result {
            Ok(r) => r,
            Err(e) => {
                self.retry_attempt += 1;
                match self.backoff.next() {
                    Some(delay) => {
                        if e.kind == ErrorKind::Timeout {
                            tracing::info!(
                                retry_attempt = %self.retry_attempt,
                                delay = ?delay,
                                error = ?e,
                                "[RetryStrategy::run()] Retrying due to timeout"
                            );
                        } else {
                            tracing::info!(
                                retry_attempt = %self.retry_attempt,
                                delay = ?delay,
                                error = ?e,
                                "[RetryStrategy::run()] Retrying due to error"
                            );
                        }
                        if delay.as_millis() > 0 {
                            std::thread::sleep(delay);
                        }
                        self.bytes_to_skip = self.bytes_returned;
                        self.bytes_skipped = 0;
                        let client = self.http_client.clone();
                        let request = self.request.clone();

                        let state = State::Request(Box::pin(client.request_async(request.into())));
                        self.inner_body = state;
                        self.timeout.reset();
                        self.poll_data(cx)
                    },
                    None => {
                        tracing::warn!(
                            retry_attempt = %self.retry_attempt,
                            error = %e,
                            "[RetryStrategy::run()] ran out of retry attempts"
                        );

                        Poll::Ready(Some(Err(e)))
                    },
                }
            },
        }
    }

    fn poll_trailers(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Option<HeaderMap<HeaderValue>>, Self::Error>> {
        match &mut self.inner_body {
            State::Body(body) => body
                .as_mut()
                .poll_trailers(cx)
                .map_err(|e| AsyncBodyError::new(ErrorKind::Other, Some(Arc::new(e)))),
            State::Request(_) => Poll::Ready(Err(AsyncBodyError::new(ErrorKind::Other, None))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        http_client::{
            async_body::{AsyncBodyError, ErrorKind},
            credential::fake_credential::FakeCredential,
            AsyncBody,
        },
        new_request,
        retry::backoff::{BackoffStrategy, ExponentialBackoffWithJitter},
        FakeHttpClient, HttpError, Method, RequestWithCredential, Wait,
    };
    use fluent_assertions::{ResultAssertions, Should};
    use hyper::Body;
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc::RecvError,
            Arc,
        },
        time::Duration,
    };

    #[test]
    fn polls_data_correctly() {
        let credential = FakeCredential::new();
        let request = new_request()
            .method("GET")
            .uri("http://test.com")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(Arc::new(credential));
        let backoff_strategy = ExponentialBackoffWithJitter::new(Duration::from_millis(100), 2);
        let bdy = Body::from("Hello world");
        let result = async move {
            let body = hyper::body::to_bytes(AsyncBody::new(
                bdy,
                Duration::from_secs(30),
                Arc::new(FakeHttpClient::default()),
                request.clone(),
                backoff_strategy.to_iter(),
            ))
            .await
            .unwrap()
            .to_vec();
            Ok::<Vec<u8>, RecvError>(body)
        }
        .wait();

        assert_eq!("Hello world", std::str::from_utf8(result.unwrap().unwrap().as_ref()).unwrap());
    }

    #[test]
    fn retry_then_works() {
        let credential = FakeCredential::new();
        let request = new_request()
            .method(Method::HEAD)
            .body(b"".to_vec())
            .unwrap()
            .with_credential(Arc::new(credential));
        let backoff_strategy = ExponentialBackoffWithJitter::new(Duration::from_millis(100), 1);
        let (tx, bdy) = Body::channel();
        tx.abort();
        let client = FakeHttpClient::default().body("Hello world");
        let result = async move {
            let body = hyper::body::to_bytes(AsyncBody::new(
                bdy,
                Duration::from_secs(30),
                Arc::new(client),
                request.clone(),
                backoff_strategy.to_iter(),
            ))
            .await
            .unwrap()
            .to_vec();
            Ok::<Vec<u8>, RecvError>(body)
        }
        .wait();

        assert_eq!("Hello world", std::str::from_utf8(result.unwrap().unwrap().as_ref()).unwrap());
    }

    #[test]
    fn fails_with_retry() {
        let credential = FakeCredential::new();
        let request = new_request()
            .method(Method::HEAD)
            .body(b"".to_vec())
            .unwrap()
            .with_credential(Arc::new(credential));
        let backoff_strategy = ExponentialBackoffWithJitter::new(Duration::from_millis(100), 1);
        let (tx, bdy) = Body::channel();
        tx.abort();
        let client = FakeHttpClient::default().error(HttpError::new(false));
        let result = async move {
            let body = hyper::body::to_bytes(AsyncBody::new(
                bdy,
                Duration::from_secs(30),
                Arc::new(client),
                request.clone(),
                backoff_strategy.to_iter(),
            ))
            .await
            .expect_err("should return error");
            Ok::<AsyncBodyError, RecvError>(body)
        }
        .wait();

        let error = result.unwrap().unwrap();
        assert_eq!(
            &format!("{}", error.to_string()),
            "Retry error (attempt number 1) while trying to poll data from the response body with inner error dummy error"
        );
        assert!(matches!(error.kind, ErrorKind::Retry(1)));
    }

    #[derive(Clone)]
    struct StubbedTimeout {
        pub is_timed_out: Arc<AtomicBool>,
    }

    impl Timeout for StubbedTimeout {
        fn reset(&mut self) {
            self.is_timed_out.store(false, Ordering::SeqCst);
        }

        fn has_timed_out(&self) -> bool {
            self.is_timed_out.load(Ordering::SeqCst)
        }
    }

    fn create_body<TBackoff: Iterator<Item = Duration>, TTimeout: Timeout>(
        request_body: Body,
        timeout: TTimeout,
        request: AuthenticatedRequest,
        backoff: TBackoff,
        client: Arc<dyn HttpClient>,
    ) -> AsyncBody<TBackoff, TTimeout> {
        AsyncBody {
            inner_body: State::Body(Box::pin(request_body)),
            timeout,
            request,
            backoff,
            retry_attempt: 0,
            http_client: client,
            bytes_returned: 0,
            bytes_to_skip: 0,
            bytes_skipped: 0,
        }
    }

    #[test]
    fn on_timeout_retries() {
        let timeout = StubbedTimeout {
            is_timed_out: Arc::new(AtomicBool::new(true)),
        };
        let request = new_request()
            .method(Method::GET)
            .body(b"".to_vec())
            .unwrap()
            .with_credential(Arc::new(FakeCredential::new()));
        let expected = "data after retry";
        let client = Arc::new(FakeHttpClient::default().with_request(|_, _| true).body(expected));
        let (_sender, request_body) = Body::channel();
        let body = create_body(request_body, timeout.clone(), request, std::iter::once(Duration::new(0, 0)), client);

        let result = async move { hyper::body::to_bytes(body).await }.wait();

        result
            .should()
            .be_ok()
            .which_value()
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal(expected);

        timeout.is_timed_out.load(Ordering::SeqCst).should().be(false);
    }

    #[test]
    fn when_body_times_out_twice_but_next_retry_succeeds() {}

    #[test]
    fn on_error_during_retry_for_timeout_retries() {}

    #[test]
    fn when_timeout_exhausts_retries_returns_error() {}

    #[test]
    fn when_timeout_and_errors_exhaust_retries_returns_error() {
        let timeout = StubbedTimeout {
            is_timed_out: Arc::new(AtomicBool::new(true)),
        };
        let request = new_request()
            .method(Method::GET)
            .body(b"".to_vec())
            .unwrap()
            .with_credential(Arc::new(FakeCredential::new()));
        let client = Arc::new(FakeHttpClient::default().with_request(|_, _| true).error(HttpError::new(true)));
        let (_sender, request_body) = Body::channel();
        let body = create_body(request_body, timeout.clone(), request, std::iter::once(Duration::new(0, 0)), client);

        let result = async move { hyper::body::to_bytes(body).await }.wait();

        let err = result.should().be_ok().which_value().should().be_err().which_value();
        format!("{}", err)
            .should()
            // One retry caused by timeout. Others are exhausted by http client errors and are not counted towards AsyncBody retries.
            .equal("Retry error (attempt number 1) while trying to poll data from the response body with inner error dummy error");
    }

    #[test]
    fn when_timeout_encountered_after_consuming_part_of_body_retry_returns_remaining_body() {}
}
