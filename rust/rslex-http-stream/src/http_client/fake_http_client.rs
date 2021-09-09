use super::{AsyncResponse, HttpClient, HttpError, Response, SpawnBlocking};
use crate::{http_client::HttpClientAsync, AuthenticatedRequest, Request};
use async_trait::async_trait;
use fluent_assertions::utils::AssertionFailure;
use hyper::{
    header::{HeaderName, HeaderValue},
    Body, Response as HttpResponse,
};
use log::{debug, trace};
use std::{
    error::Error,
    fmt::{Display, Formatter},
    sync::{Arc, Mutex},
    thread::sleep,
    time::Duration,
};

#[derive(Debug)]
pub struct DummyError;

impl Display for DummyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "dummy error")
    }
}

impl Error for DummyError {}

impl HttpError {
    pub fn new(connected: bool) -> Self {
        HttpError {
            is_connect: connected,
            boxed_error: Arc::new(DummyError),
        }
    }
}

struct Stats {
    pub num_requests: u32,
    pub assert_num_requests: Option<u32>,
}

impl Drop for Stats {
    fn drop(&mut self) {
        if let Some(assert_count) = self.assert_num_requests {
            if self.num_requests != assert_count {
                AssertionFailure::new(format!(
                    "assert_num_requests() expecting the request to be call {} times, but was called {} times",
                    assert_count, self.num_requests
                ))
                .fail();
            }
        }
    }
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            num_requests: 0,
            assert_num_requests: None,
        }
    }
}

#[derive(Clone)]
struct FakeResponse {
    pub request_pattern: Arc<dyn Fn(u32, &Request) -> bool + Send + Sync>,
    pub request_assert: Arc<dyn Fn(u32, &Request) -> () + Send + Sync>,
    pub body: String,
    pub headers: Vec<(String, String)>,
    pub error: Option<HttpError>,
    pub status: u16,
    pub stats: Arc<Mutex<Stats>>,
}

impl FakeResponse {
    pub fn new() -> FakeResponse {
        FakeResponse {
            body: String::default(),
            request_pattern: Arc::new(|_, _| true),
            request_assert: Arc::new(|_, _| ()),
            headers: Vec::default(),
            error: None,
            status: 200,
            stats: Arc::new(Mutex::new(Stats::default())),
        }
    }

    pub fn take_request(&self, id: u32, req: &Request) -> bool {
        (self.request_pattern)(id, req)
    }

    pub fn result(&self) -> Result<Response, HttpError> {
        if let Some(ref error) = self.error {
            return Err(error.clone());
        }

        let mut response = HttpResponse::builder().status(self.status);

        let headers = response.headers_mut().unwrap();

        for (ref key, ref value) in &self.headers {
            headers.insert(
                HeaderName::from_bytes(key.as_bytes()).unwrap(),
                HeaderValue::from_str(value.as_str()).unwrap(),
            );
        }

        let response = response.body(self.body.clone().as_bytes().to_vec()).unwrap();

        Ok(response)
    }
}

/// A test util to mock http connection behaviors, implementing trait [`HttpClient`].
///
/// # Example
///
/// ```
/// use rslex_http_stream::{new_request, FakeHttpClient, HttpClient, Method};
/// use std::sync::Arc;
///
/// let client = FakeHttpClient::default()
///     .header("Content-Length", "2496")
///     .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
///     .header("x-ms-creation-time", "Thu, 04 Jul 2019 20:09:52 GMT")
///     .body("this is the content");
///
/// let request = new_request().body(b"".to_vec()).unwrap();
///
/// let response = Arc::new(client).request(request.into()).unwrap();
///
/// assert_eq!(String::from_utf8(response.body().clone()).unwrap(), "this is the content");
/// assert_eq!(&response.headers()["Content-Length"], "2496");
/// assert_eq!(&response.headers()["Last-Modified"], "Wed, 04 Sep 2019 02:29:10 GMT");
/// assert_eq!(&response.headers()["x-ms-creation-time"], "Thu, 04 Jul 2019 20:09:52 GMT");
/// ```
///
/// Or if you want to verify the input request
/// ```
/// use rslex_http_stream::{new_request, FakeHttpClient, HttpClient, Method};
/// use std::sync::Arc;
///
/// let client = FakeHttpClient::default()
///     .with_request(|_id, r| {
///         r.method() == hyper::Method::GET && r.headers().contains_key("Range") && r.headers()["Range"].to_str().unwrap() == "bytes=0-4"
///     })
///     .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
///     .header("x-ms-creation-time", "Thu, 04 Jul 2019 20:09:52 GMT")
///     .body("this is the content");
///
/// let request = new_request()
///     .method(Method::GET)
///     .header("Range", "bytes=0-4")
///     .body(b"".to_vec())
///     .unwrap();
///
/// let response = Arc::new(client).request(request.into()).unwrap();
///
/// assert_eq!(String::from_utf8(response.body().clone()).unwrap(), "this is the content");
/// assert_eq!(&response.headers()["Last-Modified"], "Wed, 04 Sep 2019 02:29:10 GMT");
/// assert_eq!(&response.headers()["x-ms-creation-time"], "Thu, 04 Jul 2019 20:09:52 GMT");
/// ```
///
/// If you want to mock multiple requests, seperate each request with a with_request call. FakeHttpClient
/// will match all the patterns one by one, and return the first response matching the condition.
///
///```
/// use rslex_http_stream::{new_request, FakeHttpClient, HttpClient, Method};
/// use std::sync::Arc;
///
/// let client = FakeHttpClient::default()
///     .with_request(|_id, r| r.method() == hyper::Method::HEAD) // first request
///     .header("Content-Length", "19")
///     .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
///     .with_request(|_id, r| r.method() == hyper::Method::GET) // second request
///     .body("this is the content");
///
/// let client = Arc::new(client);
///
/// let request1 = new_request().method(Method::HEAD).body(b"".to_vec()).unwrap();
/// let response1 = client.clone().request(request1.into()).unwrap();
///
/// assert_eq!(&response1.headers()["Content-Length"], "19");
/// assert_eq!(&response1.headers()["Last-Modified"], "Wed, 04 Sep 2019 02:29:10 GMT");
///
/// let request2 = new_request().method(Method::GET).body(b"".to_vec()).unwrap();
/// let response2 = client.clone().request(request2.into()).unwrap();
///
/// assert_eq!(String::from_utf8(response2.body().clone()).unwrap(), "this is the content");
/// ```
#[derive(Clone)]
pub struct FakeHttpClient {
    responses: Vec<FakeResponse>,
    total_stats: Arc<Mutex<Stats>>,
    delay: Option<Duration>,
}

impl Default for FakeHttpClient {
    fn default() -> Self {
        FakeHttpClient {
            responses: vec![],
            total_stats: Arc::new(Mutex::new(Stats::default())),
            delay: Option::None,
        }
    }
}

impl FakeHttpClient {
    fn last_response(&mut self) -> &mut FakeResponse {
        if self.responses.is_empty() {
            self.responses.push(FakeResponse::new());
        }

        self.responses.last_mut().unwrap()
    }

    /// Setup the Fake to take request meeting condition ```pattern```. Call functions ```head```, ```status``` etc.
    /// to setup the response. The ```pattern``` condition could be specified with either id of the request (starting
    /// from 0), or by testing the request itself.
    ///
    /// # Example
    ///
    /// ```
    /// use rslex_http_stream::FakeHttpClient;
    /// let client = FakeHttpClient::default()
    ///     .with_request(|_, r| r.method() == hyper::Method::HEAD) // for request with HEAD method
    ///     .header("Content-Length", "19")
    ///     .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
    ///     .with_request(|id, _| id == 3) // for the 3rd request
    ///     .body("this is the content");
    /// ```
    ///
    /// When there a request meeting multiple patterns, the first one will be applied.
    pub fn with_request(mut self, pattern: impl Fn(u32, &Request) -> bool + Send + Sync + 'static) -> Self {
        self.responses.push(FakeResponse::new());
        self.last_response().request_pattern = Arc::new(pattern);
        self
    }

    /// Verifying that the current request (set by ```with_request```) satisfies the condition.
    ///
    /// # Example
    ///
    /// ```
    /// use rslex_http_stream::{new_request, FakeHttpClient, HttpClient, Method};
    /// use std::sync::Arc;
    ///
    /// let client = FakeHttpClient::default()
    ///     .with_request(|_, r| r.method() == hyper::Method::HEAD) // for request with HEAD method
    ///     .assert_request(|_, r| {
    ///         assert_eq!(r.uri(), "http://abc.com/something");
    ///     });
    ///
    /// let client = Arc::new(client);
    /// let request = new_request()
    ///     .method(Method::HEAD)
    ///     .uri("http://abc.com/something")
    ///     .body(b"".to_vec())
    ///     .unwrap();
    /// let _ = client.clone().request(request.into()).unwrap(); // this will panic if assertion fails
    /// ```
    pub fn assert_request(mut self, assert: impl Fn(u32, &Request) -> () + Send + Sync + 'static) -> Self {
        self.last_response().request_assert = Arc::new(assert);
        self
    }

    /// Verify that the current request (set by ```with_request```) will be called exactly
    /// ```num_request``` times.
    /// 
    /// # Example
    /// ```
    /// use rslex_http_stream::{FakeHttpClient, new_request, Method, HttpClient};
    /// use std::sync::Arc;
    ///
    /// let client = FakeHttpClient::default()
    ///     .with_request(|_, r| r.method() == hyper::Method::HEAD) // for request with HEAD method
    ///     .assert_num_requests(2);                                // will be called 1 time
    ///
    /// let client = Arc::new(client);
    /// let request1 = new_request().method(Method::HEAD).body(b"".to_vec()).unwrap();
    /// let _ = client.clone().request(request1.into()).unwrap();
    ///
    /// let request2 = new_request().method(Method::HEAD).body(b"".to_vec()).unwrap();
    /// let _ = client.clone().request(request2.into()).unwrap();
    /// // if not called twice, the client will panic in Drop
    /// ```
    pub fn assert_num_requests(mut self, num_requests: u32) -> Self {
        self.last_response().stats.lock().unwrap().assert_num_requests = Some(num_requests);

        self
    }

    /// Set the returning status code for current request (set by ```with_request```).
    ///
    /// # Example
    /// ```
    /// use rslex_http_stream::{new_request, FakeHttpClient, HttpClient, Method};
    /// use std::sync::Arc;
    /// let client = FakeHttpClient::default()
    ///     .with_request(|_, r| r.method() == Method::HEAD) // for request with HEAD method
    ///     .status(200);
    ///
    /// let request = new_request().method(Method::HEAD).body(b"".to_vec()).unwrap();
    ///
    /// let response = Arc::new(client).request(request.into()).unwrap();
    ///
    /// assert_eq!(response.status(), 200);
    /// ```
    pub fn status(mut self, code: u16) -> Self {
        self.last_response().status = code;
        self
    }

    /// Set the returning header for current request (set by ```with_request```).
    ///
    /// # Example
    /// ```
    /// use rslex_http_stream::{new_request, FakeHttpClient, HttpClient, Method};
    /// use std::sync::Arc;
    /// let client = FakeHttpClient::default().header("key1", "value1");
    ///
    /// let request = new_request().method(Method::HEAD).body(b"".to_vec()).unwrap();
    ///
    /// let response = Arc::new(client).request(request.into()).unwrap();
    ///
    /// assert_eq!(&response.headers()["key1"], "value1");
    /// ```
    pub fn header<S1: Into<String>, S2: Into<String>>(mut self, key: S1, value: S2) -> Self {
        self.last_response().headers.push((key.into(), value.into()));
        self
    }

    /// Set the returning body for current request (set by ```with_request```).
    ///
    /// # Example
    /// ```
    /// use rslex_http_stream::{new_request, FakeHttpClient, HttpClient, Method};
    /// use std::sync::Arc;
    /// let client = FakeHttpClient::default()
    ///     .with_request(|_, r| r.method() == Method::HEAD) // for request with HEAD method
    ///     .body("Hello world!");
    ///
    /// let request = new_request().method(Method::HEAD).body(b"".to_vec()).unwrap();
    ///
    /// let response = Arc::new(client).request(request.into()).unwrap();
    ///
    /// assert_eq!(String::from_utf8(response.body().clone()).unwrap(), "Hello world!");
    /// ```
    pub fn body<S: Into<String>>(mut self, body: S) -> Self {
        self.last_response().body = body.into();
        self
    }

    /// Set the returning error for current request (set by ```with_request```).
    ///
    /// # Example
    /// ```
    /// use rslex_http_stream::{new_request, FakeHttpClient, HttpClient, HttpError, Method};
    /// use std::sync::Arc;
    /// let client = FakeHttpClient::default()
    ///     .with_request(|_, r| r.method() == Method::HEAD) // for request with HEAD method
    ///     .error(HttpError::new(false));
    ///
    /// let request = new_request().method(Method::HEAD).body(b"".to_vec()).unwrap();
    ///
    /// let error = Arc::new(client).request(request.into()).expect_err("should return error");
    ///
    /// assert_eq!(error.is_connect, false);
    /// ```
    pub fn error(mut self, error: HttpError) -> Self {
        self.last_response().error = Some(error);
        self
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Option::Some(delay);
        self
    }
}

#[async_trait]
impl HttpClientAsync for FakeHttpClient {
    async fn request_async(&self, req: AuthenticatedRequest) -> Result<AsyncResponse, HttpError> {
        let client = Arc::new(self.clone());
        let response = (move || client.request(req)).spawn_blocking().await.unwrap()?;
        let (parts, body) = response.into_parts();
        let body: Body = body.into();
        Ok(AsyncResponse::from_parts(parts, body))
    }
}

#[async_trait]
impl HttpClient for FakeHttpClient {
    fn request(self: Arc<Self>, req: AuthenticatedRequest) -> Result<Response, HttpError> {
        if let Option::Some(delay) = self.delay {
            sleep(delay);
        }

        let mut total_stats = self.total_stats.lock().unwrap();
        log::debug!("Request: [{}]{:?}", total_stats.num_requests, req);

        for response in &self.responses {
            // match the pattern
            if response.take_request(total_stats.num_requests, &req.clone().into_request()?) {
                (response.request_assert)(total_stats.num_requests, &req.clone().into_request()?);
                let mut stats = response.stats.lock().unwrap();
                stats.num_requests += 1;
                let result = response.result();
                trace!("Response: {:?}", result.as_ref().map(|x| x.headers()));
                debug!("Body: {:?}", result.as_ref().map(|x| String::from_utf8(x.body().clone())));

                total_stats.num_requests += 1;
                return result;
            }
        }

        panic!(
            "cannot find pattern matching the req [{}]{:?} in FakeHttpClient",
            total_stats.num_requests, req
        );
    }

    async fn request_async(self: Arc<Self>, req: AuthenticatedRequest) -> Result<AsyncResponse, HttpError> {
        let client = self.clone();
        let response = (move || client.request(req)).spawn_blocking().await.unwrap()?;
        let (parts, body) = response.into_parts();
        let body: Body = body.into();
        Ok(AsyncResponse::from_parts(parts, body))
    }
}
