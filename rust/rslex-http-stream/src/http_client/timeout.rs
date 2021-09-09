use crate::{http_client::HttpClientAsync, AsyncResponse, AuthenticatedRequest, HttpError};
use async_trait::async_trait;
use std::{
    io::{Error, ErrorKind},
    sync::Arc,
    time::Duration,
};
use tokio::time::timeout;

// To allow big enough timeout to transfer request body to the server (that could be in 10s of MB for upload case)
// we are adjusting timeout adding a time based on body length.
// This adjustment factor will be multiplied on request body length allowing network transfer speed to be 5 KBytes/s in the worst case.
const SEND_BODY_TIMEOUT_ADJUSTMENT: u64 = 1024 / 5;
pub(crate) static DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) trait WithTimeout<H: HttpClientAsync + Clone> {
    fn with_default_timeout(self) -> RequestTimeout<H>;
    fn with_timeout(self, duration: Duration) -> RequestTimeout<H>;
}

#[derive(Clone)]
pub struct RequestTimeout<H: HttpClientAsync + Clone> {
    http_client: H,
    timeout: Duration,
}

impl<H: HttpClientAsync + Clone> WithTimeout<H> for H {
    fn with_default_timeout(self) -> RequestTimeout<H> {
        RequestTimeout {
            http_client: self,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    fn with_timeout(self, timeout: Duration) -> RequestTimeout<H> {
        RequestTimeout {
            http_client: self,
            timeout,
        }
    }
}

#[async_trait]
impl<H: HttpClientAsync + Clone> HttpClientAsync for RequestTimeout<H> {
    async fn request_async(&self, req: AuthenticatedRequest) -> Result<AsyncResponse, HttpError> {
        let client = self.http_client.clone();

        let request_future = client.request_async(req.clone());

        // Calculating request timeout based on request body size.
        let body_length = req.body_len();
        let tm = self.timeout + Duration::from_secs((body_length / SEND_BODY_TIMEOUT_ADJUSTMENT) as u64);

        timeout(tm, request_future).await.map_err(|_| HttpError {
            is_connect: true,
            boxed_error: Arc::new(Error::new(ErrorKind::TimedOut, "Request timeout")),
        })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{new_request, FakeHttpClient, HttpClient};
    use fluent_assertions::*;

    #[test]
    fn should_timeout_after_specified_time() {
        let expected_timeout = Duration::from_secs(5);

        let client_mock = FakeHttpClient::default().status(200).with_delay(expected_timeout * 2);
        let client = Arc::new(client_mock.clone().with_timeout(expected_timeout));

        let request = new_request()
            .method("GET")
            .uri("http://test.com")
            .body(Vec::<u8>::default())
            .unwrap();

        client
            .request(request.into())
            .should()
            .be_err()
            .which_value()
            .is_connect
            .should()
            .be_true();
    }

    #[test]
    fn timeout_should_be_calculated_based_on_request_body_size() {
        let expected_timeout = Duration::from_secs(1);

        // with 0 size body this code will timeout cause delay in FakeHttpClient is x2 of the timeout we set.
        let client_mock = FakeHttpClient::default().status(200).with_delay(expected_timeout * 3);
        let client = Arc::new(client_mock.clone().with_timeout(expected_timeout));

        // But with long enough body we will adjust timeout to allow enough time to transfer it over the network
        // and the fake call should not throw timeout error.
        // Body of this length should add 5 more seconds on top of the default expected_timeout
        let body = vec![0; (5 * SEND_BODY_TIMEOUT_ADJUSTMENT) as usize];

        let request = new_request().method("GET").uri("http://test.com").body(body).unwrap();

        client.request(request.into()).should().be_ok();
    }
}
