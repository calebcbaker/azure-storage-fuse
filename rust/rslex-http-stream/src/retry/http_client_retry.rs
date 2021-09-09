use super::{backoff::ExponentialBackoffWithJitter, retry::RetryStrategy};
use crate::{
    http_client::HttpClientAsync,
    retry::{backoff::BackoffStrategy, RetryCondition},
    AsyncResponse, AuthenticatedRequest, HttpError,
};
use async_trait::async_trait;
use lazy_static::lazy_static;
use std::time::Duration;

fn get_number_of_retries() -> u32 {
    if let Ok(Ok(num_retries)) = std::env::var("AZUREML_DATASET_HTTP_RETRY_COUNT").map(|r| r.parse::<u32>()) {
        tracing::info!(%num_retries, "AZUREML_DATASET_HTTP_RETRY_COUNT set in environment, using for http client: {}", num_retries);
        num_retries
    } else {
        7
    }
}

lazy_static! {
    pub(crate) static ref NUMBER_OF_RETRIES: u32 = get_number_of_retries();
}

pub(crate) static INITIAL_BACKOFF: Duration = Duration::from_millis(250);

pub(crate) trait WithRetry<H: HttpClientAsync + Clone> {
    fn with_retry<B: BackoffStrategy, C: RetryCondition>(self, condition: RetryStrategy<B, C>) -> RobustHttpClient<H, B, C>;
}

pub struct RobustHttpClient<H: HttpClientAsync + Clone, B: BackoffStrategy, C: RetryCondition> {
    http_client: H,
    retry_strategy: RetryStrategy<B, C>,
}

pub struct DefaultHttpRetryCondition {}

#[async_trait]
impl RetryCondition for DefaultHttpRetryCondition {
    type Request = AuthenticatedRequest;
    type Response = Result<AsyncResponse, HttpError>;

    async fn should_retry(&self, request: &Self::Request, response_result: Self::Response, attempt: u32) -> (bool, Self::Response) {
        let (retry_on_transient_error, result) = match response_result {
            Ok(response) => {
                let status = response.status();

                match status.as_u16() {
                    0 => (true, Ok(response)), /* this probably will never happen but special case 0 (no status code received / parsed) just in case */
                    408 | 429 => (true, Ok(response)), // throttling and timeout,
                    499 => (true, Ok(response)), // nginx client timeout
                    d if d >= 500 => (true, Ok(response)), // all unknown errors
                    _ => (false, Ok(response)),
                }
            },
            Err(e) => (true, Err(e)),
        };

        if !retry_on_transient_error {
            request.credential().should_retry(request, result, attempt).await
        } else {
            (retry_on_transient_error, result)
        }
    }
}

impl DefaultHttpRetryCondition {
    pub fn default_exponential_retry_strategy() -> RetryStrategy<ExponentialBackoffWithJitter, DefaultHttpRetryCondition> {
        Self::exponential_retry_strategy(INITIAL_BACKOFF, *NUMBER_OF_RETRIES)
    }

    pub fn exponential_retry_strategy(
        initial_backoff: Duration,
        number_of_retries: u32,
    ) -> RetryStrategy<ExponentialBackoffWithJitter, DefaultHttpRetryCondition> {
        let retry_condition = DefaultHttpRetryCondition {};
        let backoff_strategy = ExponentialBackoffWithJitter::new(initial_backoff, number_of_retries);
        RetryStrategy::new(backoff_strategy, retry_condition)
    }
}

impl<H: HttpClientAsync + Clone> WithRetry<H> for H {
    fn with_retry<B: BackoffStrategy, C: RetryCondition>(self, retry_strategy: RetryStrategy<B, C>) -> RobustHttpClient<H, B, C> {
        RobustHttpClient {
            http_client: self,
            retry_strategy,
        }
    }
}

#[async_trait]
impl<H, B, C> HttpClientAsync for RobustHttpClient<H, B, C>
where
    H: HttpClientAsync + Clone,
    B: BackoffStrategy,
    C: RetryCondition<Request = AuthenticatedRequest, Response = Result<AsyncResponse, HttpError>>,
{
    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn request_async(&self, req: AuthenticatedRequest) -> Result<AsyncResponse, HttpError> {
        let client = self.http_client.clone();

        self.retry_strategy
            .run(&req, async || client.request_async(req.clone()).await)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        http_client::{new_request, FakeHttpClient},
        FakeCredential, HttpClient, RequestWithCredential,
    };
    use std::sync::Arc;

    fn fake_retry_strategy() -> RetryStrategy<ExponentialBackoffWithJitter, DefaultHttpRetryCondition> {
        DefaultHttpRetryCondition::exponential_retry_strategy(Duration::from_millis(1), *NUMBER_OF_RETRIES)
    }

    macro_rules! retry_test {
        ($($name:ident: $value:expr,)*) => {$(
            #[test]
            fn $name() {

                let (status_code, expected_retries) = $value;
                let client_mock = FakeHttpClient::default().status(status_code as u16);
                let client = Arc::new(client_mock.clone().with_retry(fake_retry_strategy()));

                let request = new_request()
                    .method("GET")
                    .uri("http://test.com")
                    .body(Vec::<u8>::default())
                    .unwrap();

                let _result = client.request(request.into());

                client_mock.assert_num_requests(expected_retries + 1);
            }
        )*};
    }

    retry_test! {
        // most common 50* codes
        should_retry_on_status_code_500: (500, 7),
        should_retry_on_status_code_501: (501, 7),
        should_retry_on_status_code_502: (502, 7),
        should_retry_on_status_code_503: (503, 7),
        should_retry_on_status_code_504: (504, 7),
        // timeout
        should_retry_on_status_code_408: (408, 7),
        // too many requests
        should_retry_on_status_code_429: (429, 7),
        // nginx timeout
        should_retry_on_status_code_499: (499, 7),

        // most common success codes
        should_not_retry_on_status_code_100: (100, 0),
        should_not_retry_on_status_code_200: (200, 0),
        should_not_retry_on_status_code_201: (201, 0),
        should_not_retry_on_status_code_204: (204, 0),

        // redirect
        should_not_retry_on_status_code_300: (300, 0),
        should_not_retry_on_status_code_301: (301, 0),
    }

    #[test]
    fn should_retry_on_auth_error_when_applicable() {
        let client_mock = FakeHttpClient::default().status(401);
        let client = Arc::new(client_mock.clone().with_retry(fake_retry_strategy()));

        let request = new_request()
            .method("GET")
            .uri("http://test.com")
            .body(Vec::<u8>::default())
            .unwrap();
        let credential = Arc::new(FakeCredential::with_retries(3));

        let _result = client.request(request.with_credential(credential));

        client_mock.assert_num_requests(3);
    }
}
