use crate::{
    http_client::{
        hyper_client,
        redirect::WithRedirect,
        timeout::{WithTimeout, DEFAULT_TIMEOUT},
        HttpClientCreationError,
    },
    retry::{
        backoff::{BackoffStrategy, ExponentialBackoffWithJitter},
        http_client_retry::{DefaultHttpRetryCondition, INITIAL_BACKOFF, NUMBER_OF_RETRIES},
        RetryCondition, RetryStrategy, WithRetry,
    },
    AsyncResponse, AuthenticatedRequest, HttpClient, HttpError,
};
use std::time::Duration;

pub struct HttpClientBuilder<T> {
    timeout: Duration,
    retry_strategy: T,
}

impl HttpClientBuilder<RetryStrategy<ExponentialBackoffWithJitter, DefaultHttpRetryCondition>> {
    pub fn with_default_retry() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            retry_strategy: DefaultHttpRetryCondition::default_exponential_retry_strategy(),
        }
    }
}

impl<C> HttpClientBuilder<RetryStrategy<ExponentialBackoffWithJitter, C>>
where C: RetryCondition<Request = AuthenticatedRequest, Response = Result<AsyncResponse, HttpError>>
{
    pub fn with_retry_condition(retry_condition: C) -> Self {
        let backoff_strategy = ExponentialBackoffWithJitter::new(INITIAL_BACKOFF, *NUMBER_OF_RETRIES);
        let retry_strategy = RetryStrategy::new(backoff_strategy, retry_condition);

        Self {
            timeout: DEFAULT_TIMEOUT,
            retry_strategy,
        }
    }
}

impl<B, C> HttpClientBuilder<RetryStrategy<B, C>>
where
    B: BackoffStrategy,
    C: RetryCondition<Request = AuthenticatedRequest, Response = Result<AsyncResponse, HttpError>>,
{
    pub fn build(self) -> Result<impl HttpClient, HttpClientCreationError> {
        Ok(hyper_client::create()?
            .with_redirect()
            .with_timeout(self.timeout)
            .with_retry(self.retry_strategy))
    }
}
