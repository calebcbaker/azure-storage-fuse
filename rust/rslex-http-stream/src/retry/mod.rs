pub(crate) mod backoff;
pub(crate) mod http_client_retry;
mod retry;
use async_trait::async_trait;

#[async_trait]
pub trait RetryCondition: Sync + Send + 'static {
    type Request;
    type Response;

    async fn should_retry(&self, request: &Self::Request, response_result: Self::Response, attempt: u32) -> (bool, Self::Response);
}

#[cfg(test)]
pub use backoff::ExponentialBackoffWithJitter;
pub(crate) use http_client_retry::WithRetry;
pub use retry::RetryStrategy;
