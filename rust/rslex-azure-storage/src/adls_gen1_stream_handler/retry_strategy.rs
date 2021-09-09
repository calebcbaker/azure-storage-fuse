#![allow(unused_imports)]
#![allow(dead_code)]
use async_trait::async_trait;
use http::StatusCode;
use rslex_http_stream::{AsyncResponse, AuthenticatedRequest, DefaultHttpRetryCondition, HttpError, RetryCondition};
use std::sync::Arc;

/// This is a workaround for adls gen 1 issue.
/// On some requests (like attempt to get attributes of a folder <string>), it returns 500 and error:
/// "A potentially dangerous Request.Path value was detected from the client".
/// And we retry on all 500 errors. This causes a delayed response to the user even though we know that we won't succeed on retries.
/// So we short-circuit retries for this specific error message.
const ADLS_GEN1_FALSE_500_ERROR_MESSAGE: &str = "A potentially dangerous Request.Path value was detected from the client";

pub(super) struct AdlsGen1RetryCondition {
    default_retry_condition: DefaultHttpRetryCondition,
}

impl AdlsGen1RetryCondition {
    pub fn new() -> Self {
        AdlsGen1RetryCondition {
            default_retry_condition: DefaultHttpRetryCondition {},
        }
    }
}

#[async_trait]
impl RetryCondition for AdlsGen1RetryCondition {
    type Request = AuthenticatedRequest;
    type Response = Result<AsyncResponse, HttpError>;

    async fn should_retry(&self, request: &Self::Request, response_result: Self::Response, attempt: u32) -> (bool, Self::Response) {
        match response_result {
            // TODO: add retry for the spurious 403 that ADLS would return sometimes under load
            Ok(response) if response.status().as_u16() == 500 => {
                let (mut headers, body) = response.into_parts();

                match hyper::body::to_bytes(body).await.map(|b| b.to_vec()) {
                    Err(e) => (
                        true,
                        Err(HttpError {
                            is_connect: false,
                            boxed_error: Arc::new(e),
                        }),
                    ),
                    Ok(body_data) => match std::str::from_utf8(&body_data) {
                        Ok(body_str) => {
                            if body_str.contains(ADLS_GEN1_FALSE_500_ERROR_MESSAGE) {
                                headers.status = StatusCode::NOT_FOUND;
                                (false, Ok(AsyncResponse::from_parts(headers, hyper::Body::from(body_data))))
                            } else {
                                (true, Ok(AsyncResponse::from_parts(headers, hyper::Body::from(body_data))))
                            }
                        },
                        Err(_) => {
                            self.default_retry_condition
                                .should_retry(
                                    request,
                                    Ok(AsyncResponse::from_parts(headers, hyper::Body::from(body_data))),
                                    attempt,
                                )
                                .await
                        },
                    },
                }
            },
            Ok(response) => self.default_retry_condition.should_retry(request, Ok(response), attempt).await,
            Err(e) => self.default_retry_condition.should_retry(request, Err(e), attempt).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use rslex_http_stream::{new_request, RequestWithCredential};
    use tokio_test::block_on;

    #[test]
    fn should_not_retry_on_adls_specific_false_500_error() {
        let retry_condition = AdlsGen1RetryCondition::new();

        let request = new_request()
            .method("GET")
            .uri("http://test.com")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(Arc::new(()));

        let mut response = AsyncResponse::new(hyper::Body::from(ADLS_GEN1_FALSE_500_ERROR_MESSAGE));
        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

        let (should_retry, _) = block_on(retry_condition.should_retry(&request, Ok(response), 1));

        should_retry.should().be_false();
    }

    #[test]
    fn should_retry_on_http_error_result() {
        let retry_condition = AdlsGen1RetryCondition::new();

        let request = new_request()
            .method("GET")
            .uri("http://test.com")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(Arc::new(()));

        let (should_retry, _) = block_on(retry_condition.should_retry(&request, Err(HttpError::new(true)), 1));

        should_retry.should().be_true();
    }

    #[test]
    fn should_retry_on_utf8_body_error() {
        let retry_condition = AdlsGen1RetryCondition::new();

        let request = new_request()
            .method("GET")
            .uri("http://test.com")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(Arc::new(()));

        let mut response = AsyncResponse::new(hyper::Body::from(vec![0xC3, 0x28])); // [0xC3, 0X28] - invalid utf-8 sequence
        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;

        let (should_retry, _) = block_on(retry_condition.should_retry(&request, Ok(response), 1));

        should_retry.should().be_true();
    }
}
