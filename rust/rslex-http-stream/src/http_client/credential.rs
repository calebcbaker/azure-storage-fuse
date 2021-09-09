use crate::{retry::RetryCondition, AsyncResponse, AuthenticatedRequest, HttpError, Request};
use async_trait::async_trait;
use rslex_core::file_io::StreamError;
use std::fmt::Debug;

pub trait ApplyCredential:
    Send + Sync + Debug + RetryCondition<Request = AuthenticatedRequest, Response = Result<AsyncResponse, HttpError>>
{
    fn apply(&self, request: Request) -> Result<Request, StreamError>;
}

impl ApplyCredential for () {
    fn apply(&self, request: Request) -> Result<Request, StreamError> {
        Ok(request)
    }
}

#[async_trait]
impl RetryCondition for () {
    type Request = AuthenticatedRequest;
    type Response = Result<AsyncResponse, HttpError>;

    async fn should_retry(&self, _: &Self::Request, response_result: Self::Response, _: u32) -> (bool, Self::Response) {
        (false, response_result)
    }
}

#[cfg(any(feature = "fake_http_client", test))]
pub mod fake_credential {
    use crate::{new_request, retry::RetryCondition, ApplyCredential, AsyncResponse, AuthenticatedRequest, HttpError, Request};
    use async_trait::async_trait;
    use fluent_assertions::*;
    use rslex_core::file_io::StreamResult;
    use std::fmt::{Debug, Formatter};

    pub struct FakeCredential {
        retry_attempts: u32,
    }

    impl FakeCredential {
        const URI_WITH_FAKE_CREDENTIAL: &'static str = "http://uri/with/fake/credential";

        pub fn new() -> FakeCredential {
            FakeCredential { retry_attempts: 0 }
        }

        pub fn with_retries(attempts: u32) -> FakeCredential {
            FakeCredential { retry_attempts: attempts }
        }

        pub fn verify(request: &Request) {
            request.uri().should().equal_string(FakeCredential::URI_WITH_FAKE_CREDENTIAL);
        }
    }

    impl ApplyCredential for FakeCredential {
        fn apply(&self, _request: Request) -> StreamResult<Request> {
            Ok(new_request()
                .uri(FakeCredential::URI_WITH_FAKE_CREDENTIAL)
                .body(Vec::<u8>::default())
                .unwrap())
        }
    }

    #[async_trait]
    impl RetryCondition for FakeCredential {
        type Request = AuthenticatedRequest;
        type Response = Result<AsyncResponse, HttpError>;

        async fn should_retry(&self, _: &Self::Request, response_result: Self::Response, attempt: u32) -> (bool, Self::Response) {
            (attempt < self.retry_attempts, response_result)
        }
    }

    impl Debug for FakeCredential {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
            write!(f, "FakeCredential")
        }
    }
}
