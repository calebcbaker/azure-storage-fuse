use super::bearer_token::BearerToken;
use async_trait::async_trait;
use rslex_core::{
    file_io::{DestinationError, StreamError, StreamResult},
    ExternalError,
};
use rslex_http_stream::{ApplyCredential, AsyncResponse, AuthenticatedRequest, HttpError, Request, RetryCondition};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Scope {
    Storage,
    DataLake,
    Database,
}

impl Scope {
    pub fn as_str(&self) -> &'static str {
        match self {
            Scope::Storage => "https://storage.azure.com/.default",
            Scope::DataLake => "https://datalake.azure.net//.default",
            Scope::Database => "https://database.windows.net//.default",
        }
    }
}

pub trait ScopedAccessTokenResolver: Send + Sync + Debug {
    fn resolve_access_token(&self, scope: Scope) -> Result<BearerToken, ResolutionError>;

    fn reset_token(&self, scope: Scope);
}

#[derive(Debug, Clone)]
pub struct ScopedAccessToken {
    resolver: Arc<dyn ScopedAccessTokenResolver>,
    scope: Scope,
}

impl ScopedAccessToken {
    pub fn new(resolver: Arc<dyn ScopedAccessTokenResolver>, scope: Scope) -> Self {
        ScopedAccessToken { resolver, scope }
    }

    pub fn get_token(&self) -> Result<BearerToken, ResolutionError> {
        Ok(self.resolver.resolve_access_token(self.scope.clone())?)
    }

    pub fn reset_token(&self) {
        self.resolver.reset_token(self.scope.clone());
    }
}

impl ApplyCredential for ScopedAccessToken {
    fn apply(&self, request: Request) -> StreamResult<Request> {
        tracing::debug!("[ScopedAccessToken::apply()] Getting Bearer Token...");
        let token = self.get_token()?;

        token.apply(request)
    }
}

#[async_trait]
impl RetryCondition for ScopedAccessToken {
    type Request = AuthenticatedRequest;
    type Response = Result<AsyncResponse, HttpError>;

    async fn should_retry(&self, _: &Self::Request, result: Self::Response, attempt: u32) -> (bool, Self::Response) {
        if attempt > 1 {
            return (false, result);
        }
        let should_retry = match &result {
            Ok(response) => {
                let status = response.status();

                match status.as_u16() {
                    // 401 - Unauthorized
                    // 403 - Forbidden
                    // 511 - NetworkAuthenticationRequired
                    401 | 403 | 511 => {
                        self.reset_token();
                        true
                    },
                    _ => false,
                }
            },
            Err(_) => false,
        };

        (should_retry, result)
    }
}

#[derive(Debug, Clone, Error)]
pub enum ResolutionError {
    #[error("failed to authenticate {0}")]
    AuthenticationError(String),
    #[error("connection failure while resolving AccessToken {message}")]
    ConnectionFailure { message: String, source: Option<ExternalError> },
    #[error("No managed identity was found on compute.")]
    NoIdentityOnCompute,
    #[error("{0}")]
    Unknown(String),
    #[error("an unexpected error occurred while resolving AccessToken {0}")]
    Unexpected(String),
    #[error("invalid input, {message}")]
    InvalidInput { message: String, source: Option<ExternalError> },
}

impl From<ResolutionError> for StreamError {
    fn from(e: ResolutionError) -> Self {
        match e {
            ResolutionError::ConnectionFailure { message: _, source } => StreamError::ConnectionFailure { source },
            ResolutionError::Unexpected(_) => StreamError::Unknown(e.to_string(), None),
            ResolutionError::Unknown(_) => StreamError::Unknown(e.to_string(), None),
            ResolutionError::InvalidInput { message: _, source: _ } => StreamError::InvalidInput {
                message: e.to_string(),
                source: Some(Arc::new(e)),
            },
            ResolutionError::AuthenticationError(_) => StreamError::PermissionDenied {},
            ResolutionError::NoIdentityOnCompute => StreamError::PermissionDenied {},
        }
    }
}

impl From<ResolutionError> for DestinationError {
    fn from(e: ResolutionError) -> Self {
        match e {
            ResolutionError::ConnectionFailure { message: _, source } => DestinationError::ConnectionFailure { source },
            ResolutionError::Unexpected(_) => DestinationError::Unknown(e.to_string(), None),
            ResolutionError::Unknown(_) => DestinationError::Unknown(e.to_string(), None),
            ResolutionError::InvalidInput { message: _, source: _ } => DestinationError::InvalidInput {
                message: e.to_string(),
                source: Some(Arc::new(e)),
            },
            ResolutionError::AuthenticationError(error_msg) => DestinationError::AuthenticationError(error_msg),
            ResolutionError::NoIdentityOnCompute => DestinationError::AuthenticationError("".to_string()),
        }
    }
}

#[cfg(any(feature = "fake_access_token_resolver", test))]
pub mod fake_access_token_resolver {
    use super::*;

    #[derive(Clone, Debug)]
    pub struct FakeAccessTokenResolver {}

    impl FakeAccessTokenResolver {
        pub fn new() -> Self {
            FakeAccessTokenResolver {}
        }
    }

    impl ScopedAccessTokenResolver for FakeAccessTokenResolver {
        fn resolve_access_token(&self, scope: Scope) -> Result<BearerToken, ResolutionError> {
            Ok(BearerToken(format!("{:?}", scope)))
        }

        fn reset_token(&self, _scope: Scope) {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FakeAccessTokenResolver;
    use fluent_assertions::*;
    use http::StatusCode;
    use rslex_http_stream::{new_request, RequestWithCredential};
    use tokio_test::block_on;

    macro_rules! retry_on_auth_error_test {
        ($($name:ident: $value:expr,)*) => {$(
            #[test]
            fn $name() {
                let (status_code, number_of_retries) = $value;
                let credential = Arc::new(ScopedAccessToken {
                    resolver: Arc::new(FakeAccessTokenResolver::new()),
                    scope: Scope::Storage,
                });

                let request = new_request()
                    .uri("https://someaccount.blob.core.windows.net/somecontainer/?comp=list&a=b&c=d&e===")
                    .body(Vec::<u8>::default())
                    .unwrap()
                    .with_credential(credential.clone());

                for i in 0..number_of_retries {
                    let mut response = AsyncResponse::default();
                    *response.status_mut() = status_code;

                    let (should_retry, _) = block_on(credential.should_retry(&request, Ok(response), i));
                    should_retry.should().be_true();
                }

                let mut response = AsyncResponse::default();
                *response.status_mut() = status_code;

                let (should_retry, _) = block_on(credential.should_retry(&request, Ok(response), number_of_retries+1));
                should_retry.should().be_false();
            }
        )*};
    }

    retry_on_auth_error_test! {
        retries_once_on_401_status_code: (StatusCode::UNAUTHORIZED, 1),
        retries_once_on_403_status_code: (StatusCode::FORBIDDEN, 1),
        retries_once_on_511_status_code: (StatusCode::NETWORK_AUTHENTICATION_REQUIRED, 1),
        // Check that it does not retry for commonly retryable error codes
        does_not_retry_on_500_status_code: (StatusCode::INTERNAL_SERVER_ERROR, 0),
        does_not_retry_on_501_status_code: (StatusCode::BAD_GATEWAY, 0),
        does_not_retry_on_502_status_code: (StatusCode::SERVICE_UNAVAILABLE, 0),
        does_not_retry_on_504_status_code: (StatusCode::GATEWAY_TIMEOUT, 0),
        does_not_retry_on_400_status_code: (StatusCode::BAD_REQUEST, 0),
    }

    #[test]
    fn does_not_retry_on_err_response_result() {
        let credential = Arc::new(ScopedAccessToken {
            resolver: Arc::new(FakeAccessTokenResolver::new()),
            scope: Scope::Storage,
        });

        let request = new_request()
            .uri("https://someaccount.blob.core.windows.net/somecontainer/?comp=list&a=b&c=d&e===")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(credential.clone());

        let (should_retry, _) = block_on(credential.should_retry(&request, Err(HttpError::new(false)), 1));
        should_retry.should().be_false();

        let (should_retry, _) = block_on(credential.should_retry(&request, Err(HttpError::new(true)), 1));
        should_retry.should().be_false();
    }
}
