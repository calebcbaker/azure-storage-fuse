use async_trait::async_trait;
use rslex_core::file_io::StreamResult;
use rslex_http_stream::{ApplyCredential, AsyncResponse, AuthenticatedRequest, HttpError, Request, RetryCondition};
use std::fmt::Debug;

const HEADER_X_MS_VERSION: &str = "x-ms-version";
const HEADER_AUTHORIZATION: &str = "Authorization";

/// Refer to: https://swagger.io/docs/specification/authentication/bearer-authentication/
#[derive(Clone, Debug)]
pub struct BearerToken(pub String);

/// Generally displaying a raw Bearer Token is not necessary and not secure.
impl std::fmt::Display for BearerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BearerToken")
    }
}

#[async_trait]
impl RetryCondition for BearerToken {
    type Request = AuthenticatedRequest;
    type Response = Result<AsyncResponse, HttpError>;

    async fn should_retry(&self, _: &Self::Request, response_result: Self::Response, _: u32) -> (bool, Self::Response) {
        (false, response_result)
    }
}

impl ApplyCredential for BearerToken {
    fn apply(&self, request: Request) -> StreamResult<Request> {
        let (mut parts, body) = request.into_parts();

        // add authorization header
        parts
            .headers
            .insert(HEADER_AUTHORIZATION, format!("Bearer {}", self.0).parse().unwrap());

        // force a API version. earlier version of blob service doesn't support bearer token
        if parts.headers.contains_key(HEADER_X_MS_VERSION) {
            // verify the version is higher than 2015-02-21
            // earlier version is not compatible with the signature string
            let version = parts.headers[HEADER_X_MS_VERSION].to_str().unwrap();
            if version < "2017-11-09" {
                tracing::error!("x-ms-version({}) earlier than 2017-11-09 does not support Bearer token", version);
                panic!("x-ms-version({}) earlier than 2017-11-09 does not support Bearer token", version);
            }
        } else {
            // use the latest version: https://docs.microsoft.com/en-us/rest/api/storageservices/versioning-for-the-azure-storage-services#version-2019-07-07
            parts.headers.insert(HEADER_X_MS_VERSION, "2019-07-07".parse().unwrap());
        }

        Ok(Request::from_parts(parts, body))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use fluent_assertions::*;
    use http::StatusCode;
    use rslex_http_stream::{new_request, RequestWithCredential};
    use std::sync::Arc;
    use tokio_test::block_on;

    #[test]
    fn apply_credential_adds_auth_header() {
        let token = BearerToken("🍉".to_owned());
        let token = Arc::new(token);

        new_request()
            .uri("https://abc.com/")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(token.clone())
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .headers()[HEADER_AUTHORIZATION]
            .to_ref()
            .should()
            .equal(&&format!("Bearer {}", token.0));
    }

    #[test]
    fn apply_credential_adds_x_ms_version() {
        let token = BearerToken("🍉".to_owned());

        new_request()
            .uri("https://abc.com/")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(Arc::new(token))
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .headers()[HEADER_X_MS_VERSION]
            .to_ref()
            .should()
            .equal(&"2019-07-07");
    }

    #[test]
    fn apply_credential_to_old_x_ms_version_panics() {
        let token = BearerToken("🍉".to_owned());

        (|| {
            new_request()
                .uri("https://abc.com/")
                .header(HEADER_X_MS_VERSION, "1994-11-13")
                .body(Vec::<u8>::default())
                .unwrap()
                .with_credential(Arc::new(token))
                .into_request()
        })
        .should()
        .panic()
        .with_message("x-ms-version(1994-11-13) earlier than 2017-11-09 does not support Bearer token");
    }

    #[test]
    fn does_not_retry_on_error() {
        let credential = Arc::new(BearerToken("token".to_owned()));

        let request = new_request()
            .uri("https://someaccount.blob.core.windows.net/somecontainer/?comp=list&a=b&c=d&e===")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(credential.clone());

        let mut response = AsyncResponse::default();
        *response.status_mut() = StatusCode::UNAUTHORIZED;

        let (should_retry, _) = block_on(credential.should_retry(&request, Ok(response), 0));
        should_retry.should().be_false();
    }
}
