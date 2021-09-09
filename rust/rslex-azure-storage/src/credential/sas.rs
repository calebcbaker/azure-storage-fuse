use async_trait::async_trait;
use http::Uri;
use rslex_core::{file_io::StreamResult, MapErrToUnknown};
use rslex_http_stream::{ApplyCredential, AsyncResponse, AuthenticatedRequest, HttpError, Request, RetryCondition};
use serde_json::json;
use std::fmt::{Debug, Formatter};

pub(super) struct Sas {
    token: String,
}

impl Sas {
    pub fn new(token: impl Into<String>) -> Sas {
        let token_str = token.into();
        if token_str.starts_with("?") {
            Sas {
                token: token_str[1..].to_owned(),
            }
        } else {
            Sas { token: token_str }
        }
    }
}

#[async_trait]
impl RetryCondition for Sas {
    type Request = AuthenticatedRequest;
    type Response = Result<AsyncResponse, HttpError>;

    async fn should_retry(&self, _: &Self::Request, response_result: Self::Response, _: u32) -> (bool, Self::Response) {
        (false, response_result)
    }
}

impl ApplyCredential for Sas {
    fn apply(&self, request: Request) -> StreamResult<Request> {
        let (mut parts, body) = request.into_parts();

        let uri = if parts.uri.query().is_none() {
            format!("{}?{}", parts.uri.to_string(), self.token)
        } else {
            format!("{}&{}", parts.uri.to_string(), self.token)
        };

        parts.uri = uri.parse::<Uri>().map_err_to_unknown()?;

        Ok(Request::from_parts(parts, body))
    }
}

impl Debug for Sas {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            json!({
        "Sas": {
            "token": self.token}})
            .to_string()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use http::StatusCode;
    use rslex_http_stream::{new_request, RequestWithCredential};
    use std::sync::Arc;
    use tokio_test::block_on;

    #[test]
    fn apply_sas_without_queries() {
        let sas_token = "this&is&the&sas&token";
        let credential = Sas::new(sas_token);

        // RequestBuilder will call credential apply internally
        // here as we are testing the credential apply separately, we will makeup the request ourselves
        new_request()
            .uri("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv")
            .body(Vec::<u8>::default())
            .expect("create request should succeed")
            .with_credential(Arc::new(credential))
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .uri()
            .should()
            .equal_string(format!(
                "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv?{}",
                sas_token
            ));
    }

    #[test]
    fn apply_sas_with_queries() {
        let sas_token = "this&is&the&sas&token";
        let credential = Sas::new(sas_token);

        // RequestBuilder will call credential apply internally
        // here as we are testing the credential apply separately, we will makeup the request ourselves
        new_request()
            .uri("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv?a=b")
            .body(Vec::<u8>::default())
            .expect("create request should succeed")
            .with_credential(Arc::new(credential))
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .uri()
            .should()
            .equal_string(format!(
                "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv?a=b&{}",
                sas_token
            ));
    }

    #[test]
    fn apply_sas_with_leading_question_mark() {
        let sas_token = "this&is&the&sas&token";
        let sas_token_with_question_mark = format!("?{}", sas_token);
        let credential = Sas::new(sas_token_with_question_mark);

        // RequestBuilder will call credential apply internally
        // here as we are testing the credential apply separately, we will makeup the request ourselves
        new_request()
            .uri("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv?a=b")
            .body(Vec::<u8>::default())
            .expect("create request should succeed")
            .with_credential(Arc::new(credential))
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .uri()
            .should()
            .equal_string(format!(
                "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv?a=b&{}",
                sas_token
            ));
    }

    #[test]
    fn does_not_retry_on_error() {
        let sas_token = "this&is&the&sas&token";
        let credential = Arc::new(Sas::new(sas_token));

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
