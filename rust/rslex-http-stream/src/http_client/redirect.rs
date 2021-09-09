use super::{http_error::HttpError, AsyncResponse, AsyncResponseExt};
use crate::{http_client::HttpClientAsync, AuthenticatedRequest};
use async_trait::async_trait;
use http::Uri;
use tracing_sensitive::AsSensitive;

const MAX_REDIRECTS: u8 = 10;

pub(super) trait WithRedirect<H: HttpClientAsync + Clone> {
    fn with_redirect(self) -> Redirect<H>;
}

#[derive(Clone)]
pub(super) struct Redirect<T: HttpClientAsync + Clone> {
    http_client: T,
}

#[async_trait]
impl<T: HttpClientAsync + Clone> HttpClientAsync for Redirect<T> {
    async fn request_async(&self, req: AuthenticatedRequest) -> Result<AsyncResponse, HttpError> {
        let client = self.http_client.clone();

        let response = client.request_async(req.clone()).await?;

        if response.status().is_redirection() {
            tracing::debug!(
                "[Redirect::request_async()] redirection response: {}",
                response.debug().as_sensitive_ref()
            );

            let num_redirects = req.redirect_hops();

            if num_redirects >= MAX_REDIRECTS {
                tracing::warn!("[Redirect::request_async()] maximum number of redirect reached, will ignore redirect");
                Ok(response)
            } else if let Some(new_uri) = response.headers().get("location") {
                let uri: Uri = new_uri
                    .to_str()
                    .expect("location header should be valid string")
                    .parse()
                    .expect("location header should be valid uri");

                tracing::debug!(
                    "[Redirect::request_async()] {}-th redirect to {}",
                    num_redirects,
                    uri.as_sensitive_ref()
                );

                self.request_async(req.redirect(uri)).await
            } else {
                tracing::error!("[Redirect::request_async()] cannot find location header, ignore redirect");
                Ok(response)
            }
        } else {
            Ok(response)
        }
    }
}

impl<H: HttpClientAsync + Clone + ?Sized> WithRedirect<H> for H {
    fn with_redirect(self) -> Redirect<H> {
        Redirect { http_client: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{new_request, FakeHttpClient, HttpClient, ResponseExt};
    use fluent_assertions::*;
    use std::{convert::TryFrom, sync::Arc};

    #[test]
    fn request_with_300_will_redirect() {
        let uri = "https://somewhere.com/something&call=1";
        let new_uri = "https://somewhereelse.com/something&call=2";
        let redirect = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .status(307)
            .header("location", new_uri)
            .with_request(|id, _| id == 1)
            .assert_request(move |_, r| {
                r.headers()["some-header-key"]
                    .to_str()
                    .should()
                    .be_ok()
                    .with_value("some-header-value");
                r.body().should().equal_binary(b"some request body content");
                r.uri().should().be(&Uri::try_from(new_uri).unwrap());
            })
            .body("some response body content")
            .with_redirect();

        let request = new_request()
            .uri(uri)
            .header("some-header-key", "some-header-value")
            .body(String::from("some request body content").into_bytes())
            .expect("create request should succeed");

        Arc::new(redirect)
            .request(request.into())
            .should()
            .be_ok()
            .which_value()
            .into_string()
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string("some response body content");
    }

    #[test]
    fn request_with_300_but_no_location_header_will_pass_through() {
        let uri = "https://somewhere.com/something&call=1";
        let redirect = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .status(307)
            .with_request(|id, _| id == 1)
            .assert_num_requests(0)
            .body("some response body content")
            .with_redirect();

        let request = new_request()
            .uri(uri)
            .header("some-header-key", "some-header-value")
            .body(String::from("some request body content").into_bytes())
            .expect("create request should succeed");

        Arc::new(redirect)
            .request(request.into())
            .should()
            .be_ok()
            .which_value()
            .into_string()
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string("");
    }

    #[test]
    fn request_with_200_will_passthrough() {
        let uri = "https://somewhere.com/something&call=1";
        let redirect = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .status(200)
            .body("some response body content")
            .with_redirect();

        let request = new_request()
            .uri(uri)
            .header("some-header-key", "some-header-value")
            .body(String::from("some request body content").into_bytes())
            .expect("create request should succeed");

        Arc::new(redirect)
            .request(request.into())
            .should()
            .be_ok()
            .which_value()
            .into_string()
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string("some response body content");
    }

    #[test]
    fn request_with_consecutive_redirect() {
        let uri = "https://somewhere.com/something&call=1";
        let redirect = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .status(307)
            .header("location", "https://somewhere.com/something&call=2")
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.headers()["some-header-key"]
                    .to_str()
                    .should()
                    .be_ok()
                    .with_value("some-header-value");
                r.body().should().equal_binary(b"some request body content");
                r.headers()["some-header-key"]
                    .to_str()
                    .should()
                    .be_ok()
                    .with_value("some-header-value");
                r.body().should().equal_binary(b"some request body content");
            })
            .status(307)
            .header("location", "https://somewhere.com/something&call=3")
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                r.headers()["some-header-key"]
                    .to_str()
                    .should()
                    .be_ok()
                    .with_value("some-header-value");
                r.body().should().equal_binary(b"some request body content");
            })
            .body("some response body content")
            .with_redirect();

        let request = new_request()
            .uri(uri)
            .header("some-header-key", "some-header-value")
            .body(String::from("some request body content").into_bytes())
            .expect("create request should succeed");

        Arc::new(redirect)
            .request(request.into())
            .should()
            .be_ok()
            .which_value()
            .into_string()
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string("some response body content");
    }
}
