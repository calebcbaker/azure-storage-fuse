use super::bearer_token::BearerToken;
use async_trait::async_trait;
use chrono::Utc;
use http::Method;
use rslex_core::file_io::StreamResult;
use rslex_http_stream::{
    new_request, ApplyCredential, AsyncResponse, AuthenticatedRequest, HttpClient, HttpError, Request, ResponseExt, RetryCondition,
};
use serde_json::json;
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    sync::{Arc, Mutex},
};
use url::form_urlencoded;

const HEADER_CONTENT_TYPE: &str = "Content-Type";
const CONTENT_TYPE_FORM_URLENCODED: &str = "application/x-www-form-urlencoded";
const EXPIRE_BUFFER: i64 = 300; // buffer time for token expire

#[derive(Clone)]
/// Support ServicePrincipal credential
/// to access blob data, the SPI must has "Storage Blob Data Reader" access
/// https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-reader
pub(super) struct ServicePrincipal {
    http_client: Arc<dyn HttpClient>,
    resource_url: String,
    authority_url: String,
    tenant_id: String,
    client_id: String,
    client_secret: String,
    access_token: Arc<Mutex<(Option<BearerToken>, Option<i64>)>>, // (token, expired_on)
}

impl ServicePrincipal {
    pub fn new<S: Into<String>>(
        http_client: Arc<dyn HttpClient>,
        resource_url: S,
        authority_url: S,
        tenant_id: S,
        client_id: S,
        client_secret: S,
    ) -> ServicePrincipal {
        ServicePrincipal {
            http_client,
            resource_url: resource_url.into(),
            authority_url: authority_url.into(),
            tenant_id: tenant_id.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            access_token: Arc::new(Mutex::new((None, None))),
        }
    }

    fn get_access_token(&self) -> StreamResult<BearerToken> {
        {
            let access_token = self.access_token.lock().expect("retrieve mutex should succeed");

            if let Some(ref token) = access_token.0 {
                if let Some(ref exp) = access_token.1 {
                    let timestamp = Utc::now().timestamp();

                    if timestamp < exp - EXPIRE_BUFFER {
                        tracing::debug!(
                            "[ServicePrincipal::get_access_token()] cached token expired on {}, current time {}, will use cached token",
                            exp,
                            timestamp
                        );
                        return Ok(token.clone());
                    } else {
                        tracing::debug!(
                            "[ServicePrincipal::get_access_token()] cached token expired on {}, current time {}, will re-get token",
                            exp,
                            timestamp
                        );
                    }
                } else {
                    tracing::debug!("[ServicePrincipal::get_access_token()] cached token no expiration, will use cached token");
                    return Ok(token.clone());
                }
            }
        }

        let token_body: Vec<u8> = form_urlencoded::Serializer::new(String::new())
            .append_pair("grant_type", "client_credentials")
            .append_pair("client_id", self.client_id.as_str())
            .append_pair("client_secret", self.client_secret.as_str())
            .append_pair("resource", self.resource_url.as_str())
            .finish()
            .into_bytes();
        let token_request = new_request()
            .method(Method::POST)
            .uri(format!("{}/{}/oauth2/token", self.authority_url, self.tenant_id))
            .header(HEADER_CONTENT_TYPE, CONTENT_TYPE_FORM_URLENCODED)
            .body(token_body)
            .expect("build token retrieval request should succeed");

        let res = self.http_client.clone().request(token_request.into())?.success()?;

        let token_body = String::from_utf8(res.into_body().to_vec()).expect("token response should always be utf8 encoded");
        let kvs: HashMap<String, String> = serde_json::from_str(token_body.as_str()).expect("token response should be valid json");

        let token = BearerToken(kvs["access_token"].to_string());
        let expired_on: Option<i64> = kvs.get("expires_on").map(|s| s.as_str().parse().unwrap());

        {
            let mut access_token = self.access_token.lock().expect("retrieve mutex should succeed");
            *access_token = (Some(token.clone()), expired_on);
        }
        tracing::trace!(
            expires_on = expired_on.unwrap_or(-1),
            "[ServicePrincipal::get_access_token()] Service Principal access token retrieved"
        );
        Ok(token)
    }
}

impl PartialEq for ServicePrincipal {
    fn eq(&self, other: &Self) -> bool {
        self.resource_url == other.resource_url
            && self.authority_url == other.authority_url
            && self.tenant_id == other.tenant_id
            && self.client_id == other.client_id
            && self.client_secret == other.client_secret
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

#[async_trait]
impl RetryCondition for ServicePrincipal {
    type Request = AuthenticatedRequest;
    type Response = Result<AsyncResponse, HttpError>;

    async fn should_retry(&self, _: &Self::Request, response_result: Self::Response, _: u32) -> (bool, Self::Response) {
        (false, response_result)
    }
}

impl ApplyCredential for ServicePrincipal {
    fn apply(&self, request: Request) -> StreamResult<Request> {
        let token = self.get_access_token()?;
        token.apply(request)
    }
}

impl Debug for ServicePrincipal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            json!({
        "ServicePrincipal": {
            "resource_url": self.resource_url,
            "authority_url": self.authority_url,
            "tenant_id": self.tenant_id,
            "client_id": self.client_id,
            "client_secret": self.client_secret}})
            .to_string()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use rslex_http_stream::{FakeHttpClient, RequestWithCredential};
    use serde_json::json;

    const HEADER_AUTHORIZATION: &str = "Authorization";

    trait IntoServicePrincipal {
        fn into_service_principal(
            self,
            resource_url: &str,
            authority_url: &str,
            tenant_id: &str,
            client_id: &str,
            client_secret: &str,
        ) -> ServicePrincipal;
    }

    impl IntoServicePrincipal for FakeHttpClient {
        fn into_service_principal(
            self,
            resource_url: &str,
            authority_url: &str,
            tenant_id: &str,
            client_id: &str,
            client_secret: &str,
        ) -> ServicePrincipal {
            ServicePrincipal::new(Arc::new(self), resource_url, authority_url, tenant_id, client_id, client_secret)
        }
    }

    #[test]
    fn apply_service_principal_add_auth_header() {
        let resource_url = "http://resource.url.net/";
        let authority_url = "http://authrity.url.net";
        let tenant_id = "TTTT-EEEE-NNNN-AAAA-NNNN-TTTT";
        let client_id = "this is client id";
        let client_secret = "this is the client secret";
        let access_token = "this is the access token";
        let credential = FakeHttpClient::default()
            .assert_request(|_, r| {
                r.uri().should().equal_string("http://authrity.url.net/TTTT-EEEE-NNNN-AAAA-NNNN-TTTT/oauth2/token");
                r.method().should().be(&Method::POST);
                r.body().should().be(&b"grant_type=client_credentials&client_id=this+is+client+id&client_secret=this+is+the+client+secret&resource=http%3A%2F%2Fresource.url.net%2F".to_vec());
                r.headers()[HEADER_CONTENT_TYPE].to_ref().should().equal(&"application/x-www-form-urlencoded");
            })
            .body(
                json!({
                "token_type": "Bearer",
                "expires_in": "3599",
                "ext_expires_in": "3599",
                "expires_on": "1579589646",
                "not_before": "1579585746",
                "resource": resource_url,
                "access_token": access_token})
                .to_string(),
            )
            .into_service_principal(resource_url, authority_url, tenant_id, client_id, client_secret);

        new_request()
            .uri("https://abc.com/")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(Arc::new(credential))
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .headers()[HEADER_AUTHORIZATION]
            .to_ref()
            .should()
            .equal(&&format!("Bearer {}", access_token));
    }

    #[test]
    fn retrieve_access_token_is_cached_before_expired() {
        let resource_url = "http://resource.url.net/";
        let authority_url = "http://authrity.url.net/";
        let tenant_id = "TTTT-EEEE-NNNN-AAAA-NNNN-TTTT";
        let client_id = "this is client id";
        let client_secret = "this is the client secret";
        let access_token = "this is the access token";
        let credential = FakeHttpClient::default()
            .assert_num_requests(1) // will be called only once
            .body(
                json!({
                "token_type": "Bearer",     // no expire on, so will always use
                "resource": resource_url,
                "access_token": access_token})
                .to_string(),
            )
            .into_service_principal(resource_url, authority_url, tenant_id, client_id, client_secret);

        let credential = Arc::new(credential);

        // first call
        new_request()
            .uri("https://abc.com/")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(credential.clone())
            .into_request()
            .should()
            .be_ok();

        // call twice, the second call should use cache
        new_request()
            .uri("https://abc.com/")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(credential.clone())
            .into_request()
            .should()
            .be_ok();
    }

    #[test]
    fn retrieve_access_token_will_re_get_when_cache_expire() {
        let resource_url = "http://resource.url.net/";
        let authority_url = "http://authrity.url.net/";
        let tenant_id = "TTTT-EEEE-NNNN-AAAA-NNNN-TTTT";
        let client_id = "this is client id";
        let client_secret = "this is the client secret";
        let access_token = "this is the access token";

        let credential = FakeHttpClient::default()
            .assert_num_requests(2) // should be called twice
            .body(
                json!({
                "token_type": "Bearer",     // no expire on, so will always use
                "resource": resource_url,
                "expires_on": Utc::now().timestamp().to_string(),    // current time, guarantee expired on second call
                "access_token": access_token})
                .to_string(),
            )
            .into_service_principal(resource_url, authority_url, tenant_id, client_id, client_secret);

        let credential = Arc::new(credential);

        new_request()
            .uri("https://abc.com/")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(credential.clone())
            .into_request()
            .should()
            .be_ok();

        // call twice, the second call should use cache
        new_request()
            .uri("https://abc.com/")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(credential.clone())
            .into_request()
            .should()
            .be_ok();
    }
}
