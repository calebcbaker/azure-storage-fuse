use crate::{new_request, ApplyCredential, HttpError, SpawnBlocking};
use headers::{HeaderMap, HeaderValue};
use http::Uri;
use hyper::Request as HyperRequest;
use std::sync::Arc;

pub type Request = HyperRequest<Vec<u8>>;

#[derive(Debug)]
pub struct AuthenticatedRequest {
    request: Request,
    credential: Arc<dyn ApplyCredential>,
    redirect_hops: u8,
}

pub trait RequestWithCredential {
    fn with_credential(self, credential: Arc<dyn ApplyCredential + 'static>) -> AuthenticatedRequest;
}

impl AuthenticatedRequest {
    pub fn into_request(self) -> Result<Request, HttpError> {
        self.credential.apply(self.request).map_err(|e| HttpError {
            is_connect: false,
            boxed_error: Arc::new(e),
        })
    }

    pub async fn into_request_async(self) -> Result<Request, HttpError> {
        // into_request will potentially make blocking http calls (to apply credential) to retrieve AAD tokens
        // we must spawn it to Runtime engine to avoid blocking the engine.
        // spawn here will light up a new thread, keep the blocking call there so the
        // tokio runtime task queue won't be blocked
        (move || self.into_request()).spawn_blocking().await.map_err(|e| HttpError {
            is_connect: false,
            boxed_error: Arc::new(e),
        })?
    }

    pub fn uri(&self) -> &Uri {
        self.request.uri()
    }

    pub fn set_uri<T>(&mut self, uri: T)
    where Uri: From<T> {
        *self.request.uri_mut() = Uri::from(uri)
    }

    pub fn body_len(&self) -> u64 {
        self.request.body().len() as u64
    }

    pub fn credential(&self) -> Arc<dyn ApplyCredential> {
        self.credential.clone()
    }

    pub fn redirect(&self, uri: Uri) -> Self {
        let mut new_req = new_request()
            .method(self.request.method().clone())
            .uri(uri)
            .body(self.request.body().clone())
            .unwrap();
        new_req.headers_mut().extend(self.request.headers().clone());

        AuthenticatedRequest {
            request: new_req,
            credential: self.credential.clone(),
            redirect_hops: self.redirect_hops + 1,
        }
    }

    pub fn redirect_hops(&self) -> u8 {
        self.redirect_hops
    }

    pub fn headers_mut(&mut self) -> &mut HeaderMap<HeaderValue> {
        self.request.headers_mut()
    }
}

impl Clone for AuthenticatedRequest {
    fn clone(&self) -> Self {
        let mut new_req = new_request()
            .method(self.request.method().clone())
            .uri(self.request.uri().clone())
            .body(self.request.body().clone())
            .unwrap();
        new_req.headers_mut().extend(self.request.headers().clone());

        AuthenticatedRequest {
            request: new_req,
            credential: self.credential.clone(),
            redirect_hops: self.redirect_hops,
        }
    }
}

impl From<Request> for AuthenticatedRequest {
    fn from(req: Request) -> Self {
        AuthenticatedRequest {
            request: req,
            credential: Arc::new(()),
            redirect_hops: 0,
        }
    }
}

impl RequestWithCredential for Request {
    fn with_credential(self, credential: Arc<dyn ApplyCredential + 'static>) -> AuthenticatedRequest {
        AuthenticatedRequest {
            request: self,
            credential,
            redirect_hops: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{http_client::credential::fake_credential::FakeCredential, new_request, RequestWithCredential};
    use std::str::FromStr;

    #[test]
    fn into_request_should_apply_credentials_to_request() {
        let uri = Uri::from_str("http://test.com").unwrap();
        let credential = FakeCredential::new();
        let request = new_request()
            .method("GET")
            .uri("http://test.com")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(Arc::new(credential));

        assert_eq!(request.uri().clone(), uri); // request should not be changes before credential applied

        let hyper_request = request.into_request().unwrap();
        FakeCredential::verify(&hyper_request);
    }
}
