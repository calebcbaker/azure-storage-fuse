use super::{http_error::HttpError, proxy::ProxyInterceptSettings, AsyncResponse};
use crate::{
    http_client::{HttpClientAsync, HttpClientCreationError},
    AuthenticatedRequest,
};
use async_trait::async_trait;
use http::uri::Scheme;
use hyper::{
    client::{connect::Connect, HttpConnector},
    Body, Client, Error as HyperError,
};
use hyper_proxy::ProxyConnector;
use hyper_rustls::HttpsConnector;
use std::sync::Arc;

#[derive(Clone)]
pub(super) enum HyperClient {
    Proxy(
        (
            Client<ProxyConnector<HttpsConnector<HttpConnector>>>,
            ProxyConnector<HttpsConnector<HttpConnector>>,
        ),
    ),
    NoProxy(Client<HttpsConnector<HttpConnector>>),
}

#[async_trait]
impl HttpClientAsync for HyperClient {
    #[inline]
    async fn request_async(&self, req: AuthenticatedRequest) -> Result<AsyncResponse, HttpError> {
        match self {
            HyperClient::Proxy((client, proxy)) => {
                if req.uri().scheme().unwrap_or(&Scheme::HTTP).eq(&Scheme::HTTP) {
                    // From hyper_proxy docs https://docs.rs/hyper-proxy/0.9.1/hyper_proxy/
                    // Connecting to http will trigger regular GETs and POSTs.
                    // We need to manually append the relevant headers to the request
                    if let Some(proxy_headers) = proxy.http_headers(req.uri()) {
                        let mut req = req.clone();
                        req.headers_mut().extend(proxy_headers.clone().into_iter())
                    }
                }
                client.request_async(req)
            },
            HyperClient::NoProxy(client) => client.request_async(req),
        }
        .await
    }
}

pub(super) fn create() -> Result<impl HttpClientAsync + Clone, HttpClientCreationError> {
    let proxy_settings = ProxyInterceptSettings::create_from_env_variables()?;
    if let Some(proxy_settings) = proxy_settings {
        let proxy_connector = proxy_settings.try_get_proxy_connector()?;
        Ok(HyperClient::Proxy((
            Client::builder().build::<_, hyper::Body>(proxy_connector.clone()),
            proxy_connector,
        )))
    } else {
        Ok(HyperClient::NoProxy(
            Client::builder().build::<_, hyper::Body>(HttpsConnector::with_native_roots()),
        ))
    }
}

impl From<HyperError> for HttpError {
    fn from(e: HyperError) -> Self {
        HttpError {
            is_connect: e.is_connect(),
            boxed_error: Arc::new(e),
        }
    }
}

#[async_trait]
impl<C> HttpClientAsync for Client<C, Body>
where C: Connect + Clone + Send + Sync + 'static
{
    async fn request_async(&self, req: AuthenticatedRequest) -> Result<AsyncResponse, HttpError> {
        let (parts, body) = req.into_request_async().await?.into_parts();
        let req = hyper::Request::from_parts(parts, Body::from(body));
        Ok(self.request(req).await?)
    }
}
