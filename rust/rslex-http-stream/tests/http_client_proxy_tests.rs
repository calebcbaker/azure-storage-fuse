use fluent_assertions::{OptionAssertions, ResultAssertions, Should};
use http::{Request, Uri};
use httptest::{matchers::request, responders::status_code, Expectation, Server};
use hyper::Body;
use portpicker::pick_unused_port;
use rslex_http_stream::{create_http_client, new_request, HttpClient};
use simple_proxy::{
    middlewares::Logger,
    proxy::{
        error::MiddlewareError,
        middleware::{Middleware, MiddlewareResult},
        service::{ServiceContext, State},
    },
    Environment, SimpleProxy,
};
use std::sync::{Arc, Mutex};
use tokio_test::block_on;

struct RequestsTracker {
    /// list of request uris
    requests: Arc<Mutex<Vec<Uri>>>,
}

impl RequestsTracker {
    pub fn get_middleware() -> (Self, Arc<Mutex<Vec<Uri>>>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                requests: requests.clone(),
            },
            requests,
        )
    }
}

impl Middleware for RequestsTracker {
    fn name() -> String
    where Self: Sized {
        "TrackRequests".to_string()
    }

    fn before_request(
        &mut self,
        req: &mut Request<Body>,
        _ctx: &ServiceContext,
        _state: &State,
    ) -> Result<MiddlewareResult, MiddlewareError> {
        let mut requests = self.requests.lock().unwrap();
        requests.push(req.uri().clone());
        Ok(MiddlewareResult::Next)
    }
}

fn setup_test_proxy() -> (SimpleProxy, Arc<Mutex<Vec<Uri>>>, u16) {
    let port = pick_unused_port().expect("Unable to find free port to run a test proxy.");
    println!("Starting test proxy on port: {}", port);

    let mut proxy = SimpleProxy::new(port, Environment::Development);
    let logger = Logger::default();
    proxy.add_middleware(Box::new(logger));

    let (requests_tracking_middleware, requests) = RequestsTracker::get_middleware();
    proxy.add_middleware(Box::new(requests_tracking_middleware));

    (proxy, requests, port)
}

#[test]
fn http_client_should_direct_traffic_through_simple_http_proxy() {
    let http_server = Server::run();
    http_server.expect(Expectation::matching(request::method_path("GET", "/foo")).respond_with(status_code(200)));

    let (proxy, requests, port) = setup_test_proxy();
    let _proxy_thread = std::thread::spawn(move || block_on(proxy.run()));

    let http_proxy_url = format!("http://localhost:{}", port);

    std::env::set_var("http_proxy", http_proxy_url);

    let client = Arc::new(create_http_client().unwrap());
    let request = new_request()
        .method("GET")
        .uri(http_server.url("/foo"))
        .body(Vec::<u8>::default())
        .should()
        .be_ok()
        .which_value();

    client.clone().request(request.into()).should().be_ok();
    let requests = requests.lock().unwrap();
    requests.len().should().be(1);
    requests
        .get(0)
        .should()
        .be_some()
        .which_value()
        .should()
        .be(&http_server.url("/foo"));

    std::env::remove_var("http_proxy");
}
