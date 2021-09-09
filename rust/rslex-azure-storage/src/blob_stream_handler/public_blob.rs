use super::request_builder::RequestBuilder;
use chrono::{DateTime, Duration, Utc};
use rslex_core::file_io::{StreamError, StreamResult};
use rslex_http_stream::{HttpClient, ResponseExt};
use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
    ops::Add,
    sync::{Arc, Mutex},
};
use tracing_sensitive::AsSensitive;

pub(super) struct PublicBlobChecker {
    http_client: Arc<dyn HttpClient>,
    cache: Mutex<HashMap<String, (bool, DateTime<Utc>)>>,
}

impl PublicBlobChecker {
    pub fn new(http_client: Arc<dyn HttpClient>) -> Self {
        PublicBlobChecker {
            http_client,
            cache: Mutex::new(HashMap::new()),
        }
    }

    #[tracing::instrument(err, skip(self, request_builder))]
    fn try_access(&self, request_builder: &RequestBuilder) -> StreamResult<bool> {
        let blob_metadata = request_builder.metadata();
        tracing::debug!(uri = %blob_metadata.uri().to_string().as_sensitive(), "[PublicBlobChecker::try_access()] Checking Blob public access");
        let response = self.http_client.clone().request(blob_metadata)?.success();
        if response.is_ok() {
            tracing::info!("[PublicBlobChecker::try_access()] Blob is public");
            return Ok(true);
        }

        let container_metadata = request_builder.container_metadata();
        tracing::debug!(uri = %container_metadata.uri().to_string().as_sensitive(), "[PublicBlobChecker::try_access()] Checking Blob Container public access");
        let response = self.http_client.clone().request(container_metadata)?.success();
        if response.is_ok() {
            tracing::info!("[PublicBlobChecker::try_access()] Container is public.");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Expects a Azure Blob RequestBuilder and will return a bool indicating whether the Blob referred to in the Request has public access.
    #[tracing::instrument(err, skip(self, blob_url))]
    pub fn is_public(&self, blob_url: &str) -> StreamResult<bool> {
        self.is_public_with_timeout(blob_url, chrono::Duration::minutes(1))
    }

    // an internal interface which we can specify the timeout for testing
    fn is_public_with_timeout(&self, blob_url: &str, timeout: Duration) -> StreamResult<bool> {
        // build a request builder without credential
        let request_builder = RequestBuilder::new(blob_url, Arc::new(()))?;
        let mut cache = self.cache.lock().map_err(|e| StreamError::Unknown(e.to_string(), None))?;
        let container_uri = request_builder.to_container_uri();
        let entry = cache.entry(container_uri.clone());
        match entry {
            Occupied(mut entry) => {
                tracing::debug!(container_uri = %container_uri.as_sensitive_ref(), "[PublicBlobChecker::is_public()] Cache hit");
                let (mut is_public, when_checked) = entry.get();
                // if is_public check was done more than 1 min (arbitrary length) we will check again in case status changed.
                if chrono::Utc::now() > when_checked.add(timeout) {
                    tracing::debug!("[PublicBlobChecker::is_public()] Cached public status expired");
                    is_public = self.try_access(&request_builder)?;
                    entry.insert((is_public, Utc::now()));
                }
                Ok(is_public)
            },
            Vacant(entry) => {
                tracing::debug!(container_uri = %container_uri.as_sensitive_ref(), "[PublicBlobChecker::is_public()] Cache miss");
                let is_public = self.try_access(&request_builder)?;
                entry.insert((is_public, Utc::now()));
                Ok(is_public)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use http::Method;
    use rslex_http_stream::FakeHttpClient;

    trait IntoPublicBlobChecker {
        fn into_public_blob_checker(self) -> PublicBlobChecker;
    }

    impl IntoPublicBlobChecker for FakeHttpClient {
        fn into_public_blob_checker(self) -> PublicBlobChecker {
            PublicBlobChecker::new(Arc::new(self))
        }
    }

    static PUBLIC_BLOB: &'static str = "https://dpreptestfiles.blob.core.windows.net/testfiles/crime0.csv";
    static PUBLIC_BLOB_2: &'static str = "https://dpreptestfiles.blob.core.windows.net/testfiles/population.csv";
    static PUBLIC_BLOB_3: &'static str = "https://dpreptestfiles.blob.core.windows.net/othercontainer/population.csv";
    static PUBLIC_BLOB_4: &'static str = "https://public.blob.core.windows.net/testfiles/population.csv";

    mod public_blob_checker {
        use super::*;

        #[test]
        fn public_blob_returns_true() {
            FakeHttpClient::default()
                .assert_request(|_, r| {
                    r.uri().should().equal_string(format!("{}?comp=metadata", PUBLIC_BLOB));
                    r.method().should().be(&Method::HEAD);
                    r.body().should().be(&b"".to_vec());
                })
                .into_public_blob_checker()
                .is_public(PUBLIC_BLOB)
                .should()
                .be_ok()
                .with_value(true);
        }

        #[test]
        fn public_blob_status_is_cached() {
            let public_blob_checker = FakeHttpClient::default()
                .assert_num_requests(1) // will be called only once
                .into_public_blob_checker();

            // first call
            public_blob_checker.is_public(PUBLIC_BLOB).should().be_ok().with_value(true);

            // second call
            public_blob_checker.is_public(PUBLIC_BLOB).should().be_ok().with_value(true);
        }

        #[test]
        fn same_container_different_path_results_in_same_cache_entry() {
            let public_blob_checker = FakeHttpClient::default().assert_num_requests(1).into_public_blob_checker();

            public_blob_checker.is_public(PUBLIC_BLOB).should().be_ok().with_value(true);

            public_blob_checker.is_public(PUBLIC_BLOB_2).should().be_ok().with_value(true);
        }

        #[test]
        fn different_container_different_account_results_in_different_entries() {
            let public_blob_checker = FakeHttpClient::default().assert_num_requests(3).into_public_blob_checker();

            let urls: [&str; 3] = [PUBLIC_BLOB, PUBLIC_BLOB_3, PUBLIC_BLOB_4];

            for url in urls.iter() {
                public_blob_checker.is_public(url).should().be_ok().with_value(true);
            }
        }

        #[test]
        fn public_blob_status_expires() {
            let public_blob_checker = FakeHttpClient::default()
                .assert_num_requests(2) // should be called twice
                .into_public_blob_checker();

            public_blob_checker.is_public(PUBLIC_BLOB).should().be_ok().with_value(true);

            // Sleep for just over a a second to expire cached token.
            std::thread::sleep(std::time::Duration::new(1, 0));

            // call twice, the second call should NOT use cache
            public_blob_checker
                // calling an internal version so we can verify the timeout. in this case we set timeout to 0 so basically disable caching
                .is_public_with_timeout(PUBLIC_BLOB, Duration::seconds(0))
                .should()
                .be_ok()
                .with_value(true);
        }
    }

    #[cfg(feature = "component_tests")]
    mod component_tests {
        use super::*;
        use rslex_http_stream::create_http_client;

        #[test]
        fn public_checker_returns_true_for_blob() {
            let checker = PublicBlobChecker::new(Arc::new(create_http_client().unwrap()));

            checker.is_public(PUBLIC_BLOB).should().be_ok().with_value(true);
        }

        #[test]
        fn public_checker_returns_true_for_blob_container() {
            let checker = PublicBlobChecker::new(Arc::new(create_http_client().unwrap()));

            checker
                .is_public("https://dpreptestfiles.blob.core.windows.net/testfiles/")
                .should()
                .be_ok()
                .with_value(true);
        }
    }
}
