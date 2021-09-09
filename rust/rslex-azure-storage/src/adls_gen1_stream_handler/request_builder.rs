use super::{file_dto::FileStatus, HANDLER_TYPE};
use chrono::{TimeZone, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use rslex_core::{
    file_io::{StreamError, StreamResult},
    SessionProperties, StreamInfo, SyncRecord,
};
use rslex_http_stream::{
    new_request, AppendWriteRequest, ApplyCredential, AuthenticatedRequest, CreateDirectoryRequest, EncodedUrl, HeadRequest, Method,
    ParallelWriteRequest, ReadRequest, ReadSectionRequest, RemoveRequest, RequestWithCredential, Response, ResponseExt, SearchContext,
    SessionPropertiesExt,
};
use std::sync::Arc;
use uuid::Uuid;

/// This pinned version is coming from here
/// https://github.com/Azure/azure-data-lake-store-python/blob/4318349cb2aefad9397b1e0590ded6edccffef25/azure/datalake/store/core.py#L56
/// to enable "listSize" and "listAfter" query params for iterative listing.
/// For the time when this code was writen this version value was not described in the "easy to find"
/// adls gen 1 docs but it is used in adls gen 1 python SDK and it works.
const API_VERSION: &'static str = "2018-09-01";
const HEADER_CONTENT_LENGTH: &str = "Content-Length";
const HEADER_CONTENT_TYPE: &str = "Content-Type";
const HEADER_CONTENT_TYPE_JSON: &str = "application/json";

// Limited by MSCONCAT api call for adls gen 1
const MAX_BLOCK_COUNT: usize = 1999;
// Limit for single upload with CREATE api call
const MAX_BLOCK_SIZE: usize = 30000000;

/// A tool class handling building http request to work with Azure Data Lake Service.
#[derive(Clone)]
pub(super) struct RequestBuilder {
    // data lake file information
    host: String,
    // the path
    path: String,
    // extracted credential from SyncRecord
    credential: Arc<dyn ApplyCredential>,
    // Is used to upload files by chunks to temporarily hold individual chunks.
    // Unique value generated on request builder creation.
    tmp_suffix: String,
}

lazy_static! {
    static ref URI_PATTERN: Regex = Regex::new(r"adl://(?P<host>[^/]+/?)(?P<path>.*)").expect("this should never fail");
}

impl RequestBuilder {
    pub fn new(uri: &str, credential: Arc<dyn ApplyCredential>) -> StreamResult<RequestBuilder> {
        let caps = URI_PATTERN.captures(uri).ok_or(StreamError::InvalidInput {
            message: "Invalid ADLS Gen 1 URL.".to_string(),
            source: None,
        })?;
        let host = caps["host"].trim_end_matches("/").to_owned();
        let path = caps["path"].to_owned();

        Ok(RequestBuilder {
            host,
            path,
            credential,
            tmp_suffix: Uuid::new_v4().to_string(),
        })
    }

    pub fn path(&self) -> &'_ str {
        &self.path
    }

    pub fn uri(&self) -> String {
        self.path_to_uri(self.path())
    }

    pub fn path_to_uri<S: AsRef<str>>(&self, path: S) -> String {
        format!("adl://{}/{}", self.host, path.as_ref())
    }

    pub async fn find_streams_async(&self, search_context: &SearchContext) -> AuthenticatedRequest {
        // construct the request
        let request = new_request()
            .uri(format!(
                "https://{}/webhdfs/v1/{}?op=LISTSTATUS",
                self.host,
                // we encode the path in case there are unicode characters, but keep "/" unchanged
                EncodedUrl::from(search_context.prefix(true).as_ref())
            ))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    pub fn list_directory(&self, max_results: u16, continuation_token: Option<&str>) -> AuthenticatedRequest {
        let continuation_param = if let Some(continuation_token) = continuation_token {
            format!("&listAfter={}", EncodedUrl::from(continuation_token))
        } else {
            String::default()
        };

        let request = new_request()
            .uri(format!(
                "https://{}/webhdfs/v1/{}?op=LISTSTATUS&listSize={}{}&api-version={}",
                self.host,
                // we encode the path in case there are unicode characters, but keep "/" unchanged
                EncodedUrl::from(&self.path),
                max_results,
                continuation_param,
                API_VERSION
            ))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    pub fn append(&self, data: Vec<u8>) -> AuthenticatedRequest {
        let request = new_request()
            .method(Method::POST)
            .uri(format!(
                "https://{}/webhdfs/v1/{}?op=APPEND&append=true",
                self.host,
                EncodedUrl::from(&self.path)
            ))
            .body(data)
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    pub fn create(&self, data: Vec<u8>, offset: Option<u64>, tmp_suffix: Option<&str>) -> AuthenticatedRequest {
        let path = match (offset, tmp_suffix) {
            (Some(offset), Some(tmp_suffix)) => format!("{}_{}/{}", &self.path, tmp_suffix, offset),
            (Some(offset), None) => format!("{}.{}", &self.path, offset),
            (None, Some(tmp_suffix)) => format!("{}_{}", &self.path, tmp_suffix),
            (None, None) => self.path.clone(),
        };

        let request = new_request()
            .method(Method::PUT)
            .uri(format!(
                "https://{}/webhdfs/v1/{}?op=CREATE&overwrite=true&write=true",
                self.host,
                EncodedUrl::from(&path)
            ))
            .body(data)
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    pub fn delete(&self, recursive: bool) -> AuthenticatedRequest {
        let request = new_request()
            .method(Method::DELETE)
            .uri(format!(
                "https://{}/webhdfs/v1/{}?op=DELETE&recursive={}",
                self.host,
                EncodedUrl::from(&self.path),
                recursive
            ))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    pub fn concat(&self, offsets: Vec<u64>, tmp_suffix: Option<&str>) -> AuthenticatedRequest {
        let sources = if let Some(tmp_suffix) = tmp_suffix {
            offsets
                .iter()
                .map(|&x| format!("\"/{}_{}/{}\"", &self.path, tmp_suffix, x))
                .collect::<Vec<String>>()
        } else {
            offsets
                .iter()
                .map(|&x| format!("\"/{}.{}\"", &self.path, x))
                .collect::<Vec<String>>()
        };

        let sources = format!("{{\"sources\": [{}]}}", sources.join(","));

        let body = sources.as_bytes().to_vec();

        let request = new_request()
            .method(Method::POST)
            .uri(format!(
                "https://{}/webhdfs/v1/{}?op=MSCONCAT&api-version={}",
                self.host,
                EncodedUrl::from(&self.path),
                API_VERSION
            ))
            .header(HEADER_CONTENT_LENGTH, body.len())
            .header(HEADER_CONTENT_TYPE, HEADER_CONTENT_TYPE_JSON)
            .body(body)
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }
}

impl HeadRequest for RequestBuilder {
    fn head(&self) -> AuthenticatedRequest {
        let url = format!("https://{}/webhdfs/v1/{}?op=GETFILESTATUS", self.host, EncodedUrl::from(&self.path));
        let request = new_request()
            .uri(url)
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    fn parse_response(response: Response, session_properties: &mut SessionProperties) -> StreamResult<()> {
        let response = response.success()?;
        let file: FileStatus = response.into_string()?.parse()?;
        session_properties.set_modified_time(Utc.timestamp_millis(file.modification_time));
        session_properties.set_size(file.length);
        session_properties.set_is_seekable(true);

        Ok(())
    }

    fn default_is_seekable() -> bool {
        true
    }
}

impl ReadRequest for RequestBuilder {
    fn read(&self) -> AuthenticatedRequest {
        let request = new_request()
            .uri(format!(
                "https://{}/webhdfs/v1/{}?op=OPEN&read=true",
                self.host,
                EncodedUrl::from(&self.path)
            ))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }
}

impl ReadSectionRequest for RequestBuilder {
    fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
        let request = new_request()
            .uri(format!(
                "https://{}/webhdfs/v1/{}?op=OPEN&offset={}&length={}&read=true",
                self.host,
                EncodedUrl::from(&self.path),
                offset,
                length
            ))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }
}

impl ParallelWriteRequest for RequestBuilder {
    fn create(&self) -> AuthenticatedRequest {
        self.create(vec![], None, None)
    }

    fn write_block(&self, block_idx: usize, _position: usize, data: &[u8]) -> AuthenticatedRequest {
        self.create(data.to_vec(), Some(block_idx as u64), Some(&self.tmp_suffix))
    }

    fn complete(&self, block_count: usize, _position: usize) -> AuthenticatedRequest {
        self.concat((0..block_count as u64).collect(), Some(&self.tmp_suffix))
    }

    fn stream_info(&self) -> StreamInfo {
        // TODO: pass in the credential arguments - https://msdata.visualstudio.com/Vienna/_workitems/edit/931126/
        StreamInfo::new(HANDLER_TYPE, self.uri(), SyncRecord::empty())
    }

    fn max_block_count(&self) -> usize {
        MAX_BLOCK_COUNT
    }

    fn max_block_size(&self) -> usize {
        MAX_BLOCK_SIZE
    }
}

impl AppendWriteRequest for RequestBuilder {
    fn create(&self) -> AuthenticatedRequest {
        self.create(vec![], None, None)
    }

    fn write(&self, _position: usize, data: &[u8]) -> AuthenticatedRequest {
        self.append(data.to_vec())
    }
}

impl CreateDirectoryRequest for RequestBuilder {
    fn create_directory(&self) -> AuthenticatedRequest {
        let request = new_request()
            .method(Method::PUT)
            .uri(format!(
                "https://{}/webhdfs/v1/{}?op=MKDIRS&api-version={}",
                self.host,
                EncodedUrl::from(&self.path),
                API_VERSION
            ))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }
}

impl RemoveRequest for RequestBuilder {
    fn remove(&self) -> AuthenticatedRequest {
        self.delete(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use rslex_http_stream::{FakeCredential, Wait};

    //noinspection DuplicatedCode
    impl RequestBuilder {
        fn find_streams(self, search_context: &SearchContext) -> StreamResult<AuthenticatedRequest> {
            let sc = search_context.clone();
            async move { Ok::<_, StreamError>(self.find_streams_async(&sc).await) }.wait()?
        }
    }

    #[test]
    fn find_streams_with_single_file_return_single_file() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = builder.path().parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/somefile.csv?op=LISTSTATUS");
    }

    #[test]
    fn find_streams_to_the_root_of_the_storage() {
        let uri = "adl://someaccount.azuredatalakestore.net";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = builder.path().parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/?op=LISTSTATUS");
    }

    #[test]
    fn find_streams_with_credential_apply() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        let builder = RequestBuilder::new(uri, Arc::new(FakeCredential::new())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = builder.path().parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        FakeCredential::verify(&request);
    }

    #[test]
    fn find_streams_with_single_level_pattern() {
        // NOTE: as we are providing the SearchContext to the find_stream function, the uri is not used here
        // but put in the uri to give context of tests
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/a*b";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = builder.path().parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/?op=LISTSTATUS");
    }

    #[test]
    fn find_streams_with_two_single_level_patterns() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/a*b/*c";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = builder.path().parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/?op=LISTSTATUS");
    }

    #[test]
    fn find_streams_with_two_single_level_patterns_on_second_single_level_pattern() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/a*b/*c";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");

        // in this case, the search context is not parsed from URI, but from the result of first search
        // this is exactly the SearchContext returned in previous test
        let search_context: SearchContext = "somefolder/afb/*c".parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/afb/?op=LISTSTATUS");
    }

    #[test]
    fn find_streams_with_single_level_pattern_and_multi_level_literal() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/a*b/c/d/e.csv";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = builder.path().parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/?op=LISTSTATUS");
    }

    #[test]
    fn find_streams_with_multi_level_pattern_and_single_level_literal() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/**/c.csv";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = builder.path().parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/?op=LISTSTATUS");
    }

    #[test]
    fn find_streams_with_multi_level_pattern_and_multi_level_literal() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/**/b/c.csv";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = builder.path().parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/?op=LISTSTATUS");
    }

    #[test]
    fn find_streams_with_multi_level_pattern_and_single_level_literal_and_single_level_pattern() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/**/b/c*.csv";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = builder.path().parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/?op=LISTSTATUS");
    }

    #[test]
    fn find_streams_with_multi_level_pattern_and_single_level_pattern() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/**/c*.csv";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = builder.path().parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/?op=LISTSTATUS");
    }

    #[test]
    fn find_streams_with_multi_level_pattern_and_single_level_pattern_and_single_level_literal() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/**/c*/e.csv";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = builder.path().parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/?op=LISTSTATUS");
    }

    #[test]
    fn find_streams_with_multi_level_pattern() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/**";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = builder.path().parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/?op=LISTSTATUS");
    }

    #[test]
    fn find_streams_with_unicode_path() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = "文件夹/*.txt".parse().unwrap();
        let request = builder
            .find_streams(&search_context)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/%E6%96%87%E4%BB%B6%E5%A4%B9/?op=LISTSTATUS");
    }

    #[test]
    fn get_opener_with_single_file() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let request = builder.head().into_request().expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request
            .uri()
            .should()
            .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/somefile.csv?op=GETFILESTATUS");
    }

    #[test]
    fn url_with_different_host() {
        let uri = "adl://some.different.host/somefolder/somefile.csv";
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .head()
            .uri()
            .should()
            .equal_string("https://some.different.host/webhdfs/v1/somefolder/somefile.csv?op=GETFILESTATUS");
    }

    #[test]
    fn get_opener_with_credential_will_apply() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";

        let result = RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .expect("build request builder should succeed")
            .head()
            .into_request()
            .expect("unwrap created request should succeed");

        FakeCredential::verify(&result);
    }

    #[test]
    fn read_section_will_return_get_request() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        let request = RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .read_section(10, 20)
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request.uri().should().equal_string(
            "https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/somefile.csv?op=OPEN&offset=10&length=20&read=true",
        );
    }

    #[test]
    fn read_section_with_credential_will_apply() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        let result = RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .expect("creating RequestBuilder should succeed")
            .read_section(10, 20)
            .into_request()
            .expect("unwrap created request should succeed");

        FakeCredential::verify(&result);
    }

    #[test]
    fn create_will_return_put_request_with_expected_uri() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .create(vec![1, 2, 3, 4], None, None)
            .into_request()
            .expect("unwrap created request should succeed")
            .should()
            .pass(|r| {
                r.method().should().be(&Method::PUT);
                r.uri().should().equal_string(
                    "https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/somefile.csv?op=CREATE&overwrite=true&write=true",
                );
            });
    }

    #[test]
    fn create_with_optional_offset_will_return_put_request_with_expected_uri() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .create(vec![1, 2, 3, 4], Some(0), None)
            .into_request()
            .expect("unwrap created request should succeed")
            .should()
            .pass(|r| {
                r.method().should().be(&Method::PUT);
                r.uri().should().equal_string(
                    "https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/somefile.csv.0?op=CREATE&overwrite=true&write=true",
                );
            });
    }

    #[test]
    fn create_with_tmp_suffix_will_return_put_request_with_expected_uri() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .create(vec![1, 2, 3, 4], None, Some("tmp_suffix"))
            .into_request()
            .expect("unwrap created request should succeed")
            .should()
            .pass(|r| {
                r.method().should().be(&Method::PUT);
                r.uri().should().equal_string(
                    "https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/somefile.csv_tmp_suffix?op=CREATE&overwrite=true&write=true",
                );
            });
    }

    #[test]
    fn create_with_optional_offset_and_tmp_suffix_will_return_put_request_with_expected_uri() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .create(vec![1, 2, 3, 4], Some(0), Some("tmp_suffix"))
            .into_request()
            .expect("unwrap created request should succeed")
            .should()
            .pass(|r| {
                r.method().should().be(&Method::PUT);
                r.uri().should().equal_string(
                    "https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/somefile.csv_tmp_suffix/0?op=CREATE&overwrite=true&write=true",
                );
            });
    }

    #[test]
    fn append_will_return_post_request_with_expected_uri() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .append(vec![1, 2, 3, 4])
            .into_request()
            .expect("unwrap created request should succeed")
            .should()
            .pass(|r| {
                r.method().should().be(&Method::POST);
                r.uri()
                    .should()
                    .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/somefile.csv?op=APPEND&append=true");
            });
    }

    #[test]
    fn concat_will_return_post_request_with_expected_uri() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        let offsets = vec![0u64, 4, 8];
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .concat(offsets.clone(), None)
            .into_request()
            .expect("unwrap concat request should succeed")
            .should()
            .pass(|r| {
                r.method().should().be(&Method::POST);
                r.uri().should().equal_string(format!(
                    "https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/somefile.csv?op=MSCONCAT&api-version={}",
                    API_VERSION
                ));
                std::str::from_utf8(r.body())
                    .should()
                    .be_ok()
                    .which_value()
                    .should()
                    .equal(&format!(
                        "{{\"sources\": [{}]}}",
                        offsets
                            .iter()
                            .map(|&x| format!("\"/somefolder/somefile.csv.{}\"", x))
                            .collect::<Vec<String>>()
                            .join(",")
                    ));
                r.headers()
                    .get(HEADER_CONTENT_LENGTH)
                    .should()
                    .be_some()
                    .which_value()
                    .to_str()
                    .should()
                    .be_ok()
                    .which_value()
                    .should()
                    .equal(&format!("{}", r.body().len()));
                r.headers()
                    .get(HEADER_CONTENT_TYPE)
                    .should()
                    .be_some()
                    .which_value()
                    .to_str()
                    .should()
                    .be_ok()
                    .which_value()
                    .should()
                    .equal(&HEADER_CONTENT_TYPE_JSON);
            });
    }

    #[test]
    fn concat_with_tmp_suffix_will_return_post_request_with_expected_uri() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        let offsets = vec![0u64, 4, 8];
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .concat(offsets.clone(), Some("tmp_suffix"))
            .into_request()
            .expect("unwrap concat request should succeed")
            .should()
            .pass(|r| {
                r.method().should().be(&Method::POST);
                r.uri().should().equal_string(format!(
                    "https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/somefile.csv?op=MSCONCAT&api-version={}",
                    API_VERSION
                ));
                std::str::from_utf8(r.body())
                    .should()
                    .be_ok()
                    .which_value()
                    .should()
                    .equal(&format!(
                        "{{\"sources\": [{}]}}",
                        offsets
                            .iter()
                            .map(|&x| format!("\"/somefolder/somefile.csv_tmp_suffix/{}\"", x))
                            .collect::<Vec<String>>()
                            .join(",")
                    ));
                r.headers()
                    .get(HEADER_CONTENT_LENGTH)
                    .should()
                    .be_some()
                    .which_value()
                    .to_str()
                    .should()
                    .be_ok()
                    .which_value()
                    .should()
                    .equal(&format!("{}", r.body().len()));
                r.headers()
                    .get(HEADER_CONTENT_TYPE)
                    .should()
                    .be_some()
                    .which_value()
                    .to_str()
                    .should()
                    .be_ok()
                    .which_value()
                    .should()
                    .equal(&HEADER_CONTENT_TYPE_JSON);
            });
    }

    #[test]
    fn delete_will_return_delete_request_with_expected_uri() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder";
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .delete(false)
            .into_request()
            .expect("unwrap created request should succeed")
            .should()
            .pass(|r| {
                r.method().should().be(&Method::DELETE);
                r.uri()
                    .should()
                    .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder?op=DELETE&recursive=false");
            });
    }

    #[test]
    fn list_directory_without_continuation_token() {
        const BATCH_SIZE: u16 = 1000;
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/subfolder";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let request = builder
            .list_directory(BATCH_SIZE, None)
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request.uri().should().equal_string(format!(
            "https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/subfolder?op=LISTSTATUS&listSize={}&api-version={}",
            BATCH_SIZE, API_VERSION
        ));
    }

    #[test]
    fn list_directory_with_continuation_token() {
        const BATCH_SIZE: u16 = 1000;
        let continuation_token = "token";
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/subfolder";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let request = builder
            .list_directory(BATCH_SIZE, Some(continuation_token))
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request.uri().should().equal_string(format!(
            "https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/subfolder?op=LISTSTATUS&listSize={}&listAfter={}&api-version={}",
            BATCH_SIZE, continuation_token, API_VERSION
        ));
    }

    #[test]
    fn max_block_count_should_equal_to_storage_limit() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder";

        // MSCONCAT operation on adls gen 1 only allows 1999 blocks per request.
        RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .unwrap()
            .max_block_count()
            .should()
            .be(1999);
    }

    #[test]
    fn max_block_size_should_equal_to_storage_limit() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder";

        // CREATE operation on adls gen 1 only allows 30000000 bytes per request.
        RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .unwrap()
            .max_block_size()
            .should()
            .be(30000000);
    }
}
