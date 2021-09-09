use chrono::DateTime;
use http::Method;
use lazy_static::lazy_static;
use regex::Regex;
use rslex_core::{
    file_io::{StreamError, StreamResult},
    MapErrToUnknown, SessionProperties,
};
use rslex_http_stream::{
    new_request, ApplyCredential, AuthenticatedRequest, EncodedUrl, HeadRequest, ReadRequest, ReadSectionRequest, RequestWithCredential,
    Response, ResponseExt, SearchContext, SessionPropertiesExt,
};
use std::sync::Arc;
use url::form_urlencoded;

const HEADER_X_MS_VERSION: &str = "x-ms-version";
/// See https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-requests-to-azure-storage for the versioning info
const X_MS_VERSION: &str = "2019-02-02";
const HEADER_RANGE: &str = "Range";

/// A tool class handling building http request to work with Azure Blob Service.
#[derive(Clone)]
pub(super) struct RequestBuilder {
    // blob file information
    schema: String,
    host: String,
    share: String,
    path: String,

    credential: Arc<dyn ApplyCredential>,
}

lazy_static! {
    static ref URI_PATTERN: Regex =
        Regex::new(r"(?P<schema>https|http)://(?P<host>[^/]+)/(?P<share>[^/]+)/(?P<path>.*)").expect("this should never fail");
}

impl RequestBuilder {
    /// Build a RequestBuilder from uri and SyncRecord.
    /// Return InvalidInput if uri is ill-formatted.
    pub fn new(uri: &str, credential: Arc<dyn ApplyCredential>) -> StreamResult<RequestBuilder> {
        // parse uri, determining schema, host, container, path
        let caps = URI_PATTERN.captures(uri).ok_or(StreamError::InvalidInput {
            message: "Invalid Azure File Share URL.".to_string(),
            source: None,
        })?;
        let schema = caps["schema"].to_owned();
        let host = caps["host"].to_owned();
        let share = caps["share"].to_owned();
        let path = caps["path"].to_owned();

        Ok(RequestBuilder {
            schema,
            host,
            share,
            path,
            credential,
        })
    }

    pub async fn find_streams_async(&self, search_context: &SearchContext) -> StreamResult<AuthenticatedRequest> {
        const MAX_RESULTS: &str = "5000";
        let prefix = search_context.prefix(false).to_string();
        let (dir, pre) = match prefix.rfind('/') {
            Some(pos) => (&prefix[..pos], &prefix[pos + 1..]),
            None => ("", prefix.as_str()),
        };

        // construct the request
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}?{}",
                self.schema,
                self.host,
                self.share,
                EncodedUrl::from(dir),
                {
                    let mut query = form_urlencoded::Serializer::new(String::new());

                    query.append_pair("restype", "directory");
                    query.append_pair("comp", "list");
                    if !pre.is_empty() {
                        query.append_pair("prefix", pre);
                    }
                    query.append_pair("maxresults", MAX_RESULTS);

                    if search_context.is_one_pass() {
                        unimplemented!("recursive search is not supported for Azure FileShare");
                    }

                    if let Some(token) = search_context.continuation() {
                        query.append_pair("marker", token);
                    }

                    query.finish()
                }
            ))
            .header(HEADER_X_MS_VERSION, X_MS_VERSION)
            .body(Vec::<u8>::default())
            .map_err_to_unknown()?;

        Ok(request.with_credential(self.credential.clone()))
    }

    pub fn list_directory(&self, max_results: u16, continuation_token: Option<&str>) -> StreamResult<AuthenticatedRequest> {
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}?{}",
                self.schema,
                self.host,
                self.share,
                EncodedUrl::from(&self.path),
                {
                    let mut query = form_urlencoded::Serializer::new(String::new());

                    query.append_pair("restype", "directory");
                    query.append_pair("comp", "list");
                    query.append_pair("maxresults", &max_results.to_string());

                    if let Some(token) = continuation_token {
                        query.append_pair("marker", token);
                    }

                    query.finish()
                }
            ))
            .header(HEADER_X_MS_VERSION, X_MS_VERSION)
            .body(Vec::<u8>::default())
            .map_err_to_unknown()?;

        Ok(request.with_credential(self.credential.clone()))
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn path_to_uri<S: AsRef<str>>(&self, path: S) -> String {
        format!("{}://{}/{}/{}", self.schema, self.host, self.share, path.as_ref())
    }

    pub fn directory_metadata(&self) -> StreamResult<AuthenticatedRequest> {
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}?{}",
                self.schema,
                self.host,
                self.share,
                EncodedUrl::from(&self.path),
                {
                    let mut query = form_urlencoded::Serializer::new(String::new());
                    query.append_pair("restype", "directory");
                    query.finish()
                }
            ))
            .header(HEADER_X_MS_VERSION, X_MS_VERSION)
            .method(Method::HEAD)
            .body(Vec::<u8>::default())
            .map_err_to_unknown()?;

        Ok(request.with_credential(self.credential.clone()))
    }
}

impl HeadRequest for RequestBuilder {
    /// Build request to get_opener, it will simply query the file info (without reading the file)
    fn head(&self) -> AuthenticatedRequest {
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}",
                self.schema,
                self.host,
                self.share,
                EncodedUrl::from(&self.path)
            ))
            .header(HEADER_X_MS_VERSION, "2019-02-02")
            .method(Method::HEAD)
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    fn parse_response(response: Response, session_properties: &mut SessionProperties) -> StreamResult<()> {
        let response = response.success()?;

        let headers = response.headers();
        let size: u64 = headers
            .get("Content-Length")
            .ok_or(StreamError::Unknown("Content-Length missing from HTTP header".to_owned(), None))?
            .to_str()
            .map_err_to_unknown()?
            .parse::<u64>()
            .map_err_to_unknown()?;
        session_properties.set_size(size);

        if let Some(created_time) = headers
            .get("x-ms-file-creation-time")
            .map::<StreamResult<_>, _>(|s| {
                Ok(DateTime::parse_from_rfc3339(s.to_str().map_err_to_unknown()?)
                    .map_err_to_unknown()?
                    .into())
            })
            .transpose()?
        {
            session_properties.set_created_time(created_time);
        }

        if let Some(modified_time) = headers
            .get("Last-Modified")
            .map::<StreamResult<_>, _>(|s| {
                Ok(DateTime::parse_from_rfc2822(s.to_str().map_err_to_unknown()?)
                    .map_err_to_unknown()?
                    .into())
            })
            .transpose()?
        {
            session_properties.set_modified_time(modified_time);
        }

        // always seekable
        session_properties.set_is_seekable(true);

        Ok(())
    }

    fn default_is_seekable() -> bool {
        true
    }
}

impl ReadSectionRequest for RequestBuilder {
    /// in case of seekable stream (with latest version all blob streams are seekable)
    /// read section of the stream
    fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
        assert!(
            length > 0,
            "[file_stream_handler::request_builder::ReadSectionRequest::read_section] Attempt to send range request of a zero length"
        );

        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}",
                self.schema,
                self.host,
                self.share,
                EncodedUrl::from(&self.path)
            ))
            .method(Method::GET)
            .header(HEADER_RANGE, format!("bytes={}-{}", offset, offset + length - 1))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }
}

impl ReadRequest for RequestBuilder {
    /// read blob from storage
    fn read(&self) -> AuthenticatedRequest {
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}",
                self.schema,
                self.host,
                self.share,
                EncodedUrl::from(&self.path)
            ))
            .method(Method::GET)
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
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
            async move { self.find_streams_async(&sc).await }.wait()?
        }
    }

    ///
    /// --- RequestBuilder tests
    #[test]
    fn find_streams_return_request_with_list_file() {
        let uri = "https://someaccount.file.core.windows.net/someshare/somefile.csv";
        let search_context: SearchContext = "somefile.csv".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find sterams should succeed")
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result.uri().should().equal_string(
                    "https://someaccount.file.core.windows.net/someshare/?restype=directory&comp=list&prefix=somefile.csv&maxresults=5000",
                );
            });
    }

    #[test]
    fn find_streams_uri_with_credential_will_apply() {
        let uri = "https://someaccount.file.core.windows.net/someshare/somefile.csv";
        let search_context: SearchContext = "somefile.csv".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find sterams should succeed")
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                FakeCredential::verify(&result);
            });
    }

    #[test]
    fn find_streams_uri_end_with_slash_treat_as_folder() {
        let uri = "https://someaccount.file.core.windows.net/someshare/folder/";
        let search_context: SearchContext = "folder/".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find sterams should succeed")
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result
                    .uri()
                    .should()
                    .equal_string("https://someaccount.file.core.windows.net/someshare/folder?restype=directory&comp=list&maxresults=5000");
            });
    }

    #[test]
    fn find_streams_uri_with_star_match_any_file() {
        let uri = "https://someaccount.file.core.windows.net/someshare/a*b";
        let search_context: SearchContext = "a*b".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find sterams should succeed")
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result.uri().should().equal_string(
                    "https://someaccount.file.core.windows.net/someshare/?restype=directory&comp=list&prefix=a&maxresults=5000",
                );
            });
    }

    #[test]
    fn find_streams_uri_with_double_star_match_any_folder() {
        let uri = "https://someaccount.file.core.windows.net/someshare/a/c**b";
        let search_context: SearchContext = "a/c**b".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find sterams should succeed")
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result.uri().should().equal_string(
                    "https://someaccount.file.core.windows.net/someshare/a?restype=directory&comp=list&prefix=c&maxresults=5000",
                );
            });
    }

    #[test]
    fn find_streams_with_marker_add_to_uri() {
        let uri = "https://someaccount.file.core.windows.net/someshare/somefile";
        let search_context: SearchContext = "somefile".parse::<SearchContext>().unwrap().with_continuation("mmm");

        RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find sterams should succeed")
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result.uri().should().equal_string(
                    "https://someaccount.file.core.windows.net/someshare/?restype=directory&comp=list&prefix=somefile&maxresults=5000&marker=mmm",
                );
            });
    }

    #[test]
    fn find_stream_with_unicode_directory() {
        let uri = "https://someaccount.file.core.windows.net/someshare/";
        let search_context: SearchContext = "文件夹/*.txt".parse::<SearchContext>().unwrap();

        RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find sterams should succeed")
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result.uri().should().equal_string(
                    "https://someaccount.file.core.windows.net/someshare/%E6%96%87%E4%BB%B6%E5%A4%B9?restype=directory&comp=list&maxresults=5000",
                );
            });
    }

    #[test]
    fn find_streams_with_marker_will_encode() {
        let uri = "https://someaccount.file.core.windows.net/someshare/somefile";
        let search_context: SearchContext = "somefile".parse::<SearchContext>().unwrap().with_continuation("a=b");

        RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find sterams should succeed")
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result.uri().should().equal_string(
                    "https://someaccount.file.core.windows.net/someshare/?restype=directory&comp=list&prefix=somefile&maxresults=5000&marker=a%3Db",
                );
            });
    }

    #[test]
    fn new_request_builder_with_invalid_url_returns_error() {
        let invalid_url = "https://somewhere.com/nocontainer.csv";

        RequestBuilder::new(invalid_url, Arc::new(()))
            .should()
            .be_err()
            .with_value(StreamError::InvalidInput {
                message: "Invalid Azure File Share URL.".to_string(),
                source: None,
            });
    }

    #[test]
    fn get_opener_will_return_head_request() {
        let uri = "https://someaccount.file.core.windows.net/someshare/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .expect("build request builder should succeed")
            .head()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::HEAD);
                result
                    .uri()
                    .should()
                    .equal_string("https://someaccount.file.core.windows.net/someshare/somefile.csv");
            });
    }

    #[test]
    fn file_url_could_take_different_host() {
        let uri = "https://some.different.host/someshare/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .expect("build request builder should succeed")
            .head()
            .uri()
            .should()
            .equal_string("https://some.different.host/someshare/somefile.csv");
    }

    #[test]
    fn get_opener_with_credential_will_apply() {
        let uri = "https://someaccount.file.core.windows.net/someshare/somefile.csv";

        RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .expect("build request builder should succeed")
            .head()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                FakeCredential::verify(&result);
            });
    }

    #[test]
    fn read_will_return_get_request() {
        let uri = "https://someaccount.file.core.windows.net/someshare/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .expect("build request builder should succeed")
            .read()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result
                    .uri()
                    .should()
                    .equal_string("https://someaccount.file.core.windows.net/someshare/somefile.csv");
            });
    }

    #[test]
    fn read_with_credential_will_apply() {
        let uri = "https://someaccount.file.core.windows.net/someshare/somefile.csv";

        RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .expect("build request builder should succeed")
            .read()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                FakeCredential::verify(&result);
            });
    }

    #[test]
    fn read_section_will_return_get_request() {
        let uri = "https://someaccount.file.core.windows.net/someshare/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .expect("build request builder should succeed")
            .read_section(10, 20)
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result
                    .uri()
                    .should()
                    .equal_string("https://someaccount.file.core.windows.net/someshare/somefile.csv");
                result.headers()["Range"].to_ref().should().equal(&"bytes=10-29");
            });
    }

    #[test]
    #[should_panic]
    fn read_section_for_zero_length_should_panic() {
        let uri = "https://someaccount.file.core.windows.net/someshare/somefile.csv";

        // this should panic
        let _ = RequestBuilder::new(uri, Arc::new(()))
            .expect("build request builder should succeed")
            .read_section(10, 0)
            .into_request();
    }

    #[test]
    fn read_section_with_credential_will_apply() {
        let uri = "https://someaccount.file.core.windows.net/someshare/somefile.csv";
        RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .expect("build request builder should succeed")
            .read_section(10, 20)
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                FakeCredential::verify(&result);
            });
    }

    #[test]
    fn list_directory_without_continuation_token() {
        const BATCH_SIZE: u16 = 1000;
        let uri = "https://someaccount.file.core.windows.net/someshare/somefolder";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let request = builder
            .list_directory(BATCH_SIZE, None)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request.uri().should().equal_string(format!(
            "https://someaccount.file.core.windows.net/someshare/somefolder?restype=directory&comp=list&maxresults={}",
            BATCH_SIZE
        ));
    }

    #[test]
    fn list_directory_with_continuation_token() {
        const BATCH_SIZE: u16 = 1000;
        let continuation_token = "token";
        let uri = "https://someaccount.file.core.windows.net/someshare/somefolder";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let request = builder
            .list_directory(BATCH_SIZE, Some(continuation_token))
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request.uri().should().equal_string(format!(
            "https://someaccount.file.core.windows.net/someshare/somefolder?restype=directory&comp=list&maxresults={}&marker={}",
            BATCH_SIZE, continuation_token
        ));
    }
}
