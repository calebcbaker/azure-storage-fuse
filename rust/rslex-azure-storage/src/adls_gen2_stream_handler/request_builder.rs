use super::HANDLER_TYPE;
use crate::adls_gen2_stream_handler::path_dto::Path;
use http::Method;
use lazy_static::lazy_static;
use regex::Regex;
use rslex_core::{
    file_io::{StreamError, StreamResult},
    MapErrToUnknown, SessionProperties, StreamInfo, SyncRecord,
};
use rslex_http_stream::{
    new_request, AppendWriteRequest, ApplyCredential, AuthenticatedRequest, CreateDirectoryRequest, EncodedUrl, HeadRequest,
    ParallelWriteRequest, ReadRequest, ReadSectionRequest, RemoveRequest, RequestWithCredential, Response, ResponseExt, SearchContext,
    SessionPropertiesExt,
};
use std::{
    fmt::{Display, Formatter, Result},
    sync::Arc,
};
use url::form_urlencoded;

/// A tool class handling building http request to work with Azure Blob Service.
#[derive(Clone)]
pub(super) struct RequestBuilder {
    // adls gen2 account
    host: String,

    // adls gen2 file system
    file_system: String,

    // the path
    path: String,

    // extracted credential from SyncRecord
    credential: Arc<dyn ApplyCredential>,
}

lazy_static! {
    static ref URI_PATTERN: Regex =
        Regex::new(r"https://(?P<host>[^/]+)/(?P<file_system>[^/]*/?)(?P<path>.*)").expect("this should never fail");
}

#[derive(Debug)]
pub enum FileSystemResourceType {
    File,
    #[allow(dead_code)]
    Directory,
}

impl Display for FileSystemResourceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{:?}", self)
    }
}

impl RequestBuilder {
    pub fn new(uri: &str, credential: Arc<dyn ApplyCredential>) -> StreamResult<RequestBuilder> {
        let caps = URI_PATTERN.captures(uri).ok_or(StreamError::InvalidInput {
            message: "Invalid ADLS Gen 2 URL.".to_string(),
            source: None,
        })?;
        let host = caps["host"].to_owned();
        let file_system = caps["file_system"].trim_end_matches("/").to_owned();
        let path = caps["path"].to_owned();

        Ok(RequestBuilder {
            host,
            file_system,
            path,
            credential,
        })
    }

    fn uri(&self) -> String {
        format!("https://{}/{}/{}", self.host, self.file_system, self.path)
    }

    pub fn path(&self) -> &'_ str {
        &self.path
    }

    pub fn path_to_uri<S: AsRef<str>>(&self, path: S) -> String {
        format!("https://{}/{}/{}", self.host, self.file_system, path.as_ref())
    }

    pub async fn find_streams_async(&self, search_context: &SearchContext) -> StreamResult<AuthenticatedRequest> {
        const MAX_RESULTS: u32 = 5000;
        let uri = format!("https://{}/{}?{}", self.host, self.file_system, {
            let mut query = form_urlencoded::Serializer::new(String::new());

            query
                .append_pair("resource", "filesystem")
                .append_pair("maxResults", &MAX_RESULTS.to_string());

            if search_context.is_one_pass() {
                // uri += "&recursive=true";
                query.append_pair("recursive", "true");
            } else {
                // uri += "&recursive=false";
                query.append_pair("recursive", "false");
            }

            let prefix = search_context.prefix(true);
            if prefix != "" {
                query.append_pair("directory", prefix.as_ref());
            }

            if let Some(token) = search_context.continuation() {
                query.append_pair("continuation", token);
            };

            query.finish()
        });

        // construct the request
        let request = new_request().uri(uri).body(Vec::<u8>::default()).map_err_to_unknown()?;

        Ok(request.with_credential(self.credential.clone()))
    }

    pub fn list_directory(&self, max_results: u16, continuation_token: Option<&str>) -> StreamResult<AuthenticatedRequest> {
        // construct the request
        let uri = format!("https://{}/{}?{}", self.host, self.file_system, {
            let mut query = form_urlencoded::Serializer::new(String::new());

            query
                .append_pair("resource", "filesystem")
                .append_pair("maxResults", &max_results.to_string())
                .append_pair("recursive", "false")
                .append_pair("directory", &self.path);

            if let Some(token) = continuation_token {
                query.append_pair("continuation", token);
            };

            query.finish()
        });

        let request = new_request().uri(uri).body(Vec::<u8>::default()).map_err_to_unknown()?;

        Ok(request.with_credential(self.credential.clone()))
    }

    pub fn create(&self, resource_type: FileSystemResourceType) -> AuthenticatedRequest {
        let request = new_request()
            .method(Method::PUT)
            .uri(format!(
                "https://{}/{}/{}?resource={}",
                self.host,
                self.file_system,
                EncodedUrl::from(&self.path),
                resource_type.to_string().to_lowercase()
            ))
            .header("Content-Length", 0)
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    pub fn delete(&self, recursive: bool) -> AuthenticatedRequest {
        let request = new_request()
            .method(Method::DELETE)
            .uri(format!(
                "https://{}/{}/{}?recursive={}",
                self.host,
                self.file_system,
                EncodedUrl::from(&self.path),
                recursive
            ))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    pub fn append(&self, data: Vec<u8>, offset: usize) -> AuthenticatedRequest {
        let request = new_request()
            .method(Method::PATCH)
            .uri(format!(
                "https://{}/{}/{}?action=append&position={}",
                self.host,
                self.file_system,
                EncodedUrl::from(&self.path),
                offset
            ))
            .header("Content-Length", data.len())
            .body(data)
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    pub fn flush(&self, len: usize) -> AuthenticatedRequest {
        let request = new_request()
            .method(Method::PATCH)
            .uri(format!(
                "https://{}/{}/{}?action=flush&position={}",
                self.host,
                self.file_system,
                EncodedUrl::from(&self.path),
                len
            ))
            .header("Content-Length", 0)
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }
}

impl HeadRequest for RequestBuilder {
    fn head(&self) -> AuthenticatedRequest {
        let request = new_request()
            .method(Method::HEAD)
            .uri(format!(
                "https://{}/{}/{}",
                self.host,
                self.file_system,
                EncodedUrl::from(&self.path)
            ))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    fn parse_response(response: Response, session_properties: &mut SessionProperties) -> StreamResult<()> {
        let response = response.success()?;
        let headers = response.headers();
        let path = Path::try_from_response(String::default(), &response)?;

        if path.is_directory {
            return Err(StreamError::InvalidInput {
                message: "Expected a file but resource is a folder.".to_string(),
                source: None,
            });
        }

        session_properties.set_size(path.content_length);

        if path.last_modified.date() != chrono::MIN_DATE {
            session_properties.set_modified_time(path.last_modified);
        }

        let is_seekable = match headers
            .get("Accept-Ranges")
            .map::<StreamResult<_>, _>(|s| Ok(s.to_str().map_err_to_unknown()?))
            .transpose()?
        {
            Some("bytes") => true,
            _ => false,
        };
        session_properties.set_is_seekable(is_seekable);

        Ok(())
    }

    fn default_is_seekable() -> bool {
        true
    }
}

impl ReadSectionRequest for RequestBuilder {
    fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
        assert!(
            length > 0,
            "[adls_gen2_stream_handler::request_builder::ReadSectionRequest::read_section] Attempt to send range request of a zero length"
        );
        let request = new_request()
            .uri(format!(
                "https://{}/{}/{}",
                self.host,
                self.file_system,
                EncodedUrl::from(&self.path)
            ))
            .header("Range", format!("bytes={}-{}", offset, offset + length - 1))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }
}

impl ReadRequest for RequestBuilder {
    fn read(&self) -> AuthenticatedRequest {
        let request = new_request()
            .uri(format!(
                "https://{}/{}/{}",
                self.host,
                self.file_system,
                EncodedUrl::from(&self.path)
            ))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }
}

impl ParallelWriteRequest for RequestBuilder {
    fn create(&self) -> AuthenticatedRequest {
        self.create(FileSystemResourceType::File)
    }

    fn write_block(&self, _block_idx: usize, position: usize, data: &[u8]) -> AuthenticatedRequest {
        self.append(data.into(), position)
    }

    fn complete(&self, _block_count: usize, position: usize) -> AuthenticatedRequest {
        self.flush(position)
    }

    fn stream_info(&self) -> StreamInfo {
        // TODO: pass in the credential arguments - https://msdata.visualstudio.com/Vienna/_workitems/edit/931126/
        StreamInfo::new(HANDLER_TYPE, self.uri(), SyncRecord::empty())
    }
}

impl AppendWriteRequest for RequestBuilder {
    fn create(&self) -> AuthenticatedRequest {
        self.create(FileSystemResourceType::File)
    }

    fn write(&self, position: usize, data: &[u8]) -> AuthenticatedRequest {
        self.append(data.into(), position)
    }

    fn flush(&self, position: usize) -> Option<AuthenticatedRequest> {
        Some(self.flush(position))
    }
}

impl CreateDirectoryRequest for RequestBuilder {
    fn create_directory(&self) -> AuthenticatedRequest {
        self.create(FileSystemResourceType::Directory)
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
            async move { self.find_streams_async(&sc).await }.wait()?
        }
    }

    #[test]
    fn new_request_builder_throw_if_url_not_valid() {
        let uri = "https://not.adls.gen2.url";

        RequestBuilder::new(uri, Arc::new(()))
            .should()
            .be_err()
            .with_value(StreamError::InvalidInput {
                message: "Invalid ADLS Gen 2 URL.".to_string(),
                source: None,
            });
    }

    #[test]
    fn find_streams_with_prefix_add_directory() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/path1/somefile.csv";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let search_context: SearchContext = "path1/somefile.csv".parse().unwrap();

        builder
            .find_streams(&search_context)
            .should().be_ok().which_value()
            .into_request()
            .should().be_ok().which_value().should().pass(|request| {
            request.method().should().be(&Method::GET);
            request.uri().should().equal_string(
                "https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false&directory=path1%2Fsomefile.csv",
            );
        });
    }

    #[test]
    fn find_streams_to_the_root_of_the_file_system() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem";
        let search_context: SearchContext = "**".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .find_streams(&search_context)
            .should()
            .be_ok()
            .which_value()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|request| {
                request.method().should().be(&Method::GET);
                request.uri().should().equal_string(
                    "https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false",
                );
            });
    }

    #[test]
    fn find_streams_with_credential_apply() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        let search_context: SearchContext = "somefilesystem/somefile.csv".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .should()
            .be_ok()
            .which_value()
            .find_streams(&search_context)
            .should()
            .be_ok()
            .which_value()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|request| {
                FakeCredential::verify(request);
            });
    }

    #[test]
    fn find_streams_with_prefix_empty_skip_directory() {
        // NOTE: as we are providing the SearchContext to the find_stream function, the uri is not used here
        // but put in the uri to give context of tests
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/*.txt";
        let search_context: SearchContext = "*.txt".parse().unwrap(); // no common prefix

        RequestBuilder::new(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .find_streams(&search_context)
            .should()
            .be_ok()
            .which_value()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|request| {
                request.method().should().be(&Method::GET);
                request.uri().should().equal_string(
                    "https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false",
                );
            });
    }

    #[test]
    fn find_streams_with_one_batch_search_use_recursive() {
        // NOTE: as we are providing the SearchContext to the find_stream function, the uri is not used here
        // but put in the uri to give context of tests
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/**.txt";
        let search_context = "**.txt".parse::<SearchContext>().unwrap().into_one_pass_search(); // no common prefix

        RequestBuilder::new(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .find_streams(&search_context)
            .should()
            .be_ok()
            .which_value()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|request| {
                request.method().should().be(&Method::GET);
                request.uri().should().equal_string(
                    "https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=true",
                );
            });
    }

    #[test]
    fn find_streams_with_continuation_token() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/**.txt";
        let search_context = "**.txt".parse::<SearchContext>().unwrap().with_continuation("abcdefg"); // no common prefix

        RequestBuilder::new(uri, Arc::new(())).should().be_ok().which_value()
            .find_streams(&search_context)
            .should().be_ok().which_value()
            .into_request()
            .should().be_ok().which_value().should().pass(|request| {
            request.method().should().be(&Method::GET);
            request.uri().should()
                .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false&continuation=abcdefg");
        });
    }

    #[test]
    fn find_stream_with_continuation_token_url_encode() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/**.txt";
        let search_context = "**.txt".parse::<SearchContext>().unwrap().with_continuation("a=b"); // no common prefix

        RequestBuilder::new(uri, Arc::new(())).should().be_ok().which_value()
            .find_streams(&search_context)
            .should().be_ok().which_value()
            .into_request().should().be_ok().which_value().should().pass(|request| {
            request.method().should().be(&Method::GET);
            request.uri().should()
                .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false&continuation=a%3Db");
        });
    }

    #[test]
    fn get_opener_with_single_file() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        RequestBuilder::new(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .head()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|request| {
                request.method().should().be(&Method::HEAD);
                request
                    .uri()
                    .should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv");
            });
    }

    #[test]
    fn url_could_be_different_host() {
        let uri = "https://some.different.host/somefilesystem/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .head()
            .uri()
            .should()
            .equal_string("https://some.different.host/somefilesystem/somefile.csv");
    }

    #[test]
    fn get_opener_with_credential_will_apply() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .should()
            .be_ok()
            .which_value()
            .head()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|request| {
                FakeCredential::verify(&request);
            });
    }

    #[test]
    fn read_section_will_return_get_request() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .read_section(10, 20)
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|request| {
                request.method().should().be(&Method::GET);
                request
                    .uri()
                    .should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv");
                request.headers()["Range"].to_ref().should().equal(&"bytes=10-29");
            });
    }

    #[test]
    fn read_section_with_credential_will_apply() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .should()
            .be_ok()
            .which_value()
            .read_section(10, 20)
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|request| {
                FakeCredential::verify(&request);
            });
    }

    #[test]
    #[should_panic]
    fn read_section_for_zero_length_should_panic() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        // this should panic
        let _ = RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .should()
            .be_ok()
            .which_value()
            .read_section(10, 0);
    }

    #[test]
    fn read_will_return_get_request() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .read()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|request| {
                request.method().should().be(&Method::GET);
                request
                    .uri()
                    .should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv");
            });
    }

    #[test]
    fn read_with_credential_will_apply() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .expect("build request builder should succeed")
            .read()
            .into_request()
            .expect("unwrap created request should succeed")
            .should()
            .pass(|request| {
                FakeCredential::verify(&request);
            });
    }

    #[test]
    fn create_file_will_return_put_request_with_expected_uri() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .create(FileSystemResourceType::File)
            .into_request()
            .expect("unwrap created request should succeed")
            .should()
            .pass(|r| {
                r.method().should().be(&Method::PUT);
                r.headers()["Content-Length"].to_ref().should().equal(&"0");
                r.uri()
                    .should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv?resource=file");
            });
    }

    #[test]
    fn append_file_will_return_patch_request_with_expected_uri() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .append(vec![1, 2, 3], 0)
            .into_request()
            .expect("unwrap created request should succeed")
            .should()
            .pass(|r| {
                r.method().should().be(&Method::PATCH);
                r.headers()["Content-Length"].to_ref().should().equal(&"3");
                r.uri()
                    .should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv?action=append&position=0");
            });
    }

    #[test]
    fn flush_will_return_patch_request_with_expected_uri() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefoler/somefile.csv";
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .flush(777)
            .into_request()
            .expect("unwrap created request should succeed")
            .should()
            .pass(|r| {
                r.method().should().be(&Method::PATCH);
                r.headers()["Content-Length"].to_ref().should().equal(&"0");
                r.uri().should().equal_string(
                    "https://someaccount.dfs.core.windows.net/somefilesystem/somefoler/somefile.csv?action=flush&position=777",
                );
            });
    }

    #[test]
    fn create_directory_will_return_put_request_with_expected_uri() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefolder";
        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .create(FileSystemResourceType::Directory)
            .into_request()
            .expect("unwrap created request should succeed")
            .should()
            .pass(|r| {
                r.method().should().be(&Method::PUT);
                r.headers()["Content-Length"].to_ref().should().equal(&"0");
                r.uri()
                    .should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem/somefolder?resource=directory");
            });
    }

    #[test]
    fn delete_will_return_delete_request_with_expected_uri() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefoler";
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
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem/somefoler?recursive=false");
            });
    }

    #[test]
    fn list_directory_without_continuation_token() {
        const BATCH_SIZE: u16 = 1000;
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/subfolder";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let request = builder
            .list_directory(BATCH_SIZE, None)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request.uri().should().equal_string(format!(
            "https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults={}&recursive=false&directory=subfolder",
            BATCH_SIZE
        ));
    }

    #[test]
    fn list_directory_with_continuation_token() {
        const BATCH_SIZE: u16 = 1000;
        let continuation_token = "token";
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/subfolder";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let request = builder
            .list_directory(BATCH_SIZE, Some(continuation_token))
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request.uri().should().equal_string(format!(
            "https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults={}&recursive=false&directory=subfolder&continuation={}",
            BATCH_SIZE, continuation_token
        ));
    }
}
