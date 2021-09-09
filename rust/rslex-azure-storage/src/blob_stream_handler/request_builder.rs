use super::{
    blob_dto::{BlockId, BlockList},
    HANDLER_TYPE, MAX_BLOCK_COUNT, MAX_BLOCK_SIZE,
};
use chrono::DateTime;
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
use std::sync::Arc;
use url::form_urlencoded;

const HEADER_X_MS_VERSION: &str = "x-ms-version";
pub(super) const HEADER_HDI_FOLDER_METADATA: &str = "x-ms-meta-hdi_isfolder";
const HEADER_RANGE: &str = "Range";
const HEADER_X_MS_BLOB_TYPE: &str = "x-ms-blob-type";
const HEADER_CONTENT_LENGTH: &str = "Content-Length";
const HDI_ISFOLDER_METADATA_HEADER_NAME: &'static str = "x-ms-meta-hdi_isfolder";

const BLOCK_ID_PADDING_TOTAL_LENGTH: usize = 32;

/// A tool class handling building http request to work with Azure Blob Service.
#[derive(Clone)]
pub(super) struct RequestBuilder {
    // blob file information
    schema: String,
    host: String,
    container: String,
    path: String,

    credential: Arc<dyn ApplyCredential>,
}

pub(super) enum BlobType {
    BlockBlob,
    // PageBlob,
    AppendBlob,
}

lazy_static! {
    static ref URI_PATTERN: Regex =
        Regex::new(r"(?P<schema>https|http)://(?P<host>[^/]+)/(?P<container>[^/]+/?)(?P<path>.*)").expect("this should never fail");
}

impl RequestBuilder {
    /// Build a RequestBuilder from uri and SyncRecord.
    /// Return InvalidInput if uri is ill-formatted.
    pub fn new(uri: &str, credential: Arc<dyn ApplyCredential>) -> StreamResult<RequestBuilder> {
        // parse uri, determining schema, host, container, path
        let caps = URI_PATTERN.captures(uri).ok_or(StreamError::InvalidInput {
            message: "Invalid Azure Blob URL.".to_string(),
            source: None,
        })?;
        let schema = caps["schema"].to_owned();
        let host = caps["host"].to_owned();
        let container = caps["container"].trim_end_matches("/").to_owned();
        let path = caps["path"].to_owned();

        Ok(RequestBuilder {
            schema,
            host,
            container,
            path,
            credential,
        })
    }

    pub async fn find_streams_async(&self, search_context: &SearchContext) -> StreamResult<AuthenticatedRequest> {
        const MAX_RESULTS: u32 = 5000;
        // construct the request
        let request = new_request()
            .uri(format!("{}://{}/{}?{}", self.schema, self.host, self.container, {
                let mut query = form_urlencoded::Serializer::new(String::new());

                query.append_pair("restype", "container");
                query.append_pair("comp", "list");
                query.append_pair("prefix", search_context.prefix(false).as_ref());
                query.append_pair("maxresults", &MAX_RESULTS.to_string());
                query.append_pair("include", "metadata");

                if !search_context.is_one_pass() {
                    query.append_pair("delimiter", "/");
                }

                if let Some(token) = search_context.continuation() {
                    query.append_pair("marker", token);
                }

                query.finish()
            }))
            .body(Vec::<u8>::default())
            .map_err_to_unknown()?;

        Ok(request.with_credential(self.credential.clone()))
    }

    pub fn list_directory<T: AsRef<str>>(&self, max_results: u16, continuation_token: Option<T>) -> StreamResult<AuthenticatedRequest> {
        // construct the request
        let request = new_request()
            .uri(format!("{}://{}/{}?{}", self.schema, self.host, self.container, {
                let mut query = form_urlencoded::Serializer::new(String::new());

                query.append_pair("restype", "container");
                query.append_pair("comp", "list");
                query.append_pair("prefix", self.path());
                query.append_pair("maxresults", &max_results.to_string());
                query.append_pair("delimiter", "/");
                query.append_pair("include", "metadata");
                if let Some(continuation_token) = continuation_token {
                    query.append_pair("marker", continuation_token.as_ref());
                }

                query.finish()
            }))
            .body(Vec::<u8>::default())
            .map_err_to_unknown()?;

        Ok(request.with_credential(self.credential.clone()))
    }

    pub fn metadata(&self) -> AuthenticatedRequest {
        let request = new_request()
            .method(Method::HEAD)
            .uri(format!(
                "{}://{}/{}/{}?{}",
                self.schema,
                self.host,
                self.container,
                EncodedUrl::from(&self.path),
                {
                    let mut query = form_urlencoded::Serializer::new(String::new());

                    query.append_pair("comp", "metadata");

                    query.finish()
                }
            ))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    pub fn container_metadata(&self) -> AuthenticatedRequest {
        let request = new_request()
            .method(Method::HEAD)
            .uri(format!("{}://{}/{}?{}", self.schema, self.host, self.container, {
                let mut query = form_urlencoded::Serializer::new(String::new());

                query.append_pair("restype", "container");
                query.append_pair("comp", "metadata");

                query.finish()
            }))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn uri(&self) -> String {
        format!(
            "{}://{}/{}/{}",
            self.schema,
            self.host,
            self.container,
            EncodedUrl::from(&self.path)
        )
    }

    pub fn path_to_uri<S: AsRef<str>>(&self, path: S) -> String {
        format!("{}://{}/{}/{}", self.schema, self.host, self.container, path.as_ref())
    }

    pub fn to_container_uri(&self) -> String {
        format!("{}://{}/{}", self.schema, self.host, self.container)
    }

    pub fn put_blob(&self, blob_type: BlobType) -> AuthenticatedRequest {
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}",
                self.schema,
                self.host,
                self.container,
                EncodedUrl::from(&self.path)
            ))
            .header(HEADER_X_MS_VERSION, "2019-02-02")
            .header(
                HEADER_X_MS_BLOB_TYPE,
                match blob_type {
                    BlobType::BlockBlob => "BlockBlob",
                    BlobType::AppendBlob => "AppendBlob",
                    //BlobType::PageBlob => "PageBlob",
                },
            )
            .header(HEADER_CONTENT_LENGTH, 0)
            .method(Method::PUT)
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    pub fn put_block_list(&self, block_list: &BlockList) -> AuthenticatedRequest {
        let body = block_list.to_string();
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}?comp=blocklist",
                self.schema,
                self.host,
                self.container,
                EncodedUrl::from(&self.path),
            ))
            .header(HEADER_X_MS_VERSION, "2019-02-02")
            .header(HEADER_CONTENT_LENGTH, body.len())
            .method(Method::PUT)
            .body(body.as_bytes().to_vec())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    fn append_block(&self, data: Vec<u8>) -> AuthenticatedRequest {
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}?comp=appendblock",
                self.schema,
                self.host,
                self.container,
                EncodedUrl::from(&self.path)
            ))
            .header(HEADER_X_MS_VERSION, "2019-02-02")
            .header(HEADER_CONTENT_LENGTH, data.len())
            .method(Method::PUT)
            .body(data)
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    fn put_block(&self, block_idx: usize, data: Vec<u8>) -> AuthenticatedRequest {
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}?comp=block&blockid={}",
                self.schema,
                self.host,
                self.container,
                EncodedUrl::from(&self.path),
                base64::encode(format!("{:01$}", block_idx, BLOCK_ID_PADDING_TOTAL_LENGTH))
            ))
            .header(HEADER_X_MS_VERSION, "2019-02-02")
            .header(HEADER_CONTENT_LENGTH, data.len())
            .method(Method::PUT)
            .body(data)
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }

    fn delete_blob(&self) -> AuthenticatedRequest {
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}",
                self.schema,
                self.host,
                self.container,
                EncodedUrl::from(&self.path)
            ))
            .method(Method::DELETE)
            .body(vec![])
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }
}

impl HeadRequest for RequestBuilder {
    fn head(&self) -> AuthenticatedRequest {
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}",
                self.schema,
                self.host,
                self.container,
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
            .get("x-ms-creation-time")
            .map::<StreamResult<_>, _>(|s| {
                Ok(DateTime::parse_from_rfc2822(s.to_str().map_err_to_unknown()?)
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
    /// in case of seekable stream (with latest version all blob streams are seekable)
    /// read section of the stream
    fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
        assert!(
            length > 0,
            "[blob_stream_handler::request_builder::ReadSectionRequest::read_section] Attempt to send range request of a zero length"
        );
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}",
                self.schema,
                self.host,
                self.container,
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
                self.container,
                EncodedUrl::from(&self.path)
            ))
            .method(Method::GET)
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }
}

impl AppendWriteRequest for RequestBuilder {
    fn create(&self) -> AuthenticatedRequest {
        self.put_blob(BlobType::AppendBlob)
    }

    fn write(&self, _position: usize, data: &[u8]) -> AuthenticatedRequest {
        self.append_block(data.to_vec())
    }
}

impl ParallelWriteRequest for RequestBuilder {
    fn create(&self) -> AuthenticatedRequest {
        self.put_blob(BlobType::BlockBlob)
    }

    fn write_block(&self, block_idx: usize, _position: usize, data: &[u8]) -> AuthenticatedRequest {
        self.put_block(block_idx, data.to_vec())
    }

    fn complete(&self, block_count: usize, _position: usize) -> AuthenticatedRequest {
        let block_list = {
            BlockList {
                blocks: (0..block_count)
                    .map(|id| BlockId::Latest(format!("{:01$}", id, BLOCK_ID_PADDING_TOTAL_LENGTH)))
                    .collect(),
            }
        };
        self.put_block_list(&block_list)
    }

    fn stream_info(&self) -> StreamInfo {
        // TODO: pass in the credential arguments - https://msdata.visualstudio.com/Vienna/_workitems/edit/931126/
        StreamInfo::new(HANDLER_TYPE, self.uri(), SyncRecord::empty())
    }

    fn max_block_size(&self) -> usize {
        MAX_BLOCK_SIZE
    }

    fn max_block_count(&self) -> usize {
        MAX_BLOCK_COUNT
    }
}

impl CreateDirectoryRequest for RequestBuilder {
    fn create_directory(&self) -> AuthenticatedRequest {
        // Azure blob does not support folders concept.
        // To emulate empty folders creation for mount we create an empty file with special attribute.
        // The same attribute is respected by blobfuse.
        let request = new_request()
            .uri(format!(
                "{}://{}/{}/{}",
                self.schema,
                self.host,
                self.container,
                EncodedUrl::from(self.path.trim_end_matches("/"))
            ))
            .header(HEADER_X_MS_VERSION, "2019-02-02")
            .header(HEADER_X_MS_BLOB_TYPE, "AppendBlob")
            .header(HEADER_CONTENT_LENGTH, 0)
            .header(HDI_ISFOLDER_METADATA_HEADER_NAME, "true")
            .method(Method::PUT)
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.with_credential(self.credential.clone())
    }
}

impl RemoveRequest for RequestBuilder {
    fn remove(&self) -> AuthenticatedRequest {
        self.delete_blob()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob_stream_handler::blob_dto::BlockId;
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
    fn find_streams_return_request_with_list_blob() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
        let search_context: SearchContext = "somefile.csv".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed")
            .find_streams(&search_context).expect("find_stream should succeed").into_request().should().be_ok().which_value().should().pass(|result| {
            result.method().should().be(&Method::GET);
            result.uri().should().equal_string(
                "https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=somefile.csv&maxresults=5000&include=metadata&delimiter=%2F",
            );
        });
    }

    #[test]
    fn find_streams_uri_with_credential_will_apply() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
        let search_context: SearchContext = "somefile.csv".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find_stream should succeed")
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
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/folder/";
        let search_context: SearchContext = "folder/".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed")
            .find_streams(&search_context).expect("find_stream should succeed").into_request().should().be_ok().which_value().should().pass(|result| {
            result.method().should().be(&Method::GET);
            result.uri().should()
                .equal_string("https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=folder%2F&maxresults=5000&include=metadata&delimiter=%2F");
        });
    }

    #[test]
    fn find_streams_to_the_root_of_the_container() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer";
        let search_context: SearchContext = "**".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed")
            .find_streams(&search_context).expect("find_stream should succeed").into_request().should().be_ok().which_value().should().pass(|result| {
            result.method().should().be(&Method::GET);
            result.uri().should().equal_string(
                "https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=&maxresults=5000&include=metadata&delimiter=%2F",
            );
        });
    }

    #[test]
    fn find_streams_uri_with_star_match_any_file() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/a*b";
        let search_context: SearchContext = "a*b".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find_stream should succeed")
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result.uri().should().equal_string(
            "https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=a&maxresults=5000&include=metadata&delimiter=%2F",
        );
            });
    }

    #[test]
    fn find_streams_uri_with_double_star_match_any_folder() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/**b";
        let search_context: SearchContext = "**b".parse().unwrap();

        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find_stream should succeed")
            .into_request().should().be_ok().which_value().should().pass(|result| {
            result.method().should().be(&Method::GET);
            result.uri().should().equal_string(
                "https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=&maxresults=5000&include=metadata&delimiter=%2F",
            );
        });
    }

    #[test]
    fn find_streams_with_marker_add_to_uri() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile";
        let search_context: SearchContext = "somefile".parse::<SearchContext>().unwrap().with_continuation("mmm");

        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find_stream should succeed")
            .into_request()
            .should().be_ok().which_value().should().pass(|result| {
            result.method().should().be(&Method::GET);
            result.uri().should().equal_string(
                "https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=somefile&maxresults=5000&include=metadata&delimiter=%2F&marker=mmm",
            );
        });
    }

    #[test]
    fn find_streams_with_marker_will_encode() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile";
        let search_context: SearchContext = "somefile".parse::<SearchContext>().unwrap().with_continuation("a=b");

        RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find_stream should succeed")
            .into_request().should().be_ok().which_value().should().pass(|result| {
            result.method().should().be(&Method::GET);
            result.uri().should().equal_string(
                "https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=somefile&maxresults=5000&include=metadata&delimiter=%2F&marker=a%3Db",
            );
        });
    }

    #[test]
    fn find_streams_with_recursive_search_will_skip_delimiter() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/d/a**b";
        let search_context: SearchContext = "d/a**b".parse::<SearchContext>().unwrap().into_one_pass_search();

        RequestBuilder::new(uri, Arc::new(()))
            .expect("creating RequestBuilder should succeed")
            .find_streams(&search_context)
            .expect("find_stream should succeed")
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result.uri().should().equal_string(
                    "https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=d%2Fa&maxresults=5000&include=metadata",
                );
            });
    }

    #[test]
    fn new_request_builder_with_invalid_url_returns_error() {
        let invalid_url = "https://somewhere.com";

        RequestBuilder::new(invalid_url, Arc::new(()))
            .should()
            .be_err()
            .with_value(StreamError::InvalidInput {
                message: "Invalid Azure Blob URL.".to_string(),
                source: None,
            });
    }

    #[test]
    fn get_opener_will_return_head_request() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

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
                    .equal_string("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv");
            });
    }

    #[test]
    fn blob_url_could_take_different_domain() {
        let uri = "https://some.different.domain/somecontainer/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .expect("build request builder should succeed")
            .head()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .uri()
            .should()
            .equal_string("https://some.different.domain/somecontainer/somefile.csv");
    }

    #[test]
    fn get_opener_with_credential_will_apply() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

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

    // According to https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties,
    // the Accept-Range header will only be available after API version 2013-08-15
    // ''Accept-Ranges: bytes	Indicates that the service supports requests for partial blob content. Included for requests made using version 2013-08-15 and newer.''
    #[test]
    fn get_opener_will_ask_for_api_version_higher_than_2013_08_15() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .expect("build request builder should succeed")
            .head()
            .into_request()
            .expect("into_request should succeed")
            .headers()[HEADER_X_MS_VERSION]
            .to_str()
            .should()
            .be_ok()
            .which_value()
            .should()
            .be_greater_than("2013-08-15");
    }

    #[test]
    fn read_will_return_get_request() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

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
                    .equal_string("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv");
            });
    }

    #[test]
    fn read_with_credential_will_apply() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
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
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

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
                    .equal_string("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv");
                result.headers()["Range"].to_ref().should().equal(&"bytes=10-29");
            });
    }

    #[test]
    fn read_section_with_credential_will_apply() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
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
    #[should_panic]
    fn read_section_for_zero_length_should_panic() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        // this should panic
        let _ = RequestBuilder::new(uri, Arc::new(FakeCredential::new()))
            .should()
            .be_ok()
            .which_value()
            .read_section(10, 0);
    }

    #[test]
    fn put_blob_returns_put_request() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .expect("build request builder should succeed")
            .put_blob(BlobType::BlockBlob)
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::PUT);
                result
                    .uri()
                    .should()
                    .equal_string("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv");
                result.headers()["x-ms-blob-type"].to_ref().should().equal(&"BlockBlob");
            });
    }

    #[test]
    fn put_block_returns_put_request() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .expect("build request builder should succeed")
            .put_block(5, vec![1, 2, 3, 4, 5])
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::PUT);
                let x = result.uri();
                x.should().equal_string(format!(
                    "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv?comp=block&blockid={}",
                    base64::encode(format!("{:01$}", 5, BLOCK_ID_PADDING_TOTAL_LENGTH))
                ));
                result.headers()["Content-Length"].to_ref().should().equal(&"5");
                result.body().should().equal_binary(vec![1, 2, 3, 4, 5]);
            });
    }

    #[test]
    fn commit_blocks_returns_put_request() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
        let block_list = BlockList {
            blocks: vec![
                BlockId::Uncommitted("MA==".to_owned()),
                BlockId::Committed("MQ==".to_owned()),
                BlockId::Latest("Mg==".to_owned()),
            ],
        };

        RequestBuilder::new(uri, Arc::new(()))
            .expect("build request builder should succeed")
            .put_block_list(&block_list)
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                let body = block_list.to_string();
                result.method().should().be(&Method::PUT);
                result
                    .uri()
                    .should()
                    .equal_string("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv?comp=blocklist");
                result.headers()["Content-Length"]
                    .to_ref()
                    .should()
                    .equal(&body.len().to_string().as_str());
                result.body().should().equal_binary(body);
            });
    }

    #[test]
    fn append_blocks_returns_put_request() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .expect("build request builder should succeed")
            .append_block(vec![1, 2, 3, 4, 5])
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::PUT);
                result
                    .uri()
                    .should()
                    .equal_string("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv?comp=appendblock");
                result.headers()["Content-Length"].to_ref().should().equal(&"5");
                result.body().should().equal_binary(vec![1, 2, 3, 4, 5]);
            });
    }

    #[test]
    fn delete_block_returns_delete_request() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        RequestBuilder::new(uri, Arc::new(()))
            .expect("build request builder should succeed")
            .delete_blob()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::DELETE);
                result
                    .uri()
                    .should()
                    .equal_string("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv");
            });
    }

    #[test]
    fn list_directory_without_continuation_token() {
        const BATCH_SIZE: u16 = 1000;
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefolder";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let request = builder
            .list_directory::<String>(BATCH_SIZE, None)
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request.uri().should().equal_string(format!(
            "https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=somefolder&maxresults={}&delimiter=%2F&include=metadata",
            BATCH_SIZE
        ));
    }

    #[test]
    fn list_directory_with_continuation_token() {
        const BATCH_SIZE: u16 = 1000;
        let continuation_token = "token";
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefolder";
        let builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");
        let request = builder
            .list_directory(BATCH_SIZE, Some(continuation_token))
            .expect("create request should succeed")
            .into_request()
            .expect("unwrap created request should succeed");

        request.method().should().be(&Method::GET);
        request.uri().should().equal_string(format!(
            "https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=somefolder&maxresults={}&delimiter=%2F&include=metadata&marker={}",
            BATCH_SIZE, continuation_token
        ));
    }
}
