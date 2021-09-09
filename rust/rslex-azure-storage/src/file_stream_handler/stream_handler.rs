use super::{request_builder::RequestBuilder, searcher::Searcher, HANDLER_TYPE};
use crate::{
    continuation_token_iterator::ContinuationTokenIterator,
    credential::{access_token::ScopedAccessTokenResolver, CredentialInput},
    file_stream_handler::file_dto::DirectoriesAndFiles,
};
use http::Uri;
use itertools::Itertools;
use rslex_core::{
    file_io::{
        DirEntry, ListDirectoryResult, MapErrToUnknown, SearchResults, StreamAccessor, StreamError, StreamHandler, StreamOpener,
        StreamResult,
    },
    records::parse::ParsedRecord,
    SessionProperties, StreamInfo, SyncRecord,
};
use rslex_http_stream::{create_stream_opener, AsyncSearch, HeadRequest, HttpClient, Response, ResponseExt, SessionPropertiesExt};
use std::{collections::HashMap, sync::Arc};

const URI_SCHEME: &str = "azfs";

pub(super) struct FileStreamHandler {
    http_client: Arc<dyn HttpClient>,
    #[allow(dead_code)]
    access_token_resolver: Option<Arc<dyn ScopedAccessTokenResolver>>,
}

impl FileStreamHandler {
    pub fn new(http_client: impl HttpClient) -> Self {
        FileStreamHandler {
            http_client: Arc::new(http_client),
            access_token_resolver: None,
        }
    }

    pub fn with_access_token_resolver(mut self, resolver: Arc<dyn ScopedAccessTokenResolver>) -> Self {
        self.access_token_resolver = Some(resolver);
        self
    }
}

impl StreamHandler for FileStreamHandler {
    fn handler_type(&self) -> &'static str {
        HANDLER_TYPE
    }

    type GetOpenerArguments = CredentialInput;

    fn get_opener(
        &self,
        uri: &str,
        arguments: ParsedRecord<Self::GetOpenerArguments>,
        session_properties: &SessionProperties,
        _: &StreamAccessor,
    ) -> StreamResult<Arc<(dyn StreamOpener)>> {
        let credential = arguments.to_credential(&self.http_client).ok_or(StreamError::InvalidInput {
            message: "Azure File Share requires input credentials and received None.".into(),
            source: None,
        })?;
        let request_builder = RequestBuilder::new(uri, credential)?;

        Ok(Arc::new(create_stream_opener(
            request_builder,
            self.http_client.clone(),
            session_properties.clone(),
        )))
    }

    type FindStreamsArguments = CredentialInput;

    fn find_streams(
        &self,
        uri: &str,
        arguments: ParsedRecord<Self::FindStreamsArguments>,
        _: &StreamAccessor,
    ) -> StreamResult<Box<(dyn SearchResults)>> {
        let credential = arguments.to_credential(&self.http_client).ok_or(StreamError::InvalidInput {
            message: "Azure File Share requires input credentials and received None.".into(),
            source: None,
        })?;
        let request_builder = RequestBuilder::new(uri, credential).map_err_to_unknown()?;

        Searcher::new(request_builder, self.http_client.clone(), arguments.get_record().clone()).into_search_results()
    }

    fn list_directory(
        &self,
        uri: &str,
        arguments: ParsedRecord<Self::FindStreamsArguments>,
        _: &StreamAccessor,
    ) -> StreamResult<ListDirectoryResult> {
        const BATCH_SIZE: u16 = 5000;
        const BATCHES_TO_PRE_LOAD: u8 = 5;

        let credential = arguments.to_credential(&self.http_client).ok_or(StreamError::InvalidInput {
            message: "Azure File Share requires input credentials and received None.".into(),
            source: None,
        })?;
        let request_builder = RequestBuilder::new(uri, credential).map_err_to_unknown()?;
        let arguments_record = arguments.get_record().clone();
        let http_client = self.http_client.clone();

        let result_iterator = ContinuationTokenIterator::new(BATCHES_TO_PRE_LOAD, move |token| {
            let request = request_builder.list_directory(BATCH_SIZE, token)?;

            let res = file_share_request_success(http_client.clone().request(request.into())?)?;

            let dir_entries: DirectoriesAndFiles = res.into_string()?.parse()?;

            let directory_path = if dir_entries.directory_path.is_empty() {
                "".to_owned()
            } else {
                format!("{}/", &dir_entries.directory_path)
            };

            let result = dir_entries
                .files
                .iter()
                .map(|file| {
                    let file_name = format!("{}{}", directory_path, file.name);
                    let stream_info = StreamInfo::new(HANDLER_TYPE, request_builder.path_to_uri(&file_name), arguments_record.clone());

                    if let Some(size_str) = file.properties.get("Content-Length") {
                        let session_properties = HashMap::default().with_size(size_str.parse().unwrap_or(0));

                        DirEntry::Stream(stream_info.with_session_properties(session_properties))
                    } else {
                        DirEntry::Stream(stream_info)
                    }
                })
                .chain(dir_entries.directories.iter().map(|dir| {
                    let dir_name = format!("{}{}", directory_path, dir.name);
                    DirEntry::Directory(request_builder.path_to_uri(&dir_name))
                }))
                .collect_vec();

            Ok((result, dir_entries.next_marker))
        })?;

        Ok(Box::new(result_iterator))
    }

    fn get_entry(&self, uri: &str, arguments: ParsedRecord<Self::FindStreamsArguments>, _: &StreamAccessor) -> StreamResult<DirEntry> {
        let credential = arguments.to_credential(&self.http_client).ok_or(StreamError::InvalidInput {
            message: "Azure File Share requires input credentials and received None.".into(),
            source: None,
        })?;
        let request_builder = RequestBuilder::new(uri, credential.clone()).map_err_to_unknown()?;
        let arguments_record = arguments.get_record().clone();
        let http_client = self.http_client.clone();

        // Checking if provided url is a file first
        let file_meta_request = request_builder.head();
        let file_meta_response = http_client.clone().request(file_meta_request.into())?.success();

        match file_meta_response {
            Ok(response) => {
                let mut props = SessionProperties::new();

                RequestBuilder::parse_response(response, &mut props)?;
                Ok(DirEntry::Stream(
                    StreamInfo::new(
                        HANDLER_TYPE,
                        request_builder.path_to_uri(request_builder.path()),
                        arguments_record.clone(),
                    )
                    .with_session_properties(props),
                ))
            },
            Err(e) if e.status_code == 404 => {
                // file not found, but could be a directory
                let dir_request = request_builder.directory_metadata()?;
                http_client.clone().request(dir_request.into())?.success()?;

                Ok(DirEntry::Directory(request_builder.path_to_uri(request_builder.path()).to_string()))
            },
            Err(e) => Err(e.into()),
        }
    }

    fn parse_uri(&self, uri: &str, arguments: &SyncRecord) -> StreamResult<StreamInfo> {
        let uri_struct = uri.to_string().parse::<Uri>().map_err(|_e| StreamError::InvalidInput {
            message: "invalid uri format".to_string(),
            source: None,
        })?;
        let uri_scheme = uri_struct.scheme().ok_or(StreamError::InvalidInput {
            message: "missing uri scheme".to_string(),
            source: None,
        })?;
        if uri_scheme.to_string() != URI_SCHEME {
            panic!(
                "unable to parse {} scheme, a file stream handler should only parse azfs URI",
                uri_scheme.to_string()
            );
        }
        let authority = uri_struct
            .authority()
            .ok_or(StreamError::InvalidInput {
                message: "missing authority information".to_string(),
                source: None,
            })?
            .to_string();

        let authority_parts: Vec<&str> = authority.split(&['.', '@'][..]).collect();
        if authority_parts.len() <= 3 {
            return Err(StreamError::InvalidInput {
                message: "invalid authority information".to_string(),
                source: None,
            });
        }
        if authority_parts[2] != "file" {
            return Err(StreamError::InvalidInput {
                message: "mismatched authority information".to_string(),
                source: None,
            });
        }
        let storage_end_point = authority_parts[3..].join(".");
        let resource_id = format!(
            "https://{}.file.{}/{}{}",
            authority_parts[1],
            storage_end_point,
            authority_parts[0],
            uri_struct.path()
        );
        return Ok(StreamInfo::new(HANDLER_TYPE, resource_id, arguments.clone()));
    }

    fn uri_scheme(&self) -> String {
        return URI_SCHEME.to_string();
    }
}

fn file_share_request_success(response: Response) -> Result<Response, StreamError> {
    response.success().map_err(|e| {
        if e.status_code == 404 {
            tracing::warn!(
                "[FileShareStreamHandler] file share service return 404 - not found, but this could mostly be authentication failure"
            );
            StreamError::PermissionDenied
        } else {
            e.into()
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_stream_handler::file_dto::{DirectoriesAndFiles, Directory, File};
    use chrono::{TimeZone, Utc};
    use fluent_assertions::*;
    use http::Method;
    use lazy_static::lazy_static;
    use rslex_core::{file_io::StreamProperties, sync_record};
    use rslex_http_stream::FakeHttpClient;
    use std::collections::HashMap;

    trait IntoFileStreamHandler {
        fn into_handler(self) -> FileStreamHandler;
    }

    impl IntoFileStreamHandler for FakeHttpClient {
        fn into_handler(self) -> FileStreamHandler {
            FileStreamHandler::new(self)
        }
    }

    lazy_static! {
        static ref STREAM_ACCESSOR: StreamAccessor = StreamAccessor::default();
    }

    #[test]
    fn parse_file_uri() {
        let uri = "azfs://somecontainer@someaccount.file.core.windows.net/temp.csv";
        let expected_path = "https://someaccount.file.core.windows.net/somecontainer/temp.csv";
        let expected_stream_info = StreamInfo::new(HANDLER_TYPE, expected_path, SyncRecord::empty());

        let handler = FakeHttpClient::default().into_handler();
        let stream_info = handler.parse_uri(uri, &SyncRecord::empty()).should().be_ok().which_value();
        assert_eq!(stream_info, expected_stream_info);
    }

    #[test]
    fn parse_file_uri_with_empty_path() {
        let uri = "azfs://somecontainer@someaccount.file.core.windows.net/";
        let expected_path = "https://someaccount.file.core.windows.net/somecontainer/";
        let expected_stream_info = StreamInfo::new(HANDLER_TYPE, expected_path, SyncRecord::empty());

        let handler = FakeHttpClient::default().into_handler();
        let stream_info = handler.parse_uri(uri, &SyncRecord::empty()).should().be_ok().which_value();
        assert_eq!(stream_info, expected_stream_info);
    }

    #[test]
    fn parse_file_uri_with_invalid_uri_format() {
        let uri = "azfs:/somecontainer@someaccount.blob.core.windows.net/";
        let expected_error = StreamError::InvalidInput {
            message: "invalid uri format".to_string(),
            source: None,
        };

        let handler = FakeHttpClient::default().into_handler();
        handler
            .parse_uri(uri, &SyncRecord::empty())
            .should()
            .be_err()
            .which_value()
            .should()
            .equal(&expected_error);
    }

    #[test]
    fn parse_file_uri_with_no_scheme() {
        let uri = "/somecontainer/blob/";
        let expected_error = StreamError::InvalidInput {
            message: "missing uri scheme".to_string(),
            source: None,
        };

        let handler = FakeHttpClient::default().into_handler();
        handler
            .parse_uri(uri, &SyncRecord::empty())
            .should()
            .be_err()
            .which_value()
            .should()
            .equal(&expected_error);
    }

    #[test]
    #[should_panic]
    fn parse_file_uri_with_mismatched_scheme() {
        let uri = "AmlDatastore://somecontainer@someaccount.blob.core.windows.net/";
        let handler = FakeHttpClient::default().into_handler();
        handler.parse_uri(uri, &SyncRecord::empty()).unwrap();
    }

    #[test]
    fn parse_file_uri_with_invalid_authority_info() {
        let uri = "azfs://somecontainer/temp.txt";
        let expected_error = StreamError::InvalidInput {
            message: "invalid authority information".to_string(),
            source: None,
        };

        let handler = FakeHttpClient::default().into_handler();
        handler
            .parse_uri(uri, &SyncRecord::empty())
            .should()
            .be_err()
            .which_value()
            .should()
            .equal(&expected_error);
    }

    #[test]
    fn parse_blob_uri_with_mismatched_authority_info() {
        let uri = "azfs://somecontainer@someaccount.dfs.core.windows.net/";
        let expected_error = StreamError::InvalidInput {
            message: "mismatched authority information".to_string(),
            source: None,
        };

        let handler = FakeHttpClient::default().into_handler();
        handler
            .parse_uri(uri, &SyncRecord::empty())
            .should()
            .be_err()
            .which_value()
            .should()
            .equal(&expected_error);
    }

    #[test]
    fn find_streams_pattern_with_matches_returns_search_results() {
        let file_path = "https://someaccount.file.core.windows.net/somecontainer/somefile.csv";
        let result = DirectoriesAndFiles {
            directory_path: "".to_owned(),
            files: vec![
                File {
                    name: "somefile.csv".to_string(),
                    properties: HashMap::default(),
                },
                File {
                    name: "somefile.csv.1".to_string(),
                    properties: HashMap::default(),
                },
            ],
            directories: vec![],
            next_marker: None,
        }
        .to_string();

        FakeHttpClient::default()
            .body(result)
            .into_handler()
            .find_streams(file_path, sync_record! { "sas" => "sassy" }.parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok();
    }

    trait IntoOpener {
        fn into_opener<S: AsRef<str>>(self, uri: S) -> StreamResult<Arc<(dyn StreamOpener + 'static)>>;
    }

    impl IntoOpener for FakeHttpClient {
        fn into_opener<S: AsRef<str>>(self, uri: S) -> StreamResult<Arc<(dyn StreamOpener + 'static)>> {
            let stream_handler = self.into_handler();

            stream_handler.get_opener(
                uri.as_ref(),
                sync_record! { "sas" => "sassy" }.parse().unwrap(),
                &HashMap::default(),
                &STREAM_ACCESSOR,
            )
        }
    }

    #[test]
    fn new_for_non_existing_blob_returns_not_found() {
        let file_path = "https://someaccount.file.core.windows.net/somecontainer/noexist.csv";

        FakeHttpClient::default()
            .status(404)
            .into_opener(file_path)
            .unwrap()
            .get_properties()
            .should()
            .be_err()
            .with_value(StreamError::NotFound);
    }

    #[test]
    fn new_with_authentication_failure_returns_permission_denied() {
        let file_path = "https://someaccount.file.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .status(403)
            .into_opener(file_path)
            .unwrap()
            .get_properties()
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn new_for_existing_blob_returns_opener() {
        let file_path = "https://someaccount.file.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "2496")
            .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
            .into_opener(file_path)
            .should()
            .be_ok();
    }

    #[test]
    fn get_properties_for_existing_blob_returns_properties() {
        let file_path = "https://someaccount.file.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "2496")
            .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
            .header("x-ms-file-creation-time", "2018-12-21T01:09:00.7672994Z")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .get_properties()
            .should()
            .be_ok()
            .with_value(StreamProperties {
                size: 2496,
                created_time: Some(Utc.ymd(2018, 12, 21).and_hms_nano(1, 9, 00, 767299400)),
                modified_time: Some(Utc.ymd(2019, 9, 4).and_hms(2, 29, 10)),
            });
    }

    #[test]
    fn open_return_result() {
        let file_path = "https://someaccount.file.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "100")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .should()
            .pass(|opener| {
                opener.open().should().be_ok();
                opener.can_seek().should().be_true();
            });
    }

    #[test]
    fn copy_to_copies_the_content() {
        let content = "content of the file";
        let uri = "https://someaccount.file.core.windows.net/somecontainer/somefile.csv";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.method().should().be(&Method::HEAD);
            }) // first request
            .header("Content-Length", "19")
            .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::GET);
            }) // second request
            .body(content)
            .into_opener(uri)
            .expect("build opener should succeed")
            .write_to(&mut output)
            .should()
            .be_ok();

        String::from_utf8(output)
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string(content);
    }

    #[test]
    fn copy_to_with_403_return_permission_denied() {
        let uri = "https://someaccount.file.core.windows.net/somecontainer/somefile.csv";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.method().should().be(&Method::HEAD);
            }) // first request
            .header("Content-Length", "19")
            .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::GET);
            }) // second request
            .status(403)
            .into_opener(uri)
            .expect("build opener should succeed")
            .write_to(&mut output)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn try_as_seekable_return_self_if_can_seek() {
        let file_path = "https://someaccount.file.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "100")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .try_as_seekable()
            .should()
            .be_some();
    }

    #[test]
    fn copy_section_to_copies_the_section() {
        let content = "12345678901234567890";
        let uri = "https://someaccount.file.core.windows.net/somecontainer/somefile.csv";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .assert_num_requests(1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::GET);
                r.headers()["Range"].clone().should().equal("bytes=10-29");
            })
            .body(content)
            .into_opener(uri)
            .expect("build opener should succeed")
            .try_as_seekable()
            .expect("try as seekable should succeed")
            .copy_section_to(10, &mut output)
            .should()
            .be_ok();

        String::from_utf8(output)
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string(content);
    }

    #[test]
    fn copy_section_to_with_authentication_failure_return_permission_denied() {
        let uri = "https://someaccount.file.core.windows.net/somecontainer/somefile.csv";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.method().should().be(&Method::GET);
                r.headers()["Range"].clone().should().equal("bytes=10-29");
            })
            .status(403)
            .into_opener(uri)
            .expect("build opener should succeed")
            .try_as_seekable()
            .expect("try as seekable should succeed")
            .copy_section_to(10, &mut output)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn open_seekable_return_result() {
        let file_path = "https://someaccount.file.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "100")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .try_as_seekable()
            .expect("try as seekable should succeed")
            .open_seekable()
            .should()
            .be_ok();
    }

    #[test]
    fn list_directory_returns_directory_entries() {
        let file_path = "https://someaccount.file.core.windows.net/somecontainer/somefolder";
        let batch1 = DirectoriesAndFiles {
            directory_path: "somefolder".to_owned(),
            files: vec![
                File {
                    name: "file1.bin".to_string(),
                    properties: HashMap::default(),
                },
                File {
                    name: "file2.bin".to_string(),
                    properties: HashMap::default(),
                },
            ],
            directories: vec![Directory {
                name: "folder1".to_string(),
            }],
            next_marker: Some("token".to_string()),
        }
        .to_string();
        let batch2 = DirectoriesAndFiles {
            directory_path: "somefolder".to_owned(),
            files: vec![
                File {
                    name: "file3.bin".to_string(),
                    properties: HashMap::default(),
                },
                File {
                    name: "file4.bin".to_string(),
                    properties: HashMap::default(),
                },
            ],
            directories: vec![Directory {
                name: "folder2".to_string(),
            }],
            next_marker: None,
        }
        .to_string();

        let response = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .body(batch1)
            .with_request(|id, _| id == 1) // last record is a continuation token
            .body(batch2)
            .into_handler()
            .list_directory(file_path, sync_record! { "sas" => "sassy" }.parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .collect_vec();

        response.should().all_be_ok().which_inner_values().should().have_length(6);
    }
}
