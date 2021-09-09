use super::{request_builder::RequestBuilder, searcher::Searcher, HANDLER_TYPE};
use crate::{
    adls_gen2_stream_handler::{
        destination::ADLSGen2DestinationBuilder,
        path_dto::{Path, PathList},
    },
    continuation_token_iterator::ContinuationTokenIterator,
    credential::{
        access_token::{Scope, ScopedAccessToken, ScopedAccessTokenResolver},
        CredentialInput,
    },
};
use http::Uri;
use itertools::Itertools;
use rslex_core::{
    file_io::{
        ArgumentError, Destination, DestinationAccessor, DestinationError, DestinationHandler, DirEntry, IfDestinationExists,
        ListDirectoryResult, SearchResults, StreamAccessor, StreamError, StreamHandler, StreamOpener, StreamResult,
    },
    records::parse::ParsedRecord,
    MapErrToUnknown, SessionProperties, StreamInfo, SyncRecord,
};
use rslex_http_stream::{
    create_stream_opener, ApplyCredential, AsyncSearch, DestinationBuilder, HeadRequest, HttpClient, ResponseExt, SessionPropertiesExt,
};
use std::{borrow::Cow, collections::HashMap, sync::Arc};

const URI_SCHEME: &str = "abfss";

pub(super) struct ADLSGen2StreamHandler {
    http_client: Arc<dyn HttpClient>,
    fallback_credential: Arc<dyn ApplyCredential>,
}

impl ADLSGen2StreamHandler {
    pub fn new(http_client: impl HttpClient) -> Self {
        ADLSGen2StreamHandler {
            http_client: Arc::new(http_client),
            fallback_credential: Arc::new(()),
        }
    }

    pub fn with_access_token_resolver(mut self, resolver: Arc<dyn ScopedAccessTokenResolver>) -> Self {
        self.fallback_credential = Arc::new(ScopedAccessToken::new(resolver, Scope::Storage));
        self
    }

    fn get_path(http_client: Arc<dyn HttpClient>, request_builder: &RequestBuilder) -> StreamResult<Path> {
        let request = request_builder.head();
        let res = http_client.clone().request(request.into())?.success()?;

        Path::try_from_response(request_builder.path().to_string(), &res)
    }
}

impl StreamHandler for ADLSGen2StreamHandler {
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
    ) -> StreamResult<Arc<(dyn StreamOpener + 'static)>> {
        let credential = arguments
            .to_credential(&self.http_client)
            .unwrap_or_else(|| self.fallback_credential.clone());
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
    ) -> StreamResult<Box<(dyn SearchResults + 'static)>> {
        let credential = arguments
            .to_credential(&self.http_client)
            .unwrap_or_else(|| self.fallback_credential.clone());
        let request_builder = RequestBuilder::new(uri, credential)?;
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

        let credential = arguments
            .to_credential(&self.http_client)
            .unwrap_or_else(|| self.fallback_credential.clone());
        let request_builder = RequestBuilder::new(uri, credential)?;
        let arguments_record = arguments.get_record().clone();
        let http_client = self.http_client.clone();

        let result_iterator = ContinuationTokenIterator::new(BATCHES_TO_PRE_LOAD, move |token| {
            let list_request = request_builder.list_directory(BATCH_SIZE, token)?;
            let res = http_client.clone().request(list_request.into())?.success()?;

            let continuation_token = if let Some(token_header) = res.headers().get("x-ms-continuation") {
                Some(token_header.to_str().map_err_to_unknown()?.to_string())
            } else {
                None
            };

            let path_list: PathList = res.into_string()?.parse()?;

            let result = path_list
                .paths
                .iter()
                .map(|path| {
                    if path.is_directory {
                        DirEntry::Directory(request_builder.path_to_uri(&path.name))
                    } else {
                        let session_properties = HashMap::default()
                            .with_size(path.content_length)
                            .with_modified_time(path.last_modified);
                        DirEntry::Stream(
                            StreamInfo::new(HANDLER_TYPE, request_builder.path_to_uri(&path.name), arguments_record.clone())
                                .with_session_properties(session_properties),
                        )
                    }
                })
                .collect_vec();

            Ok((result, continuation_token))
        })?;

        Ok(Box::new(result_iterator))
    }

    fn get_entry(&self, uri: &str, arguments: ParsedRecord<Self::FindStreamsArguments>, _: &StreamAccessor) -> StreamResult<DirEntry> {
        let credential = arguments
            .to_credential(&self.http_client)
            .unwrap_or_else(|| self.fallback_credential.clone());
        let request_builder = RequestBuilder::new(uri, credential)?;
        if request_builder.path() == "" {
            return Ok(DirEntry::Directory(request_builder.path_to_uri("")));
        }
        let http_client = self.http_client.clone();

        let path = ADLSGen2StreamHandler::get_path(http_client, &request_builder)?;

        Ok(if path.is_directory {
            DirEntry::Directory(request_builder.path_to_uri(&path.name))
        } else {
            let arguments_record = arguments.get_record().clone();
            let session_properties = HashMap::default()
                .with_size(path.content_length)
                .with_modified_time(path.last_modified);
            DirEntry::Stream(
                StreamInfo::new(HANDLER_TYPE, request_builder.path_to_uri(&path.name), arguments_record.clone())
                    .with_session_properties(session_properties),
            )
        })
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
                "unable to parse {} scheme, an adls gen2 storage handler should only parse abfss URI",
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
        if authority_parts[2] != "dfs" {
            return Err(StreamError::InvalidInput {
                message: "mismatched authority information".to_string(),
                source: None,
            });
        }
        let storage_end_point = authority_parts[3..].join(".");
        let resource_id = format!(
            "https://{}.dfs.{}/{}{}",
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

impl DestinationHandler for ADLSGen2StreamHandler {
    type Arguments = CredentialInput;

    fn handler_type(&self) -> &str {
        HANDLER_TYPE
    }

    fn get_destination<'a>(
        &self,
        base_path: Cow<'a, str>,
        arguments: Self::Arguments,
        if_exists: IfDestinationExists,
        _: &DestinationAccessor,
    ) -> Result<Arc<dyn Destination + 'a>, DestinationError> {
        let credential = arguments
            .to_credential(&self.http_client)
            .unwrap_or_else(|| self.fallback_credential.clone());
        match if_exists {
            IfDestinationExists::MergeWithOverwrite => Ok(()),
            IfDestinationExists::Fail => {
                let request_builder = RequestBuilder::new(base_path.as_ref(), credential.clone())?;
                match ADLSGen2StreamHandler::get_path(self.http_client.clone(), &request_builder) {
                    Ok(_) => Err(DestinationError::NotEmpty),
                    Err(e) if e == StreamError::NotFound => Ok(()),
                    e @ Err(_) => e.map(|_| ()).map_err(StreamError::into),
                }
            },
            _ => Err(DestinationError::InvalidArguments(ArgumentError::InvalidArgument {
                argument: "if_exists".to_owned(),
                expected: format!("{:#?}", IfDestinationExists::MergeWithOverwrite),
                actual: format!("{:#?}", if_exists),
            })),
        }?;
        Ok(ADLSGen2DestinationBuilder { credential }.build(base_path, self.http_client.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adls_gen2_stream_handler::path_dto::{Path, PathList};
    use chrono::{TimeZone, Utc};
    use fluent_assertions::*;
    use lazy_static::lazy_static;
    use rslex_core::{
        file_io::{StreamError, StreamProperties},
        SyncRecord,
    };
    use rslex_http_stream::FakeHttpClient;
    use std::collections::HashMap;

    trait IntoAdlsGen2Handlers {
        fn into_stream_handler(self) -> ADLSGen2StreamHandler;

        fn into_opener<S: AsRef<str>>(self, uri: S) -> StreamResult<Arc<(dyn StreamOpener + 'static)>>
        where Self: Sized {
            let stream_handler = self.into_stream_handler();

            stream_handler.get_opener(
                uri.as_ref(),
                SyncRecord::empty().parse().unwrap(),
                &HashMap::default(),
                &STREAM_ACCESSOR,
            )
        }
    }

    impl IntoAdlsGen2Handlers for FakeHttpClient {
        fn into_stream_handler(self) -> ADLSGen2StreamHandler {
            ADLSGen2StreamHandler::new(self)
        }
    }

    lazy_static! {
        static ref STREAM_ACCESSOR: StreamAccessor = StreamAccessor::default();
    }

    #[test]
    fn parse_adls_gen2_uri() {
        let uri = "abfss://somecontainer@someaccount.dfs.core.windows.net/temp.csv";
        let expected_path = "https://someaccount.dfs.core.windows.net/somecontainer/temp.csv";
        let expected_stream_info = StreamInfo::new(HANDLER_TYPE, expected_path, SyncRecord::empty());

        let handler = FakeHttpClient::default().into_stream_handler();
        let stream_info = handler.parse_uri(uri, &SyncRecord::empty()).should().be_ok().which_value();
        assert_eq!(stream_info, expected_stream_info);
    }

    #[test]
    fn parse_adls_gen2_uri_with_empty_path() {
        let uri = "abfss://somecontainer@someaccount.dfs.core.windows.net/";
        let expected_path = "https://someaccount.dfs.core.windows.net/somecontainer/";
        let expected_stream_info = StreamInfo::new(HANDLER_TYPE, expected_path, SyncRecord::empty());

        let handler = FakeHttpClient::default().into_stream_handler();
        let stream_info = handler.parse_uri(uri, &SyncRecord::empty()).should().be_ok().which_value();
        assert_eq!(stream_info, expected_stream_info);
    }

    #[test]
    fn parse_adls_gen2_uri_with_invalid_uri_format() {
        let uri = "abfss:/somecontainer@someaccount.blob.core.windows.net/";
        let expected_error = StreamError::InvalidInput {
            message: "invalid uri format".to_string(),
            source: None,
        };

        let handler = FakeHttpClient::default().into_stream_handler();
        handler
            .parse_uri(uri, &SyncRecord::empty())
            .should()
            .be_err()
            .which_value()
            .should()
            .equal(&expected_error);
    }

    #[test]
    fn parse_adls_gen2_uri_with_no_scheme() {
        let uri = "/somecontainer/blob/";
        let expected_error = StreamError::InvalidInput {
            message: "missing uri scheme".to_string(),
            source: None,
        };

        let handler = FakeHttpClient::default().into_stream_handler();
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
    fn parse_adls_gen2_uri_with_mismatched_scheme() {
        let uri = "AmlDatastore://somecontainer@someaccount.blob.core.windows.net/";
        let handler = FakeHttpClient::default().into_stream_handler();
        handler.parse_uri(uri, &SyncRecord::empty()).unwrap();
    }

    #[test]
    fn parse_adls_gen2_uri_with_invalid_authority_info() {
        let uri = "abfss://somecontainer/temp.txt";
        let expected_error = StreamError::InvalidInput {
            message: "invalid authority information".to_string(),
            source: None,
        };

        let handler = FakeHttpClient::default().into_stream_handler();
        handler
            .parse_uri(uri, &SyncRecord::empty())
            .should()
            .be_err()
            .which_value()
            .should()
            .equal(&expected_error);
    }

    #[test]
    fn parse_adls_gen2_uri_with_mismatched_authority_info() {
        let uri = "abfss://somecontainer@someaccount.blob.core.windows.net/";
        let expected_error = StreamError::InvalidInput {
            message: "mismatched authority information".to_string(),
            source: None,
        };

        let handler = FakeHttpClient::default().into_stream_handler();
        handler
            .parse_uri(uri, &SyncRecord::empty())
            .should()
            .be_err()
            .which_value()
            .should()
            .equal(&expected_error);
    }

    #[test]
    fn should_have_correct_handler_type() {
        StreamHandler::handler_type(&FakeHttpClient::default().into_stream_handler())
            .should()
            .be(&HANDLER_TYPE);

        DestinationHandler::handler_type(&FakeHttpClient::default().into_stream_handler())
            .should()
            .be(&HANDLER_TYPE);
    }

    #[test]
    fn find_streams_pattern_with_matches_returns_search_results() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        let result = PathList {
            paths: vec![Path {
                content_length: 100,
                last_modified: Utc.ymd(2020, 2, 18).and_hms(9, 40, 0),
                name: "somefile.csv".to_owned(),
                is_directory: false,
            }],
        }
        .to_string();

        FakeHttpClient::default()
            .body(result)
            .into_stream_handler()
            .find_streams(file_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok();
    }

    #[test]
    fn get_opener_for_existing_adls_file_returns_opener() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        FakeHttpClient::default()
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .header("x-ms-resource-type", "file")
            .into_stream_handler()
            .get_opener(
                file_path,
                SyncRecord::empty().parse().unwrap(),
                &HashMap::default(),
                &STREAM_ACCESSOR,
            )
            .should()
            .be_ok();
    }

    #[test]
    fn new_for_non_existing_blob_returns_not_found() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        FakeHttpClient::default()
            .status(404)
            .into_opener(file_path)
            .should()
            .be_ok()
            .which_value()
            .get_properties()
            .should()
            .be_err()
            .with_value(StreamError::NotFound);
    }

    #[test]
    fn new_with_authentication_failure_returns_permission_denied() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        FakeHttpClient::default()
            .status(401)
            .into_opener(file_path)
            .should()
            .be_ok()
            .which_value()
            .get_properties()
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn new_for_existing_file_returns_opener() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        FakeHttpClient::default()
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .header("x-ms-resource-type", "file")
            .into_opener(file_path)
            .should()
            .be_ok();
    }

    #[test]
    fn new_for_existing_dir_returns_invalid_input() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somedir";

        FakeHttpClient::default()
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .header("x-ms-resource-type", "directory")
            .into_opener(file_path)
            .should()
            .be_ok()
            .which_value()
            .get_properties()
            .should()
            .be_err()
            .with_value(StreamError::InvalidInput {
                message: "Expected a file but resource is a folder.".to_string(),
                source: None,
            });
    }

    #[test]
    fn get_properties_for_existing_file_returns_properties() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        FakeHttpClient::default()
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .header("x-ms-resource-type", "file")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .get_properties()
            .should()
            .be_ok()
            .with_value(StreamProperties {
                size: 200,
                created_time: None,
                modified_time: Some(Utc.ymd(2019, 7, 9).and_hms(17, 51, 15)),
            });
    }

    #[test]
    fn open_return_result() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        FakeHttpClient::default()
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .header("x-ms-resource-type", "file")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .open()
            .should()
            .be_ok();
    }

    #[test]
    fn can_seek_return_true_with_accept_range() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        FakeHttpClient::default()
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .header("x-ms-resource-type", "file")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .can_seek()
            .should()
            .be_true();
    }

    #[test]
    fn copy_to_copies_the_content() {
        let content = "content of the file";
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .with_request(|id, _| id == 0) // first request
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "19")
            .header("x-ms-resource-type", "file")
            .with_request(|id, _| id == 1) // second request
            .body(content)
            .into_opener(file_path)
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
    fn copy_to_if_not_seekable_copies_the_content() {
        let content = "content of the file";
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .with_request(|id, _| id == 0) // first request
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Content-Length", "200")
            .header("x-ms-resource-type", "file")
            .with_request(|id, _| id == 1) // second request
            .assert_request(|_, r| {
                r.headers().keys().map(|k| k.as_str()).should().not_contain("Range");
            })
            .body(content)
            .into_opener(file_path)
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
    fn copy_to_with_401_return_permission_denied() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .with_request(|id, _| id == 0) // first request
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .header("x-ms-resource-type", "file")
            .with_request(|id, _| id == 1) // second request
            .status(401)
            .into_opener(file_path)
            .expect("build opener should succeed")
            .write_to(&mut output)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn try_as_seekable_return_self() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        FakeHttpClient::default()
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .header("x-ms-resource-type", "file")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .try_as_seekable()
            .should()
            .be_some();
    }

    #[test]
    fn copy_section_to_copies_the_section() {
        let content = "content of the file!";
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .assert_num_requests(1)
            .assert_request(|_, r| {
                r.headers()["Range"]
                    .to_str()
                    .should()
                    .be_ok()
                    .which_value()
                    .should()
                    .equal_string("bytes=10-29");
            })
            .body(content)
            .into_opener(file_path)
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
    fn open_seekable_return_result() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        FakeHttpClient::default()
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .header("x-ms-resource-type", "file")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .try_as_seekable()
            .should()
            .be_some()
            .which_value()
            .open_seekable()
            .should()
            .be_ok();
    }

    #[test]
    fn open_destination_returns_result() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        FakeHttpClient::default()
            .into_stream_handler()
            .get_destination(
                file_path.into(),
                CredentialInput::None,
                IfDestinationExists::MergeWithOverwrite,
                &Default::default(),
            )
            .should()
            .be_ok();
    }

    #[test]
    fn list_directory_returns_directory_entries() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somedir";
        let batch1 = PathList {
            paths: vec![
                Path {
                    content_length: 100,
                    last_modified: Utc.ymd(2020, 2, 18).and_hms(9, 40, 0),
                    name: "file1.bin".to_owned(),
                    is_directory: false,
                },
                Path {
                    content_length: 200,
                    last_modified: Utc.ymd(2020, 2, 18).and_hms(9, 40, 0),
                    name: "file2.bin".to_owned(),
                    is_directory: false,
                },
                Path {
                    content_length: 0,
                    last_modified: Utc.ymd(2020, 2, 18).and_hms(9, 40, 0),
                    name: "folder1".to_owned(),
                    is_directory: true,
                },
            ],
        }
        .to_string();

        let batch2 = PathList {
            paths: vec![
                Path {
                    content_length: 100,
                    last_modified: Utc.ymd(2020, 2, 18).and_hms(9, 40, 0),
                    name: "file3.bin".to_owned(),
                    is_directory: false,
                },
                Path {
                    content_length: 200,
                    last_modified: Utc.ymd(2020, 2, 18).and_hms(9, 40, 0),
                    name: "file4.bin".to_owned(),
                    is_directory: false,
                },
                Path {
                    content_length: 0,
                    last_modified: Utc.ymd(2020, 2, 18).and_hms(9, 40, 0),
                    name: "folder2".to_owned(),
                    is_directory: true,
                },
            ],
        }
        .to_string();

        let response = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .body(batch1)
            .header("x-ms-continuation", "continuation")
            .with_request(|id, _| id == 1) // last record is a continuation token
            .body(batch2)
            .into_stream_handler()
            .list_directory(file_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .collect_vec();

        response.should().all_be_ok().which_inner_values().should().have_length(6);
    }

    #[test]
    fn get_entry_returns_directory_info() {
        let dir_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somedir";

        FakeHttpClient::default()
            .status(200)
            .header("Content-Length", "0")
            .header("x-ms-resource-type", "directory")
            .into_stream_handler()
            .get_entry(dir_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .should()
            .be(DirEntry::Directory(dir_path.to_string()));
    }

    #[test]
    fn get_entry_returns_file_info() {
        let file_path = "https://someaccount.dfs.core.windows.net/somefilesystem/somedir/somefile.bin";
        let last_modified_date = Utc::now();
        let size = 100500;

        let session_properties = HashMap::default().with_size(size).with_modified_time(last_modified_date);

        FakeHttpClient::default()
            .status(200)
            .header("Content-Length", format!("{}", size))
            .header("x-ms-resource-type", "file")
            .header("Last-Modified", last_modified_date.to_rfc2822())
            .into_stream_handler()
            .get_entry(file_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .should()
            .be(DirEntry::Stream(
                StreamInfo::new(HANDLER_TYPE, file_path, SyncRecord::empty()).with_session_properties(session_properties),
            ));
    }

    #[test]
    fn get_entry_from_root_of_the_filesystem_should_not_make_requests() {
        let dir_path = "https://someaccount.dfs.core.windows.net/somefilesystem/";

        FakeHttpClient::default()
            .into_stream_handler()
            .get_entry(dir_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .should()
            .be(DirEntry::Directory(dir_path.to_string()));
    }
}
