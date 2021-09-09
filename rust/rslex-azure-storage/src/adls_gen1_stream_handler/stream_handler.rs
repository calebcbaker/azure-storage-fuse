use super::{request_builder::RequestBuilder, searcher::Searcher, HANDLER_TYPE};
use crate::{
    adls_gen1_stream_handler::{
        destination::ADLSGen1DestinationBuilder,
        file_dto::{FileList, FileStatus, FileType},
    },
    continuation_token_iterator::ContinuationTokenIterator,
    credential::{
        access_token::{Scope, ScopedAccessToken, ScopedAccessTokenResolver},
        CredentialInput,
    },
};
use chrono::{TimeZone, Utc};
use itertools::Itertools;
use rslex_core::{
    file_io::{
        ArgumentError, Destination, DestinationAccessor, DestinationError, DestinationHandler, DirEntry, IfDestinationExists,
        ListDirectoryResult, SearchResults, StreamAccessor, StreamError, StreamHandler, StreamOpener, StreamResult,
    },
    records::parse::ParsedRecord,
    SessionProperties, StreamInfo, SyncRecord,
};
use rslex_http_stream::{
    create_stream_opener, ApplyCredential, AsyncSearch, DestinationBuilder, HeadRequest, HttpClient, ResponseExt, SessionPropertiesExt,
};
use std::{borrow::Cow, collections::HashMap, sync::Arc};

const URI_SCHEME: &str = "adl";

pub struct ADLSGen1StreamHandler {
    http_client: Arc<dyn HttpClient>,
    fallback_credential: Arc<dyn ApplyCredential>,
}

impl ADLSGen1StreamHandler {
    pub fn new(http_client: impl HttpClient) -> Self {
        ADLSGen1StreamHandler {
            http_client: Arc::new(http_client),
            fallback_credential: Arc::new(()),
        }
    }

    pub fn with_access_token_resolver(mut self, resolver: Arc<dyn ScopedAccessTokenResolver>) -> Self {
        self.fallback_credential = Arc::new(ScopedAccessToken::new(resolver, Scope::DataLake));
        self
    }

    fn get_file_status(http_client: Arc<dyn HttpClient>, request_builder: &RequestBuilder) -> StreamResult<FileStatus> {
        let request = request_builder.head();
        let res = http_client.clone().request(request.into())?.success()?;

        let file_status: FileStatus = res.into_string()?.parse()?;
        Ok(file_status)
    }
}

impl StreamHandler for ADLSGen1StreamHandler {
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
        Ok(Searcher::new(request_builder, self.http_client.clone(), arguments.get_record().clone()).into_search_results()?)
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
        let http_client = self.http_client.clone();
        let arguments_record = arguments.get_record().clone();

        let result = ContinuationTokenIterator::new(BATCHES_TO_PRE_LOAD, move |token| {
            let request = request_builder.list_directory(BATCH_SIZE, token);
            let res = http_client.clone().request(request.into())?.success()?;

            let file_list: FileList = res.into_string()?.parse()?;

            let continuation_token = file_list.files.last().map(|f| f.path_suffix.clone());

            let dir_entries = file_list
                .files
                .iter()
                .map(|file| {
                    let full_path = format!(
                        "{}/{}",
                        request_builder.path().trim_end_matches('/'),
                        file.path_suffix.trim_end_matches('/')
                    );

                    match file.file_type {
                        FileType::FILE => {
                            let session_properties = HashMap::default()
                                .with_size(file.length)
                                .with_modified_time(Utc.timestamp_millis(file.modification_time))
                                .with_is_seekable(true);
                            DirEntry::Stream(
                                StreamInfo::new(HANDLER_TYPE, request_builder.path_to_uri(&full_path), arguments_record.clone())
                                    .with_session_properties(session_properties),
                            )
                        },
                        FileType::DIRECTORY => DirEntry::Directory(request_builder.path_to_uri(&full_path)),
                    }
                })
                .collect_vec();

            Ok((dir_entries, continuation_token))
        })?;

        Ok(Box::new(result))
    }

    fn get_entry(&self, uri: &str, arguments: ParsedRecord<Self::FindStreamsArguments>, _: &StreamAccessor) -> StreamResult<DirEntry> {
        let credential = arguments
            .to_credential(&self.http_client)
            .unwrap_or_else(|| self.fallback_credential.clone());
        let request_builder = RequestBuilder::new(uri, credential)?;
        let http_client = self.http_client.clone();
        let arguments_record = arguments.get_record().clone();

        let file_status = ADLSGen1StreamHandler::get_file_status(http_client, &request_builder)?;

        let full_path = format!("{}/{}", request_builder.path().trim_end_matches('/'), file_status.path_suffix);
        let full_path = full_path.trim_end_matches("/");

        if file_status.file_type == FileType::FILE {
            let session_properties = HashMap::default()
                .with_size(file_status.length)
                .with_modified_time(Utc.timestamp_millis(file_status.modification_time))
                .with_is_seekable(true);

            Ok(DirEntry::Stream(
                StreamInfo::new(HANDLER_TYPE, request_builder.path_to_uri(&full_path), arguments_record.clone())
                    .with_session_properties(session_properties),
            ))
        } else {
            Ok(DirEntry::Directory(request_builder.path_to_uri(&full_path)))
        }
    }

    fn parse_uri(&self, uri: &str, arguments: &SyncRecord) -> StreamResult<StreamInfo> {
        let uri_parts: Vec<&str> = uri.split("://").collect();
        if uri_parts.len() != 2 {
            return Err(StreamError::InvalidInput {
                message: "invalid uri format".to_string(),
                source: None,
            });
        }
        let uri_scheme = uri_parts[0];
        if uri_scheme.to_string() != URI_SCHEME {
            panic!(
                "unable to parse {} scheme, an adls gen1 should only parse adl URI",
                uri_scheme.to_string()
            );
        }
        if uri_parts[1].trim().len() == 0 {
            return Err(StreamError::InvalidInput {
                message: "missing storage resource information".to_string(),
                source: None,
            });
        }
        let resource_id = format!("https://{}", uri_parts[1].trim());
        return Ok(StreamInfo::new(HANDLER_TYPE, resource_id, arguments.clone()));
    }

    fn uri_scheme(&self) -> String {
        return URI_SCHEME.to_string();
    }
}

impl DestinationHandler for ADLSGen1StreamHandler {
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
                match ADLSGen1StreamHandler::get_file_status(self.http_client.clone(), &request_builder) {
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
        Ok(ADLSGen1DestinationBuilder { credential }.build(base_path, self.http_client.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adls_gen1_stream_handler::file_dto::{FileList, FileStatus, FileType};
    use chrono::{TimeZone, Utc};
    use fluent_assertions::*;
    use lazy_static::lazy_static;
    use rslex_core::{
        file_io::{StreamError, StreamProperties},
        SyncRecord,
    };
    use rslex_http_stream::FakeHttpClient;
    use std::collections::HashMap;

    trait IntoADLSGen1Handlers {
        fn into_stream_handler(self) -> ADLSGen1StreamHandler;
    }

    impl IntoADLSGen1Handlers for FakeHttpClient {
        fn into_stream_handler(self) -> ADLSGen1StreamHandler {
            ADLSGen1StreamHandler::new(self)
        }
    }

    lazy_static! {
        static ref STREAM_ACCESSOR: StreamAccessor = StreamAccessor::default();
    }

    #[test]
    fn parse_adls_gen1_uri() {
        let uri = "adl://someaccount.azuredatalakestore.net/temp.csv";
        let expected_path = "https://someaccount.azuredatalakestore.net/temp.csv";
        let expected_stream_info = StreamInfo::new(HANDLER_TYPE, expected_path, SyncRecord::empty());

        let handler = FakeHttpClient::default().into_stream_handler();
        let stream_info = handler.parse_uri(uri, &SyncRecord::empty()).should().be_ok().which_value();
        assert_eq!(stream_info, expected_stream_info);
    }

    #[test]
    fn parse_adls_gen1_uri_with_invalid_uri_format() {
        let uri = "adl:/sdsdsds ";
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
    #[should_panic]
    fn parse_adls_gen1_uri_with_mismatched_scheme() {
        let uri = "AmlDatastore://somecontainer@someaccount.blob.core.windows.net/";
        let handler = FakeHttpClient::default().into_stream_handler();
        handler.parse_uri(uri, &SyncRecord::empty()).unwrap();
    }

    #[test]
    fn parse_adls_gen1_uri_with_no_storage_resource_information() {
        let uri = "adl:// ";
        let expected_error = StreamError::InvalidInput {
            message: "missing storage resource information".to_string(),
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
    fn find_streams_pattern_with_matches_returns_search_results() {
        let file_path = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";
        let result = FileList {
            files: vec![FileStatus {
                length: 100,
                path_suffix: String::default(),
                file_type: FileType::FILE,
                modification_time: 1234567890,
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

    trait IntoOpener {
        fn into_opener<S: AsRef<str>>(self, uri: S) -> StreamResult<Arc<(dyn StreamOpener + 'static)>>;
    }

    impl IntoOpener for FakeHttpClient {
        fn into_opener<S: AsRef<str>>(self, uri: S) -> StreamResult<Arc<(dyn StreamOpener + 'static)>> {
            let stream_handler = self.into_stream_handler();

            stream_handler.get_opener(
                uri.as_ref(),
                SyncRecord::empty().parse().unwrap(),
                &HashMap::default(),
                &STREAM_ACCESSOR,
            )
        }
    }

    #[test]
    fn new_for_existing_file_returns_opener() {
        let file_path = "adl://someaccount.azuredatalakestore.net/somefile.csv";

        FakeHttpClient::default()
            .body(
                FileStatus {
                    length: 200,
                    path_suffix: "".to_owned(),
                    file_type: FileType::FILE,
                    modification_time: 100,
                }
                .to_string(),
            )
            .into_opener(file_path)
            .should()
            .be_ok();
    }

    #[test]
    fn get_properties_for_existing_file_returns_properties() {
        let file_path = "adl://someaccount.azuredatalakestore.net/somefile.csv";
        let modified_time = Utc.ymd(2019, 7, 4).and_hms(20, 9, 52);

        FakeHttpClient::default()
            .body(
                FileStatus {
                    length: 200,
                    path_suffix: "".to_owned(),
                    file_type: FileType::FILE,
                    modification_time: modified_time.timestamp_millis(),
                }
                .to_string(),
            )
            .into_opener(file_path)
            .should()
            .be_ok()
            .which_value()
            .get_properties()
            .should()
            .be_ok()
            .with_value(StreamProperties {
                size: 200,
                created_time: None,
                modified_time: Some(modified_time),
            });
    }

    #[test]
    fn open_return_result() {
        let file_path = "adl://someaccount.azuredatalakestore.net/somefile.csv";

        FakeHttpClient::default()
            .body(
                FileStatus {
                    length: 200,
                    path_suffix: "".to_owned(),
                    file_type: FileType::FILE,
                    modification_time: 100,
                }
                .to_string(),
            )
            .into_opener(file_path)
            .should()
            .be_ok()
            .which_value()
            .open()
            .should()
            .be_ok();
    }

    #[test]
    fn can_seek_return_true() {
        let file_path = "adl://someaccount.azuredatalakestore.net/somefile.csv";

        FakeHttpClient::default()
            .body(
                FileStatus {
                    length: 200,
                    path_suffix: "".to_owned(),
                    file_type: FileType::FILE,
                    modification_time: 100,
                }
                .to_string(),
            )
            .into_opener(file_path)
            .should()
            .be_ok()
            .which_value()
            .can_seek()
            .should()
            .be_true();
    }

    #[test]
    fn copy_to_copies_the_content() {
        let content = "content of the file";
        let file_path = "adl://someaccount.azuredatalakestore.net/somefile.csv";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .with_request(|_, r| r.uri().to_string().contains("GETFILESTATUS")) // first request
            .body(
                FileStatus {
                    length: 19,
                    path_suffix: "".to_owned(),
                    file_type: FileType::FILE,
                    modification_time: 100,
                }
                .to_string(),
            )
            .with_request(|_, r| r.uri().to_string().contains("OPEN")) // second request
            .body(content)
            .into_opener(file_path)
            .should()
            .be_ok()
            .which_value()
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
        let file_path = "adl://someaccount.azuredatalakestore.net/somefile.csv";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .with_request(|_, r| r.uri().to_string().contains("GETFILESTATUS")) // first request
            .body(
                FileStatus {
                    length: 200,
                    path_suffix: "".to_owned(),
                    file_type: FileType::FILE,
                    modification_time: 100,
                }
                .to_string(),
            )
            .with_request(|_, r| r.uri().to_string().contains("OPEN")) // second request
            .status(401)
            .into_opener(file_path)
            .should()
            .be_ok()
            .which_value()
            .write_to(&mut output)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn try_as_seekable_return_self() {
        let file_path = "adl://someaccount.azuredatalakestore.net/somefile.csv";

        FakeHttpClient::default()
            .body(
                FileStatus {
                    length: 200,
                    path_suffix: "".to_owned(),
                    file_type: FileType::FILE,
                    modification_time: 100,
                }
                .to_string(),
            )
            .into_opener(file_path)
            .should()
            .be_ok()
            .which_value()
            .try_as_seekable()
            .should()
            .be_some();
    }

    #[test]
    fn copy_section_to_copies_the_section() {
        let content = "content of the file!";
        let file_path = "adl://someaccount.azuredatalakestore.net/somefile.csv";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .with_request(|_, r| r.uri().to_string().contains("GETFILESTATUS")) // first request
            .body(
                FileStatus {
                    length: 200,
                    path_suffix: "".to_owned(),
                    file_type: FileType::FILE,
                    modification_time: 100,
                }
                .to_string(),
            )
            .with_request(|_, r| r.uri().to_string().contains("OPEN")) // second request
            .assert_request(|_, r| {
                r.uri().should().contain("offset=10");
                r.uri().should().contain("length=20");
            })
            .body(content)
            .into_opener(file_path)
            .should()
            .be_ok()
            .which_value()
            .try_as_seekable()
            .should()
            .be_some()
            .which_value()
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
    fn copy_section_to_with_401_return_permission_denied() {
        let file_path = "adl://someaccount.azuredatalakestore.net/somefile.csv";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .with_request(|_, r| r.uri().to_string().contains("GETFILESTATUS")) // first request
            .body(
                FileStatus {
                    length: 200,
                    path_suffix: "".to_owned(),
                    file_type: FileType::FILE,
                    modification_time: 100,
                }
                .to_string(),
            )
            .with_request(|_, r| r.uri().to_string().contains("OPEN")) // second request
            .status(401)
            .into_opener(file_path)
            .should()
            .be_ok()
            .which_value()
            .try_as_seekable()
            .should()
            .be_some()
            .which_value()
            .copy_section_to(10, &mut output)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn open_seekable_return_result() {
        let file_path = "adl://someaccount.azuredatalakestore.net/somefile.csv";

        FakeHttpClient::default()
            .body(
                FileStatus {
                    length: 200,
                    path_suffix: "".to_owned(),
                    file_type: FileType::FILE,
                    modification_time: 100,
                }
                .to_string(),
            )
            .into_opener(file_path)
            .should()
            .be_ok()
            .which_value()
            .try_as_seekable()
            .should()
            .be_some()
            .which_value()
            .open_seekable()
            .should()
            .be_ok();
    }

    #[test]
    fn list_directory_returns_directory_entries() {
        let file_path = "adl://someaccount.azuredatalakestore.net/somefolder";
        let batch1 = FileList {
            files: vec![
                FileStatus {
                    length: 100,
                    path_suffix: "file1.bin".to_string(),
                    file_type: FileType::FILE,
                    modification_time: 1234567890,
                },
                FileStatus {
                    length: 200,
                    path_suffix: "file2.bin".to_string(),
                    file_type: FileType::FILE,
                    modification_time: 1234567890,
                },
                FileStatus {
                    length: 0,
                    path_suffix: "folder1".to_string(),
                    file_type: FileType::DIRECTORY,
                    modification_time: 1234567890,
                },
            ],
        }
        .to_string();

        let batch2 = FileList {
            files: vec![
                FileStatus {
                    length: 300,
                    path_suffix: "file3.bin".to_string(),
                    file_type: FileType::FILE,
                    modification_time: 1234567890,
                },
                FileStatus {
                    length: 400,
                    path_suffix: "file4.bin".to_string(),
                    file_type: FileType::FILE,
                    modification_time: 1234567890,
                },
                FileStatus {
                    length: 0,
                    path_suffix: "folder2".to_string(),
                    file_type: FileType::DIRECTORY,
                    modification_time: 1234567890,
                },
            ],
        }
        .to_string();
        let batch3 = FileList { files: vec![] }.to_string();

        let response = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .body(batch1)
            .with_request(|_, r| r.uri().to_string().contains("folder1")) // last record is a continuation token
            .body(batch2)
            .with_request(|_, r| r.uri().to_string().contains("folder2")) // last record is a continuation token
            .body(batch3) // empty response - read to end
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
        let dir_path = "adl://someaccount.azuredatalakestore.net/somefolder";

        let dir_status = FileStatus {
            length: 0,
            file_type: FileType::DIRECTORY,
            path_suffix: "".to_owned(),
            modification_time: Utc::now().timestamp(),
        };

        FakeHttpClient::default()
            .status(200)
            .body(dir_status.to_string())
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
        let file_path = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.bin";
        let last_modified_date = Utc::now();
        let size = 100500;

        let file_status = FileStatus {
            length: size,
            file_type: FileType::FILE,
            path_suffix: "".to_owned(),
            modification_time: last_modified_date.timestamp(),
        };

        let session_properties = HashMap::default().with_size(size).with_modified_time(last_modified_date);

        FakeHttpClient::default()
            .status(200)
            .body(file_status.to_string())
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
}
