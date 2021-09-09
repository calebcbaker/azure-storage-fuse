use super::{
    destination::BlobDestinationBuilder, public_blob::PublicBlobChecker, request_builder::RequestBuilder, searcher::Searcher, HANDLER_TYPE,
};
use crate::{
    blob_stream_handler::{
        blob_dto::{BlobEntry, BlobList},
        BlobUriScheme,
    },
    continuation_token_iterator::ContinuationTokenIterator,
    credential::{
        access_token::{Scope, ScopedAccessToken, ScopedAccessTokenResolver},
        CredentialInput,
    },
};
use http::Uri;
use rslex_core::{
    file_io::{
        ArgumentError, Destination, DestinationAccessor, DestinationError, DestinationHandler, DirEntry, IfDestinationExists,
        ListDirectoryResult, SearchResults, StreamAccessor, StreamError, StreamHandler, StreamOpener, StreamResult,
    },
    records::parse::ParsedRecord,
    SessionProperties, StreamInfo, SyncRecord,
};
use rslex_http_stream::{create_stream_opener, ApplyCredential, AsyncSearch, DestinationBuilder, HttpClient, Response, ResponseExt};
use std::{borrow::Cow, sync::Arc};

pub(super) struct BlobStreamHandler {
    http_client: Arc<dyn HttpClient>,
    public_blob_checker: Arc<PublicBlobChecker>,
    fallback_credential: Option<Arc<dyn ApplyCredential>>,
    uri_scheme: BlobUriScheme,
}

impl BlobStreamHandler {
    pub fn new(http_client: impl HttpClient, uri_scheme: BlobUriScheme) -> Self {
        let http_client = Arc::new(http_client);
        BlobStreamHandler {
            http_client: http_client.clone(),
            public_blob_checker: Arc::new(PublicBlobChecker::new(http_client)),
            fallback_credential: None,
            uri_scheme,
        }
    }

    pub fn with_access_token_resolver(mut self, resolver: Arc<dyn ScopedAccessTokenResolver>) -> Self {
        self.fallback_credential = Some(Arc::new(ScopedAccessToken::new(resolver, Scope::Storage)));
        self
    }

    fn get_credential(&self, uri: &str, arguments: &ParsedRecord<CredentialInput>) -> StreamResult<Arc<dyn ApplyCredential>> {
        Ok(match (arguments.to_credential(&self.http_client), &self.fallback_credential) {
            (Some(cred), _) => cred,
            (None, Some(_)) if self.public_blob_checker.is_public(uri)? => Arc::new(()),
            (None, Some(cred)) => cred.clone(),
            (None, None) => Arc::new(()),
        })
    }

    /// When listing blobs with prefix azure blob may return < max_results number of records and continuation token.
    /// It can even return an empty result and continuation token.
    /// This wrapper continues to request the subsequent batches until it can get the requested amount of data.
    fn list_exact(
        http_client: Arc<dyn HttpClient>,
        request_builder: &RequestBuilder,
        max_results: u16,
        continuation_token: Option<String>,
    ) -> StreamResult<BlobList> {
        let mut result_list = Vec::with_capacity(max_results as usize);
        let mut next_continuation_token = continuation_token;

        loop {
            let request = request_builder.list_directory(max_results - result_list.len() as u16, next_continuation_token)?;
            let response = blob_request_success(http_client.clone().request(request)?)?;
            let blob_list: BlobList = response.into_string()?.parse()?;

            result_list.extend(blob_list.blobs);
            next_continuation_token = blob_list.next_marker;

            if next_continuation_token.is_none() || result_list.len() >= max_results as usize {
                break;
            }
        }

        if result_list.len() > max_results as usize {
            result_list = result_list[..max_results as usize].to_vec()
        }

        Ok(BlobList {
            blobs: result_list,
            next_marker: next_continuation_token.map(|s| s.to_string()),
        })
    }
}

// Azure blob has no concept of a folder.
// So, to create an empty folder from the fuse driver, we create an empty file with a specific marker (hdi_folder = true) to emulate an empty folder.
// When listing entries for a specific path we may get back two entries - one for the "shadow" file and one for the actual folder
// (if folder is not empty azure blob API will return it as a folder).
// When iterating through the results of list_blobs call we need when we get a shadow file with hdi_folder marker, we need to check if we have a duplicate record.
// Shadow files in most of the cases are close to the actual "folder" (if any).
// So, instead of maintaining a hash map to remove duplicates, we are looking for duplicate entries starting from the place where we found hdi_folder in both directions.
fn find_dir_entry_around_index(entries: &Vec<BlobEntry>, index: usize, search: &str) -> bool {
    let len = index.max(entries.len() - index);

    let mut left = index as i64 - 1 as i64;
    let mut right = index as i64 + 1 as i64;

    for _ in 0..len {
        if left >= 0 {
            match &entries[left as usize] {
                BlobEntry::BlobPrefix(name) if name[..name.len() - 1].eq(search) => return true,
                _ => {},
            };
        }

        if right < entries.len() as i64 {
            match &entries[right as usize] {
                BlobEntry::BlobPrefix(name) if name[..name.len() - 1].eq(search) => return true,
                _ => {},
            };
        }

        left -= 1;
        right += 1;
    }

    false
}

impl StreamHandler for BlobStreamHandler {
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
        let credential = self.get_credential(uri, &arguments)?;
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
        let credential = self.get_credential(uri, &arguments)?;
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

        let list_request_uri = if uri.ends_with("/") {
            Cow::Borrowed(uri)
        } else {
            Cow::Owned(format!("{}/", uri))
        };

        let credential = self.get_credential(uri, &arguments)?;
        let request_builder = RequestBuilder::new(&list_request_uri, credential)?;
        let arguments_record = arguments.get_record().clone();
        let http_client = self.http_client.clone();

        let result_iterator = ContinuationTokenIterator::new(BATCHES_TO_PRE_LOAD, move |token| {
            let list_request = request_builder.list_directory(BATCH_SIZE, token)?;
            let res = blob_request_success(http_client.clone().request(list_request)?)?;
            let blob_list: BlobList = res.into_string()?.parse()?;
            let mut result = Vec::with_capacity(blob_list.blobs.len());

            for i in 0..blob_list.blobs.len() {
                let blob_entry = &blob_list.blobs[i];
                match blob_entry {
                    BlobEntry::Blob(blob) => {
                        if blob.is_hdi_folder() {
                            if !find_dir_entry_around_index(&blob_list.blobs, i, &blob.name) {
                                result.push(DirEntry::Directory(request_builder.path_to_uri(&blob.name)))
                            }
                        } else {
                            result.push(DirEntry::Stream(blob.to_stream_info(&request_builder, arguments_record.clone())))
                        }
                    },
                    BlobEntry::BlobPrefix(dir_name) => result.push(DirEntry::Directory(request_builder.path_to_uri(dir_name))),
                }
            }

            Ok((result, blob_list.next_marker))
        })?;

        Ok(Box::new(result_iterator))
    }

    fn get_entry(&self, uri: &str, arguments: ParsedRecord<Self::FindStreamsArguments>, _: &StreamAccessor) -> StreamResult<DirEntry> {
        let credential = self.get_credential(uri, &arguments)?;
        let arguments_record = arguments.get_record().clone();
        let http_client = self.http_client.clone();

        let mut uri = uri.to_string();

        // List directory .take(1) in most of the cases gives azure blob or folder as the first element if exists.
        // If there is a matching blob it will always be the first element.
        // But if there is no matching blob but matching folder it is possible to have entries before it cause storage sorts blobs in lexicographical order.
        // Example:
        //  - a/b! - blob
        //  - a/b/ - folder
        // In this rare case we need to make one more request to storage to specifically check if there is a matching folder
        for _ in 0..2 {
            let request_builder = RequestBuilder::new(&uri, credential.clone())?;
            let blob_list: BlobList = BlobStreamHandler::list_exact(http_client.clone(), &request_builder, 1, None)?;

            if blob_list.blobs.is_empty() {
                return Err(StreamError::NotFound);
            } else {
                let path = request_builder.path();
                // If provided url was a directory url and we found something - there is a matching directory.
                if path.ends_with("/") || path.is_empty() {
                    return Ok(DirEntry::Directory(request_builder.path_to_uri(path)));
                }
                let blob = blob_list
                    .blobs
                    .get(0)
                    .expect("[blob_stream_handler::get_entry] this should never fail");

                match blob {
                    BlobEntry::Blob(blob) if blob.name == path => {
                        return if blob.is_hdi_folder() {
                            Ok(DirEntry::Directory(request_builder.path_to_uri(&blob.name)))
                        } else {
                            Ok(DirEntry::Stream(blob.to_stream_info(&request_builder, arguments_record.clone())))
                        };
                    },
                    BlobEntry::BlobPrefix(directory) if directory.trim_end_matches("/") == path.trim_end_matches("/") => {
                        return Ok(DirEntry::Directory(request_builder.path_to_uri(directory)))
                    },
                    _ => {
                        uri.push('/');
                    },
                }
            }
        }

        Err(StreamError::NotFound)
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
        if uri_scheme.to_string() != self.uri_scheme.to_string() {
            panic!(
                "unable to parse {} scheme, a blob storage handler should only parse wasb and wasbs URI",
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
        if authority_parts[2] != "blob" {
            return Err(StreamError::InvalidInput {
                message: "mismatched authority information".to_string(),
                source: None,
            });
        }
        let storage_end_point = authority_parts[3..].join(".");
        let resource_id;
        match self.uri_scheme {
            BlobUriScheme::Wasb => {
                resource_id = format!(
                    "http://{}.blob.{}/{}{}",
                    authority_parts[1],
                    storage_end_point,
                    authority_parts[0],
                    uri_struct.path()
                )
            },
            BlobUriScheme::Wasbs => {
                resource_id = format!(
                    "https://{}.blob.{}/{}{}",
                    authority_parts[1],
                    storage_end_point,
                    authority_parts[0],
                    uri_struct.path()
                )
            },
        }
        return Ok(StreamInfo::new(HANDLER_TYPE, resource_id, arguments.clone()));
    }

    fn uri_scheme(&self) -> String {
        return self.uri_scheme.to_string();
    }
}

fn blob_request_success(response: Response) -> Result<Response, StreamError> {
    response.success().map_err(|e| {
        if e.status_code == 404 {
            // when authentication fail, service will return 404 - NotFound. But if it's actually NotFound, it will return an empty list.
            // so we map 404 to permission failure
            tracing::warn!("[BlobStreamHandler] blob service return 404 - not found, but this could mostly be authentication failure");
            StreamError::PermissionDenied
        } else {
            e.into()
        }
    })
}

impl DestinationHandler for BlobStreamHandler {
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
            .unwrap_or_else(|| self.fallback_credential.clone().unwrap_or(Arc::new(())));
        let should_overwrite = match if_exists {
            IfDestinationExists::MergeWithOverwrite => Ok(true),
            // For Append case, blob already exists so we don't want to create and overwrite it.
            IfDestinationExists::Append => Ok(false),
            IfDestinationExists::Fail => {
                let request_builder = RequestBuilder::new(base_path.as_ref(), credential.clone())?;
                let blob_list: BlobList = BlobStreamHandler::list_exact(self.http_client.clone(), &request_builder, 1, None)?;
                if blob_list.blobs.is_empty() {
                    Ok(false)
                } else {
                    Err(DestinationError::NotEmpty)
                }
            },
            _ => Err(DestinationError::InvalidArguments(ArgumentError::InvalidArgument {
                argument: "if_exists".to_owned(),
                expected: format!("{:#?}", IfDestinationExists::MergeWithOverwrite),
                actual: format!("{:#?}", if_exists),
            })),
        }?;
        Ok(BlobDestinationBuilder {
            credential,
            should_overwrite: should_overwrite,
            http_client: self.http_client.clone(),
        }
        .build(base_path, self.http_client.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::blob_dto::{Blob, BlobList},
        *,
    };
    use crate::credential::{access_token::ResolutionError, bearer_token::BearerToken};
    use chrono::{TimeZone, Utc};
    use fluent_assertions::*;
    use http::Method;
    use itertools::Itertools;
    use lazy_static::lazy_static;
    use maplit::hashmap;
    use rslex_core::{
        file_io::{StreamError, StreamProperties},
        SyncRecord,
    };
    use rslex_http_stream::FakeHttpClient;
    use std::collections::HashMap;

    trait IntoBlobStreamHandler {
        fn into_handler(self) -> BlobStreamHandler;
        fn into_handler_with_token_resolver(self, token_resolver: Arc<dyn ScopedAccessTokenResolver>) -> BlobStreamHandler;
    }

    impl IntoBlobStreamHandler for FakeHttpClient {
        fn into_handler(self) -> BlobStreamHandler {
            BlobStreamHandler::new(self, BlobUriScheme::Wasbs)
        }

        fn into_handler_with_token_resolver(self, token_resolver: Arc<dyn ScopedAccessTokenResolver>) -> BlobStreamHandler {
            let handler = self.into_handler();
            handler.with_access_token_resolver(token_resolver)
        }
    }

    #[derive(Debug)]
    struct DummyTokenResolver {}

    impl DummyTokenResolver {
        pub fn new() -> Self {
            DummyTokenResolver {}
        }
    }

    impl ScopedAccessTokenResolver for DummyTokenResolver {
        fn resolve_access_token(&self, scope: Scope) -> Result<BearerToken, ResolutionError> {
            Ok(BearerToken(scope.as_str().into()))
        }

        fn reset_token(&self, _scope: Scope) {}
    }

    lazy_static! {
        static ref STREAM_ACCESSOR: StreamAccessor = StreamAccessor::default();
    }

    #[test]
    fn find_streams_pattern_with_matches_returns_search_results() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/*.csv";
        let result = BlobList {
            blobs: vec![
                BlobEntry::Blob(Blob {
                    name: "somefile.csv".to_string(),
                    properties: HashMap::default(),
                    metadata: HashMap::default(),
                }),
                BlobEntry::Blob(Blob {
                    name: "somefile.csv.1".to_string(),
                    properties: HashMap::default(),
                    metadata: HashMap::default(),
                }),
            ],
            next_marker: None,
        }
        .to_string();

        FakeHttpClient::default()
            .body(result)
            .into_handler()
            .find_streams(file_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok();
    }

    #[test]
    fn parse_blob_uri() {
        let uri = "wasbs://somecontainer@someaccount.blob.core.windows.net/temp.csv";
        let expected_path = "https://someaccount.blob.core.windows.net/somecontainer/temp.csv";
        let expected_stream_info = StreamInfo::new(HANDLER_TYPE, expected_path, SyncRecord::empty());

        let handler = FakeHttpClient::default().into_handler();
        let stream_info = handler.parse_uri(uri, &SyncRecord::empty()).should().be_ok().which_value();
        assert_eq!(stream_info, expected_stream_info);
    }

    #[test]
    fn parse_blob_uri_with_empty_file_path() {
        let uri = "wasbs://somecontainer@someaccount.blob.core.windows.net/";
        let expected_path = "https://someaccount.blob.core.windows.net/somecontainer/";
        let expected_stream_info = StreamInfo::new(HANDLER_TYPE, expected_path, SyncRecord::empty());

        let handler = FakeHttpClient::default().into_handler();
        let stream_info = handler.parse_uri(uri, &SyncRecord::empty()).should().be_ok().which_value();
        assert_eq!(stream_info, expected_stream_info);
    }

    #[test]
    fn parse_blob_uri_with_invalid_uri_format() {
        let uri = "wasbs:/somecontainer@someaccount.blob.core.windows.net/";
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
    fn parse_blob_uri_with_no_scheme() {
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
    fn parse_blob_uri_with_mismatched_scheme() {
        let uri = "AmlDatastore://somecontainer@someaccount.blob.core.windows.net/";
        let handler = FakeHttpClient::default().into_handler();
        handler.parse_uri(uri, &SyncRecord::empty()).unwrap();
    }

    #[test]
    fn parse_blob_uri_with_invalid_authority_info() {
        let uri = "wasbs://somecontainer/temp.txt";
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
        let uri = "wasbs://somecontainer@someaccount.dfs.core.windows.net/";
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
    fn stream_accessor_parse_wasbs_blob_uri() {
        let uri = "wasbs://somecontainer@someaccount.blob.core.windows.net/temp.csv";
        let expected_path = "https://someaccount.blob.core.windows.net/somecontainer/temp.csv";
        let expected_stream_info = StreamInfo::new(HANDLER_TYPE, expected_path, SyncRecord::empty());

        let wasb_handler = BlobStreamHandler::new(FakeHttpClient::default(), BlobUriScheme::Wasb);
        let wasbs_handler = BlobStreamHandler::new(FakeHttpClient::default(), BlobUriScheme::Wasbs);
        let stream_accessor = StreamAccessor::default().add_handler(wasb_handler).add_handler(wasbs_handler);

        let stream_info = stream_accessor.parse_uri(uri, &SyncRecord::empty()).should().be_ok().which_value();
        assert_eq!(stream_info, expected_stream_info);
    }

    #[test]
    fn stream_accessor_parse_wasb_blob_uri() {
        let uri = "wasb://somecontainer@someaccount.blob.core.windows.net/temp.csv";
        let expected_path = "http://someaccount.blob.core.windows.net/somecontainer/temp.csv";
        let expected_stream_info = StreamInfo::new(HANDLER_TYPE, expected_path, SyncRecord::empty());

        let wasb_handler = BlobStreamHandler::new(FakeHttpClient::default(), BlobUriScheme::Wasb);
        let wasbs_handler = BlobStreamHandler::new(FakeHttpClient::default(), BlobUriScheme::Wasbs);
        let stream_accessor = StreamAccessor::default().add_handler(wasb_handler).add_handler(wasbs_handler);

        let stream_info = stream_accessor.parse_uri(uri, &SyncRecord::empty()).should().be_ok().which_value();
        assert_eq!(stream_info, expected_stream_info);
    }

    trait IntoBlobOpener {
        fn into_opener<S: AsRef<str>>(self, uri: S) -> StreamResult<Arc<(dyn StreamOpener + 'static)>>;

        fn into_opener_with_credential<S: AsRef<str>>(
            self,
            uri: S,
            credential: SyncRecord,
        ) -> StreamResult<Arc<(dyn StreamOpener + 'static)>>;

        fn into_opener_with_token_resolver<S: AsRef<str>>(self, uri: S) -> StreamResult<Arc<(dyn StreamOpener + 'static)>>;
    }

    impl IntoBlobOpener for FakeHttpClient {
        fn into_opener<S: AsRef<str>>(self, uri: S) -> StreamResult<Arc<(dyn StreamOpener + 'static)>> {
            self.into_opener_with_credential(uri, SyncRecord::empty())
        }

        fn into_opener_with_token_resolver<S: AsRef<str>>(self, uri: S) -> StreamResult<Arc<(dyn StreamOpener + 'static)>> {
            let stream_handler = self.into_handler_with_token_resolver(Arc::new(DummyTokenResolver::new()));

            stream_handler.get_opener(
                uri.as_ref(),
                SyncRecord::empty().parse().unwrap(),
                &HashMap::default(),
                &STREAM_ACCESSOR,
            )
        }

        fn into_opener_with_credential<S: AsRef<str>>(
            self,
            uri: S,
            credential: SyncRecord,
        ) -> StreamResult<Arc<(dyn StreamOpener + 'static)>> {
            let stream_handler = self.into_handler();

            stream_handler.get_opener(uri.as_ref(), credential.parse().unwrap(), &HashMap::default(), &STREAM_ACCESSOR)
        }
    }

    #[test]
    fn new_for_non_existing_blob_returns_not_found() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/noexist.csv";

        FakeHttpClient::default()
            .status(404)
            .into_opener(file_path)
            .expect("into_opener should succeed")
            .get_properties()
            .should()
            .be_err()
            .with_value(StreamError::NotFound);
    }

    #[test]
    fn new_with_authentication_failure_returns_permission_denied() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .status(403)
            .into_opener(file_path)
            .expect("into_opener should succeed")
            .get_properties()
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn new_for_existing_blob_returns_opener() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "2496")
            .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
            .into_opener(file_path)
            .should()
            .be_ok();
    }

    #[test]
    fn get_properties_for_existing_blob_returns_properties() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "2496")
            .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
            .header("x-ms-creation-time", "Thu, 04 Jul 2019 20:09:52 GMT")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .get_properties()
            .should()
            .be_ok()
            .with_value(StreamProperties {
                size: 2496,
                created_time: Some(Utc.ymd(2019, 7, 4).and_hms(20, 9, 52)),
                modified_time: Some(Utc.ymd(2019, 9, 4).and_hms(2, 29, 10)),
            });
    }

    #[test]
    fn open_return_result() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "100")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .open()
            .should()
            .be_ok();
    }

    #[test]
    fn can_seek_without_accept_range_return_false() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        let opener = FakeHttpClient::default()
            .header("Content-Length", "100")
            .into_opener(file_path)
            .expect("build opener should succeed");
        let _ = opener.get_properties().unwrap(); // this will trigger read header

        opener.can_seek().should().be_false();
    }

    #[test]
    fn can_seek_with_accept_range_return_true() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "100")
            .header("Accept-Ranges", "bytes")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .can_seek()
            .should()
            .be_true();
    }

    #[test]
    fn copy_to_copies_the_content_with_token_resolver() {
        let content = "content of the file";
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.method().should().be(&Method::HEAD);
                r.uri().should().end_with("?comp=metadata");
            }) // first request will be public blob check
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::HEAD);
            }) // second request to get metadata
            .header("Content-Length", "19")
            .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                r.method().should().be(&Method::GET);
            }) // third request to get content
            .body(content)
            .into_opener_with_token_resolver(uri)
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
    fn copy_to_copies_the_content() {
        let content = "content of the file";
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
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
    fn copy_to_with_403_return_permission_denied_with_token_resolver() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.method().should().be(&Method::HEAD);
                r.uri().should().end_with("?comp=metadata");
            }) // first request will be public blob check
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::HEAD);
            }) // second request to get metadata
            .header("Content-Length", "19")
            .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                r.method().should().be(&Method::GET);
            }) // second request
            .status(403)
            .into_opener_with_token_resolver(uri)
            .expect("build opener should succeed")
            .write_to(&mut output)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn copy_to_with_403_return_permission_denied() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
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
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "100")
            .header("Accept-Ranges", "bytes")
            .into_opener(file_path)
            .expect("build opener should succeed")
            .try_as_seekable()
            .should()
            .be_some();
    }

    #[test]
    fn copy_section_to_copies_the_section_with_token_resolver() {
        let content = "12345678901234567890";
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.method().should().be(&Method::HEAD);
                r.uri().should().end_with("?comp=metadata");
            }) // first request will be public blob check
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::GET);
                r.headers()["Range"].to_str().should().be_ok().with_value("bytes=10-29");
            })
            .body(content)
            .into_opener_with_token_resolver(uri)
            .expect("build opener should succeed")
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
    fn copy_section_to_copies_the_section() {
        let content = "12345678901234567890";
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.method().should().be(&Method::GET);
                r.headers()["Range"].to_str().should().be_ok().with_value("bytes=10-29");
            })
            .body(content)
            .into_opener(uri)
            .expect("build opener should succeed")
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
    fn copy_section_to_with_authentication_failure_return_permission_denied_with_token_resolver() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.method().should().be(&Method::HEAD);
                r.uri().should().end_with("?comp=metadata");
            }) // first request will be public blob check
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::GET);
                r.headers()["Range"].to_str().should().be_ok().with_value("bytes=10-29");
            })
            .status(403)
            .into_opener_with_token_resolver(uri)
            .expect("build opener should succeed")
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
    fn copy_section_to_with_authentication_failure_return_permission_denied() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .assert_num_requests(1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::GET);
                r.headers()["Range"].to_str().should().be_ok().with_value("bytes=10-29");
            })
            .status(403)
            .into_opener(uri)
            .expect("build opener should succeed")
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
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "100")
            .header("Accept-Ranges", "bytes")
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
    fn non_public_blob_with_public_container() {
        let content = "content of the file";
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.method().should().be(&Method::HEAD);
                r.uri().should().end_with("?comp=metadata");
            }) // first request will be public blob check
            .status(403)
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::HEAD);
                r.uri().should().end_with("somecontainer?restype=container&comp=metadata");
            }) // second request will be public container check
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                r.method().should().be(&Method::HEAD);
            }) // third request to get metadata
            .header("Content-Length", "19")
            .header("Last-Modified", "Wed, 04 Sep 2019 02:29:10 GMT")
            .with_request(|id, _| id == 3)
            .assert_request(|_, r| {
                r.method().should().be(&Method::GET);
            }) // third request to get content
            .body(content)
            .into_opener_with_token_resolver(uri)
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
    fn blob_stream_handler_as_destination_handler_returns_handler_type() {
        let handler = FakeHttpClient::default().into_handler();

        DestinationHandler::handler_type(&handler).should().equal_string(HANDLER_TYPE);
    }

    #[test]
    fn blob_stream_handler_returns_destination() {
        FakeHttpClient::default()
            .into_handler()
            .get_destination(
                "https://dataprepe2etest.blob.core.windows.net/test/upload".into(),
                SyncRecord::empty().parse().unwrap(),
                Default::default(),
                &Default::default(),
            )
            .should()
            .be_ok();
    }

    #[test]
    fn blob_stream_handler_with_append_returns_destination() {
        FakeHttpClient::default()
            .into_handler()
            .get_destination(
                "https://dataprepe2etest.blob.core.windows.net/test/upload".into(),
                SyncRecord::empty().parse().unwrap(),
                IfDestinationExists::Append,
                &Default::default(),
            )
            .should()
            .be_ok();
    }

    #[test]
    fn list_directory_returns_directory_entries() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/somefolder";
        let batch1 = BlobList {
            blobs: vec![
                BlobEntry::Blob(Blob {
                    name: "file1.bin".to_string(),
                    properties: HashMap::default(),
                    metadata: HashMap::default(),
                }),
                BlobEntry::Blob(Blob {
                    name: "file2.bin".to_string(),
                    properties: HashMap::default(),
                    metadata: HashMap::default(),
                }),
                // this blob will not be listed cause it is marked as folder
                BlobEntry::Blob(Blob {
                    name: "file3.bin".to_string(),
                    properties: HashMap::default(),
                    metadata: hashmap! {
                        "hdi_isfolder".to_owned() => "true".to_owned()
                    },
                }),
                BlobEntry::BlobPrefix("folder1".to_string()),
            ],
            next_marker: Some("token".to_string()),
        }
        .to_string();

        let batch2 = BlobList {
            blobs: vec![
                BlobEntry::Blob(Blob {
                    name: "file3.bin".to_string(),
                    properties: HashMap::default(),
                    metadata: HashMap::default(),
                }),
                BlobEntry::Blob(Blob {
                    name: "file4.bin".to_string(),
                    properties: HashMap::default(),
                    metadata: HashMap::default(),
                }),
                BlobEntry::BlobPrefix("folder2".to_string()),
            ],
            next_marker: None,
        }
        .to_string();

        let response = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .body(batch1)
            .with_request(|id, _| id == 1) // last record is a continuation token
            .body(batch2)
            .into_handler()
            .list_directory(file_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .collect_vec();

        response.should().all_be_ok().which_inner_values().should().have_length(7);
    }

    #[test]
    fn get_entry_returns_blob_info_when_found() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/somefolder/file.csv";

        let response_body = BlobList {
            blobs: vec![BlobEntry::Blob(Blob {
                name: "somefolder/file.csv".to_string(),
                properties: HashMap::default(),
                metadata: HashMap::default(),
            })],
            next_marker: None,
        }
        .to_string();

        FakeHttpClient::default()
            .body(response_body)
            .status(200)
            .into_handler()
            .get_entry(file_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .should()
            .satisfy(|entry| match entry {
                DirEntry::Stream(stream_info) => {
                    println!("{:?}; {:?}", stream_info.resource_id(), file_path);
                    true
                },
                _ => false,
            });
    }

    #[test]
    fn get_entry_returns_folder_info_when_path_ends_with_slash() {
        let folder_path = "https://someaccount.blob.core.windows.net/somecontainer/somefolder/";

        let response_body = BlobList {
            blobs: vec![BlobEntry::Blob(Blob {
                name: "somefolder/file.csv".to_string(),
                properties: HashMap::default(),
                metadata: HashMap::default(),
            })],
            next_marker: None,
        }
        .to_string();

        FakeHttpClient::default()
            .body(response_body)
            .status(200)
            .into_handler()
            .get_entry(folder_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .should()
            .satisfy(|entry| match entry {
                DirEntry::Directory(dir_path) if dir_path == folder_path => true,
                _ => false,
            });
    }

    #[test]
    fn get_entry_returns_folder_info_when_exact_matching_folder_found() {
        let folder_path = "https://someaccount.blob.core.windows.net/somecontainer/somefolder";

        let response_body = BlobList {
            blobs: vec![BlobEntry::BlobPrefix("somefolder".to_string())],
            next_marker: None,
        }
        .to_string();

        FakeHttpClient::default()
            .body(response_body)
            .status(200)
            .into_handler()
            .get_entry(folder_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .should()
            .satisfy(|entry| match entry {
                DirEntry::Directory(dir_path) if dir_path == folder_path => true,
                _ => false,
            });
    }

    #[test]
    fn get_entry_returns_not_found_error_when_no_blobs_found() {
        let folder_path = "https://someaccount.blob.core.windows.net/somecontainer/somefolder";

        let response_body = BlobList {
            blobs: vec![],
            next_marker: None,
        }
        .to_string();

        FakeHttpClient::default()
            .body(response_body)
            .status(200)
            .into_handler()
            .get_entry(folder_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_err()
            .which_value()
            .should()
            .be(StreamError::NotFound);
    }

    #[test]
    fn get_entry_checks_for_folder_match_when_no_file_was_found() {
        let folder_path = "https://someaccount.blob.core.windows.net/somecontainer/somefolder";

        // our first request to storage will return different entry higher in lexicographic order
        let file_response_body = BlobList {
            blobs: vec![BlobEntry::Blob(Blob {
                name: "somefolder!/file.csv".to_string(),
                properties: HashMap::default(),
                metadata: HashMap::default(),
            })],
            next_marker: None,
        }
        .to_string();

        // The second request should be to check for exact folder match and should succeed
        let folder_response_body = BlobList {
            blobs: vec![BlobEntry::Blob(Blob {
                name: "somefolder/file.csv".to_string(),
                properties: HashMap::default(),
                metadata: HashMap::default(),
            })],
            next_marker: None,
        }
        .to_string();

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri().to_string().should().equal("https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=somefolder&maxresults=1&delimiter=%2F&include=metadata");
            })
            .body(file_response_body)
            .status(200)
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.uri().to_string().should().equal("https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=somefolder%2F&maxresults=1&delimiter=%2F&include=metadata");
            })
            .body(folder_response_body)
            .status(200)
            .into_handler()
            .get_entry(folder_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .should()
            .satisfy(|entry| match entry {
                DirEntry::Directory(dir_path) if dir_path.eq(&format!("{}/", folder_path)) => true,
                _ => false,
            });
    }

    #[test]
    fn get_entry_treats_hdi_marked_blob_as_directory() {
        let folder_path = "https://someaccount.blob.core.windows.net/somecontainer/somefolder";

        // our first request to storage will return different entry higher in lexicographic order
        let file_response_body = BlobList {
            blobs: vec![BlobEntry::Blob(Blob {
                name: "somefolder".to_string(),
                properties: HashMap::default(),
                metadata: hashmap! {
                    "hdi_isfolder".to_string() => "true".to_string()
                },
            })],
            next_marker: None,
        }
        .to_string();

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.uri().to_string().should().equal("https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=somefolder&maxresults=1&delimiter=%2F&include=metadata");
            })
            .body(file_response_body)
            .status(200)
            .into_handler()
            .get_entry(folder_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .should()
            .satisfy(|entry| match entry {
                DirEntry::Directory(dir_path) if dir_path.eq(&format!("{}", folder_path)) => true,
                _ => false,
            });
    }

    #[test]
    /// In some cases on request "list 1" with prefix azure blob returns 0 result and continuation token.
    /// This is the special case test for this situation.
    fn get_entry_when_azure_blob_returns_zero_first_batch() {
        let folder_path = "https://someaccount.blob.core.windows.net/somecontainer/somefolder/";

        let empty_first_batch = BlobList {
            blobs: vec![],
            next_marker: Some("1".to_string()),
        }
        .to_string();
        let empty_second_batch = BlobList {
            blobs: vec![],
            next_marker: Some("2".to_string()),
        }
        .to_string();
        let batch_with_data = BlobList {
            blobs: vec![
                BlobEntry::Blob(Blob {
                    name: "somefolder!/file.csv".to_string(),
                    properties: HashMap::default(),
                    metadata: HashMap::default(),
                }),
                BlobEntry::Blob(Blob {
                    name: "somefolder!/file_2.csv".to_string(),
                    properties: HashMap::default(),
                    metadata: HashMap::default(),
                }),
                BlobEntry::Blob(Blob {
                    name: "somefolder!/file_3.csv".to_string(),
                    properties: HashMap::default(),
                    metadata: HashMap::default(),
                }),
            ],
            next_marker: Some("3".to_string()),
        }
        .to_string();

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri().to_string().should().equal("https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=somefolder%2F&maxresults=1&delimiter=%2F&include=metadata");
            })
            .body(empty_first_batch)
            .status(200)
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.uri().to_string().should().equal(&format!("https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=somefolder%2F&maxresults=1&delimiter=%2F&include=metadata&marker=1"));
            })
            .body(empty_second_batch)
            .status(200)
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                r.uri().to_string().should().equal("https://someaccount.blob.core.windows.net/somecontainer?restype=container&comp=list&prefix=somefolder%2F&maxresults=1&delimiter=%2F&include=metadata&marker=2");
            })
            .body(batch_with_data)
            .status(200)
            .into_handler()
            .get_entry(folder_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .should()
            .satisfy(|entry| match entry {
                DirEntry::Directory(dir_path) if dir_path.eq(&folder_path) => true,
                _ => false,
            });
    }
}
