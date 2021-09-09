use super::{request_builder::RequestBuilder, HANDLER_TYPE};
use crate::{
    create_stream_opener, http_stream_handler::HttpUriScheme, HeadRequest, HttpClient, ResponseExt, SearchResults, StreamAccessor,
    StreamHandler, StreamInfo, StreamOpener, StreamResult,
};
use rslex_core::{
    file_io::{DirEntry, ListDirectoryResult, StreamError},
    records::parse::ParsedRecord,
    SessionProperties, SyncRecord,
};
use std::{collections::HashMap, iter, sync::Arc};

pub(super) struct HttpStreamHandler {
    http_client: Arc<dyn HttpClient>,
    uri_scheme: HttpUriScheme,
}

impl HttpStreamHandler {
    pub fn new(http_client: impl HttpClient, uri_scheme: HttpUriScheme) -> Self {
        HttpStreamHandler {
            http_client: Arc::new(http_client),
            uri_scheme,
        }
    }
}

impl StreamHandler for HttpStreamHandler {
    fn handler_type(&self) -> &'static str {
        HANDLER_TYPE
    }

    type GetOpenerArguments = ();

    fn get_opener(
        &self,
        uri: &str,
        _: ParsedRecord<Self::GetOpenerArguments>,
        session_properties: &SessionProperties,
        _: &StreamAccessor,
    ) -> StreamResult<Arc<(dyn StreamOpener + 'static)>> {
        // lazy to making http connections, this function won't make any http call
        Ok(Arc::new(create_stream_opener(
            RequestBuilder::new(uri)?,
            self.http_client.clone(),
            session_properties.clone(),
        )))
    }

    type FindStreamsArguments = ();

    fn find_streams(
        &self,
        uri: &str,
        arguments: ParsedRecord<Self::FindStreamsArguments>,
        accessor: &StreamAccessor,
    ) -> StreamResult<Box<(dyn SearchResults + 'static)>> {
        let stream_info_entry = self
            .list_directory(uri, arguments, accessor)?
            .nth(0)
            .unwrap_or(Err(StreamError::NotFound))?;

        struct Single(StreamInfo);

        impl SearchResults for Single {
            fn iter(&self) -> Box<dyn Iterator<Item = StreamResult<StreamInfo>>> {
                box iter::once(Ok(self.0.clone()))
            }
        }

        if let DirEntry::Stream(stream_info) = stream_info_entry {
            Ok(box Single(stream_info))
        } else {
            panic!("[http-stream-handler::find_streams] list_directory for http source should always return a single stream source");
        }
    }

    fn list_directory(
        &self,
        resource_id: &str,
        arguments: ParsedRecord<Self::FindStreamsArguments>,
        _: &StreamAccessor,
    ) -> StreamResult<ListDirectoryResult> {
        let mut session_properties = HashMap::default();
        let request_builder = RequestBuilder::new(resource_id)?;
        let request = request_builder.head();
        let response = self.http_client.clone().request(request.into())?.success()?;
        RequestBuilder::parse_response(response, &mut session_properties)?;

        let stream_info =
            StreamInfo::new(HANDLER_TYPE, resource_id, arguments.get_record().clone()).with_session_properties(session_properties);

        Ok(box iter::once(Ok(DirEntry::Stream(stream_info))))
    }

    fn get_entry(
        &self,
        resource_id: &str,
        arguments: ParsedRecord<Self::FindStreamsArguments>,
        accessor: &StreamAccessor,
    ) -> StreamResult<DirEntry> {
        self.list_directory(resource_id, arguments, accessor)?
            .next()
            .map_or(Err(StreamError::NotFound), |e| e)
    }

    fn parse_uri(&self, uri: &str, _arguments: &SyncRecord) -> StreamResult<StreamInfo> {
        Ok(StreamInfo::new(HANDLER_TYPE, uri, SyncRecord::empty()))
    }

    fn uri_scheme(&self) -> String {
        return self.uri_scheme.to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{http_stream_handler::HttpUriScheme, FakeHttpClient, SessionPropertiesExt, StreamError, StreamInfo, SyncRecord};
    use chrono::{TimeZone, Utc};
    use fluent_assertions::*;
    use lazy_static::lazy_static;
    use rslex_core::file_io::StreamProperties;
    use std::collections::HashMap;

    trait IntoHttpStreamHandler {
        fn into_stream_handler(self) -> HttpStreamHandler;
    }

    impl IntoHttpStreamHandler for FakeHttpClient {
        fn into_stream_handler(self) -> HttpStreamHandler {
            HttpStreamHandler::new(self, HttpUriScheme::Https)
        }
    }

    lazy_static! {
        static ref STREAM_ACCESSOR: StreamAccessor = StreamAccessor::default();
    }

    #[test]
    fn find_streams_pattern_return_itself_if_exist() {
        let file_path = "https://somesite/somepath/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "200")
            .header("Last-Modified", "Thu, 04 Jul 2019 20:09:52 GMT")
            .header("Accept-Ranges", "bytes")
            .into_stream_handler()
            .find_streams(file_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .iter()
            .should()
            .be_single()
            .which_value()
            .should()
            .be_ok()
            .with_value(StreamInfo::new("Http", file_path, SyncRecord::empty()));
    }

    #[test]
    fn find_streams_pattern_include_session_properties() {
        let file_path = "https://somesite/somepath/somefile.csv";

        FakeHttpClient::default()
            .header("Content-Length", "200")
            .header("Last-Modified", "Thu, 04 Jul 2019 20:09:52 GMT")
            .header("Accept-Ranges", "bytes")
            .into_stream_handler()
            .find_streams(file_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .iter()
            .should()
            .be_single()
            .which_value()
            .should()
            .be_ok()
            .which_value()
            .session_properties()
            .should()
            .pass(|sp| {
                sp.stream_properties().should().be_some().with_value(StreamProperties {
                    size: 200,
                    created_time: None,
                    modified_time: Some(Utc.ymd(2019, 7, 4).and_hms(20, 9, 52)),
                });
                sp.is_seekable().should().be_some().with_value(true);
            });
    }

    #[test]
    fn find_streams_pattern_return_not_found_if_not_exist() {
        let file_path = "https://somesite/somepath/somefile.csv";

        FakeHttpClient::default()
            .status(404)
            .into_stream_handler()
            .find_streams(file_path, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_err()
            .with_value(StreamError::NotFound);
    }

    trait IntoOpener {
        fn into_opener<S: AsRef<str>>(self, uri: S, session_properties: SessionProperties)
            -> StreamResult<Arc<dyn StreamOpener + 'static>>;
    }

    impl IntoOpener for FakeHttpClient {
        fn into_opener<S: AsRef<str>>(
            self,
            uri: S,
            session_properties: SessionProperties,
        ) -> StreamResult<Arc<dyn StreamOpener + 'static>> {
            self.into_stream_handler().get_opener(
                uri.as_ref(),
                SyncRecord::empty().parse().unwrap(),
                &session_properties,
                &STREAM_ACCESSOR,
            )
        }
    }

    #[test]
    fn new_for_existing_file_returns_opener() {
        let file_path = "https://somesite/somepath/somefile.csv";

        FakeHttpClient::default()
            .into_opener(file_path, HashMap::default())
            .should()
            .be_ok();
    }

    #[test]
    fn get_properties_for_existing_file_returns_properties() {
        let file_path = "https://somesite/somepath/somefile.csv";

        FakeHttpClient::default()
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .into_opener(file_path, HashMap::default())
            .expect("build opener should succeed")
            .get_properties()
            .expect("get properties should succeed")
            .should()
            .be(StreamProperties {
                size: 200,
                created_time: None,
                modified_time: Some(Utc.ymd(2019, 7, 9).and_hms(17, 51, 15)),
            });
    }

    #[test]
    fn get_properties_with_session_properties() {
        let file_path = "https://somesite/somepath/somefile.csv";

        // in this case it won't trigger http calls
        FakeHttpClient::default()
            .into_opener(
                file_path,
                HashMap::default()
                    .with_size(200)
                    .with_modified_time(Utc.ymd(2019, 7, 9).and_hms(17, 51, 15)),
            )
            .expect("build opener should succeed")
            .get_properties()
            .expect("get properties should succeed")
            .should()
            .be(StreamProperties {
                size: 200,
                created_time: None,
                modified_time: Some(Utc.ymd(2019, 7, 9).and_hms(17, 51, 15)),
            });
    }

    #[test]
    fn open_return_result() {
        let file_path = "https://somesite/somepath/somefile.csv";

        FakeHttpClient::default()
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .into_opener(file_path, HashMap::default())
            .expect("build opener should succeed")
            .open()
            .should()
            .be_ok();
    }

    #[test]
    fn can_seek_return_true_with_accept_range() {
        let file_path = "https://somesite/somepath/somefile.csv";

        FakeHttpClient::default()
            .into_opener(file_path, HashMap::default().with_is_seekable(true))
            .expect("build opener should succeed")
            .can_seek()
            .should()
            .be_true();
    }

    #[test]
    fn write_to_copies_the_content() {
        let content = "content of the file ";
        let file_path = "https://somesite/somepath/somefile.csv";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .with_request(|id, _| id == 0) // first request
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "20")
            .header("x-ms-resource-type", "file")
            .with_request(|id, _| id == 1) // second request
            .body(content)
            .into_opener(file_path, HashMap::default())
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
    fn write_to_if_not_seekable_copies_the_content() {
        let content = "content of the file";
        let file_path = "https://somesite/somepath/somefile.csv";
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
            .into_opener(file_path, HashMap::default())
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
    fn write_to_with_401_return_permission_denied() {
        let file_path = "https://somesite/somepath/somefile.csv";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .with_request(|id, _| id == 0) // first request
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .header("x-ms-resource-type", "file")
            .with_request(|id, _| id == 1) // second request
            .status(401)
            .into_opener(file_path, HashMap::default())
            .expect("build opener should succeed")
            .write_to(&mut output)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn copy_section_to_copies_the_section() {
        let content = "content of the file!";
        let file_path = "https://somesite/somepath/somefile.csv";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.headers()["Range"].to_str().should().be_ok().with_value("bytes=10-29");
            })
            .body(content)
            .into_opener(file_path, HashMap::default().with_is_seekable(true))
            .expect("build opener should succeed")
            .try_as_seekable()
            .expect("return seekable should be ok")
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
        let file_path = "https://somesite/somepath/somefile.csv";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.headers()["Range"].to_str().should().be_ok().with_value("bytes=10-29");
            })
            .status(401)
            .into_opener(file_path, HashMap::default().with_is_seekable(true))
            .expect("build opener should succeed")
            .try_as_seekable()
            .expect("return seekable should be ok")
            .copy_section_to(10, &mut output)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn open_seekable_return_result() {
        let file_path = "https://somesite/somepath/somefile.csv";

        FakeHttpClient::default()
            .header("Last-Modified", "Tue, 09 Jul 2019 17:51:15 GMT")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", "200")
            .header("x-ms-resource-type", "file")
            .into_opener(file_path, HashMap::default().with_is_seekable(true))
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
    fn parse_http_stream_uri() {
        let expected_file_path = "https://somesite/somepath/somefile.csv";
        let handler = FakeHttpClient::default().into_stream_handler();
        let stream_info = handler.parse_uri(expected_file_path, &SyncRecord::empty()).unwrap();
        assert_eq!(stream_info.resource_id(), expected_file_path);
        assert_eq!(stream_info.handler(), HANDLER_TYPE);
    }

    #[test]
    fn stream_accessor_call_parse_http_stream_uri() {
        let expected_file_path = "https://somesite/somepath/somefile.csv";
        let handler = FakeHttpClient::default().into_stream_handler();
        let stream_accessor = StreamAccessor::default().add_handler(handler);
        let stream_info = stream_accessor.parse_uri(expected_file_path, &SyncRecord::empty()).unwrap();
        assert_eq!(stream_info.resource_id(), expected_file_path);
        assert_eq!(stream_info.handler(), HANDLER_TYPE);
    }
}
