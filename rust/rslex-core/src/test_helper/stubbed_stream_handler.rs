// Ignoring from code coverage cause this is a test stub only
#![cfg(not(tarpaulin_include))]
use crate::{
    file_io::{DirEntry, ListDirectoryResult, SearchResults, StreamAccessor, StreamError, StreamHandler, StreamOpener, StreamResult},
    records::parse::ParsedRecord,
    SessionProperties, StreamInfo, SyncRecord,
};
use std::sync::Arc;

pub struct StubbedStreamHandler {
    get_opener_fn: Box<dyn Fn(&str) -> StreamResult<Arc<dyn StreamOpener>> + Send + Sync>,
}

impl StubbedStreamHandler {
    pub fn with_opener(self, opener: Box<dyn Fn(&str) -> StreamResult<Arc<dyn StreamOpener>> + Send + Sync>) -> StubbedStreamHandler {
        StubbedStreamHandler { get_opener_fn: opener }
    }
}

impl Default for StubbedStreamHandler {
    fn default() -> Self {
        StubbedStreamHandler {
            get_opener_fn: Box::new(|_| Err(StreamError::NotFound)),
        }
    }
}

impl StreamHandler for StubbedStreamHandler {
    fn handler_type(&self) -> &'static str {
        "RsLex.Stubbed"
    }

    type GetOpenerArguments = ();

    fn get_opener(
        &self,
        resource_id: &str,
        _arguments: ParsedRecord<Self::GetOpenerArguments>,
        _session_stream_properties: &SessionProperties,
        _accessor: &StreamAccessor,
    ) -> StreamResult<Arc<dyn StreamOpener>> {
        (self.get_opener_fn)(resource_id)
    }

    type FindStreamsArguments = ();

    fn find_streams(
        &self,
        _search_pattern: &str,
        _arguments: ParsedRecord<Self::FindStreamsArguments>,
        _accessor: &StreamAccessor,
    ) -> StreamResult<Box<dyn SearchResults>> {
        todo!()
    }

    fn list_directory(
        &self,
        _resource_id: &str,
        _arguments: ParsedRecord<Self::FindStreamsArguments>,
        _accessor: &StreamAccessor,
    ) -> StreamResult<ListDirectoryResult> {
        todo!()
    }

    fn get_entry(
        &self,
        _resource_id: &str,
        _arguments: ParsedRecord<Self::FindStreamsArguments>,
        _accessor: &StreamAccessor,
    ) -> StreamResult<DirEntry> {
        todo!()
    }

    fn parse_uri(&self, _: &str, _: &SyncRecord) -> StreamResult<StreamInfo> {
        todo!()
    }

    fn uri_scheme(&self) -> String {
        "https".to_string()
    }
}
