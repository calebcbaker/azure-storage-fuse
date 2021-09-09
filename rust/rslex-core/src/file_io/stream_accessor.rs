use super::StreamResult;
use crate::{
    file_io::{ArgumentError, SeekableRead, StreamError},
    records::parse::{ParseRecord, ParsedRecord},
    SessionProperties, StreamInfo, SyncRecord,
};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom, Write},
    sync::Arc,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamProperties {
    pub size: u64,
    pub created_time: Option<DateTime<Utc>>,
    pub modified_time: Option<DateTime<Utc>>,
}

/// Provides access to a binary stream of data.
pub trait StreamOpener: Send + Sync {
    /// Opens the stream and returns a [`Read`] that can access its data.
    fn open(&self) -> StreamResult<Box<dyn Read + Send>>;

    /// Copies the contents of the source stream to the specified destination.
    fn write_to(&self, target: &mut dyn Write) -> StreamResult<()> {
        let mut read = self.open()?;
        std::io::copy(&mut *read, target)?;
        Ok(())
    }

    /// Copies the contents of the source stream to the specified destination.
    ///
    /// Passing in a slice smaller than the source stream will result in undefined behavior.
    fn copy_to(&self, target: &mut [u8]) -> StreamResult<()> {
        let mut read = self.open()?;
        read.read_exact(target).map_err(StreamError::from)
    }

    /// Returns a [`Record`] containing properties for the source stream, such as file size and last modified time.
    fn get_properties(&self) -> StreamResult<StreamProperties>;

    /// Whether the underlying data source supports seeking within a stream of data.
    fn can_seek(&self) -> bool;

    /// If the source supports seeking, returns a [`SeekableStreamOpener`] for the underlying stream.
    fn try_as_seekable(&self) -> Option<&dyn SeekableStreamOpener>;
}

/// Provides access to a binary stream of data that supports seeking.
pub trait SeekableStreamOpener: StreamOpener {
    /// Opens the stream and returns a [`SeekableRead`] that can access its data.
    fn open_seekable(&self) -> StreamResult<Box<dyn SeekableRead>>;

    /// Copies a section of the source stream to the specified destination.
    fn copy_section_to(&self, offset: usize, target: &mut [u8]) -> StreamResult<()> {
        let mut read = self.open_seekable()?;
        read.seek(SeekFrom::Start(offset as u64))?;
        read.read_exact(target)?;

        Ok(())
    }
}

/// The results returned by [`StreamAccessor::find_streams`].
pub trait SearchResults: Send + Sync {
    /// Returns an iterator to the streams found.
    fn iter(&self) -> Box<dyn Iterator<Item = StreamResult<StreamInfo>>>;
}

/// The single item returned by [`StreamAccessor::list_directory`] iterator.
#[derive(PartialEq, Debug)]
pub enum DirEntry {
    Stream(StreamInfo),
    Directory(String),
}

/// The result returned by [`StreamAccessor::list_directory`]
pub type ListDirectoryResult = Box<dyn Iterator<Item = StreamResult<DirEntry>>>;

/// Strongly typed Record to be used as arguments to `StreamHandler` interface.
pub trait StreamArguments<'r>: ParseRecord<'r> {
    fn validate(&self, _stream_accessor: &StreamAccessor) -> Result<(), ArgumentError> {
        Ok(())
    }
}

/// Some StreamHandlers don't need arguments
impl ParseRecord<'_> for () {
    fn parse(_: &SyncRecord) -> std::result::Result<Self, ArgumentError> {
        Ok(())
    }
}

impl StreamArguments<'_> for () {}

/// Interface to interact with a source of binary streams.
///
/// StreamHandlers are used by [`StreamAccessor`] to interface with multiple data sources. As such,
/// implementing StreamHandlers is a way to extend the data sources supported by the runtime.
pub trait StreamHandler: Send + Sync {
    /// Returns a string identifier that uniquely identifies the data source represented by this StreamHandler.
    fn handler_type(&self) -> &'static str;

    type GetOpenerArguments: for<'r> StreamArguments<'r>;

    /// Returns a [`StreamOpener`] for the specified resource.
    fn get_opener(
        &self,
        resource_id: &str,
        arguments: ParsedRecord<Self::GetOpenerArguments>,
        session_stream_properties: &SessionProperties,
        accessor: &StreamAccessor,
    ) -> StreamResult<Arc<dyn StreamOpener>>;

    type FindStreamsArguments: for<'r> StreamArguments<'r>;

    /// List streams with glob pattern. It returns a [`SearchResults`].
    ///
    /// # Remark
    /// `stream_pattern` take a glob pattern.
    /// - **`?`** -- match any single character except `/`
    /// - **`*`** -- match any number of any characters except `/`
    /// - **`**`** -- match any number of any characters including `/`, so this will search across folder levels
    fn find_streams(
        &self,
        search_pattern: &str,
        arguments: ParsedRecord<Self::FindStreamsArguments>,
        accessor: &StreamAccessor,
    ) -> StreamResult<Box<dyn SearchResults>>;

    /// List single directory level.
    fn list_directory(
        &self,
        resource_id: &str,
        arguments: ParsedRecord<Self::FindStreamsArguments>,
        accessor: &StreamAccessor,
    ) -> StreamResult<ListDirectoryResult>;

    /// Gets info about a single storage entry.
    fn get_entry(
        &self,
        resource_id: &str,
        arguments: ParsedRecord<Self::FindStreamsArguments>,
        accessor: &StreamAccessor,
    ) -> StreamResult<DirEntry>;

    /// Parse uri to StreamInfo
    fn parse_uri(&self, uri: &str, arguments: &SyncRecord) -> StreamResult<StreamInfo>;

    /// Gets uri scheme
    fn uri_scheme(&self) -> String;
}

/// StreamHandler is not object-safe because of the trait associated type, so cannot be wrapped in Arc<dyn>
/// convert it to this trait, so we can keep list of StreamHandler s in StreamAccessor.
trait DynStreamHandler: Send + Sync {
    fn get_opener(
        &self,
        resource_id: &str,
        arguments: &SyncRecord,
        session_stream_properties: &SessionProperties,
        accessor: &StreamAccessor,
    ) -> StreamResult<Arc<dyn StreamOpener>>;

    fn find_streams(&self, search_pattern: &str, arguments: &SyncRecord, accessor: &StreamAccessor)
        -> StreamResult<Box<dyn SearchResults>>;

    fn validate_arguments_for_find_streams(&self, argument: &SyncRecord, stream_accessor: &StreamAccessor) -> Result<(), ArgumentError>;

    fn validate_arguments_for_get_opener(&self, argument: &SyncRecord, stream_accessor: &StreamAccessor) -> Result<(), ArgumentError>;

    fn list_directory(&self, resource_id: &str, arguments: &SyncRecord, accessor: &StreamAccessor) -> StreamResult<ListDirectoryResult>;

    fn get_entry(&self, resource_id: &str, arguments: &SyncRecord, accessor: &StreamAccessor) -> StreamResult<DirEntry>;

    fn parse_uri(&self, uri: &str, arguments: &SyncRecord) -> StreamResult<StreamInfo>;

    fn uri_scheme(&self) -> String;
}

impl std::fmt::Debug for dyn DynStreamHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DynStreamHandler({:?})", self.uri_scheme())
    }
}

impl<S: StreamHandler> DynStreamHandler for S {
    fn get_opener(
        &self,
        resource_id: &str,
        arguments: &SyncRecord,
        session_stream_properties: &SessionProperties,
        accessor: &StreamAccessor,
    ) -> StreamResult<Arc<dyn StreamOpener>> {
        self.get_opener(resource_id, arguments.parse()?, session_stream_properties, accessor)
    }

    fn find_streams(
        &self,
        search_pattern: &str,
        arguments: &SyncRecord,
        accessor: &StreamAccessor,
    ) -> StreamResult<Box<dyn SearchResults>> {
        self.find_streams(search_pattern, arguments.parse()?, accessor)
    }

    fn validate_arguments_for_find_streams(&self, arguments: &SyncRecord, stream_accessor: &StreamAccessor) -> Result<(), ArgumentError> {
        S::FindStreamsArguments::parse(arguments)?.validate(stream_accessor)
    }

    fn validate_arguments_for_get_opener(&self, arguments: &SyncRecord, stream_accessor: &StreamAccessor) -> Result<(), ArgumentError> {
        S::GetOpenerArguments::parse(arguments)?.validate(stream_accessor)
    }

    fn list_directory(&self, resource_id: &str, arguments: &SyncRecord, accessor: &StreamAccessor) -> StreamResult<ListDirectoryResult> {
        self.list_directory(resource_id, arguments.parse()?, accessor)
    }

    fn get_entry(&self, resource_id: &str, arguments: &SyncRecord, accessor: &StreamAccessor) -> StreamResult<DirEntry> {
        self.get_entry(resource_id, arguments.parse()?, accessor)
    }

    fn parse_uri(&self, uri: &str, arguments: &SyncRecord) -> StreamResult<StreamInfo> {
        self.parse_uri(uri, arguments)
    }

    fn uri_scheme(&self) -> String {
        self.uri_scheme()
    }
}

/// Provides access to streams of data from multiple sources through different [`StreamHandlers`](StreamHandler).
#[derive(Default)]
pub struct StreamAccessor {
    handlers: HashMap<String, Arc<dyn DynStreamHandler>>,
    scheme_handler_mapping: HashMap<String, Arc<dyn DynStreamHandler>>,
}

impl std::fmt::Debug for StreamAccessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamAccessor({:?})", self.handlers)
    }
}

impl StreamAccessor {
    pub fn add_handler(mut self, handler: impl StreamHandler + 'static) -> Self {
        let handler_type = handler.handler_type();
        let handler_scheme = handler.uri_scheme();
        let handler_reference = Arc::new(handler);
        self.handlers.insert(handler_type.to_string(), handler_reference.clone());
        self.scheme_handler_mapping.insert(handler_scheme, handler_reference.clone());
        self
    }

    /// Find streams matching the desired `search_pattern` using the `handler` specified.
    ///
    /// # Remarks
    /// `stream_pattern` take a glob pattern.
    /// - **`?`** -- match any single character except `/`
    /// - **`*`** -- match any number of any characters except `/`
    /// - **`**`** -- match any number of any characters including `/`, so this will search across folder levels
    pub fn find_streams(&self, handler: &str, search_pattern: &str, arguments: &SyncRecord) -> StreamResult<Box<dyn SearchResults>> {
        match self.handlers.get(handler) {
            Some(stream_handler) => stream_handler.find_streams(search_pattern, arguments, self),
            None => Err(StreamError::NoHandler(handler.to_string())),
        }
    }

    /// Returns a [`StreamOpener`] for the specified [`StreamInfo`].
    pub fn get_opener(&self, stream_info: &StreamInfo) -> StreamResult<Arc<dyn StreamOpener>> {
        match self.handlers.get(stream_info.handler()) {
            Some(stream_handler) => stream_handler.get_opener(
                stream_info.resource_id(),
                stream_info.arguments(),
                stream_info.session_properties(),
                self,
            ),
            None => Err(StreamError::NoHandler(stream_info.handler().to_string())),
        }
    }

    pub fn validate_arguments_for_find_streams(&self, handler: &str, arguments: &SyncRecord) -> Result<(), ArgumentError> {
        match self.handlers.get(handler) {
            Some(stream_handler) => stream_handler.validate_arguments_for_find_streams(arguments, self),
            None => Err(ArgumentError::InvalidArgument {
                argument: "handler".to_owned(),
                expected: self.handlers.keys().join("|"),
                actual: handler.to_owned(),
            }),
        }
    }

    pub fn validate_arguments_for_get_opener(&self, handler: &str, arguments: &SyncRecord) -> Result<(), ArgumentError> {
        match self.handlers.get(handler) {
            Some(stream_handler) => stream_handler.validate_arguments_for_get_opener(arguments, self),
            None => Err(ArgumentError::InvalidArgument {
                argument: "handler".to_owned(),
                expected: self.handlers.keys().join("|"),
                actual: handler.to_owned(),
            }),
        }
    }

    pub fn list_directory(&self, handler: &str, resource_id: &str, arguments: &SyncRecord) -> StreamResult<ListDirectoryResult> {
        match self.handlers.get(handler) {
            Some(stream_handler) => stream_handler.list_directory(resource_id, arguments, self),
            None => Err(StreamError::NoHandler(handler.to_string())),
        }
    }

    pub fn get_entry(&self, handler: &str, resource_id: &str, arguments: &SyncRecord) -> StreamResult<DirEntry> {
        match self.handlers.get(handler) {
            Some(stream_handler) => stream_handler.get_entry(resource_id, arguments, self),
            None => Err(StreamError::NoHandler(handler.to_string())),
        }
    }

    pub fn parse_uri(&self, uri: &str, arguments: &SyncRecord) -> StreamResult<StreamInfo> {
        let uri_parts: Vec<&str> = uri.split("://").collect();
        if uri_parts.len() != 2 {
            return Err(StreamError::InvalidInput {
                message: "invalid uri format".to_string(),
                source: None,
            });
        }
        let uri_scheme = uri_parts[0];
        match self.scheme_handler_mapping.get(uri_scheme) {
            Some(handler) => handler.parse_uri(uri, arguments),
            None => Err(StreamError::InvalidUriScheme),
        }
    }
}

directory_display!(StreamAccessor, handlers);

struct CachedSearchResultIterator {
    streams: Arc<Vec<StreamInfo>>,
    index: Option<usize>,
}

impl Iterator for CachedSearchResultIterator {
    type Item = StreamResult<StreamInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        self.index = Some(self.index.map_or(0, |i| i + 1));
        if self.index.unwrap() < self.streams.len() {
            Some(Ok(self.streams[self.index.unwrap()].clone()))
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct CachedSearchResults {
    streams: Arc<Vec<StreamInfo>>,
}

impl CachedSearchResults {
    pub fn new(streams: Vec<StreamInfo>) -> CachedSearchResults {
        CachedSearchResults {
            streams: Arc::new(streams),
        }
    }
}

impl SearchResults for CachedSearchResults {
    fn iter(&self) -> Box<dyn Iterator<Item = StreamResult<StreamInfo>>> {
        Box::new(CachedSearchResultIterator {
            streams: self.streams.clone(),
            index: None,
        })
    }
}

impl From<Vec<StreamInfo>> for Box<dyn SearchResults> {
    fn from(results: Vec<StreamInfo>) -> Self {
        Box::new(CachedSearchResults::new(results))
    }
}
