use crate::{
    file_io::{
        stream_accessor::{DirEntry, ListDirectoryResult},
        MapErrToUnknown, PathExt, SearchResults, SeekableRead, SeekableStreamOpener, StreamAccessor, StreamError, StreamHandler,
        StreamProperties, StreamResult,
    },
    iterator_extensions::SharedVecIter,
    records::parse::ParsedRecord,
    SessionProperties, StreamInfo, StreamOpener, SyncRecord,
};
use itertools::Itertools;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    io::{Cursor, Read, Write},
    ops::Deref,
    sync::Arc,
};

pub const HANDLER_TYPE: &str = "Microsoft.RsLex.InMemoryStreamHandler";
pub const HANDLER_SCHEME: &str = "inmemory";

#[derive(Debug)]
struct Results {
    results: Arc<Vec<StreamResult<StreamInfo>>>,
}

impl SearchResults for Results {
    fn iter(&self) -> Box<dyn Iterator<Item = StreamResult<StreamInfo>>> {
        Box::new(SharedVecIter::new(self.results.clone()))
    }
}

/// the class is more a tool to mock the stream accessor
/// without actual storage-system access. Keep this as public
/// API as other crate might need this as well.
#[derive(Debug, Clone)]
pub struct InMemoryStreamHandler {
    streams: Vec<(String, StreamResult<Arc<Vec<u8>>>)>,
    error_patterns: HashMap<String, StreamError>,
    allow_seeking: bool,
}

impl InMemoryStreamHandler {
    pub fn new() -> InMemoryStreamHandler {
        InMemoryStreamHandler {
            streams: vec![],
            error_patterns: HashMap::new(),
            allow_seeking: true,
        }
    }

    pub fn set_seekable(mut self, seekable: bool) -> InMemoryStreamHandler {
        self.allow_seeking = seekable;

        self
    }

    pub fn add_stream<S: Into<String>>(mut self, resource_id: S, stream_result: StreamResult<S>) -> InMemoryStreamHandler {
        self.streams
            .push((resource_id.into(), stream_result.map(|s| Arc::new(s.into().into_bytes()))));

        self
    }

    pub fn add_stream_non_utf8<S: Into<String>>(mut self, resource_id: S, stream_result: StreamResult<Vec<u8>>) -> InMemoryStreamHandler {
        self.streams.push((resource_id.into(), stream_result.map(Arc::new)));

        self
    }

    pub fn add_error_pattern<S: Into<String>>(mut self, pattern: S, error: StreamError) -> InMemoryStreamHandler {
        self.error_patterns.insert(pattern.into(), error);

        self
    }

    pub fn remove_stream<S: AsRef<str>>(mut self, resource_id: S) -> InMemoryStreamHandler {
        let (stream_idx, _) = self
            .streams
            .iter()
            .find_position(|(r, _)| r.as_str() == resource_id.as_ref())
            .unwrap();
        let _ = self.streams.remove(stream_idx);
        self
    }

    pub fn into_stream_accessor(self) -> StreamAccessor {
        StreamAccessor::default().add_handler(self)
    }
}

impl Default for InMemoryStreamHandler {
    fn default() -> Self {
        InMemoryStreamHandler::new()
    }
}

impl StreamHandler for InMemoryStreamHandler {
    fn handler_type(&self) -> &'static str {
        HANDLER_TYPE
    }

    type GetOpenerArguments = ();

    fn get_opener(
        &self,
        resource_id: &str,
        _: ParsedRecord<()>,
        _: &SessionProperties,
        _: &StreamAccessor,
    ) -> StreamResult<Arc<dyn StreamOpener>> {
        match self.streams.iter().find(|s| s.0.as_str() == resource_id) {
            Some(entry) => entry
                .1
                .clone()
                .map::<Arc<dyn StreamOpener>, _>(|data| Arc::new(InMemoryStreamOpener::new(data).set_seekable(self.allow_seeking))),
            None => Err(StreamError::NotFound),
        }
    }

    type FindStreamsArguments = ();

    fn find_streams(&self, search_pattern: &str, arguments: ParsedRecord<()>, _: &StreamAccessor) -> StreamResult<Box<dyn SearchResults>> {
        if let Some(error) = self.error_patterns.get(search_pattern) {
            Err(error.clone())
        } else {
            let search_pattern = search_pattern.to_regex();
            let matching_streams = self
                .streams
                .iter()
                .filter(|entry| search_pattern.is_match(entry.0.as_str()))
                .map(|entry| {
                    if entry.1.is_ok() {
                        Ok(StreamInfo::new(HANDLER_TYPE, entry.0.as_str(), arguments.get_record().clone()))
                    } else {
                        Err(entry.1.clone().unwrap_err())
                    }
                })
                .collect_vec();
            if matching_streams.is_empty() {
                Err(StreamError::NotFound)
            } else {
                Ok(Box::new(Results {
                    results: Arc::new(matching_streams),
                }))
            }
        }
    }

    fn list_directory(
        &self,
        resource_id: &str,
        arguments: ParsedRecord<Self::FindStreamsArguments>,
        _: &StreamAccessor,
    ) -> StreamResult<ListDirectoryResult> {
        if let Some(error) = self.error_patterns.get(resource_id) {
            Err(error.clone())
        } else {
            let mut found_dirs = HashSet::new();
            let resource_id = resource_id.trim_end_matches("/");
            let matching_streams = self
                .streams
                .iter()
                .filter(|entry| entry.0.starts_with(&format!("{}/", resource_id)))
                .filter_map(move |entry| {
                    let split_parts = entry.0[resource_id.len()..].split("/").skip_while(|p| p.is_empty()).collect_vec();
                    if split_parts.len() <= 1 {
                        Some(Ok(DirEntry::Stream(StreamInfo::new(
                            HANDLER_TYPE,
                            entry.0.as_str(),
                            arguments.get_record().clone(),
                        ))))
                    } else {
                        let dir_resource_id = format!("{}/{}", resource_id, split_parts[0]);
                        if found_dirs.contains(&dir_resource_id) {
                            None
                        } else {
                            found_dirs.insert(dir_resource_id.clone());
                            Some(Ok(DirEntry::Directory(dir_resource_id)))
                        }
                    }
                })
                .collect_vec();

            if matching_streams.is_empty() {
                Err(StreamError::NotFound)
            } else {
                Ok(Box::new(matching_streams.into_iter()))
            }
        }
    }

    fn get_entry(
        &self,
        resource_id: &str,
        arguments: ParsedRecord<Self::FindStreamsArguments>,
        _: &StreamAccessor,
    ) -> StreamResult<DirEntry> {
        let (file_resource_id, dir_resource_id): (Option<&str>, Cow<str>) = if resource_id.ends_with("/") {
            (None, resource_id.into())
        } else {
            (Some(resource_id), format!("{}/", resource_id).into())
        };

        let matches = self
            .streams
            .iter()
            .filter(|(path, _)| file_resource_id.map_or(true, |f| f == path) || path.starts_with(dir_resource_id.as_ref()))
            .collect_vec();

        match matches.len() {
            0 => Err(StreamError::NotFound),
            _ if file_resource_id.is_some() => {
                let file_match = matches.iter().filter(|(path, _)| path == file_resource_id.unwrap()).next();
                Ok(file_match
                    .map(|(path, _)| DirEntry::Stream(StreamInfo::new(HANDLER_TYPE, path.as_str(), arguments.get_record().clone())))
                    .unwrap_or_else(|| DirEntry::Directory(dir_resource_id.trim_end_matches("/").to_string())))
            },
            _ => Ok(DirEntry::Directory(dir_resource_id.trim_end_matches("/").to_string())),
        }
    }

    fn parse_uri(&self, uri: &str, arguments: &SyncRecord) -> StreamResult<StreamInfo> {
        let uri_parts: Vec<&str> = uri.split("://").collect();
        debug_assert!(
            {
                if uri_parts.len() != 2 {
                    return Err(StreamError::InvalidInput {
                        message: "invalid uri format".to_string(),
                        source: None,
                    });
                }
                let uri_scheme = uri_parts[0];
                if uri_scheme.to_string() != HANDLER_SCHEME.to_string() {
                    false
                } else {
                    true
                }
            },
            "InMemoryStreamHandler should only parse 'inmemory' URI"
        );

        Ok(StreamInfo::new(HANDLER_TYPE, uri_parts[1].trim(), arguments.clone()))
    }

    fn uri_scheme(&self) -> String {
        return HANDLER_SCHEME.to_string();
    }
}

///
/// InMemoryStreamOpener - cache content of file in memory and wrapper into a StreamOpener

/// Keep data in Arc<Vec<u8>> so it can be used in other places as well.
/// Arc<Vec<u8>> doesn't impl AsRef<[u8]>, which is required by Cursor<>.
/// So wrapper it in OpenerData and put a AsRef impl
#[derive(Clone, Debug)]
struct OpenerData(Arc<Vec<u8>>);

impl Deref for OpenerData {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for OpenerData {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

#[derive(Debug)]
pub struct InMemoryStreamOpener {
    data: OpenerData,
    can_seek: bool,
}

impl InMemoryStreamOpener {
    pub fn new(data: impl Into<Arc<Vec<u8>>>) -> Self {
        InMemoryStreamOpener {
            data: OpenerData(data.into()),
            can_seek: true,
        }
    }

    pub fn set_seekable(mut self, seekable: bool) -> Self {
        self.can_seek = seekable;

        self
    }
}

impl StreamOpener for InMemoryStreamOpener {
    fn open(&self) -> StreamResult<Box<dyn Read + Send>> {
        Ok(Box::new(Cursor::new(self.data.clone())))
    }

    fn copy_to(&self, target: &mut [u8]) -> StreamResult<()> {
        target.copy_from_slice(self.data.as_slice());
        Ok(())
    }

    fn get_properties(&self) -> StreamResult<StreamProperties> {
        Ok(StreamProperties {
            size: self.data.len() as u64,
            created_time: None,
            modified_time: None,
        })
    }

    fn can_seek(&self) -> bool {
        self.can_seek
    }

    fn try_as_seekable(&self) -> Option<&dyn SeekableStreamOpener> {
        if self.can_seek {
            Some(self)
        } else {
            None
        }
    }
}

impl SeekableStreamOpener for InMemoryStreamOpener {
    fn open_seekable(&self) -> Result<Box<dyn SeekableRead>, StreamError> {
        let cursor = Cursor::new(self.data.clone());
        Ok(Box::new(cursor))
    }

    fn copy_section_to(&self, offset: usize, mut target: &mut [u8]) -> StreamResult<()> {
        let slice = &self.data[offset..offset + target.len()];
        target.write_all(slice).map_err_to_unknown()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{file_io::stream_accessor::DirEntry, sync_record};
    use fluent_assertions::*;
    use lazy_static::lazy_static;

    lazy_static! {
        static ref STREAM_ACCESSOR: StreamAccessor = StreamAccessor::default();
    }

    #[test]
    fn list_directory_should_list_dir_level() {
        let arguments = sync_record! {
            "handler" => HANDLER_TYPE
        };
        let handler = InMemoryStreamHandler::new()
            .add_stream("/dir1/subdir1/file1.txt", Ok("content"))
            .add_stream("/dir1/subdir1/file2.txt", Ok("content"))
            .add_stream("/dir1/subdir1/file3.txt", Ok("content"))
            .add_stream("/dir1/subdir2/file1.txt", Ok("content"))
            .add_stream("/dir1/subdir2/subdir3/file1.txt", Ok("content"))
            .add_stream("/dir1/dir_file.txt", Ok("content"))
            .add_stream("/dir2/dir_file.txt", Ok("content"))
            .add_stream("/root_file.txt", Ok("content"))
            .add_stream("/dir1_subdir/file1.txt", Ok("content"));

        let result = handler.list_directory("/dir1", arguments.parse().unwrap(), &STREAM_ACCESSOR);
        let result = result.should().be_ok().which_value().map(|d| d.unwrap()).collect_vec();

        result
            .should()
            .have_length(3)
            .and()
            .should()
            .contain_all_of(&vec![
                DirEntry::Directory("/dir1/subdir1".to_string()),
                DirEntry::Directory("/dir1/subdir2".to_string()),
            ])
            .and()
            .should()
            .contain_item_that(|e| {
                if let DirEntry::Stream(e) = e {
                    e.resource_id() == "/dir1/dir_file.txt"
                } else {
                    false
                }
            });
    }

    #[test]
    fn list_directory_should_fail_when_called_on_file() {
        let arguments = sync_record! {
            "handler" => HANDLER_TYPE
        };
        let handler = InMemoryStreamHandler::new().add_stream("/dir1/subdir1/file1.txt", Ok("content"));

        handler
            .list_directory("/dir1/subdir1/file1.txt", arguments.parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_err()
            .which_value()
            .should()
            .equal(&StreamError::NotFound);
    }

    #[test]
    fn get_entry_should_fail_when_no_entry_were_found() {
        let arguments = sync_record! {
            "handler" => HANDLER_TYPE
        };
        let handler = InMemoryStreamHandler::new().add_stream("/dir1/subdir1/file1.txt", Ok("content"));

        handler
            .get_entry("/dir2/file.bin", arguments.parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_err()
            .which_value()
            .should()
            .equal(&StreamError::NotFound);
    }

    #[test]
    fn get_entry_should_return_stream_info_if_file_exists() {
        let arguments = sync_record! {
            "handler" => HANDLER_TYPE
        };
        let handler = InMemoryStreamHandler::new()
            .add_stream("/dir1/subdir1/file1.txt", Ok("content"))
            .add_stream("/dir1/subdir1/file1.txt/file2.txt", Ok("content"));

        handler
            .get_entry("/dir1/subdir1/file1.txt", arguments.parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .should()
            .satisfy(|entry| match entry {
                DirEntry::Directory(_) => false,
                DirEntry::Stream(stream) => stream.resource_id() == "/dir1/subdir1/file1.txt",
            });
    }

    #[test]
    fn get_entry_should_return_dir_entry_if_exists_and_no_matching_file() {
        let arguments = sync_record! {
            "handler" => HANDLER_TYPE
        };
        let handler = InMemoryStreamHandler::new()
            .add_stream("/dir1/subdir1/file1.txt", Ok("content"))
            .add_stream("/dir1/subdir1/file1.txt/file2.txt", Ok("content"));

        handler
            .get_entry("/dir1/subdir1", arguments.parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal(&DirEntry::Directory("/dir1/subdir1".to_string()));
    }

    #[test]
    fn get_entry_should_return_dir_entry_if_mathcing_file_exists_but_directory_requested() {
        let arguments = sync_record! {
            "handler" => HANDLER_TYPE
        };
        let handler = InMemoryStreamHandler::new()
            .add_stream("/dir1/subdir1/file1.txt", Ok("content"))
            .add_stream("/dir1/subdir1/file1.txt/file2.txt", Ok("content"));

        handler
            .get_entry("/dir1/subdir1/file1.txt/", arguments.parse().unwrap(), &STREAM_ACCESSOR)
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal(&DirEntry::Directory("/dir1/subdir1/file1.txt".to_string()));
    }
}
