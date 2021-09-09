/// Implements Display for structs resembling a directory.
///
/// ```rust
/// // In the crate root module:
/// #[macro_use]
/// extern crate rslex_core;
/// # use std::collections::HashMap;
/// # fn main() {
/// struct Directory {
///     entries: HashMap<String, ()>,
/// }
///
/// impl Directory {
///     pub fn add_entry(&mut self, identifier: impl Into<String>, entity: ()) -> () {
///         self.entries.insert(identifier.into(), entity);
///     }
/// }
/// // Implements Display for Directory
/// directory_display!(Directory, entries);
///
/// let mut dir = Directory {
///     entries: HashMap::default(),
/// };
/// dir.add_entry("Entry1", ());
/// dir.add_entry("Entry2", ());
///
/// // TODO: Reneable after debugging on Ubuntu machine, works on macos/windows
/// // assert!(format!("{}", dir) == "Directory(Entry1, Entry2)");
/// # }
/// ```
#[macro_export]
macro_rules! directory_display {
    ($dir_name:ident, $map_name:ident) => {
        impl std::fmt::Display for $dir_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let entries = itertools::join(self.$map_name.keys(), ", ");
                write!(f, "{}({})", stringify!($dir_name), entries)
            }
        }
    };
}

mod block_buffered_read;
pub(crate) mod destination_accessor;
mod file_block_downloader;
pub mod in_memory_stream_handler;
mod path_ext;
mod seekable_read;
mod stream_accessor;
mod stream_copier;
pub(crate) mod stream_result;

pub use block_buffered_read::{BlockBufferedRead, BlockFuture, FileBlockProvider, GetBlockError, GetBlockResult};
pub use destination_accessor::{
    BlockWriter, CompletionStatus, Destination, DestinationAccessor, DestinationError, DestinationHandler, EmptyArgs, IfDestinationExists,
    OutputStream, ParallelWriteError, ParallelWriter,
};
pub use file_block_downloader::FileBlockDownloader;
pub use path_ext::PathExt;
pub use seekable_read::SeekableRead;
pub use stream_accessor::{
    DirEntry, ListDirectoryResult, SearchResults, SeekableStreamOpener, StreamAccessor, StreamArguments, StreamHandler, StreamOpener,
    StreamProperties,
};
pub use stream_copier::{CopyError, StreamCopier};
pub use stream_result::{ArgumentError, MapErrToUnknown, StreamError, StreamResult};
