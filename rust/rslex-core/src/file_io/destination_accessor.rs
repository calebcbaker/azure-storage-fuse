use crate::{
    binary_buffer_pool::PooledBuffer,
    file_io::{ArgumentError, StreamError},
    ExternalError, StreamInfo, SyncRecord,
};
use derivative::Derivative;
#[cfg(any(feature = "test_helper", test))]
use fluent_assertions::EnumAssertions;
use std::{
    borrow::Cow,
    collections::HashMap,
    convert::{TryFrom, TryInto},
    fmt::Debug,
    io::{Error as IOError, ErrorKind, Write},
    sync::Arc,
};
use thiserror::Error;

#[derive(Clone, Debug, Error, Derivative)]
#[derivative(PartialEq)]
pub enum DestinationError {
    #[error("Destination handler not found for {0}.")]
    NoHandler(String),
    #[error("Error while parsing destination arguments {0}.")]
    InvalidArguments(#[from] ArgumentError),
    #[error("The specified destination was not found.")]
    NotFound,
    #[error("Destination handler authentication error: {0}.")]
    AuthenticationError(String),
    #[error("Permission denied while trying to write: {0:?}.")]
    PermissionDenied(#[derivative(PartialEq = "ignore")] Option<ExternalError>),
    #[error("Connection to destination failed.")]
    ConnectionFailure {
        #[derivative(PartialEq = "ignore")]
        source: Option<ExternalError>,
    },
    #[error("Invalid input.")]
    InvalidInput {
        message: String,
        #[derivative(PartialEq = "ignore")]
        source: Option<ExternalError>,
    },
    #[error("Destination path is not empty.")]
    NotEmpty,
    #[error("An unknown error occurred: {0}. {1:?}.")]
    Unknown(
        String,
        #[derivative(PartialEq = "ignore")]
        #[source]
        Option<ExternalError>,
    ),
}

impl From<IOError> for DestinationError {
    fn from(e: IOError) -> Self {
        tracing::error!(
            error = ?e,
            "[DestinationError::from::IOError]"
        );
        match e.kind() {
            ErrorKind::NotFound => DestinationError::NotFound,
            ErrorKind::PermissionDenied => DestinationError::PermissionDenied(Some(Arc::new(e))),
            _ => DestinationError::Unknown(e.to_string(), Some(Arc::new(e))),
        }
    }
}

impl From<StreamError> for DestinationError {
    fn from(e: StreamError) -> Self {
        tracing::error!(
            error = ?e,
            "[DestinationError::from::StreamError]"
        );
        match e {
            StreamError::NotFound => DestinationError::NotFound,
            StreamError::NoHandler(s) => DestinationError::NoHandler(s),
            StreamError::PermissionDenied => DestinationError::PermissionDenied(Some(Arc::new(e))),
            StreamError::ConnectionFailure { source } => DestinationError::ConnectionFailure { source },
            _ => DestinationError::Unknown(format!("{:?}", e), Some(Arc::new(e))),
        }
    }
}

pub type Result<T> = std::result::Result<T, DestinationError>;

pub trait OutputStream: Write + Send {
    fn resource_id(&self) -> &str;
}

#[cfg_attr(any(feature = "test_helper", test), derive(EnumAssertions))]
#[derive(Clone, PartialEq)]
pub enum CompletionStatus {
    InProgress,
    Completed,
    Error(ParallelWriteError),
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum ParallelWriteError {
    #[error("Error while writing data: {0}")]
    DestinationError(#[from] DestinationError),
    #[error("Error while reading data: {0}")]
    InputError(#[from] StreamError),
}

pub trait BlockWriter: Send + Sync {
    fn get_block_buffer(&self, block_idx: usize) -> PooledBuffer;
    fn set_input_error(&self, error: StreamError);
    fn write_block(&self, block_idx: usize, data: PooledBuffer) -> std::result::Result<(), ParallelWriteError>;

    #[cfg(feature = "test_helper")]
    fn write_binary(&self, block_idx: usize, data: &[u8]) -> std::result::Result<(), ParallelWriteError> {
        let mut buffer = self.get_block_buffer(block_idx);
        buffer.copy_from_slice(data);

        self.write_block(block_idx, buffer)
    }
}

pub trait ParallelWriter {
    fn block_size(&self) -> usize;
    fn block_count(&self) -> usize;
    fn expected_data_size(&self) -> usize;
    fn completion_status(&self) -> CompletionStatus;
    fn get_block_writer(&self) -> Arc<dyn BlockWriter>;
    fn wait_for_completion(&mut self) -> std::result::Result<StreamInfo, ParallelWriteError>;
}

pub trait Destination: Send + Sync {
    /// A base path into which this Destination will write.
    fn base_path(&self) -> &str;

    /// Opens an output stream to a new file identified by the specified resource_id, relative to the base_path.
    fn open_output_stream(&self, resource_id: &str) -> Result<Box<dyn OutputStream>>;

    /// Attempts to open a parallel writer to a new file identified by the specified resource_id, relative to the base_path. If the Destination
    /// does not support parallel writing, None is returned.
    ///
    /// The data to upload must be exactly `total_size` bytes. This parameter in combination with the `block_size_hint` and `target_parallelization`
    /// parameters is used by the Destination to determine the best way to perform the parallel write, taking into account any restrictions on
    /// block size and maximum number of blocks allowed. The actual block size might be different than the hint provided and the caller must
    /// check the `block_size` on the [`BlockWriter`] to determine it.
    #[allow(unused_variables)]
    fn try_open_parallel_writer(
        &self,
        resource_id: &str,
        total_size: usize,
        block_size_hint: usize,
        target_parallelization: usize,
    ) -> Option<Result<Box<dyn ParallelWriter>>> {
        None
    }

    fn create_file(&self, resource_id: &str) -> Result<()>;

    fn create_directory(&self, resource_id: &str) -> Result<()>;

    fn remove_directory(&self, resource_id: &str) -> Result<()>;

    /// Removes the file identified by the specified resource_id, relative to the base_path.
    fn remove(&self, resource_id: &str) -> Result<()>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IfDestinationExists {
    MergeWithOverwrite,
    Append,
    Replace,
    Fail,
}

impl Default for IfDestinationExists {
    fn default() -> Self {
        IfDestinationExists::MergeWithOverwrite
    }
}

pub trait DestinationHandler: Send + Sync {
    type Arguments: for<'a> TryFrom<&'a SyncRecord, Error = ArgumentError>;

    fn handler_type(&self) -> &str;

    fn get_destination<'a>(
        &self,
        base_path: Cow<'a, str>,
        arguments: Self::Arguments,
        if_exists: IfDestinationExists,
        accessor: &DestinationAccessor,
    ) -> Result<Arc<dyn Destination + 'a>>;
}

trait DynDestinationHandler: Send + Sync {
    fn get_destination<'a>(
        &self,
        base_path: Cow<'a, str>,
        arguments: &SyncRecord,
        if_exists: IfDestinationExists,
        accessor: &DestinationAccessor,
    ) -> Result<Arc<dyn Destination + 'a>>;
}

impl<T: DestinationHandler> DynDestinationHandler for T {
    fn get_destination<'a>(
        &self,
        base_path: Cow<'a, str>,
        arguments: &SyncRecord,
        if_exists: IfDestinationExists,
        accessor: &DestinationAccessor,
    ) -> Result<Arc<dyn Destination + 'a>> {
        self.get_destination(base_path, arguments.try_into()?, if_exists, accessor)
    }
}

pub struct EmptyArgs {}

impl Default for EmptyArgs {
    fn default() -> Self {
        EmptyArgs {}
    }
}

impl TryFrom<&SyncRecord> for EmptyArgs {
    type Error = ArgumentError;

    fn try_from(_: &SyncRecord) -> std::result::Result<Self, Self::Error> {
        Ok(EmptyArgs {})
    }
}

#[derive(Default)]
pub struct DestinationAccessor {
    handlers: HashMap<String, Arc<dyn DynDestinationHandler>>,
}

impl DestinationAccessor {
    pub fn add_handler(mut self, handler: impl DestinationHandler + 'static) -> Self {
        self.handlers.insert(handler.handler_type().to_owned(), Arc::new(handler));
        self
    }

    pub fn get_destination<'a>(
        &self,
        handler: impl AsRef<str>,
        base_path: impl Into<Cow<'a, str>>,
        arguments: &SyncRecord,
        if_exists: IfDestinationExists,
    ) -> Result<Arc<dyn Destination + 'a>> {
        match self.handlers.get(handler.as_ref()) {
            Some(h) => h.get_destination(base_path.into(), arguments, if_exists, self),
            None => Err(DestinationError::NoHandler(handler.as_ref().to_owned())),
        }
    }
}

directory_display!(DestinationAccessor, handlers);
