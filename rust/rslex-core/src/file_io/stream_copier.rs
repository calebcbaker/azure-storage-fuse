use crate::{
    binary_buffer_pool::BinaryBufferPool,
    file_io::{Destination, DestinationError, ParallelWriteError, ParallelWriter, StreamAccessor, StreamError, StreamOpener},
    ExecutionError, ParallelizationDegree, StreamInfo,
};
use crossbeam_channel::{unbounded, Receiver, Sender};
use derivative::Derivative;
use std::{
    borrow::Cow,
    io::{Error, ErrorKind, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
};
use thiserror::Error;
use tracing::warn;
use tracing_sensitive::AsSensitive;

#[cfg(unix)]
const PATH_SEPARATOR_CHARS: &'static [char] = &['/'];
#[cfg(windows)]
const PATH_SEPARATOR_CHARS: &'static [char] = &['/', '\\'];

#[derive(Clone, Debug, Error, Derivative, PartialEq)]
pub enum CopyError {
    #[error("An error was encountered while reading data: {0}")]
    InputError(#[from] StreamError),
    #[error("An error was encountered while writing data to disk: {0}")]
    OutputError(#[from] DestinationError),
}

impl From<ParallelWriteError> for CopyError {
    fn from(e: ParallelWriteError) -> Self {
        match e {
            ParallelWriteError::DestinationError(de) => CopyError::OutputError(de),
            ParallelWriteError::InputError(ie) => CopyError::InputError(ie),
        }
    }
}

impl From<CopyError> for ExecutionError {
    fn from(error: CopyError) -> Self {
        match error {
            CopyError::InputError(e) => ExecutionError::StreamError(e),
            CopyError::OutputError(e) => ExecutionError::DestinationError(e),
        }
    }
}

/// Provides capabilities to download [`StreamInfos`](crate::StreamInfo) into a target directory.
///
/// # Remarks
/// Copy will be executed in parallel whenever possible using up to a determined number of download threads.
///
/// The source streams will be downloaded into target_path and will preserve any directory
/// structure that was present based on their resource_ids. Copying two streams with the
/// resource_ids "/parent/child/file.txt" and "/parent/child_2/file.txt" will result in the
/// following structure:
/// - target_path
///   - child
///     - file.txt
///   - child_2
///     - file.txt
/// ```
pub struct StreamCopier<'a> {
    stream_accessor: Arc<StreamAccessor>,
    destination: Arc<dyn Destination>,
    base_path: Cow<'a, str>,
    block_size: usize,
    sequential_buffer_pool: BinaryBufferPool,
    target_num_threads: usize,
    num_failed_thread_creations: AtomicUsize,
    copy_threads: Mutex<Vec<JoinHandle<()>>>,
    num_active_tasks: Arc<AtomicUsize>,
    copy_tasks_sender: Sender<Box<dyn FnOnce() + Send>>,
    copy_tasks_receiver: Receiver<Box<dyn FnOnce() + Send>>,
}

impl<'a> StreamCopier<'a> {
    /// Creates a new `StreamCopier` that can copy data into the specified target destination.
    pub fn new(stream_accessor: Arc<StreamAccessor>, destination: Arc<dyn Destination>) -> StreamCopier<'a> {
        let (copy_tasks_sender, copy_tasks_receiver) = unbounded();
        let block_size = 8 * 1024 * 1024;
        StreamCopier {
            stream_accessor,
            destination,
            base_path: "".into(),
            block_size,
            sequential_buffer_pool: BinaryBufferPool::new(ParallelizationDegree::Auto.to_thread_count(), block_size),
            target_num_threads: ParallelizationDegree::Auto.to_thread_count() * 4,
            num_failed_thread_creations: AtomicUsize::new(0),
            copy_threads: Mutex::new(Vec::new()),
            num_active_tasks: Arc::new(AtomicUsize::new(0)),
            copy_tasks_sender,
            copy_tasks_receiver,
        }
    }

    /// The target destination for the StreamInfos.
    pub fn destination(&self) -> &dyn Destination {
        self.destination.as_ref()
    }

    /// Sets a path to treat as root for the streams to copy to. If the resource ID for the input
    /// stream is relative to this base path, the base path will be trimmed from the output.
    pub fn with_base_path(mut self, base_path: impl Into<Cow<'a, str>>) -> StreamCopier<'a> {
        self.base_path = base_path.into();
        self
    }

    /// The size of the blocks to process when copying in parallel.
    ///
    /// Streams larger than one block and that support seeking (see [`StreamOpener::can_seek`]) will be
    /// copied using multiple threads. To do this, the file is split into multiple blocks.
    pub fn with_block_size(mut self, block_size: usize) -> StreamCopier<'a> {
        assert!(block_size > 0);
        self.block_size = block_size;
        self
    }

    /// The maximum number of threads to use for parallel copy.
    ///
    /// This can only be changed before [`copy`](Self::copy) is called.
    pub fn with_threads(mut self, threads: usize) -> StreamCopier<'a> {
        if self.copy_threads.lock().unwrap().len() != 0 {
            panic!("The number of copy threads can only be specified before copy is called.")
        }

        self.target_num_threads = threads;
        self
    }

    fn ensure_threads(&self, expected_num_tasks: usize) -> std::io::Result<usize> {
        let active_tasks = self.num_active_tasks.load(Ordering::SeqCst);
        let required_num_threads = std::cmp::min(expected_num_tasks + active_tasks, self.target_num_threads);
        let mut copy_threads = self.copy_threads.lock().unwrap();
        let current_thread_count = copy_threads.len();
        let num_threads_to_create = required_num_threads - std::cmp::min(current_thread_count, required_num_threads);
        let mut num_threads_created = 0;
        if num_threads_to_create > 0 && self.num_failed_thread_creations.load(Ordering::SeqCst) < 5 {
            for i in 0..num_threads_to_create {
                let copy_tasks_receiver = self.copy_tasks_receiver.clone();
                let builder = std::thread::Builder::new();
                let spawn_result = builder.spawn(move || {
                    while let Ok(task) = copy_tasks_receiver.recv() {
                        task();
                    }
                });
                match spawn_result {
                    Ok(join_handle) => {
                        num_threads_created += 1;
                        copy_threads.push(join_handle);
                    },
                    Err(e) => warn!(num_threads_created, thread_num = i, "A Thread failed to create with error: {}", e),
                }
            }
            if num_threads_created < num_threads_to_create {
                self.num_failed_thread_creations.fetch_add(1, Ordering::SeqCst);
            }
        }

        let new_thread_count = num_threads_created + current_thread_count;
        if new_thread_count <= 1 {
            let num_of_failed_creations = self.num_failed_thread_creations.load(Ordering::SeqCst);
            let error = Error::new(
                ErrorKind::Other,
                format!(
                    "No new threads were created. Number of failed attempts: {}",
                    num_of_failed_creations
                ),
            );
            tracing::error!(error = %error, "[StreamCopier::ensure_threads] Unable to create enough threads");
            Err(error)
        } else {
            Ok(new_thread_count)
        }
    }

    #[allow(clippy::unit_arg)]
    #[tracing::instrument(err, skip(self, stream_opener, target_path))]
    fn do_sequential_copy(&self, stream_opener: Arc<dyn StreamOpener>, target_path: &str, file_size: usize) -> Result<String, CopyError> {
        if file_size <= self.block_size {
            // If the file is smaller than the block size, re-use a buffer.
            let mut data = self.sequential_buffer_pool.check_out().truncate(file_size);
            stream_opener.copy_to(&mut data)?;
            let mut output_stream = self.destination.open_output_stream(target_path)?;
            output_stream.write_all(&data).map_err(DestinationError::from)?;
            output_stream.flush().map_err(DestinationError::from)?;
            Ok(output_stream.resource_id().to_string())
        } else {
            let mut data = Vec::with_capacity(file_size);
            unsafe { data.set_len(file_size) };
            stream_opener.copy_to(&mut data)?;
            let mut output_stream = self.destination.open_output_stream(target_path)?;
            output_stream.write_all(&data).map_err(DestinationError::from)?;
            output_stream.flush().map_err(DestinationError::from)?;
            Ok(output_stream.resource_id().to_string())
        }
    }

    #[allow(clippy::unit_arg)]
    #[tracing::instrument(err, skip(self, stream_opener, target_path, parallel_writer))]
    fn queue_chunked_copy(
        &self,
        stream_opener: Arc<dyn StreamOpener>,
        mut parallel_writer: Box<dyn ParallelWriter>,
        target_path: &str,
        file_size: usize,
    ) -> Result<String, CopyError> {
        let chunked_writer = parallel_writer.get_block_writer();
        let num_blocks = parallel_writer.block_count();
        let block_size = parallel_writer.block_size();
        let num_available_threads = self.ensure_threads(num_blocks).unwrap_or(1);
        if num_available_threads == 1 {
            self.do_sequential_copy(stream_opener, target_path, file_size)
        } else {
            let block_idx = Arc::new(AtomicUsize::new(0));
            self.num_active_tasks.fetch_add(num_available_threads, Ordering::Relaxed);
            for _ in 0..num_available_threads {
                let stream_opener = stream_opener.clone();
                let chunked_writer = chunked_writer.clone();
                let block_idx = block_idx.clone();
                self.copy_tasks_sender
                    .send(Box::new(move || loop {
                        let block_idx = block_idx.fetch_add(1, Ordering::Relaxed);
                        if block_idx >= num_blocks {
                            break;
                        }

                        let offset = block_idx * block_size;
                        let mut data = chunked_writer.get_block_buffer(block_idx);
                        if let Err(e) = stream_opener.try_as_seekable().unwrap().copy_section_to(offset, &mut data) {
                            tracing::error!(error = %e, "[StreamCopier::queue_chunked_copy()] error while copying StreamInfo chunk");
                            chunked_writer.set_input_error(e);
                            break;
                        } else {
                            if let Err(_) = chunked_writer.write_block(block_idx, data) {
                                break;
                            }
                        }
                    }))
                    .unwrap_or_else(|e| panic!("Copy tasks channel was disconnected unexpectedly: {}", e));
            }

            self.num_active_tasks.fetch_sub(num_available_threads, Ordering::Relaxed);
            let output_stream = parallel_writer.wait_for_completion()?;
            Ok(output_stream.resource_id().to_string())
        }
    }

    pub fn encode_to_safe_path(path: &str) -> Cow<str> {
        let mut encoding_needed = 0;
        let mut prior_was_slash = false;
        for c in path.chars() {
            let mut slash = false;
            let c = if cfg!(windows) && c == '\\' { '/' } else { c };
            match c {
                '/' => {
                    if prior_was_slash {
                        encoding_needed += 1;
                    }
                    slash = true;
                },
                '%' | '&' | '*' | '"' | '<' | '>' | ':' | '|' | '?' => {
                    encoding_needed += 1;
                },
                _ => (),
            }
            prior_was_slash = slash;
        }
        if encoding_needed == 0 {
            return Cow::from(path);
        }

        let mut result = String::with_capacity(path.len() + encoding_needed * 2);
        prior_was_slash = false;
        for c in path.chars() {
            let mut slash = false;
            let c = if cfg!(windows) && c == '\\' { '/' } else { c };
            match c {
                '/' => {
                    if prior_was_slash {
                        // Keep repeating slashes encoded so encoding is reversable
                        // which makes filter push down optimizations possible.
                        result.push_str("%2F");
                    } else {
                        result.push('/');
                    }
                    slash = true;
                },
                '%' => result.push_str("%25"),
                '&' => result.push_str("%26"),
                '*' => result.push_str("%2A"),
                '"' => result.push_str("%22"),
                '<' => result.push_str("%3C"),
                '>' => result.push_str("%3E"),
                ':' => result.push_str("%3A"),
                '|' => result.push_str("%7C"),
                '?' => result.push_str("%3F"),
                _ => result.push(c),
            }
            prior_was_slash = slash;
        }
        Cow::from(result)
    }

    pub fn strip_path_prefix(path: &'a str, prefix: &str) -> &'a str {
        // We remove trailing slashes from the prefix so we can keep the leading slash for the relative
        // path after the prefix has been removed.
        let prefix = prefix.strip_suffix(PATH_SEPARATOR_CHARS).unwrap_or(prefix);
        let stripped_path = path.strip_prefix(prefix).unwrap_or(path);

        // If the stripped path starts with PATH_SEPARATOR_CHARS, then it means the prefix was an actual parent
        // path of the input as opposed to just a substring. If it doesn't, then the prefix
        // is just a substring, not a path segment. In that case, we don't modify the input.
        if stripped_path.starts_with(PATH_SEPARATOR_CHARS) {
            stripped_path
        } else {
            path
        }
    }

    fn get_target_path(&self, stream: &'a StreamInfo) -> Cow<'a, str> {
        let resource_id = stream.resource_id();
        let target_path = StreamCopier::strip_path_prefix(resource_id, self.base_path.as_ref());
        StreamCopier::encode_to_safe_path(target_path)
    }

    /// Copy the specified [`StreamInfo`] and return the output path.
    #[tracing::instrument(err, skip(self, stream))]
    pub fn copy(&self, stream: &StreamInfo) -> Result<String, CopyError> {
        let target_path = self.get_target_path(stream);
        self.copy_to(stream, target_path.as_ref())
    }

    /// Copy the specified ['StreamInfo'] to the copier's destination using the specified relative path.
    pub fn copy_to(&self, stream: &StreamInfo, target_path: &str) -> Result<String, CopyError> {
        tracing::debug!(stream = %stream.as_sensitive(), "[StreamCopier::download()] copy StreamInfo");
        let target_path = target_path.trim_start_matches(PATH_SEPARATOR_CHARS);
        let opener = self.stream_accessor.get_opener(stream)?;
        let properties = opener.get_properties()?;
        let output_path = if properties.size <= self.block_size as u64 {
            tracing::info!(stream = %stream.as_sensitive(), handler = stream.handler(), "[do_copy()] Input StreamInfo is smaller than block_size. Doing sequential download.");
            self.do_sequential_copy(opener, target_path, properties.size as usize)?
        } else if let Some(_) = opener.try_as_seekable() {
            if let Some(parallel_writer_result) =
                self.destination
                    .try_open_parallel_writer(target_path, properties.size as usize, self.block_size, self.target_num_threads)
            {
                let parallel_writer = parallel_writer_result?;
                self.queue_chunked_copy(opener, parallel_writer, target_path, properties.size as usize)
                    .map_err(|e| {
                        tracing::error!(error = %e, stream = %stream.as_sensitive(), source_handler = %stream.handler(), size = %properties.size, "[do_copy()] chunked copy failed");
                        e
                    })?
            } else {
                tracing::info!("[do_copy()] Destination does not support parallel write. Doing sequential copy.");
                self.do_sequential_copy(opener, target_path, properties.size as usize)?
            }
        } else {
            tracing::info!(stream = %stream.as_sensitive(), handler = stream.handler(), "[do_copy()] Input StreamInfo is not seekable. Doing sequential download.");
            self.do_sequential_copy(opener, target_path, properties.size as usize)?
        };

        tracing::info!(size = properties.size, "[do_copy()] copied file, size: {}", properties.size);
        Ok(output_path)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        file_io::{CopyError, DestinationError, StreamCopier, StreamError},
        ExecutionError,
    };
    use fluent_assertions::*;

    #[test]
    #[rustfmt::skip] // requires cases be of mixed format and hence less readable.
    fn encode_to_safe_path() {
        let cases = [
            (
                "C:/My/Path/File.csv",
                "C%3A/My/Path/File.csv",
            ),
            (
                "C:/My/Path/File\"with\"special?characters.csv",
                "C%3A/My/Path/File%22with%22special%3Fcharacters.csv",
            ),
            (
                "C:/My/Path/double_%encoding%25characters.csv",
                "C%3A/My/Path/double_%25encoding%2525characters.csv",
            ),
            (
                "/My/Path/File.csv",
                "/My/Path/File.csv",
            ),
            (
                "/My//Path/File.csv",
                "/My/%2FPath/File.csv",
            ),
            (
                "/My/Path:1/File|with,commas.csv",
                "/My/Path%3A1/File%7Cwith,commas.csv",
            ),
            (
                "https://www.myurl.com/My/Path/file.csv",
                "https%3A/%2Fwww.myurl.com/My/Path/file.csv",
            )
        ];

        for (path, expected) in cases.iter() {
            let result = StreamCopier::encode_to_safe_path(path);
            result.should().equal_string(expected);
        }
    }

    #[test]
    #[cfg(windows)]
    fn encodes_to_safe_path_with_windows_separator() {
        let cases = [
            ("C:\\My\\Path\\File.csv", "C%3A/My/Path/File.csv"),
            (
                "C:\\My\\\\Path\\File\"with\"special?characters.csv",
                "C%3A/My/%2FPath/File%22with%22special%3Fcharacters.csv",
            ),
        ];

        for (path, expected) in cases.iter() {
            let result = StreamCopier::encode_to_safe_path(path);
            result.should().equal_string(expected);
        }
    }

    #[test]
    fn input_copy_error_should_be_convertible_to_execution_error() {
        ExecutionError::from(CopyError::InputError(StreamError::NotFound))
            .should()
            .equal(&ExecutionError::StreamError(StreamError::NotFound));
    }

    #[test]
    fn destination_copy_error_should_be_convertible_to_execution_error() {
        ExecutionError::from(CopyError::OutputError(DestinationError::NotFound))
            .should()
            .equal(&ExecutionError::DestinationError(DestinationError::NotFound));
    }
}
