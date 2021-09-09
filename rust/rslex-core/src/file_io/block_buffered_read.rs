// Pre-fetch count: pre-fetch beyond buffer blocks
// Buffer block count: number of blocks for buffer
// Block size: size of 1 block

use crate::{
    file_io::{SeekableRead, StreamAccessor, StreamError},
    session_properties_ext::SessionPropertiesExt,
    StreamInfo,
};
use itertools::Itertools;
use num::Integer;
use std::{
    collections::{hash_map::Entry, HashMap},
    future::{Future, Ready},
    io::{Read, Seek, SeekFrom},
    pin::Pin,
    sync::Arc,
};
use thiserror::Error;
use tracing_sensitive::AsSensitive;
use uuid::Uuid;

#[derive(Clone, Debug, Error, PartialEq)]
pub enum GetBlockError {
    #[error("Requested block {idx}, but source only has {block_count} blocks.")]
    BlockOutOfBounds { idx: usize, block_count: usize },
    #[error("Error encountered while downloading block: {0}.")]
    StreamError(#[from] StreamError),
}

pub type GetBlockResult<T> = Result<T, GetBlockError>;

pub trait BlockFutureTrait: Future<Output = GetBlockResult<Arc<Vec<u8>>>> + Send + 'static {
    /// Ensures acquisition of the block starts immediately. This might block if the block
    /// is not yet available and has to be acquired. Once this returns, polling the future is
    /// guaranteed to return Poll::Ready.
    fn fetch_now(&self);
}

impl BlockFutureTrait for Ready<GetBlockResult<Arc<Vec<u8>>>> {
    fn fetch_now(&self) {
        // This is already ready! No fetching required.
    }
}

pub type BlockFuture = Pin<Box<dyn BlockFutureTrait>>;

pub trait FileBlockProvider: Send + Sync {
    fn block_size(&self) -> usize;
    fn get_block(&self, stream_info: Arc<StreamInfo>, block_idx: usize) -> GetBlockResult<BlockFuture>;
}

pub struct BlockBufferedRead {
    stream_info: Arc<StreamInfo>,
    stream_len: u64,
    stream_pos: u64,
    stream_block_count: usize,
    current_block: Option<Arc<Vec<u8>>>,
    current_block_idx: usize,
    buffer_block_count: usize,
    pre_fetched_blocks: HashMap<usize, BlockFuture>,
    downloader: Arc<dyn FileBlockProvider>,
    correlation_id: Uuid,
}

const NO_CURRENT_BLOCK: usize = usize::MAX;

impl BlockBufferedRead {
    pub fn new(
        stream_info: StreamInfo,
        buffer_block_count: usize,
        stream_accessor: &StreamAccessor,
        downloader: Arc<dyn FileBlockProvider>,
    ) -> GetBlockResult<BlockBufferedRead> {
        let size = stream_accessor.get_opener(&stream_info)?.get_properties()?.size;
        let block_size = downloader.block_size() as u64;

        let correlation_id = Uuid::new_v4();
        tracing::trace!(correlation_id = %correlation_id,
            stream_info = %stream_info.as_sensitive_ref(),
            file_size = ?size,
            block_size = ?block_size,
            "[BlockBufferedRead::new] BlockBufferedRead initialized.");

        Ok(BlockBufferedRead {
            stream_info: Arc::new(stream_info.with_session_properties(HashMap::new().with_size(size))),
            stream_len: size,
            stream_pos: 0,
            stream_block_count: size.div_ceil(&block_size) as usize,
            current_block: None,
            current_block_idx: NO_CURRENT_BLOCK,
            buffer_block_count,
            pre_fetched_blocks: HashMap::new(),
            downloader,
            correlation_id,
        })
    }

    fn get_block(&mut self, block_idx: usize) -> GetBlockResult<BlockFuture> {
        let block = if let Some(b) = self.pre_fetched_blocks.remove(&block_idx) {
            tracing::trace!(correlation_id=%self.correlation_id,
                current_block_idx=self.current_block_idx,
                block_idx_to_get=block_idx,
                "[BlockBufferedRead::get_block] Block future already present in buffer.");
            Ok(b)
        } else {
            tracing::trace!(correlation_id=%self.correlation_id,
                current_block_idx=self.current_block_idx,
                block_idx_to_get=block_idx,
                "[BlockBufferedRead::get_block] Retrieving block from provider.");
            self.downloader.get_block(self.stream_info.clone(), block_idx)
        };

        let available_blocks = self.pre_fetched_blocks.keys().cloned().collect_vec();
        for available_block in available_blocks {
            if available_block <= block_idx || available_block >= block_idx + self.buffer_block_count {
                self.pre_fetched_blocks.remove(&available_block);
            }
        }

        let blocks_available_for_buffer = std::cmp::min(self.stream_block_count - block_idx, self.buffer_block_count);
        for i in block_idx + 1..block_idx + blocks_available_for_buffer {
            if let Entry::Vacant(e) = self.pre_fetched_blocks.entry(i) {
                e.insert(self.downloader.get_block(self.stream_info.clone(), i)?);
            }
        }

        assert!(self.pre_fetched_blocks.len() < self.buffer_block_count);
        block
    }

    fn ensure_current_block(&mut self) -> GetBlockResult<()> {
        let required_block_idx = (self.stream_pos / self.downloader.block_size() as u64) as usize;
        if self.current_block_idx == NO_CURRENT_BLOCK || self.current_block_idx != required_block_idx {
            tracing::trace!(correlation_id=%self.correlation_id,
                current_block_idx=self.current_block_idx,
                required_block_idx=required_block_idx,
                "[BlockBufferedRead::ensure_current_block] Current block needs to be replaced with required block.");

            let block_future = self.get_block(required_block_idx)?;
            block_future.fetch_now();
            let block = futures::executor::block_on(block_future)?;

            tracing::trace!(correlation_id=%self.correlation_id,
                current_block_idx=self.current_block_idx,
                required_block_idx=required_block_idx,
                block_len=block.len(),
                "[BlockBufferedRead::ensure_current_block] Required block retrieved.");

            self.current_block = Some(block);
            self.current_block_idx = required_block_idx;
            Ok(())
        } else {
            tracing::trace!(correlation_id=%self.correlation_id,
                current_block_idx=self.current_block_idx,
                required_block_idx=required_block_idx,
                "[BlockBufferedRead::ensure_current_block] Current block is the required block.");
            Ok(())
        }
    }
}

impl Read for BlockBufferedRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        tracing::trace!(correlation_id=%self.correlation_id,
            stream_pos=self.stream_pos,
            buf_len=buf.len(),
            "[BlockBufferedRead::read]");
        self.ensure_current_block().map_err(|e| {
            tracing::debug!(correlation_id=%self.correlation_id,
                error=%e.as_sensitive_ref(),
                "[BlockBufferedRead::read] Error encountered while ensuring current block.");
            match e {
                GetBlockError::StreamError(e) => std::io::Error::from(e),
                e @ GetBlockError::BlockOutOfBounds { .. } => std::io::Error::new(std::io::ErrorKind::Other, e),
            }
        })?;
        let current_block = self.current_block.as_ref().unwrap();
        let offset_in_block = (self.stream_pos - (self.current_block_idx * self.downloader.block_size()) as u64) as usize;
        let available_data = std::cmp::min(current_block.len() - offset_in_block, buf.len());
        let new_stream_pos = self.stream_pos + available_data as u64;
        tracing::trace!(correlation_id=%self.correlation_id,
            block_idx=self.current_block_idx,
            offset_in_block=offset_in_block,
            data_to_copy=available_data,
            stream_pos=self.stream_pos,
            new_stream_pos=new_stream_pos,
            "[BlockBufferedRead::read] Copying data to output buffer.");
        buf[0..available_data].copy_from_slice(&current_block[offset_in_block..offset_in_block + available_data]);
        self.stream_pos = new_stream_pos;
        Ok(available_data)
    }
}

impl Seek for BlockBufferedRead {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let offset = match pos {
            SeekFrom::Start(i) => i,
            SeekFrom::Current(i) if i >= 0 => self.stream_pos + i as u64,
            SeekFrom::Current(i) if i < 0 && i.abs() as u64 > self.stream_pos => {
                tracing::debug!(correlation_id=%self.correlation_id,
                    stream_pos=self.stream_pos,
                    stream_len=self.stream_len,
                    seek_offset=?pos,
                    "[BlockBufferedRead::seek] Attempted to seek from current to before start of file.");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Attempted to seek before beginning of file.",
                ));
            },
            SeekFrom::Current(i) if i < 0 => self.stream_pos - i.abs() as u64,
            SeekFrom::End(i) if i >= 0 => self.stream_len + i as u64,
            SeekFrom::End(i) if i < 0 && i.abs() as u64 > self.stream_len => {
                tracing::debug!(correlation_id=%self.correlation_id,
                    stream_pos=self.stream_pos,
                    stream_len=self.stream_len,
                    seek_offset=?pos,
                    "[BlockBufferedRead::seek] Attempted to seek from end to before start of file.");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Attempted to seek from end to before start of file.",
                ));
            },
            SeekFrom::End(i) if i < 0 => self.stream_len - i.abs() as u64,
            _ => unreachable!(),
        };

        if offset > self.stream_len {
            tracing::debug!(correlation_id=%self.correlation_id,
                stream_pos=self.stream_pos,
                stream_len=self.stream_len,
                seek_offset=?pos,
                "[BlockBufferedRead::seek] Attempted to seek past end-of-file.");
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Attempted to seek beyond end of file.",
            ))
        } else {
            tracing::trace!(correlation_id=%self.correlation_id,
                old_stream_pos=self.stream_pos,
                new_stream_pos=offset,
                stream_len=self.stream_len,
                seek_offset=?pos,
                "[BlockBufferedRead::seek] Seek successful.");
            self.stream_pos = offset;
            Ok(self.stream_pos)
        }
    }
}

impl SeekableRead for BlockBufferedRead {
    fn length(&self) -> u64 {
        self.stream_len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        file_io::{
            file_block_downloader::FileBlockDownloader,
            in_memory_stream_handler::{InMemoryStreamHandler, HANDLER_TYPE},
        },
        SyncRecord,
    };
    use fluent_assertions::{ResultAssertions, Should};
    use std::sync::Mutex;

    fn create_read(data: &str, block_size: usize, buffer_block_count: usize) -> BlockBufferedRead {
        let stream_info = StreamInfo::new(HANDLER_TYPE, "File", SyncRecord::empty());
        let accessor = Arc::new(InMemoryStreamHandler::default().add_stream("File", Ok(data)).into_stream_accessor());
        let downloader = FileBlockDownloader::new(accessor.clone(), block_size, 1);
        BlockBufferedRead::new(stream_info, buffer_block_count, &accessor, Arc::new(downloader)).unwrap()
    }

    // Empty file no data
    #[test]
    fn read_empty_file_returns_0() {
        let mut read = create_read("", 4, 1);
        let mut dst = [0; 4];
        read.read(&mut dst).unwrap().should().equal(&0);
        dst.should().equal(&[0; 4]);
    }

    #[test]
    fn read_all_data_for_file_smaller_than_one_block() {
        let mut read = create_read("1234", 8, 1);
        let mut dst = [0; 4];
        read.read_exact(&mut dst).unwrap();
        dst.should().equal("1234".as_bytes());
    }

    #[test]
    fn read_all_data_for_file_larger_than_one_block_but_smaller_than_total_buffer() {
        let mut read = create_read("123456", 4, 2);
        let mut dst = [0; 6];
        read.read_exact(&mut dst).unwrap();
        dst.should().equal("123456".as_bytes());
    }

    #[test]
    fn read_all_data_in_multiple_reads_for_file_larger_than_one_block_but_smaller_than_total_buffer() {
        let mut read = create_read("123456", 4, 2);
        let mut dst = [0; 2];
        let _ = read.read(&mut dst).unwrap();
        dst.should().equal("12".as_bytes());
        let _ = read.read(&mut dst).unwrap();
        dst.should().equal("34".as_bytes());
        let _ = read.read(&mut dst).unwrap();
        dst.should().equal("56".as_bytes());
    }

    #[test]
    fn read_all_data_into_buffer_larger_than_one_block_for_file_smaller_than_total_buffer() {
        let mut read = create_read("1234567890", 4, 3);
        let mut dst = [0; 10];
        read.read_exact(&mut dst).unwrap();
        dst.should().equal("1234567890".as_bytes());
    }

    #[test]
    fn read_all_data_into_buffer_larger_than_one_block_for_file_larger_than_total_buffer() {
        let mut read = create_read("1234567890", 4, 2);
        let mut dst = [0; 10];
        read.read_exact(&mut dst).unwrap();
        dst.should().equal("1234567890".as_bytes());
    }

    struct TestBlockProvider {
        underlying: Box<dyn FileBlockProvider>,
        error_to_return: Arc<Mutex<Option<GetBlockError>>>,
    }

    impl FileBlockProvider for TestBlockProvider {
        fn block_size(&self) -> usize {
            self.underlying.block_size()
        }

        fn get_block(&self, stream_info: Arc<StreamInfo>, block_idx: usize) -> GetBlockResult<BlockFuture> {
            if let Some(e) = self.error_to_return.lock().unwrap().as_ref() {
                Err(e.clone())
            } else {
                self.underlying.get_block(stream_info, block_idx)
            }
        }
    }

    fn create_read_with_error_opt(
        error_opt: Arc<Mutex<Option<GetBlockError>>>,
        data: &str,
        block_size: usize,
        buffer_block_count: usize,
    ) -> BlockBufferedRead {
        let stream_info = StreamInfo::new(HANDLER_TYPE, "File", SyncRecord::empty());
        let accessor = Arc::new(InMemoryStreamHandler::default().add_stream("File", Ok(data)).into_stream_accessor());
        let accessor_clone = accessor.clone();
        BlockBufferedRead::new(
            stream_info,
            buffer_block_count,
            &accessor,
            Arc::new(TestBlockProvider {
                underlying: Box::new(FileBlockDownloader::new(accessor_clone, block_size, 1)),
                error_to_return: error_opt,
            }),
        )
        .unwrap()
    }

    #[test]
    fn read_first_block_for_file_smaller_than_buffer_remaining_buffer_is_filled() {
        let error_opt = Arc::new(Mutex::new(None));
        let mut read = create_read_with_error_opt(error_opt.clone(), "123456", 4, 2);
        let mut dst = [0; 4];
        read.read_exact(&mut dst[0..2]).unwrap();
        error_opt.lock().unwrap().replace(GetBlockError::StreamError(StreamError::NotFound));
        read.read_exact(&mut dst).unwrap();
        dst.should().equal("3456".as_bytes());
    }

    // Errors
    #[test]
    fn read_exact_into_buffer_larger_than_file_returns_error() {
        let mut read = create_read("1234", 2, 2);
        let mut dst = [0; 10];
        read.read_exact(&mut dst).should().be_err();
    }

    #[test]
    fn read_after_seek_from_beginning() {
        let mut read = create_read("123456", 4, 1);
        read.seek(SeekFrom::Start(2)).unwrap();
        let mut dst = [0; 4];
        read.read_exact(&mut dst).unwrap();
        dst.should().equal("3456".as_bytes());
    }

    #[test]
    fn read_after_seek_from_end() {
        let mut read = create_read("123456", 4, 1);
        read.seek(SeekFrom::End(-4)).unwrap();
        let mut dst = [0; 4];
        read.read_exact(&mut dst).unwrap();
        dst.should().equal("3456".as_bytes());
    }

    #[test]
    fn read_after_seek_from_current_positive() {
        let mut read = create_read("123456", 4, 1);
        read.seek(SeekFrom::Start(3)).unwrap();
        read.seek(SeekFrom::Current(2)).unwrap();
        let mut dst = [0; 1];
        read.read_exact(&mut dst).unwrap();
        dst.should().equal("6".as_bytes());
    }

    #[test]
    fn read_after_seek_from_current_negative() {
        let mut read = create_read("123456", 4, 1);
        read.seek(SeekFrom::Start(3)).unwrap();
        read.seek(SeekFrom::Current(-2)).unwrap();
        let mut dst = [0; 5];
        read.read_exact(&mut dst).unwrap();
        dst.should().equal("23456".as_bytes());
    }

    #[test]
    fn seek_to_position_equal_to_total_file_length_succeeds() {
        let mut read = create_read("123456", 4, 1);
        read.seek(SeekFrom::Start(6)).unwrap();
        let mut dst = [];
        read.read_exact(&mut dst).unwrap();
        dst.should().equal("".as_bytes());
    }

    #[test]
    fn seek_from_start_to_position_equal_to_total_file_length_plus_one_returns_error() {
        let mut read = create_read("123456", 4, 1);
        read.seek(SeekFrom::Start(7)).should().be_err();
    }

    #[test]
    fn seek_from_current_to_position_equal_to_total_file_length_plus_one_returns_error() {
        let mut read = create_read("123456", 4, 1);
        read.seek(SeekFrom::Current(7)).should().be_err();
    }

    #[test]
    fn seek_from_current_to_position_before_start_returns_error() {
        let mut read = create_read("123456", 4, 1);
        read.seek(SeekFrom::Current(-1)).should().be_err();
    }

    #[test]
    fn seek_from_end_to_position_equal_to_total_file_length_plus_one_returns_error() {
        let mut read = create_read("123456", 4, 1);
        read.seek(SeekFrom::End(1)).should().be_err();
    }

    #[test]
    fn seek_from_end_to_position_before_file_start_returns_error() {
        let mut read = create_read("123456", 4, 1);
        read.seek(SeekFrom::End(-7)).should().be_err();
    }
}
