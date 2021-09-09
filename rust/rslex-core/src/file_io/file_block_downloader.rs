use crate::{
    file_io::{
        block_buffered_read::{BlockFuture, BlockFutureTrait, FileBlockProvider, GetBlockError, GetBlockResult},
        StreamAccessor, StreamOpener,
    },
    StreamInfo,
};
use num::Integer;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
use threadpool::ThreadPool;

struct NotifierData {
    pub wakers: Vec<Waker>,
    pub result: Option<GetBlockResult<Arc<Vec<u8>>>>,
    pub needs_download: bool,
}

pub struct DownloadNotifier {
    data: Mutex<NotifierData>,
    opener: Arc<dyn StreamOpener>,
    offset: u64,
    len: usize,
}

impl DownloadNotifier {
    pub fn new(opener: Arc<dyn StreamOpener>, offset: u64, len: usize) -> DownloadNotifier {
        DownloadNotifier {
            data: Mutex::new(NotifierData {
                wakers: Vec::new(),
                result: None,
                needs_download: true,
            }),
            opener,
            offset,
            len,
        }
    }

    pub fn poll(&self, waker: Waker) -> Poll<GetBlockResult<Arc<Vec<u8>>>> {
        let mut data = self
            .data
            .lock()
            .expect("[DownloadNotifier::register_waker] Unexpected error acquiring mutex.");
        if let Some(result) = &data.result {
            Poll::Ready(result.clone())
        } else {
            data.wakers.push(waker);
            Poll::Pending
        }
    }

    pub fn fetch_now(&self) {
        {
            let mut data = self
                .data
                .lock()
                .expect("[DownloadNotifier::register_waker] Unexpected error acquiring mutex.");
            if !data.needs_download {
                return;
            } else {
                data.needs_download = false;
            }
        }

        let mut data = Vec::with_capacity(self.len);
        unsafe { data.set_len(self.len as usize) };
        let result = self
            .opener
            .try_as_seekable()
            .unwrap()
            .copy_section_to(self.offset as usize, &mut data)
            .map_err(GetBlockError::from)
            .map(move |()| Arc::new(data));

        let wakers = {
            let mut data = self
                .data
                .lock()
                .expect("[DownloadNotifier::register_waker] Unexpected error acquiring mutex.");
            data.result = Some(result);
            std::mem::replace(&mut data.wakers, Vec::new())
        };

        for waker in wakers {
            waker.wake();
        }
    }
}

impl Drop for DownloadNotifier {
    fn drop(&mut self) {
        let mut data = self
            .data
            .lock()
            .expect("[DownloadNotifier::register_waker] Unexpected error acquiring mutex.");
        data.needs_download = false;
    }
}

#[derive(Clone)]
pub struct FileBlockDownload {
    notifier: Arc<DownloadNotifier>,
}

impl FileBlockDownload {
    pub fn new(notifier: Arc<DownloadNotifier>) -> FileBlockDownload {
        FileBlockDownload { notifier }
    }
}

impl BlockFutureTrait for FileBlockDownload {
    fn fetch_now(&self) {
        self.notifier.fetch_now();
    }
}

impl Future for FileBlockDownload {
    type Output = GetBlockResult<Arc<Vec<u8>>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.notifier.poll(cx.waker().clone())
    }
}

pub struct FileBlockDownloader {
    stream_accessor: Arc<StreamAccessor>,
    block_size: usize,
    thread_pool: Mutex<ThreadPool>,
    empty_block: Arc<Vec<u8>>,
}

impl FileBlockDownloader {
    pub fn new(stream_accessor: Arc<StreamAccessor>, block_size: usize, parallelization_degree: usize) -> FileBlockDownloader {
        FileBlockDownloader {
            stream_accessor,
            block_size,
            thread_pool: Mutex::new(ThreadPool::new(parallelization_degree)),
            empty_block: Arc::new(Vec::new()),
        }
    }

    fn get_data_range_for_block(&self, file_size: u64, block_idx: usize) -> GetBlockResult<(u64, usize)> {
        let offset = (block_idx * self.block_size) as u64;
        if offset > file_size {
            Err(GetBlockError::BlockOutOfBounds {
                idx: block_idx,
                block_count: file_size.div_ceil(&(self.block_size as u64)) as usize,
            })
        } else {
            let len = std::cmp::min(self.block_size, (file_size - offset) as usize);
            Ok((offset, len))
        }
    }
}

impl FileBlockProvider for FileBlockDownloader {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn get_block(&self, stream_info: Arc<StreamInfo>, block_idx: usize) -> GetBlockResult<BlockFuture> {
        let opener = self.stream_accessor.get_opener(stream_info.as_ref())?;
        let size = opener.get_properties()?.size;
        let (offset, len) = self.get_data_range_for_block(size, block_idx)?;
        if len == 0 {
            Ok(Box::pin(std::future::ready(Ok(self.empty_block.clone()))) as BlockFuture)
        } else {
            let notifier = Arc::new(DownloadNotifier::new(opener, offset, len));
            let download_future = FileBlockDownload::new(notifier);
            let future_clone = download_future.clone();
            self.thread_pool.lock().unwrap().execute(move || future_clone.fetch_now());
            Ok(Box::pin(download_future) as BlockFuture)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        file_io::{
            in_memory_stream_handler::{InMemoryStreamHandler, HANDLER_TYPE},
            SeekableRead, SeekableStreamOpener, StreamHandler, StreamProperties, StreamResult,
        },
        test_helper::StubbedStreamHandler,
        SyncRecord,
    };
    use fluent_assertions::*;
    use std::{collections::HashMap, io::Read};

    fn create_downloader(
        block_size: usize,
        parallelization_degree: usize,
        files: Vec<(String, StreamResult<String>)>,
    ) -> FileBlockDownloader {
        let mut handler = InMemoryStreamHandler::new();
        for file in files {
            handler = handler.add_stream(file.0, file.1);
        }

        let accessor = Arc::new(StreamAccessor::default().add_handler(handler));
        FileBlockDownloader::new(accessor, block_size, parallelization_degree)
    }

    fn test_get_block(data: &str, block_size: usize, block_idx: usize, expected_data: &str) {
        let downloader = create_downloader(block_size, 1, vec![("File".to_string(), Ok(data.to_string()))]);
        let result = downloader.get_block(Arc::new(StreamInfo::new(HANDLER_TYPE, "File", SyncRecord::empty())), block_idx);
        let data = tokio_test::block_on(result.unwrap());
        data.should()
            .be_ok()
            .which_value()
            .as_slice()
            .should()
            .equal(&expected_data.as_bytes());
    }

    #[test]
    fn download_first_block_for_empty_file_returns_empty_block() {
        test_get_block("", 4, 0, "");
    }

    #[test]
    fn download_first_block_for_file_larger_than_block_size_returns_correct_data() {
        test_get_block("1234", 2, 0, "12");
    }

    #[test]
    fn download_first_block_for_file_smaller_than_block_size_returns_entire_file() {
        test_get_block("1234", 8, 0, "1234");
    }

    #[test]
    fn download_second_block_for_file_larger_than_two_blocks_returns_correct_data() {
        test_get_block("1234", 2, 1, "34");
    }

    #[test]
    fn download_second_block_for_file_smaller_than_two_blocks_returns_correct_data() {
        test_get_block("123", 2, 1, "3");
    }

    #[test]
    fn download_block_after_last_block_returns_error() {
        let downloader = create_downloader(2, 1, vec![("File".to_string(), Ok("1".to_string()))]);
        let stream = Arc::new(StreamInfo::new(HANDLER_TYPE, "File", SyncRecord::empty()));
        downloader
            .get_block(stream, 1)
            .should()
            .be_err()
            .which_value()
            .should()
            .equal(&GetBlockError::BlockOutOfBounds { idx: 1, block_count: 1 });
    }

    #[test]
    fn force_download_on_future_downloads_synchronously_if_queue_is_occupied() {
        #[derive(Clone)]
        struct TestOpener {
            pub data: StreamResult<Arc<(u64, Mutex<Vec<u8>>)>>,
        }

        impl StreamOpener for TestOpener {
            fn open(&self) -> StreamResult<Box<dyn Read + Send>> {
                unimplemented!()
            }

            fn get_properties(&self) -> StreamResult<StreamProperties> {
                Ok(StreamProperties {
                    size: self.data.clone()?.as_ref().0,
                    created_time: None,
                    modified_time: None,
                })
            }

            fn can_seek(&self) -> bool {
                unimplemented!()
            }

            fn try_as_seekable(&self) -> Option<&dyn SeekableStreamOpener> {
                Some(self)
            }
        }

        impl SeekableStreamOpener for TestOpener {
            fn open_seekable(&self) -> StreamResult<Box<dyn SeekableRead>> {
                unimplemented!()
            }

            fn copy_section_to(&self, offset: usize, target: &mut [u8]) -> StreamResult<()> {
                target.copy_from_slice(&self.data.as_ref().unwrap().1.lock().unwrap()[offset..offset + target.len()]);
                Ok(())
            }
        }

        let hanging_file = Arc::new((0, Mutex::new(Vec::new())));
        let working_file = Arc::new((4, Mutex::new(vec![1, 2, 3, 4])));
        let test_files: Arc<HashMap<String, Arc<(u64, Mutex<Vec<u8>>)>>> = Arc::new(
            vec![
                ("first".to_string(), hanging_file.clone()),
                ("second".to_string(), working_file.clone()),
            ]
            .into_iter()
            .collect(),
        );

        // We use this to ensure the first file hangs.
        let test_files_clone = test_files.clone();
        let handler = StubbedStreamHandler::default().with_opener(Box::new(move |r| {
            Ok(Arc::new(TestOpener {
                data: Ok(test_files_clone[r].clone()),
            }))
        }));
        let handler_type = handler.handler_type().to_string();
        let accessor = Arc::new(StreamAccessor::default().add_handler(handler));
        let downloader = FileBlockDownloader::new(accessor, 8, 1);

        let first_file_lock = test_files["first"].clone();
        let _guard = first_file_lock.1.lock().unwrap();

        let _hanging_future = downloader
            .get_block(Arc::new(StreamInfo::new(handler_type.to_string(), "first", SyncRecord::empty())), 0)
            .unwrap();
        let working_future = downloader
            .get_block(
                Arc::new(StreamInfo::new(handler_type.to_string(), "second", SyncRecord::empty())),
                0,
            )
            .unwrap();

        // This should cause data access to run synchronously in this thread, ignoring the thread pool which is blocked by the hanging future.
        working_future.fetch_now();

        let data = futures::executor::block_on(working_future).unwrap();
        data.as_ref().should().equal(&&vec![1u8, 2u8, 3u8, 4u8]);
    }

    #[test]
    fn download_block_when_download_fails_future_returns_error() {}

    #[test]
    fn download_block_for_wrong_handler_returns_error() {}
}
