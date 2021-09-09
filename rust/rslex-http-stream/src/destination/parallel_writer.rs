use crate::{create_http_client, AuthenticatedRequest, HttpClient, HttpClientCreationError, ResponseExt};
use rslex_core::{
    binary_buffer_pool::{BinaryBufferPool, PooledBuffer},
    file_io::{
        BlockWriter as BlockWrite, CompletionStatus, DestinationError, ParallelWriteError, ParallelWriter as ParallelWrite, StreamError,
    },
    StreamInfo,
};
use std::{
    cmp,
    collections::HashSet,
    sync::{Arc, Condvar, Mutex},
};

thread_local! {
    static HTTP_CLIENT: Result<Arc<dyn HttpClient>, HttpClientCreationError> = create_http_client().map::<Arc<dyn HttpClient>, _>(|client| Arc::new(client));
}

pub trait ParallelWriteRequest {
    fn create(&self) -> AuthenticatedRequest;

    fn write_block(&self, block_idx: usize, position: usize, data: &[u8]) -> AuthenticatedRequest;

    fn complete(&self, block_count: usize, position: usize) -> AuthenticatedRequest;

    fn stream_info(&self) -> StreamInfo;

    fn max_block_size(&self) -> usize {
        usize::max_value()
    }

    fn max_block_count(&self) -> usize {
        usize::max_value()
    }
}

struct SyncData {
    pub blocks_processed: HashSet<usize>,
    pub completion_status: CompletionStatus,
}

struct BlockWriter<Q> {
    request_builder: Q,
    block_size: usize,
    block_count: usize,
    total_size: usize,
    sync_data: Mutex<SyncData>,
    signal: Condvar,
    buffer_pool: BinaryBufferPool,
    #[cfg(any(feature = "fake_http_client", test))]
    fake_http_client: Arc<dyn HttpClient>,
}

pub(crate) struct ParallelWriter<Q>(Arc<BlockWriter<Q>>);

impl<Q: ParallelWriteRequest + Send + Sync> ParallelWriter<Q> {
    pub fn new(
        request_builder: Q,
        total_size: usize,
        block_size_hint: usize,
        #[cfg(any(feature = "fake_http_client", test))] fake_http_client: Arc<dyn HttpClient>,
    ) -> Result<Self, DestinationError> {
        macro_rules! div {
            ($a:expr, $b:expr) => {{
                (($a - 1) / $b) + 1
            }};
        }

        let max_block_size = request_builder.max_block_size();
        let max_block_count = request_builder.max_block_count();
        let max_total_size = if max_block_count != usize::max_value() && max_block_size != usize::max_value() {
            max_block_size * max_block_count
        } else {
            usize::max_value()
        };

        assert!(
            total_size < max_total_size,
            "total_size ({}) exceed max_total_size ({})",
            total_size,
            max_total_size
        );

        let min_block_size = div!(total_size, max_block_count);
        let block_size = if block_size_hint > max_block_size {
            log::warn!(
                "block_size_hint({}) > max_block_size({}), adjust to {}",
                block_size_hint,
                max_block_size,
                max_block_size
            );
            max_block_size
        } else if block_size_hint < min_block_size {
            log::warn!(
                "block_count({}) > max_block_count({}), adjust block_size to {}",
                div!(total_size, block_size_hint),
                max_block_count,
                min_block_size
            );
            min_block_size
        } else {
            block_size_hint
        };
        let block_count = div!(total_size, block_size);

        debug_assert!(0 < block_size && block_size <= max_block_size);
        debug_assert!(0 < block_count && block_count <= max_block_count);

        let sync_data = Mutex::new(SyncData {
            blocks_processed: HashSet::new(),
            completion_status: CompletionStatus::InProgress,
        });

        let block_writer = BlockWriter {
            request_builder,
            block_size,
            block_count,
            total_size,
            sync_data,
            signal: Condvar::new(),
            buffer_pool: BinaryBufferPool::new(block_count, block_size),
            #[cfg(any(feature = "fake_http_client", test))]
            fake_http_client,
        };

        block_writer.create()?;

        Ok(ParallelWriter(Arc::new(block_writer)))
    }
}

impl<Q: ParallelWriteRequest + Send + Sync> BlockWriter<Q> {
    #[cfg(not(any(feature = "fake_http_client", test)))]
    fn http_client(&self) -> Result<Arc<dyn HttpClient>, DestinationError> {
        HTTP_CLIENT.with(|client_result| match client_result {
            Ok(client) => Ok(client.clone()),
            Err(e) => Err(DestinationError::from(e.clone())),
        })
    }

    #[cfg(any(feature = "fake_http_client", test))]
    fn http_client(&self) -> Result<Arc<dyn HttpClient>, DestinationError> {
        Ok(self.fake_http_client.clone())
    }

    fn create(&self) -> Result<(), DestinationError> {
        let request = self.request_builder.create();
        self.http_client()?.request(request)?.success()?;

        Ok(())
    }

    fn write_block(&self, block_idx: usize, data: &[u8]) -> Result<(), DestinationError> {
        assert!(
            block_idx < self.block_count,
            "block_idx ({}) out of range ({})",
            block_idx,
            self.block_count
        );

        let request = self.request_builder.write_block(block_idx, self.block_size * block_idx, data);
        self.http_client()?.request(request)?.success()?;

        let finish = {
            let mut sync_data = self.sync_data.lock().unwrap();
            let inserted = sync_data.blocks_processed.insert(block_idx);
            assert!(inserted, "block has been processed");

            sync_data.blocks_processed.len() == self.block_count
        };

        if finish {
            self.complete()?;
        }

        Ok(())
    }

    fn complete(&self) -> Result<(), DestinationError> {
        if let Err(e) = self.complete_request() {
            self.sync_data.lock().unwrap().completion_status = CompletionStatus::Error(e.clone().into());
            self.signal.notify_all();

            Err(e)
        } else {
            self.sync_data.lock().unwrap().completion_status = CompletionStatus::Completed;
            self.signal.notify_all();
            Ok(())
        }
    }

    fn complete_request(&self) -> Result<(), DestinationError> {
        let request = self.request_builder.complete(self.block_count, self.total_size);
        self.http_client()?.request(request)?.success()?;

        Ok(())
    }

    fn wait_for_completion(&self) -> Result<(), ParallelWriteError> {
        let mut lock = self.sync_data.lock().unwrap();
        while let CompletionStatus::InProgress = &lock.completion_status {
            lock = self.signal.wait(lock).unwrap();
        }

        match &lock.completion_status {
            CompletionStatus::Error(e) => Err(e.clone()),
            CompletionStatus::Completed => Ok(()),
            CompletionStatus::InProgress => {
                panic!("[parallel_writer::BlockWriter::wait_for_completion] execution completed while task is in progress");
            },
        }
    }
}

impl<Q: ParallelWriteRequest + Send + Sync + 'static> ParallelWrite for ParallelWriter<Q> {
    fn block_size(&self) -> usize {
        self.0.block_size
    }

    fn block_count(&self) -> usize {
        self.0.block_count
    }

    fn expected_data_size(&self) -> usize {
        self.0.total_size
    }

    fn completion_status(&self) -> CompletionStatus {
        self.0.sync_data.lock().unwrap().completion_status.clone()
    }

    fn get_block_writer(&self) -> Arc<dyn BlockWrite> {
        self.0.clone()
    }

    fn wait_for_completion(&mut self) -> Result<StreamInfo, ParallelWriteError> {
        self.0.wait_for_completion()?;

        Ok(self.0.request_builder.stream_info())
    }
}

impl<Q: ParallelWriteRequest + Send + Sync> BlockWrite for BlockWriter<Q> {
    fn get_block_buffer(&self, block_idx: usize) -> PooledBuffer {
        let data_start_offset = block_idx * self.block_size;
        let data_end_offset = cmp::min(data_start_offset + self.block_size, self.total_size);
        let buffer_length = data_end_offset - data_start_offset;
        self.buffer_pool.check_out().truncate(buffer_length)
    }

    fn set_input_error(&self, error: StreamError) {
        self.sync_data.lock().unwrap().completion_status = CompletionStatus::Error(ParallelWriteError::InputError(error));
        self.signal.notify_all();
    }

    fn write_block(&self, block_idx: usize, data: PooledBuffer) -> Result<(), ParallelWriteError> {
        if let CompletionStatus::Error(e) = &self.sync_data.lock().unwrap().completion_status {
            return Err(e.clone());
        }

        self.write_block(block_idx, &*data)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{destination::fake_request_builder::RequestBuilder, FakeHttpClient};
    use fluent_assertions::*;
    use rslex_core::test_helper::*;
    use std::thread;

    trait IntoWriter {
        fn into_writer(self, total_size: usize, block_size_hint: usize, url: &str) -> ParallelWriter<RequestBuilder>;
    }

    impl IntoWriter for FakeHttpClient {
        fn into_writer(self, total_size: usize, block_size_hint: usize, url: &str) -> ParallelWriter<RequestBuilder> {
            ParallelWriter::new(RequestBuilder(url.to_owned()), total_size, block_size_hint, Arc::new(self)).unwrap()
        }
    }

    #[test]
    fn create_blob_writer_with_block_size_hint_returns_block_size_and_block_count_and_expected_size() {
        let writer = FakeHttpClient::default().status(201).into_writer(1024, 256, "https://someresource");

        writer.block_size().should().be(256);
        writer.block_count().should().be(4);
        writer.expected_data_size().should().be(1024);
    }

    #[test]
    fn when_total_size_not_multiple_blocks_returns_one_more_block() {
        let writer = FakeHttpClient::default().status(201).into_writer(1025, 256, "https://someresource");

        writer.block_size().should().be(256);
        writer.block_count().should().be(5);
        writer.expected_data_size().should().be(1025);
    }

    #[test]
    fn when_block_size_hint_exceed_max_will_be_clipped() {
        let max_block_size = RequestBuilder("https://someresource".to_owned()).max_block_size();
        let total_size = max_block_size * 10;
        let writer = FakeHttpClient::default()
            .status(201)
            .into_writer(total_size, total_size, "https://someresource");

        writer.block_size().should().be(max_block_size);
        writer.block_count().should().be(10);
        writer.expected_data_size().should().be(total_size);
    }

    #[test]
    fn when_block_count_exceed_max_will_increase_block_size() {
        let max_block_count = RequestBuilder("https://someresource".to_owned()).max_block_count();
        let total_size = max_block_count * 10;
        let writer = FakeHttpClient::default()
            .status(201)
            .into_writer(total_size, 1, "https://someresource");

        writer.block_size().should().be(10);
        writer.block_count().should().be(max_block_count);
        writer.expected_data_size().should().be(total_size);
    }

    #[test]
    fn when_created_complete_status_should_be_in_progress() {
        let writer = FakeHttpClient::default().status(201).into_writer(1024, 256, "https://someresource");

        writer.completion_status().should().be_in_progress();
    }

    #[test]
    fn write_first_block_will_create_blob_and_write_one_block() {
        let writer = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_create(r);
            })
            .status(201)
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_write_block(r, 0, 0, vec![0, 1, 2, 3, 4]);
            })
            .status(201)
            .into_writer(25, 5, "https://someresource");

        writer
            .get_block_writer()
            .write_binary(0, vec![0, 1, 2, 3, 4].as_ref())
            .should()
            .be_ok();
    }

    #[test]
    fn write_two_blocks_put_blob_will_be_called_only_once() {
        let writer = FakeHttpClient::default()
            // setup first call -- put_blob
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_create(r);
            })
            .status(201)
            .assert_num_requests(1)
            // setup second call -- put 1st block
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_write_block(r, 0, 0, vec![0, 1, 2, 3, 4]);
            })
            .status(201)
            // setup third call -- put 2nd block
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_write_block(r, 1, 5, vec![5, 6, 7, 8, 9]);
            })
            .status(201)
            .into_writer(20, 5, "https://someresource");

        writer
            .get_block_writer()
            .write_binary(0, vec![0, 1, 2, 3, 4].as_ref())
            .should()
            .be_ok();
        writer
            .get_block_writer()
            .write_binary(1, vec![5, 6, 7, 8, 9].as_ref())
            .should()
            .be_ok();
    }

    #[test]
    fn when_all_blocks_written_will_commit_blocks() {
        let writer = FakeHttpClient::default()
            // first call, put blob
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_create(r);
            })
            .status(201)
            // second call, put block
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_write_block(r, 0, 0, vec![0, 1, 2, 3, 4]);
            })
            .status(201)
            // third call, put block list
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_complete(r, 1, 5);
            })
            .assert_num_requests(1)
            .status(201)
            .into_writer(5, 5, "https://someresource");

        writer
            .get_block_writer()
            .write_binary(0, vec![0, 1, 2, 3, 4].as_ref())
            .should()
            .be_ok();
    }

    #[test]
    fn after_set_input_error_write_block_will_throw_error() {
        let writer = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_create(r);
            })
            .status(201)
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_write_block(r, 0, 0, vec![0, 1, 2, 3, 4]);
            })
            .status(201)
            .into_writer(100, 5, "https://someresource");

        writer.get_block_writer().set_input_error(StreamError::NotFound);
        writer
            .completion_status()
            .should()
            .be_error()
            .which_value()
            .should()
            .be(ParallelWriteError::InputError(StreamError::NotFound));

        writer
            .get_block_writer()
            .write_binary(0, vec![0, 1, 2, 3, 4].as_ref())
            .should()
            .be_err()
            .with_value(ParallelWriteError::InputError(StreamError::NotFound));
    }

    #[test]
    fn wait_for_completion_will_wait_until_all_blocks_written() {
        let mut writer = FakeHttpClient::default()
            // first call, put blob
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_create(r);
            })
            .status(201)
            // second call, put block
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_write_block(r, 0, 0, vec![0, 1, 2, 3, 4]);
            })
            .status(201)
            // third call, put block list
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_parallel_complete(r, 1, 5);
            })
            .status(201)
            .into_writer(5, 5, "https://someresource");

        // the test the logic, we will start two threads,
        //  thread 1: handle the write_block task,
        //  thread 2: wait_for_completion
        // to verify the system works properly, we will use one global stage to verify the steps are followed one by one:
        // stage = 0: main thread set this
        // stage = 1: thread 1 will update the status to 1 before it start write
        // stage = 2: thread 2 will update the status to 2 wait for completion
        let stage = Arc::new(Mutex::new(0));

        let writer1 = writer.get_block_writer();
        writer.completion_status().should().be_in_progress();
        let stage1 = stage.clone();
        thread::spawn(move || {
            {
                let mut s = stage1.lock().unwrap();
                (*s).should().be(0);
                *s = 1;
            }

            writer1.write_binary(0, vec![0, 1, 2, 3, 4].as_ref()).should().be_ok();
        });

        writer.wait_for_completion().should().be_ok();
        {
            let s = stage.lock().unwrap();
            (*s).should().be(1);
        }
        writer.completion_status().should().be_completed();
    }
}
