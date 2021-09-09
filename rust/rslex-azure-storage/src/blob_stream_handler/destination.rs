use super::request_builder::RequestBuilder;
use rslex_core::file_io::DestinationError;
use rslex_http_stream::{ApplyCredential, DestinationBuilder, HeadRequest, HttpClient, ResponseExt};
use std::sync::Arc;

pub(super) struct BlobDestinationBuilder {
    pub credential: Arc<dyn ApplyCredential>,
    pub should_overwrite: bool,
    pub http_client: Arc<dyn HttpClient>,
}

impl DestinationBuilder for BlobDestinationBuilder {
    type RequestBuilder = RequestBuilder;

    fn create_request_builder(&self, uri: &str) -> Result<Self::RequestBuilder, DestinationError> {
        let request_builder = RequestBuilder::new(uri, self.credential.clone())?;

        // This check can go somewhere else too, but putting it here requires minimum change in existing
        // traits.
        self.validate_overwrite(uri, &request_builder)?;

        Ok(request_builder)
    }

    fn should_overwrite(&self) -> bool {
        self.should_overwrite
    }
}

impl BlobDestinationBuilder {
    fn validate_overwrite(&self, uri: &str, request_builder: &RequestBuilder) -> Result<(), DestinationError> {
        if !self.should_overwrite() {
            // If we don't need to overwrite an existing blob, then verify that it already exists and is
            // an append blob.
            let blob_metadata = request_builder.head();
            // If blob doesn't exist then it will throw an error(404) here.
            let response = self.http_client.clone().request(blob_metadata)?.success()?;
            match response.headers().get("x-ms-blob-type") {
                None => {
                    return Err(DestinationError::InvalidInput {
                        message: format!("Blob type not found for {} blob", uri),
                        source: None,
                    });
                },
                Some(blob_type) => {
                    let blob_type_string = blob_type.to_str().map_err(|err| DestinationError::Unknown(err.to_string(), None))?;
                    if blob_type_string != "AppendBlob" {
                        return Err(DestinationError::InvalidInput {
                            message: format!(
                                "{} blob is {}. Only AppendBlobs are supported for Append operations.",
                                uri, blob_type_string
                            ),
                            source: None,
                        });
                    }
                },
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob_stream_handler::{
        blob_dto::{BlockId, BlockList},
        MAX_BLOCK_COUNT, MAX_BLOCK_SIZE,
    };
    use fluent_assertions::*;
    use http::Method;
    use itertools::Itertools;
    use rslex_core::{
        file_io::{Destination, OutputStream, ParallelWriteError, ParallelWriter, StreamError},
        test_helper::*,
    };
    use rslex_http_stream::FakeHttpClient;
    use std::{sync::Mutex, thread};

    const BLOCK_ID_PADDING_TOTAL_LENGTH: usize = 32;

    trait IntoBlobDestination {
        fn into_destination<'a>(self, base_path: &'a str, should_overwrite: &'a bool) -> Arc<dyn Destination + 'a>;

        fn into_output_stream(self, uri: &str, should_overwrite: &bool) -> Result<Box<dyn OutputStream>, DestinationError>
        where Self: Sized {
            let uri = uri.rsplitn(2, '/').collect_vec();
            assert_eq!(uri.len(), 2);

            self.into_destination(&uri[1], should_overwrite).open_output_stream(&uri[0])
        }

        fn into_parallel_writer(
            self,
            uri: &str,
            total_size: usize,
            block_size_hint: usize,
        ) -> Result<Box<dyn ParallelWriter>, DestinationError>
        where
            Self: Sized,
        {
            let uri = uri.rsplitn(2, '/').collect_vec();
            assert_eq!(uri.len(), 2);

            self.into_destination(&uri[1], &true)
                .try_open_parallel_writer(&uri[0], total_size, block_size_hint, 1)
                .expect("parallel writer exists")
        }
    }

    impl IntoBlobDestination for FakeHttpClient {
        fn into_destination<'a>(self, base_path: &'a str, should_overwrite: &'a bool) -> Arc<dyn Destination + 'a> {
            let fake_http_client = Arc::new(self);
            BlobDestinationBuilder {
                credential: Arc::new(()),
                should_overwrite: should_overwrite.clone(),
                http_client: fake_http_client.clone(),
            }
            .build(base_path.into(), fake_http_client.clone())
        }
    }

    fn get_padded_blockid(blockid: usize) -> String {
        format!("{:01$}", blockid, BLOCK_ID_PADDING_TOTAL_LENGTH)
    }

    #[test]
    fn blob_destination_returns_base_path() {
        let base_path = "http://somesite/somecontainer/somedir";

        FakeHttpClient::default()
            .into_destination(base_path, &true)
            .base_path()
            .should()
            .equal_string(base_path);
    }

    #[test]
    fn try_open_parallel_writer_returns_blob_writer() {
        FakeHttpClient::default()
            .status(201)
            .into_parallel_writer("http://somesite/somecontainer/somedir", 1024, 16)
            .should()
            .be_ok();
    }

    #[test]
    fn open_output_stream_returns_blob_output_stream() {
        FakeHttpClient::default()
            .status(201)
            .into_output_stream("http://somesite/somecontainer/somedir/a.csv", &true)
            .should()
            .be_ok();
    }

    #[test]
    fn remove_will_delete_blob() {
        const BASE_PATH: &str = "https://somesite/somecontainer/somedir";
        const FILE: &str = "a.txt";

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.method().should().be(&Method::DELETE);
                r.uri().should().equal_string(format!("{}/{}", BASE_PATH, FILE));
            })
            .status(201)
            .into_destination(BASE_PATH, &true)
            .remove(FILE)
            .should()
            .be_ok();
    }

    // output stream
    #[test]
    fn create_output_stream_will_put_blob() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.uri().should().equal_string(URI);
                r.method().should().be(&Method::PUT);
                r.headers()["x-ms-blob-type"].to_ref().should().equal(&"AppendBlob");
            })
            .assert_num_requests(1)
            .status(201)
            .into_output_stream(URI, &true)
            .should()
            .be_ok();
    }

    #[test]
    fn create_output_stream_without_overwrite_checks_blob_type() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.uri().should().equal_string(URI);
                r.method().should().be(&Method::HEAD);
                // r.headers()["x-ms-blob-type"].to_ref().should().equal(&"AppendBlob");
            })
            .header("x-ms-blob-type", "AppendBlob")
            .assert_num_requests(1)
            .status(200)
            .into_output_stream(URI, &false)
            .should()
            .be_ok();
    }

    #[test]
    fn resource_id_returns_resource_uri() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        FakeHttpClient::default()
            .status(201)
            .into_output_stream(URI, &true)
            .should()
            .be_ok()
            .which_value()
            .resource_id()
            .should()
            .equal_string(URI);
    }

    #[test]
    fn write_will_append_block() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        FakeHttpClient::default()
            // put blob
            .with_request(|id, _| id == 0)
            .status(201)
            // append blob
            .with_request(|id, _| id == 1)
            .assert_request(|_, result| {
                result.method().should().be(&Method::PUT);
                result.uri().should().equal_string(format!("{}?comp=appendblock", URI));
                result.headers()["Content-Length"].to_ref().should().equal(&"5");
                result.body().should().equal_binary(vec![1, 2, 3, 4, 5]);
            })
            .status(201)
            .into_output_stream(URI, &true)
            .should()
            .be_ok()
            .which_value()
            .write(vec![1, 2, 3, 4, 5].as_slice())
            .should()
            .be_ok()
            .with_value(5);
    }

    #[test]
    fn write_will_append_block_with_no_overwrite() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        FakeHttpClient::default()
            // append blob
            .with_request(|id, _| id == 0)
            .header("x-ms-blob-type", "AppendBlob")
            .assert_request(|_, result| {
                result.method().should().be(&Method::HEAD);
                result.uri().should().equal_string(URI);
            })
            .status(200)
            .with_request(|id, _| id == 1)
            .assert_request(|_, result| {
                result.method().should().be(&Method::PUT);
                result.uri().should().equal_string(format!("{}?comp=appendblock", URI));
                result.headers()["Content-Length"].to_ref().should().equal(&"5");
                result.body().should().equal_binary(vec![1, 2, 3, 4, 5]);
            })
            .status(201)
            .into_output_stream(URI, &false)
            .should()
            .be_ok()
            .which_value()
            .write(vec![1, 2, 3, 4, 5].as_slice())
            .should()
            .be_ok()
            .with_value(5);
    }

    // parallel writer
    #[test]
    fn create_blob_writer_with_block_size_hint_returns_block_size_and_block_count_and_expected_size() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        let writer = FakeHttpClient::default().status(201).into_parallel_writer(URI, 1024, 256).unwrap();

        writer.block_size().should().be(256);
        writer.block_count().should().be(4);
        writer.expected_data_size().should().be(1024);
    }

    #[test]
    fn when_total_size_not_multiple_blocks_returns_one_more_block() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        let writer = FakeHttpClient::default().status(201).into_parallel_writer(URI, 1025, 256).unwrap();

        writer.block_size().should().be(256);
        writer.block_count().should().be(5);
        writer.expected_data_size().should().be(1025);
    }

    #[test]
    fn when_block_size_hint_exceed_max_will_be_clipped() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";
        let total_size = MAX_BLOCK_SIZE * 10;

        let writer = FakeHttpClient::default()
            .status(201)
            .into_parallel_writer(URI, total_size, total_size)
            .unwrap();

        writer.block_size().should().be(MAX_BLOCK_SIZE);
        writer.block_count().should().be(10);
        writer.expected_data_size().should().be(total_size);
    }

    #[test]
    fn when_block_count_exceed_max_will_increase_block_size() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        let total_size = MAX_BLOCK_COUNT * 10;
        let writer = FakeHttpClient::default()
            .status(201)
            .into_parallel_writer(URI, total_size, 1)
            .unwrap();

        writer.block_size().should().be(10);
        writer.block_count().should().be(MAX_BLOCK_COUNT);
        writer.expected_data_size().should().be(total_size);
    }

    #[test]
    fn when_created_complete_status_should_be_in_progress() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        let writer = FakeHttpClient::default().status(201).into_parallel_writer(URI, 1024, 256).unwrap();

        writer.completion_status().should().be_in_progress();
    }

    #[test]
    fn write_first_block_will_create_blob_and_write_one_block() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        let writer = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri().should().equal_string(URI);
                r.method().should().be(&Method::PUT);
                r.headers()["x-ms-blob-type"].to_ref().should().equal(&"BlockBlob");
            })
            .status(201)
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::PUT);
                r.uri()
                    .should()
                    .equal_string(format!("{}?comp=block&blockid=MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=", URI));
                r.headers()["Content-Length"].to_ref().should().equal(&"5");
                r.body().should().equal_binary(vec![0, 1, 2, 3, 4]);
            })
            .status(201)
            .into_parallel_writer(URI, 100, 5)
            .unwrap();

        writer
            .get_block_writer()
            .write_binary(0, vec![0, 1, 2, 3, 4].as_ref())
            .should()
            .be_ok();
    }

    #[test]
    fn write_two_blocks_put_blob_will_be_called_only_once() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        let writer = FakeHttpClient::default()
            // setup first call -- put_blob
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri().should().equal_string(URI);
                r.method().should().be(&Method::PUT);
                r.headers()["x-ms-blob-type"].to_ref().should().equal(&"BlockBlob");
            })
            .status(201)
            .assert_num_requests(1)
            // setup second call -- put 1st block
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::PUT);
                r.uri()
                    .should()
                    .equal_string(format!("{}?comp=block&blockid=MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=", URI));
                r.headers()["Content-Length"].to_ref().should().equal(&"5");
                r.body().should().equal_binary(vec![0, 1, 2, 3, 4]);
            })
            .status(201)
            // setup third call -- put 2nd block
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                r.method().should().be(&Method::PUT);
                r.uri()
                    .should()
                    .equal_string(format!("{}?comp=block&blockid=MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDE=", URI));
                r.headers()["Content-Length"].to_ref().should().equal(&"5");
                r.body().should().equal_binary(vec![5, 6, 7, 8, 9]);
            })
            .status(201)
            .into_parallel_writer(URI, 100, 5)
            .unwrap();

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
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        let writer = FakeHttpClient::default()
            // first call, put blob
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri().should().equal_string(URI);
                r.method().should().be(&Method::PUT);
                r.headers()["x-ms-blob-type"].to_ref().should().equal(&"BlockBlob");
            })
            .status(201)
            // second call, put block
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::PUT);
                r.uri()
                    .should()
                    .equal_string(format!("{}?comp=block&blockid=MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=", URI));
                r.headers()["Content-Length"].to_ref().should().equal(&"5");
                r.body().should().equal_binary(vec![0, 1, 2, 3, 4]);
            })
            .status(201)
            // third call, put block list
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                let body = BlockList {
                    blocks: vec![BlockId::Latest(get_padded_blockid(0))],
                }
                .to_string();
                r.method().should().be(&Method::PUT);
                r.uri().should().equal_string(format!("{}?comp=blocklist", URI));
                r.headers()["Content-Length"]
                    .to_ref()
                    .should()
                    .equal(&body.len().to_string().as_str());
                r.body().should().equal_binary(body.as_bytes());
            })
            .assert_num_requests(1)
            .status(201)
            .into_parallel_writer(URI, 5, 5)
            .unwrap();

        writer
            .get_block_writer()
            .write_binary(0, vec![0, 1, 2, 3, 4].as_ref())
            .should()
            .be_ok();
    }

    #[test]
    fn block_will_be_sorted_in_order_when_committed() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        let writer = FakeHttpClient::default()
            // call 0, put blob
            .with_request(|id, _| id == 0)
            .status(201)
            // call 1- 4 , put block
            .with_request(|id, _| id > 0 && id < 5)
            .status(201)
            // call 5, put block list
            .with_request(|id, _| id == 5)
            .assert_request(|_, r| {
                // body must be sorted
                let body = BlockList {
                    blocks: vec![
                        BlockId::Latest(get_padded_blockid(0)),
                        BlockId::Latest(get_padded_blockid(1)),
                        BlockId::Latest(get_padded_blockid(2)),
                        BlockId::Latest(get_padded_blockid(3)),
                    ],
                }
                .to_string();
                r.body().should().equal_binary(body.as_bytes());
            })
            .assert_num_requests(1)
            .status(201)
            .into_parallel_writer(URI, 20, 5)
            .unwrap();

        // random order, but when committed should be sorted
        writer
            .get_block_writer()
            .write_binary(2, vec![0, 1, 2, 3, 4].as_ref())
            .should()
            .be_ok();
        writer
            .get_block_writer()
            .write_binary(3, vec![0, 1, 2, 3, 4].as_ref())
            .should()
            .be_ok();
        writer
            .get_block_writer()
            .write_binary(1, vec![0, 1, 2, 3, 4].as_ref())
            .should()
            .be_ok();
        writer
            .get_block_writer()
            .write_binary(0, vec![0, 1, 2, 3, 4].as_ref())
            .should()
            .be_ok();
    }

    #[test]
    fn after_set_input_error_write_block_will_throw_error() {
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        let writer = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri().should().equal_string(URI);
                r.method().should().be(&Method::PUT);
                r.headers()["x-ms-blob-type"].to_ref().should().equal(&"BlockBlob");
            })
            .status(201)
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::PUT);
                r.uri().should().equal_string(format!(
                    "{}?comp=block&blockid=MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMA==",
                    URI
                ));
                r.headers()["Content-Length"].to_ref().should().equal(&"5");
                r.body().should().equal_binary(vec![0, 1, 2, 3, 4]);
            })
            .status(201)
            .into_parallel_writer(URI, 100, 5)
            .unwrap();

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
        const URI: &str = "https://somesite/somecontainer/somedir/a.txt";

        let mut writer = FakeHttpClient::default()
            // first call, put blob
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri().should().equal_string(URI);
                r.method().should().be(&Method::PUT);
                r.headers()["x-ms-blob-type"].to_ref().should().equal(&"BlockBlob");
            })
            .status(201)
            // second call, put block
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::PUT);
                r.uri()
                    .should()
                    .equal_string(format!("{}?comp=block&blockid=MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=", URI));
                r.headers()["Content-Length"].to_ref().should().equal(&"5");
                r.body().should().equal_binary(vec![0, 1, 2, 3, 4]);
            })
            .status(201)
            // third call, put block list
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                let body = BlockList {
                    blocks: vec![BlockId::Latest(get_padded_blockid(0))],
                }
                .to_string();
                r.method().should().be(&Method::PUT);
                r.uri().should().equal_string(format!("{}?comp=blocklist", URI));
                r.headers()["Content-Length"]
                    .to_ref()
                    .should()
                    .equal(&body.len().to_string().as_str());
                r.body().should().equal_binary(body.as_bytes());
            })
            .status(201)
            .into_parallel_writer(URI, 5, 5)
            .unwrap();

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
