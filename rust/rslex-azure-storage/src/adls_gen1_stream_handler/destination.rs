use super::request_builder::RequestBuilder;
use rslex_core::file_io::DestinationError;
use rslex_http_stream::{ApplyCredential, DestinationBuilder};
use std::sync::Arc;

pub(super) struct ADLSGen1DestinationBuilder {
    pub credential: Arc<dyn ApplyCredential>,
}

impl DestinationBuilder for ADLSGen1DestinationBuilder {
    type RequestBuilder = RequestBuilder;

    fn create_request_builder(&self, uri: &str) -> Result<Self::RequestBuilder, DestinationError> {
        Ok(RequestBuilder::new(uri, self.credential.clone())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adls_gen1_stream_handler::HANDLER_TYPE;
    use fluent_assertions::*;
    use itertools::Itertools;
    use rslex_core::{
        file_io::{Destination, OutputStream, ParallelWriteError, ParallelWriter, StreamError},
        test_helper::*,
        StreamInfo, SyncRecord,
    };
    use rslex_http_stream::{FakeHttpClient, Method};

    trait IntoDestination {
        fn into_destination<'a>(self, base_path: &'a str) -> Arc<dyn Destination + 'a>;

        fn into_output_stream(self, uri: &str) -> Result<Box<dyn OutputStream>, DestinationError>
        where Self: Sized {
            let uri = uri.rsplitn(2, '/').collect_vec();
            assert_eq!(uri.len(), 2);

            self.into_destination(&uri[1]).open_output_stream(&uri[0])
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

            self.into_destination(&uri[1])
                .try_open_parallel_writer(&uri[0], total_size, block_size_hint, 1)
                .expect("parallel writer exists")
        }
    }

    impl IntoDestination for FakeHttpClient {
        fn into_destination<'a>(self, base_path: &'a str) -> Arc<dyn Destination + 'a> {
            ADLSGen1DestinationBuilder { credential: Arc::new(()) }.build(base_path.into(), Arc::new(self))
        }
    }

    #[test]
    fn writer_writes_with_expected_http_requestes() {
        const CONTENT: &[u8] = b"file content";
        const URI: &str = "adl://someaccount.azuredatalakestore.net/somefile.csv";

        FakeHttpClient::default()
            // create
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.body().should().be_empty();
                r.uri().should().contain("CREATE");
                r.method().should().be(&Method::PUT);
            })
            .status(201)
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.body().should().equal_binary(CONTENT);
                r.uri().should().contain("APPEND");
                r.method().should().be(&Method::POST);
            })
            .status(200)
            .into_output_stream(URI)
            .should()
            .be_ok()
            .which_value()
            .write(CONTENT)
            .should()
            .be_ok();
    }

    #[test]
    fn get_output_stream_writes_to_correct_path() {
        const URI: &str = "adl://someaccount.azuredatalakestore.net/somefolder/target_file.csv";

        let mut output = FakeHttpClient::default()
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .start_with("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/target_file.csv");
            })
            .status(200)
            .into_output_stream(URI)
            .expect("Open output stream should return a write trait object.");

        output.write_all(&[1, 2, 3]).should().be_ok();
        output.flush().should().be_ok();
    }

    #[test]
    fn remove_should_trigger_delete_request() {
        const BASE_PATH: &str = "adl://someaccount.azuredatalakestore.net/somefolder";
        const FILE: &str = "target_file.csv";

        FakeHttpClient::default()
            .assert_request(move |_, r| {
                r.body().should().be_empty();
                r.method().should().be(&Method::DELETE);
                r.uri()
                    .should()
                    .start_with("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/target_file.csv");
            })
            .status(200)
            .into_destination(BASE_PATH)
            .remove(FILE)
            .should()
            .be_ok();
    }

    #[test]
    fn try_parallel_writer_returns_parallel_writer() {
        FakeHttpClient::default()
            .status(201)
            .into_destination("adl://somesite/somedir/")
            .try_open_parallel_writer("somefile.txt", 64, 16, 2)
            .should()
            .be_some()
            .which_value()
            .should()
            .be_ok();
    }

    #[test]
    fn create_successfully_returns_size() {
        FakeHttpClient::default()
            .status(201)
            .into_parallel_writer("adl://somesite/somedir/somefile.csv", 65, 16)
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|writer| {
                writer.block_count().should().be(5);
                writer.block_size().should().be(16);
                writer.expected_data_size().should().be(65);
                writer.completion_status().should().be_in_progress();
            });
    }

    #[test]
    fn create_writer_will_call_create_rest_api() {
        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.method().should().be(&Method::PUT);
                r.uri()
                    .should()
                    .equal_string("https://somesite/webhdfs/v1/somedir/somefile.csv?op=CREATE&overwrite=true&write=true");
                r.body().should().equal_binary(b"");
            })
            .assert_num_requests(1)
            .status(201)
            .into_parallel_writer("adl://somesite/somedir/somefile.csv", 65, 16)
            .should()
            .be_ok();
    }

    #[test]
    fn create_writer_with_create_failed_returns_err() {
        FakeHttpClient::default()
            .status(500)
            .into_parallel_writer("adl://somesite/somedir/somefile.csv", 65, 16)
            .should()
            .be_err();
    }

    #[test]
    fn write_block_create_one_block_file() {
        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .status(201)
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.method().should().be(&Method::PUT);
                r.uri()
                    .should()
                    .match_regex(r"https://somesite/webhdfs/v1/somedir/somefile\.csv_\S+?/0\?op=CREATE&overwrite=true&write=true");
                r.body().should().equal_binary(vec![1, 2, 3, 4, 5]);
            })
            .status(201)
            .into_parallel_writer("adl://somesite/somedir/somefile.csv", 65, 5)
            .expect("build writer should succeed")
            .get_block_writer()
            .write_binary(0, vec![1u8, 2, 3, 4, 5].as_ref())
            .should()
            .be_ok();
    }

    #[test]
    fn set_input_error_changes_status() {
        let writer = FakeHttpClient::default()
            .status(201)
            .into_parallel_writer("adl://somesite/somedir/somefile.csv", 65, 5)
            .expect("build writer should succeed");

        writer.get_block_writer().set_input_error(StreamError::NotFound);
        writer
            .completion_status()
            .should()
            .be_error()
            .which_value()
            .should()
            .be(ParallelWriteError::InputError(StreamError::NotFound));
    }

    #[test]
    fn last_block_written_calls_concat() {
        let mut writer = FakeHttpClient::default()
            // create root file
            .with_request(|id, _| id == 0)
            .status(201)
            // create block 0 & 1
            .with_request(|id, _| id == 1 || id == 2)
            .status(201)
            // concat block 0 to root
            .with_request(|id, _| id == 3)
            .assert_request(|_, r| {
                r.method().should().be(&Method::POST);
                r.uri()
                    .should()
                    .equal_string("https://somesite/webhdfs/v1/somedir/somefile.csv?op=MSCONCAT&api-version=2018-09-01");

                let expr = format!(
                    "\\{{\"sources\": \\[{}\\]\\}}",
                    vec![0, 1]
                        .iter()
                        .map(|&x| format!("\"/somedir/somefile\\.csv_\\S+?/{}\"", x))
                        .collect::<Vec<String>>()
                        .join(",")
                )
                .to_string();

                std::str::from_utf8(r.body())
                    .should()
                    .be_ok()
                    .which_value()
                    .should()
                    .match_regex(&expr);
            })
            .status(201)
            .into_parallel_writer("adl://somesite/somedir/somefile.csv", 10, 5)
            .expect("build writer should succeed");

        writer
            .get_block_writer()
            .write_binary(0, vec![1u8, 2, 3, 4, 5].as_ref())
            .should()
            .be_ok();

        writer
            .get_block_writer()
            .write_binary(1, vec![1, 2, 3, 4, 5].as_ref())
            .should()
            .be_ok();

        writer.wait_for_completion().should().be_ok().with_value(StreamInfo::new(
            HANDLER_TYPE,
            "adl://somesite/somedir/somefile.csv",
            SyncRecord::empty(),
        ));
        writer.completion_status().should().be_completed();
    }
}
