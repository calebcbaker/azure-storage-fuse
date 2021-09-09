use crate::adls_gen2_stream_handler::request_builder::RequestBuilder;
use rslex_core::file_io::DestinationError;
use rslex_http_stream::{ApplyCredential, DestinationBuilder};
use std::sync::Arc;

pub(super) struct ADLSGen2DestinationBuilder {
    pub credential: Arc<dyn ApplyCredential>,
}

impl DestinationBuilder for ADLSGen2DestinationBuilder {
    type RequestBuilder = RequestBuilder;

    fn create_request_builder(&self, uri: &str) -> Result<Self::RequestBuilder, DestinationError> {
        Ok(RequestBuilder::new(uri, self.credential.clone())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adls_gen2_stream_handler::{request_builder::FileSystemResourceType, HANDLER_TYPE};
    use fluent_assertions::*;
    use itertools::Itertools;
    use rslex_core::{
        file_io::{Destination, OutputStream, ParallelWriter},
        StreamInfo, SyncRecord,
    };
    use rslex_http_stream::{FakeHttpClient, Method};

    trait IntoADLSGen2Destination {
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

    impl IntoADLSGen2Destination for FakeHttpClient {
        fn into_destination<'a>(self, base_path: &'a str) -> Arc<dyn Destination + 'a> {
            ADLSGen2DestinationBuilder { credential: Arc::new(()) }.build(base_path.into(), Arc::new(self))
        }
    }

    #[test]
    fn writer_writes_with_expected_http_requestes() {
        let content = "file content";
        let mut writer = FakeHttpClient::default()
            .with_request(move |_, r| {
                r.body().len() == 0
                    && r.uri()
                        .to_string()
                        .contains(&format!("resource={}", FileSystemResourceType::File.to_string().to_lowercase()))
                    && r.method() == Method::PUT
                    && r.headers()["Content-Length"].to_str().unwrap() == "0"
            })
            .status(201)
            .with_request(move |_, r| {
                String::from_utf8(r.body().clone()).unwrap().eq(&content)
                    && r.uri().to_string().contains(&format!("action=append&position={}", 0))
                    && r.method() == Method::PATCH
                    && r.headers()["Content-Length"].to_str().unwrap() == content.len().to_string()
            })
            .status(200)
            .with_request(move |_, r| {
                String::from_utf8(r.body().clone()).unwrap().eq("")
                    && r.uri().to_string().contains(&format!("action=flush&position={}", content.len()))
                    && r.method() == Method::PATCH
                    && r.headers()["Content-Length"].to_str().unwrap() == "0"
            })
            .status(200)
            .into_output_stream("https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv")
            .unwrap();

        writer.write(&content.as_bytes()).should().be_ok();
        writer.flush().should().be_ok();
    }

    #[test]
    fn remove_should_trigger_delete_request() {
        const FILE: &str = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.uri().should().start_with(FILE);
                r.body().should().be_empty();
                r.method().should().be(&Method::DELETE);
            })
            .status(200)
            .into_destination("")
            .remove(FILE)
            .should()
            .be_ok();
    }

    #[test]
    fn create_parallel_write_will_create_file() {
        const FILE: &str = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.uri().should().start_with(FILE).and().should().contain("resource=file");
                r.body().should().be_empty();
                r.method().should().be(&Method::PUT);
            })
            .status(200)
            .into_parallel_writer(FILE, 100, 10)
            .should()
            .be_ok();
    }

    #[test]
    fn parallel_write_will_append() {
        const FILE: &str = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        const CONTENT: &[u8] = b"ABCDE";
        FakeHttpClient::default()
            // create
            .with_request(|id, _| id == 0)
            .status(200)
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .start_with(FILE)
                    .and()
                    .should()
                    .contain("action=append")
                    .and()
                    .should()
                    .contain("position=0");
                r.body().should().equal_binary(CONTENT);
                r.method().should().be(&Method::PATCH);
                r.headers()["Content-Length"].to_ref().should().equal(&"5");
            })
            .status(200)
            .into_parallel_writer(FILE, 100, 5)
            .should()
            .be_ok()
            .which_value()
            .get_block_writer()
            .write_binary(0, CONTENT)
            .should()
            .be_ok();
    }

    #[test]
    fn parallel_write_multiple_blocks_will_append_with_position() {
        const FILE: &str = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        const CONTENT: &[u8] = b"ABCDE";
        let writer = FakeHttpClient::default()
            // create
            .with_request(|id, _| id == 0)
            .status(200)
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .start_with(FILE)
                    .and()
                    .should()
                    .contain("action=append")
                    .and()
                    .should()
                    .contain("position=0");
                r.body().should().equal_binary(CONTENT);
                r.method().should().be(&Method::PATCH);
                r.headers()["Content-Length"].to_ref().should().equal(&"5");
            })
            .status(200)
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .start_with(FILE)
                    .and()
                    .should()
                    .contain("action=append")
                    .and()
                    .should()
                    .contain("position=5");
                r.body().should().equal_binary(CONTENT);
                r.method().should().be(&Method::PATCH);
                r.headers()["Content-Length"].to_ref().should().equal(&"5");
            })
            .status(200)
            .into_parallel_writer(FILE, 100, 5)
            .should()
            .be_ok()
            .which_value()
            .get_block_writer();

        writer.write_binary(0, CONTENT).should().be_ok();
        writer.write_binary(1, CONTENT).should().be_ok();
    }

    #[test]
    fn parallel_write_complete_will_flush() {
        const FILE: &str = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";
        const CONTENT: &[u8] = b"ABCDE";
        let mut writer = FakeHttpClient::default()
            // create & write block
            .with_request(|id, _| id == 0 || id == 1)
            .status(200)
            // flush
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .start_with(FILE)
                    .and()
                    .should()
                    .contain("action=flush")
                    .and()
                    .should()
                    .contain("position=5");
                r.body().should().be_empty();
                r.method().should().be(&Method::PATCH);
            })
            .status(200)
            .into_parallel_writer(FILE, 5, 5)
            .should()
            .be_ok()
            .which_value();

        writer.get_block_writer().write_binary(0, CONTENT).should().be_ok();
        writer
            .wait_for_completion()
            .should()
            .be_ok()
            .with_value(StreamInfo::new(HANDLER_TYPE, FILE, SyncRecord::empty()));
    }
}
