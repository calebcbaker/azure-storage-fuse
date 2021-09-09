use crate::{
    destination::{
        append_writer::{AppendWriteRequest, AppendWriter},
        chunked_writer::ChunkedWriter,
        parallel_writer::{ParallelWriteRequest, ParallelWriter},
    },
    AuthenticatedRequest, HttpClient, ResponseExt,
};
use rslex_core::file_io::{
    Destination as BaseDestination, DestinationError, OutputStream as BaseOutputStream, ParallelWriter as BaseParallelWriter,
};
use std::{borrow::Cow, sync::Arc};

const CHUNK_SIZE: usize = 4 * 1024 * 1024;

pub trait CreateDirectoryRequest {
    fn create_directory(&self) -> AuthenticatedRequest;
}

pub trait RemoveRequest {
    fn remove(&self) -> AuthenticatedRequest;

    fn remove_directory(&self) -> AuthenticatedRequest {
        self.remove()
    }
}

pub trait DestinationBuilder: Send + Sync + Sized + 'static {
    type RequestBuilder: AppendWriteRequest + ParallelWriteRequest + CreateDirectoryRequest + RemoveRequest + Send + Sync + 'static;

    fn create_request_builder(&self, uri: &str) -> Result<Self::RequestBuilder, DestinationError>;

    fn build<'a>(self, base_path: Cow<'a, str>, http_client: Arc<dyn HttpClient>) -> Arc<dyn BaseDestination + 'a> {
        Arc::new(Destination::new(base_path, self, http_client))
    }

    fn should_overwrite(&self) -> bool {
        true
    }
}

fn merge_paths(left: &str, right: &str) -> String {
    format!("{}/{}", left.trim_end_matches('/'), right.trim_start_matches('/'))
}

pub(super) struct Destination<'s, B> {
    base_path: Cow<'s, str>,
    dest_builder: B,
    http_client: Arc<dyn HttpClient>,
}

impl<'s, B: DestinationBuilder> Destination<'s, B> {
    fn new(base_path: Cow<'s, str>, dest_builder: B, http_client: Arc<dyn HttpClient>) -> Self {
        Destination {
            base_path,
            dest_builder,
            http_client,
        }
    }

    fn get_writer(
        &self,
        resource_id: &str,
        total_size: usize,
        block_size_hint: usize,
    ) -> Result<impl BaseParallelWriter, DestinationError> {
        let path = merge_paths(self.base_path.as_ref(), resource_id);
        let request_builder = self.dest_builder.create_request_builder(&path)?;

        ParallelWriter::new(
            request_builder,
            total_size,
            block_size_hint,
            #[cfg(any(feature = "fake_http_client", test))]
            self.http_client.clone(),
        )
    }
}

impl<'s, B: DestinationBuilder + Send + Sync + 'static> BaseDestination for Destination<'s, B> {
    fn base_path(&self) -> &str {
        self.base_path.as_ref()
    }

    fn open_output_stream(&self, resource_id: &str) -> Result<Box<dyn BaseOutputStream>, DestinationError> {
        let path = merge_paths(self.base_path.as_ref(), resource_id);
        let request_builder = self.dest_builder.create_request_builder(&path)?;
        let should_overwrite = self.dest_builder.should_overwrite();
        let writer = AppendWriter::new(request_builder, self.http_client.clone(), should_overwrite)?;
        let writer = ChunkedWriter::new(path.into(), writer, CHUNK_SIZE);
        Ok(Box::new(writer))
    }

    fn try_open_parallel_writer(
        &self,
        resource_id: &str,
        total_size: usize,
        block_size_hint: usize,
        _target_parallelization: usize,
    ) -> Option<Result<Box<dyn BaseParallelWriter>, DestinationError>> {
        Some(
            self.get_writer(resource_id, total_size, block_size_hint)
                .map(|w| Box::new(w) as Box<dyn BaseParallelWriter>),
        )
    }

    fn create_file(&self, resource_id: &str) -> Result<(), DestinationError> {
        let path = merge_paths(self.base_path.as_ref(), resource_id);
        let request_builder = self.dest_builder.create_request_builder(&path)?;

        let request = ParallelWriteRequest::create(&request_builder);
        self.http_client.clone().request(request)?.success()?;

        Ok(())
    }

    fn create_directory(&self, resource_id: &str) -> Result<(), DestinationError> {
        let path = merge_paths(self.base_path.as_ref(), resource_id);
        let request_builder = self.dest_builder.create_request_builder(&path)?;

        let request = request_builder.create_directory();
        self.http_client.clone().request(request)?.success()?;

        Ok(())
    }

    fn remove_directory(&self, resource_id: &str) -> Result<(), DestinationError> {
        let path = merge_paths(self.base_path.as_ref(), resource_id);
        let request_builder = self.dest_builder.create_request_builder(&path)?;

        let request = request_builder.remove();
        self.http_client.clone().request(request)?.success()?;

        Ok(())
    }

    fn remove(&self, resource_id: &str) -> Result<(), DestinationError> {
        let path = merge_paths(self.base_path.as_ref(), resource_id);
        let request_builder = self.dest_builder.create_request_builder(&path)?;
        let request = request_builder.remove();

        self.http_client.clone().request(request)?.success()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{destination::fake_request_builder::RequestBuilder, FakeHttpClient};
    use fluent_assertions::*;

    struct FakeDestinationBuilder;

    impl DestinationBuilder for FakeDestinationBuilder {
        type RequestBuilder = RequestBuilder;

        fn create_request_builder(&self, uri: &str) -> Result<Self::RequestBuilder, DestinationError> {
            Ok(RequestBuilder(uri.to_owned()))
        }
    }

    trait IntoDestination {
        fn into_destination<'a>(self, base_path: &'a str) -> Arc<dyn BaseDestination + 'a>;
    }

    impl IntoDestination for FakeHttpClient {
        fn into_destination<'a>(self, base_path: &'a str) -> Arc<dyn BaseDestination + 'a> {
            FakeDestinationBuilder.build(base_path.into(), Arc::new(self))
        }
    }

    #[test]
    fn blob_destination_returns_base_path() {
        let base_path = "http://somesite/somecontainer/somedir";

        FakeHttpClient::default()
            .into_destination(base_path)
            .base_path()
            .should()
            .equal_string(base_path);
    }

    #[test]
    fn try_open_parallel_writer_returns_blob_writer() {
        let base_path = "http://somesite/somecontainer/somedir";

        FakeHttpClient::default()
            .status(201)
            .into_destination(base_path)
            .try_open_parallel_writer("somefile.csv", 1024, 16, 2)
            .should()
            .be_some()
            .which_value()
            .should()
            .be_ok();
    }

    #[test]
    fn open_output_stream_returns_blob_output_stream() {
        let base_path = "http://somesite/somecontainer/somedir";

        FakeHttpClient::default()
            .status(201)
            .into_destination(base_path)
            .open_output_stream("a.csv")
            .should()
            .be_ok();
    }

    #[test]
    fn remove_will_delete_blob() {
        let base_path = "https://somesite/somecontainer/somedir";

        FakeHttpClient::default()
            .assert_request(|_, r| {
                RequestBuilder("https://somesite/somecontainer/somedir/a.txt".to_owned()).verify_remove(r);
            })
            .status(201)
            .into_destination(base_path)
            .remove("a.txt")
            .should()
            .be_ok();
    }

    #[test]
    fn get_output_stream_writer_should_write_with_expected_http_requests_of_chunk_size() {
        const OVERFLOW_SIZE: usize = 50;
        let write_size_sequence: Vec<usize> = vec![CHUNK_SIZE / 2, CHUNK_SIZE / 2, CHUNK_SIZE, OVERFLOW_SIZE];

        let mut writer = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                RequestBuilder("https://somesite/somecontainer/somedir/a.txt".to_owned()).verify_append_create(r);
            })
            .status(201)
            .with_request(|id, _| id == 1 || id == 2)
            .assert_request(|id, r| {
                RequestBuilder("https://somesite/somecontainer/somedir/a.txt".to_owned()).verify_append_write(
                    r,
                    (id as usize - 1) * CHUNK_SIZE,
                    vec![0; CHUNK_SIZE],
                );
            })
            .status(200)
            .with_request(|id, _| id == 3)
            .assert_request(|_, r| {
                RequestBuilder("https://somesite/somecontainer/somedir/a.txt".to_owned()).verify_append_write(
                    r,
                    (2 * CHUNK_SIZE) as usize,
                    vec![0; OVERFLOW_SIZE],
                );
            })
            .status(200)
            .into_destination("https://somesite/somecontainer/somedir")
            .open_output_stream("a.txt")
            .unwrap();

        // for the given write size sequence, the 4 writes should trigger 1 Create request of CHUNK_SIZE,
        // 1 APPEND request of CHUNK_SIZE and 1 APPEND request of overflow.
        let v: Vec<usize> = write_size_sequence
            .iter()
            .map(|s| writer.write(&vec![0; *s]).expect("Writes should be successful"))
            .collect();
        v.should().equal_iterator(write_size_sequence);
    }
}
