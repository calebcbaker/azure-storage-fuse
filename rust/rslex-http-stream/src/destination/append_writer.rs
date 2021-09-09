use crate::{AuthenticatedRequest, HttpClient, ResponseExt};
use rslex_core::file_io::DestinationError;
use std::{
    io::{Error as IoError, Write},
    sync::Arc,
};

pub trait AppendWriteRequest {
    fn create(&self) -> AuthenticatedRequest;

    fn write(&self, position: usize, data: &[u8]) -> AuthenticatedRequest;

    fn flush(&self, _position: usize) -> Option<AuthenticatedRequest> {
        None
    }
}

pub(super) struct AppendWriter<Q> {
    request_builder: Q,
    http_client: Arc<dyn HttpClient>,
    bytes_written: usize,
}

impl<Q: AppendWriteRequest + Send + Sync> AppendWriter<Q> {
    pub fn new(request_builder: Q, http_client: Arc<dyn HttpClient>, should_overwrite: bool) -> Result<Self, DestinationError> {
        let result = AppendWriter {
            request_builder,
            http_client,
            bytes_written: 0,
        };

        // If blob already exists, like for AppendOnly blobs, then we don't
        // want to create and overwrite it.
        if should_overwrite {
            // create the blob, if we don't have permission will fail earlier
            result.create()?;
        }

        Ok(result)
    }

    fn create(&self) -> Result<(), DestinationError> {
        let request = self.request_builder.create();
        self.http_client.clone().request(request)?.success()?;
        Ok(())
    }
}

impl<Q: AppendWriteRequest + Send + Sync> Write for AppendWriter<Q> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, IoError> {
        let request = self.request_builder.write(self.bytes_written, buf);
        self.http_client.clone().request(request)?.success()?;

        self.bytes_written += buf.len();

        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), IoError> {
        if let Some(request) = self.request_builder.flush(self.bytes_written) {
            self.http_client.clone().request(request)?.success()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{destination::fake_request_builder::RequestBuilder, FakeHttpClient};
    use fluent_assertions::*;

    trait ToOutputStream {
        fn to_output_stream(self, url: &str) -> Result<AppendWriter<RequestBuilder>, DestinationError>;
    }

    impl ToOutputStream for FakeHttpClient {
        fn to_output_stream(self, url: &str) -> Result<AppendWriter<RequestBuilder>, DestinationError> {
            AppendWriter::new(RequestBuilder(url.to_owned()), Arc::new(self), true)
        }
    }

    #[test]
    fn create_output_stream_will_put_blob() {
        FakeHttpClient::default()
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_append_create(r);
            })
            .assert_num_requests(1)
            .status(201)
            .to_output_stream("https://someresource")
            .should()
            .be_ok();
    }

    #[test]
    fn write_will_append_block() {
        FakeHttpClient::default()
            // put blob
            .with_request(|id, _| id == 0)
            .status(201)
            // append blob
            .with_request(|id, _| id == 1)
            .assert_request(|_, result| {
                RequestBuilder("https://someresource".to_owned()).verify_append_write(result, 0, vec![1, 2, 3, 4, 5]);
            })
            .status(201)
            .to_output_stream("https://someresource")
            .should()
            .be_ok()
            .which_value()
            .write(vec![1, 2, 3, 4, 5].as_slice())
            .should()
            .be_ok()
            .with_value(5);
    }

    #[test]
    fn flush_will_call_flush() {
        let mut output_stream = FakeHttpClient::default()
            // create and write
            .with_request(|id, _| id == 0 || id == 1)
            .status(201)
            .with_request(|id, _| id == 2)
            .assert_request(|_, r| {
                RequestBuilder("https://someresource".to_owned()).verify_append_flush(r, 5);
            })
            .status(200)
            .to_output_stream("https://someresource")
            .expect("output stream shoudl be created");

        output_stream.write(vec![1, 2, 3, 4, 5].as_slice()).should().be_ok().with_value(5);

        output_stream.flush().should().be_ok();
    }
}
