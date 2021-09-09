use crate::{AuthenticatedRequest, HeadRequest, HttpClient, ResponseExt, SeekableRead, SessionPropertiesExt};
use rslex_core::{file_io::StreamResult, SessionProperties};
use std::{
    cmp::min,
    io::{Error as IoError, Read, Seek, SeekFrom},
    sync::Arc,
};

/// Trait implemented by a Searcher to produce a Http Request reading section of stream.
/// See ['HttpStreamOpener'](crate::HttpStreamOpener).
///
/// ## Example
/// ```
/// use http::{HeaderMap, HeaderValue};
/// use rslex_core::file_io::StreamResult;
/// use rslex_http_stream::{new_request, AuthenticatedRequest, ReadSectionRequest, Request, SessionPropertiesExt};
///
/// struct RequestBuilder {
///     uri: String,
/// };
///
/// // an implementation which translate the offset/length to Range header
/// impl ReadSectionRequest for RequestBuilder {
///     fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
///         let request = new_request()
///             .uri(self.uri.as_str())
///             .header("Range", format!("bytes={}-{}", offset, offset + length - 1))
///             .body(Vec::<u8>::default())
///             .unwrap();
///
///         request.into()
///     }
/// }
/// ```
pub trait ReadSectionRequest {
    fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest;
}

pub fn create_seekable_read<Q: HeadRequest + ReadSectionRequest + Send + 'static>(
    request_builder: Q,
    http_client: Arc<dyn HttpClient>,
    mut session_properties: SessionProperties,
) -> StreamResult<Box<dyn SeekableRead>> {
    let size = if let Some(size) = session_properties.size() {
        size
    } else {
        // if there is no length information, make a HEAD call to retrieve the length
        let request = request_builder.head();
        let response = http_client.clone().request(request)?;

        Q::parse_response(response, &mut session_properties)?;

        session_properties.size().expect("headers should fill in the length information")
    };

    Ok(box SeekableStream::new(request_builder, http_client, size))
}

struct SeekableStream<Q: ReadSectionRequest + Send> {
    size: u64,
    pos: u64,
    request_builder: Q,
    http_client: Arc<dyn HttpClient>,
}

impl<Q: ReadSectionRequest + Send> SeekableStream<Q> {
    pub fn new(request_builder: Q, http_client: Arc<dyn HttpClient>, size: u64) -> SeekableStream<Q> {
        SeekableStream {
            size,
            pos: 0,
            request_builder,
            http_client,
        }
    }
}

impl<Q: ReadSectionRequest + Send> Read for SeekableStream<Q> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IoError> {
        let len = min(buf.len() as u64, self.size - self.pos);

        if len == 0 {
            Ok(0)
        } else {
            let request = self.request_builder.read_section(self.pos, len);
            let response = self.http_client.clone().request(request)?.success()?;

            let mut content: &[u8] = response.body().as_ref();
            let mut target = buf;
            assert_eq!(len, content.len() as u64);
            self.pos += len;

            Ok(std::io::copy(&mut content, &mut target)? as usize)
        }
    }
}

impl<Q: ReadSectionRequest + Send> Seek for SeekableStream<Q> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, IoError> {
        let pos = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::Current(d) => self.pos as i64 + d,
            SeekFrom::End(d) => self.size as i64 + d,
        };

        if pos > self.size as i64 {
            tracing::warn!(
                "[SeekableStream::seek()] Seek target ({}) larger than file length ({}) , move to end of file",
                pos,
                self.size
            );

            self.pos = self.size;
        } else if pos < 0 {
            tracing::error!("[SeekableStream::seek()] Seek target ({}) <0", pos);
            return Err(IoError::from(std::io::ErrorKind::InvalidInput));
        } else {
            self.pos = pos as u64;
        }

        Ok(self.pos)
    }

    fn stream_position(&mut self) -> Result<u64, std::io::Error> {
        Ok(self.pos)
    }
}

impl<Q: ReadSectionRequest + Send> SeekableRead for SeekableStream<Q> {
    fn length(&self) -> u64 {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{new_request, FakeHttpClient, Request};
    use fluent_assertions::*;
    use std::{io::ErrorKind, sync::Arc};

    const READ_SECTION_REQUEST: &str = "http://fakereadrequest/readsection";

    #[derive(Clone)]
    struct FakeReadRequest;
    impl ReadSectionRequest for FakeReadRequest {
        fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
            new_request()
                .uri(format!("{}?offset={}&length={}", READ_SECTION_REQUEST, offset, length))
                .body(Vec::<u8>::default())
                .unwrap()
                .into()
        }
    }

    impl FakeReadRequest {
        fn verify_read_section(offset: u64, length: u64, req: &Request) {
            req.uri()
                .should()
                .equal_string(format!("{}?offset={}&length={}", READ_SECTION_REQUEST, offset, length));
        }
    }

    trait IntoHttpStream {
        fn into_stream(self, size: u64) -> SeekableStream<FakeReadRequest>;
    }

    impl IntoHttpStream for FakeHttpClient {
        fn into_stream(self, size: u64) -> SeekableStream<FakeReadRequest> {
            SeekableStream::new(FakeReadRequest, Arc::new(self), size)
        }
    }

    #[test]
    fn read_send_request_with_range_start_from_beginning() {
        let content = "abcde";
        let mut stream = FakeHttpClient::default()
            .assert_request(|_, r| {
                FakeReadRequest::verify_read_section(0, 5, r);
            })
            .body(content)
            .into_stream(200);

        let mut buffer = [0; 5];

        stream.read(&mut buffer).should().be_ok().with_value(5);
        String::from_utf8(buffer.to_vec())
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string(content);
        stream.pos.should().be(5);
    }

    #[test]
    fn read_two_times() {
        let mut stream = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                FakeReadRequest::verify_read_section(0, 10, r);
            })
            .body("1234567890")
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                FakeReadRequest::verify_read_section(10, 5, r);
            })
            .body("ABCDE")
            .into_stream(15);

        let mut buffer = [0; 10];

        stream.read(&mut buffer).should().be_ok().with_value(10);
        String::from_utf8(buffer.to_vec())
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string("1234567890");
        stream.pos.should().be(10);

        let mut buffer = [0; 10];

        stream.read(&mut buffer).should().be_ok().with_value(5);
        String::from_utf8(buffer[..5].to_vec())
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string("ABCDE");
        stream.pos.should().be(15);
    }

    #[test]
    fn read_with_401_return_permission_denied() {
        let mut stream = FakeHttpClient::default().status(401).into_stream(200);

        let mut buffer = [0; 5];

        stream
            .read(&mut buffer)
            .should()
            .be_err()
            .which_value()
            .kind()
            .should()
            .be(ErrorKind::PermissionDenied);
    }

    #[test]
    fn read_with_request_larger_than_file_size_will_return_file_size() {
        let content = "abcde";
        let mut stream = FakeHttpClient::default()
            .assert_request(|_, r| {
                FakeReadRequest::verify_read_section(0, 5, r);
            })
            .body(content)
            .into_stream(5);

        let mut buffer = [0; 20];

        stream.read(&mut buffer).should().be_ok().with_value(5);
        String::from_utf8(buffer[..5].to_vec())
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string(content);
    }

    #[test]
    fn seek_from_start_and_read_send_request_with_range() {
        let content = "abcde";
        let mut stream = FakeHttpClient::default()
            .assert_request(|_, r| {
                FakeReadRequest::verify_read_section(10, 5, r);
            })
            .body(content)
            .into_stream(200);

        stream.seek(SeekFrom::Start(10)).should().be_ok().with_value(10);
        let mut buffer = [0; 5];

        stream.read(&mut buffer).should().be_ok().with_value(5);
        String::from_utf8(buffer.to_vec())
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string(content);
    }

    #[test]
    fn seek_from_current_and_read_send_request_with_range() {
        let content = "abcde";
        let mut stream = FakeHttpClient::default()
            .assert_request(|_, r| {
                FakeReadRequest::verify_read_section(10, 5, r);
            })
            .body(content)
            .into_stream(200);

        stream.seek(SeekFrom::Start(20)).unwrap();

        stream.seek(SeekFrom::Current(-10)).should().be_ok().with_value(10);
        let mut buffer = [0; 5];

        stream.read(&mut buffer).should().be_ok().with_value(5);
        String::from_utf8(buffer.to_vec())
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string(content);
    }

    #[test]
    fn seek_from_end_and_read_send_request_with_range() {
        let content = "abcde";
        let mut stream = FakeHttpClient::default()
            .assert_request(|_, r| {
                FakeReadRequest::verify_read_section(190, 5, r);
            })
            .body(content)
            .into_stream(200);

        stream.seek(SeekFrom::End(-10)).should().be_ok().with_value(190);
        let mut buffer = [0; 5];

        stream.read(&mut buffer).should().be_ok().with_value(5);
        String::from_utf8(buffer.to_vec())
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string(content);
    }

    #[test]
    fn seek_to_beyond_end_of_file_will_align_to_end_of_file() {
        FakeHttpClient::default()
            .into_stream(200)
            .seek(SeekFrom::Start(300))
            .should()
            .be_ok()
            .with_value(200);
    }

    #[test]
    fn seek_to_negative_returns_error() {
        FakeHttpClient::default()
            .into_stream(200)
            .seek(SeekFrom::Current(-10))
            .should()
            .be_err();
    }
}
