use crate::{AsyncResponseExt, AuthenticatedRequest, HttpClient, HttpError, Spawn, StreamError, Wait};
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use std::{
    cmp::min,
    io::{Error as IoError, ErrorKind as IoErrorKind, Read, Write},
    sync::{
        mpsc::{channel, Receiver},
        Arc,
    },
};
use tokio::sync::Notify;
use tracing::Instrument;

/// Trait implemented by a Searcher to produce a Http Request reading the whole stream.
/// Usually with non-seekable streams.
/// See ['HttpStreamOpener'](crate::HttpStreamOpener).
///
/// ## Example
/// ```
/// use rslex_core::file_io::StreamResult;
/// use rslex_http_stream::{new_request, AuthenticatedRequest, ReadRequest, Request};
///
/// struct RequestBuilder {
///     uri: String,
/// };
///
/// // a simple implementation
/// impl ReadRequest for RequestBuilder {
///     fn read(&self) -> AuthenticatedRequest {
///         let request = new_request().uri(self.uri.as_str()).body(Vec::<u8>::default()).unwrap();
///
///         request.into()
///     }
/// }
/// ```
pub trait ReadRequest {
    fn read(&self) -> AuthenticatedRequest;
}

/// If http stream does not support seek, we must stream the whole content
/// this will be very fragile if there is connection failure we won't be able to
/// recover from connection point. also, as we have to download the file as a whole,
/// there will be memory issue.
/// The implementation below read-on-demand, and use a signal (notify) to throttle
/// the downloading. Only when the previous batch read the next batch will be load.
/// This could be improved by pre-loading several batches, but as this branch will
/// only be hit if http stream not support seek, which is very rare, we leave the optimization
/// later if needed. [rogeryu]
pub fn create_unseekable_read<Q: ReadRequest + 'static>(
    request_builder: Q,
    http_client: Arc<dyn HttpClient>,
) -> Result<impl Read, StreamError> {
    let request = request_builder.read();
    let response = async move { Ok::<_, StreamError>(http_client.request_async(request.into()).await?.success().await?) }.wait()??;

    let body = response.into_body();

    //let read = Cursor::new(response.into_body());

    Ok(UnSeekableStream::new(body.map_err(|e| StreamError::from(HttpError::from(e)))))
}

struct UnSeekableStream {
    receiver: Receiver<Option<Result<Bytes, StreamError>>>,
    notify: Arc<Notify>,
    buf: Bytes,
    end_of_stream: bool,
}

impl UnSeekableStream {
    pub fn new<S>(mut stream: S) -> Self
    where S: Stream<Item = Result<Bytes, StreamError>> + Unpin + Send + 'static {
        let (sender, receiver) = channel();
        let notify = Arc::new(Notify::new());
        let notified = notify.clone();

        async move {
            let mut exit = false;

            while !exit {
                notified.notified().await;

                let result = stream.next().await;
                exit |= result.is_none();

                exit |= sender.send(result).is_err();
            }

            drop(sender);
        }
        .instrument(tracing::info_span!(parent: &tracing::Span::current(), "UnSeekableStream"))
        .spawn();

        UnSeekableStream {
            receiver,
            notify,
            buf: Bytes::new(),
            end_of_stream: false,
        }
    }
}

impl Read for UnSeekableStream {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, IoError> {
        let mut total_written = 0;

        while !buf.is_empty() && !self.end_of_stream {
            if self.buf.is_empty() {
                self.notify.notify_one();

                match self.receiver.recv() {
                    Ok(Some(Ok(bytes))) => {
                        self.buf = bytes;
                    },
                    Ok(None) => {
                        // end of stream, return
                        self.end_of_stream = true;
                        break;
                    },
                    Ok(Some(Err(e))) => {
                        return Err(e.into());
                    },
                    Err(e) => {
                        return Err(IoError::new(IoErrorKind::Other, format!("{:?}", e)));
                    },
                }
            }

            let bytes_to_write = min(buf.len(), self.buf.len());
            let source = self.buf.split_to(bytes_to_write);
            let written = buf.write(source.as_ref())?;

            // this should always be true
            debug_assert!(written == bytes_to_write);

            total_written += written;
        }

        Ok(total_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use futures::stream::iter;

    #[test]
    fn read_will_trigger_one_load() {
        let chunks: Vec<Result<_, StreamError>> = vec![Ok(Bytes::from_static(b"abcde"))];
        let stream = iter(chunks);

        let mut buffer = [0; 5];

        UnSeekableStream::new(stream).read(&mut buffer).should().be_ok().with_value(5);
        String::from_utf8(buffer.to_vec())
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string("abcde");
    }

    #[test]
    fn read_twice_with_one_load() {
        let chunks: Vec<Result<_, StreamError>> = vec![Ok(Bytes::from_static(b"1234567890"))];
        let stream = iter(chunks);

        let mut read = UnSeekableStream::new(stream);
        let mut buffer = [0; 5];

        // first read
        read.read(&mut buffer).should().be_ok().with_value(5);
        String::from_utf8(buffer.to_vec())
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string("12345");

        // second read
        read.read(&mut buffer).should().be_ok().with_value(5);
        String::from_utf8(buffer.to_vec())
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string("67890");
    }

    #[test]
    fn read_once_with_two_loads() {
        let chunks: Vec<Result<_, StreamError>> = vec![Ok(Bytes::from_static(b"12345")), Ok(Bytes::from_static(b"67890"))];
        let stream = iter(chunks);

        let mut buffer = [0; 10];

        UnSeekableStream::new(stream).read(&mut buffer).should().be_ok().with_value(10);
        String::from_utf8(buffer.to_vec())
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string("1234567890");
    }

    #[test]
    fn read_not_enough_bytes() {
        let chunks: Vec<Result<_, StreamError>> = vec![Ok(Bytes::from_static(b"12345"))];
        let stream = iter(chunks);
        let mut buffer = [0; 10];

        UnSeekableStream::new(stream).read(&mut buffer).should().be_ok().with_value(5);
        String::from_utf8(buffer[..5].to_vec())
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string("12345");
    }

    #[test]
    fn read_with_eos_read_nothing() {
        let chunks: Vec<Result<_, StreamError>> = vec![Ok(Bytes::from_static(b"12345"))];
        let stream = iter(chunks);

        let mut read = UnSeekableStream::new(stream);
        let mut buffer = [0; 10];

        // first read consume the stream
        read.read(&mut buffer).should().be_ok().with_value(5);

        // second read got nothing
        read.read(&mut buffer).should().be_ok().with_value(0);
    }

    #[test]
    fn read_with_stream_error_return_error() {
        let chunks: Vec<Result<_, StreamError>> = vec![Err(StreamError::Unknown("something wrong".to_owned(), None))];
        let stream = iter(chunks);
        let mut buffer = [0; 10];

        UnSeekableStream::new(stream)
            .read(&mut buffer)
            .should()
            .be_err()
            .which_value()
            .kind()
            .should()
            .be(IoErrorKind::Other);
    }
}
