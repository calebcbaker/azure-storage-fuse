use super::{seekable_read::create_seekable_read, unseekable_read::create_unseekable_read};
use crate::{
    HeadRequest, HttpClient, ReadRequest, ReadSectionRequest, ResponseExt, SeekableRead, SeekableStreamOpener, SessionPropertiesExt,
    StreamOpener, StreamResult,
};
use rslex_core::{file_io::StreamProperties, SessionProperties};
use std::{
    cmp::min,
    io::Read,
    sync::{Arc, RwLock},
};

/// An implementation of [`StreamOpener`], wrapping common behaviors of HTTP-based
/// stream handlers. To implement StreamOpener, it requires the following information:
/// - A requst_builder, which implements [`ReadRequest`] for non-seekable read and [`ReadSectionRequest`] for seekable read. The request builder
///   will construct a HTTP [`Request`](rslex_http_stream::Request) for data retrieval.
/// - File properties, ```length```, ```created_time```, ```modified_time```, ```extension```, as requested by [`StreamOpener::get_properties`].
/// - Whether the http stream support Seek (Accept-Ranges=bytes).
/// The information are usually avaiable in the HEAD queries of the target stream, but different storage system will have different protocol
/// for that information.
pub(super) struct HttpStreamOpener<T> {
    request_builder: T,
    http_client: Arc<dyn HttpClient>,
    session_properties: RwLock<SessionProperties>,
}

impl<T> HttpStreamOpener<T> {
    pub fn new(request_builder: T, http_client: Arc<dyn HttpClient>, session_properties: SessionProperties) -> HttpStreamOpener<T> {
        HttpStreamOpener {
            request_builder,
            http_client,
            session_properties: RwLock::new(session_properties),
        }
    }
}

impl<T: HeadRequest> HttpStreamOpener<T> {
    fn fill_session_properties(&self) -> StreamResult<()> {
        let request = self.request_builder.head();
        let response = self.http_client.clone().request(request.into())?;

        let mut sp = self.session_properties.write().unwrap();
        T::parse_response(response, &mut *sp)?;
        Ok(())
    }
}

impl<T: HeadRequest + ReadRequest + ReadSectionRequest + Clone + Send + Sync + 'static> StreamOpener for HttpStreamOpener<T> {
    #[tracing::instrument(skip(self))]
    fn open(&self) -> StreamResult<Box<dyn Read + Send>> {
        let is_seekable = { self.session_properties.read().unwrap().is_seekable() };

        let can_seek = match is_seekable {
            Some(b) => b,
            None => {
                self.fill_session_properties()?;
                self.session_properties
                    .read()
                    .unwrap()
                    .is_seekable()
                    .expect("session properties must contain is_seekable information")
            },
        };

        if can_seek {
            Ok(box create_seekable_read(
                self.request_builder.clone(),
                self.http_client.clone(),
                self.session_properties.read().unwrap().clone(),
            )?)
        } else {
            Ok(box create_unseekable_read(self.request_builder.clone(), self.http_client.clone())?)
        }
    }

    fn copy_to(&self, target: &mut [u8]) -> StreamResult<()> {
        let request = self.request_builder.read();
        let response = self.http_client.clone().request(request.into())?.success()?;

        let content: &[u8] = response.body().as_ref();

        assert!(target.len() >= content.len());
        unsafe { std::ptr::copy_nonoverlapping(content.as_ptr(), target.as_mut_ptr(), min(content.len(), target.len())) }

        Ok(())
    }

    fn get_properties(&self) -> StreamResult<StreamProperties> {
        let stream_properties = { self.session_properties.read().unwrap().stream_properties() };

        match stream_properties {
            Some(sp) => Ok(sp),
            None => {
                // only trigger this if session properties not there
                self.fill_session_properties()?;
                Ok(self
                    .session_properties
                    .read()
                    .unwrap()
                    .stream_properties()
                    .expect("cannot find stream properties from headers"))
            },
        }
    }

    fn can_seek(&self) -> bool {
        // try to read from session properties, if not is_seekable return false
        // if cannot find this info, we will default to T::default_is_seekable()

        let sp = self.session_properties.read().unwrap();

        if let Some(is) = sp.is_seekable() {
            is
        } else {
            let is = T::default_is_seekable();
            tracing::debug!(
                "[HttpStreamOpener::can_seek()] cannot find is_seekable information from session properties. default to {}.",
                is
            );
            is
        }
    }

    fn try_as_seekable(&self) -> Option<&dyn SeekableStreamOpener> {
        if self.can_seek() {
            Some(self)
        } else {
            None
        }
    }
}

impl<T: HeadRequest + ReadRequest + ReadSectionRequest + Clone + Send + Sync + 'static> SeekableStreamOpener for HttpStreamOpener<T> {
    fn open_seekable(&self) -> StreamResult<Box<dyn SeekableRead>> {
        Ok(create_seekable_read(
            self.request_builder.clone(),
            self.http_client.clone(),
            self.session_properties.read().unwrap().clone(),
        )?)
    }

    /// overload this function, it won't call open_seekable, which will avoid one HTTP call
    fn copy_section_to(&self, offset: usize, target: &mut [u8]) -> StreamResult<()> {
        let len = target.len();
        if len > 0 {
            let request = self.request_builder.read_section(offset as u64, len as u64);
            let response = self.http_client.clone().request(request.into())?.success()?;

            let content: &[u8] = response.body().as_ref();

            unsafe { std::ptr::copy_nonoverlapping(content.as_ptr(), target.as_mut_ptr(), content.len()) }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{new_request, AuthenticatedRequest, FakeHttpClient, HeadRequest, Response, StreamError, StreamResult};
    use fluent_assertions::*;
    use std::collections::HashMap;

    trait IntoOpener<T> {
        fn into_opener(self, request_builder: T, session_properties: SessionProperties) -> HttpStreamOpener<T>;
    }

    impl<T> IntoOpener<T> for FakeHttpClient {
        fn into_opener(self, request_builder: T, session_properties: SessionProperties) -> HttpStreamOpener<T> {
            HttpStreamOpener::new(request_builder, Arc::new(self), session_properties)
        }
    }

    /// put default implementation
    pub trait MockRequestBuilder {}

    impl<T: MockRequestBuilder> HeadRequest for T {
        default fn head(&self) -> AuthenticatedRequest {
            unimplemented!()
        }

        default fn parse_response(_resonse: Response, _session_properties: &mut SessionProperties) -> StreamResult<()> {
            unimplemented!()
        }

        default fn default_is_seekable() -> bool {
            unimplemented!()
        }
    }

    impl<T: MockRequestBuilder> ReadRequest for T {
        default fn read(&self) -> AuthenticatedRequest {
            unimplemented!()
        }
    }

    impl<T: MockRequestBuilder> ReadSectionRequest for T {
        default fn read_section(&self, _offset: u64, _length: u64) -> AuthenticatedRequest {
            unimplemented!()
        }
    }

    #[test]
    fn open_return_result() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}

        let session_properties = HashMap::default().with_size(200).with_is_seekable(true);

        FakeHttpClient::default()
            .into_opener(Dummy, session_properties)
            .open()
            .should()
            .be_ok();
    }

    #[test]
    fn open_without_session_properties_return_result() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}
        impl HeadRequest for Dummy {
            fn head(&self) -> AuthenticatedRequest {
                new_request()
                    .body(String::from("fill session properties").into_bytes())
                    .unwrap()
                    .into()
            }

            fn parse_response(_response: Response, session_properties: &mut SessionProperties) -> StreamResult<()> {
                session_properties.set_is_seekable(true);
                session_properties.set_size(200);
                Ok(())
            }

            fn default_is_seekable() -> bool {
                unimplemented!()
            }
        }

        let session_properties = HashMap::default();

        FakeHttpClient::default()
            .assert_num_requests(1)
            .assert_request(|_, r| {
                r.body().should().equal_binary(b"fill session properties");
            })
            .into_opener(Dummy, session_properties)
            .open()
            .should()
            .be_ok();
    }

    #[test]
    fn can_seek_return_true() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}

        FakeHttpClient::default()
            .into_opener(Dummy, HashMap::default().with_is_seekable(true))
            .can_seek()
            .should()
            .be_true();
    }

    #[test]
    fn can_seek_return_false() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}

        FakeHttpClient::default()
            .into_opener(Dummy, HashMap::default().with_is_seekable(false))
            .can_seek()
            .should()
            .be_false();
    }

    #[test]
    fn write_to_if_not_seekable_to_copies_the_content() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}
        impl ReadRequest for Dummy {
            fn read(&self) -> AuthenticatedRequest {
                new_request().body(String::from("ReadRequest").into_bytes()).unwrap().into()
            }
        }

        let content = "content of the file";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.body().should().equal_binary(b"ReadRequest");
            })
            .body(content)
            .into_opener(Dummy, HashMap::default().with_size(200).with_is_seekable(false))
            .write_to(&mut output)
            .should()
            .be_ok();

        String::from_utf8(output)
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string(content);
    }

    #[test]
    fn write_to_if_seekable_to_copies_the_content() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}
        impl ReadSectionRequest for Dummy {
            fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
                new_request()
                    .body(String::from(format!("ReadSectionRequest:{}-{}", offset, length)).into_bytes())
                    .unwrap()
                    .into()
            }
        }

        let content = "content of the file";
        let mut output = Vec::new();

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.body().should().equal_binary(b"ReadSectionRequest:0-19");
            })
            .body(content)
            .into_opener(Dummy, HashMap::default().with_size(19).with_is_seekable(true))
            .write_to(&mut output)
            .should()
            .be_ok();

        String::from_utf8(output)
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string(content);
    }

    #[test]
    fn copy_if_seekable_to_copies_the_content() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}
        impl ReadRequest for Dummy {
            fn read(&self) -> AuthenticatedRequest {
                new_request().body(String::from("Read").into_bytes()).unwrap().into()
            }
        }

        let content = "content of the file";
        let mut target = vec![0; content.len()];

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.body().should().equal_binary(b"Read");
            })
            .assert_num_requests(1)
            .body(content)
            .into_opener(Dummy, HashMap::default())
            .copy_to(target.as_mut_slice())
            .should()
            .be_ok();

        String::from_utf8(target)
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string(content);
    }

    #[test]
    fn copy_to_with_401_return_permission_denied() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}
        impl ReadSectionRequest for Dummy {
            fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
                new_request()
                    .body(String::from(format!("ReadSectionRequest:{}-{}", offset, length)).into_bytes())
                    .unwrap()
                    .into()
            }
        }

        let mut output = Vec::new();
        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.body().should().equal_binary(b"ReadSectionRequest:0-200");
            })
            .status(401)
            .into_opener(Dummy, HashMap::default().with_size(200).with_is_seekable(true))
            .write_to(&mut output)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn try_as_seekable_return_self() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}

        FakeHttpClient::default()
            .into_opener(Dummy, HashMap::default().with_is_seekable(true))
            .try_as_seekable()
            .should()
            .be_some();
    }

    #[test]
    fn try_as_seekable_return_none_if_not_seekable() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}

        FakeHttpClient::default()
            .into_opener(Dummy, HashMap::default().with_is_seekable(false))
            .try_as_seekable()
            .should()
            .be_none();
    }

    #[test]
    fn copy_section_to_copies_the_section() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}
        impl ReadSectionRequest for Dummy {
            fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
                new_request()
                    .body(format!("ReadSectionRequest:{}-{}", offset, length).into_bytes())
                    .unwrap()
                    .into()
            }
        }

        let content = "content of the file ";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.body().should().equal_binary(b"ReadSectionRequest:10-20");
            })
            .body(content)
            .into_opener(Dummy, HashMap::default().with_size(200).with_is_seekable(true))
            .copy_section_to(10, &mut output)
            .should()
            .be_ok();

        String::from_utf8(output)
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string(content);
    }

    #[test]
    fn copy_section_to_succeeds_for_zero_length_buffer() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}
        impl ReadSectionRequest for Dummy {
            fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
                new_request()
                    .body(format!("ReadSectionRequest:{}-{}", offset, length).into_bytes())
                    .unwrap()
                    .into()
            }
        }

        let content = "content of the file ";
        let mut output = vec![];

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.body().should().equal_binary(b"ReadSectionRequest:10-20");
            })
            .assert_num_requests(0)
            .body(content)
            .into_opener(Dummy, HashMap::default().with_size(200).with_is_seekable(true))
            .copy_section_to(10, &mut output)
            .should()
            .be_ok();
    }

    #[test]
    fn copy_section_to_with_401_return_permission_denied() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}
        impl ReadSectionRequest for Dummy {
            fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
                new_request()
                    .body(format!("ReadSectionRequest:{}-{}", offset, length).into_bytes())
                    .unwrap()
                    .into()
            }
        }

        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.body().should().equal_binary(b"ReadSectionRequest:10-20");
            })
            .status(401)
            .into_opener(Dummy, HashMap::default().with_size(200).with_is_seekable(true))
            .copy_section_to(10, &mut output)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn open_seekable_return_result() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}

        FakeHttpClient::default()
            .into_opener(Dummy, HashMap::default().with_size(200).with_is_seekable(true))
            .open_seekable()
            .should()
            .be_ok();
    }

    #[test]
    fn copy_section_to_without_session_properties_will_make_only_one_call() {
        #[derive(Clone)]
        struct Dummy;
        impl MockRequestBuilder for Dummy {}
        impl ReadSectionRequest for Dummy {
            fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
                new_request()
                    .body(format!("ReadSectionRequest:{}-{}", offset, length).into_bytes())
                    .unwrap()
                    .into()
            }
        }

        let content = "content of the file ";
        let mut output = vec![0; 20];

        FakeHttpClient::default()
            .assert_num_requests(1)
            .assert_request(|_, r| {
                r.body().should().equal_binary(b"ReadSectionRequest:10-20");
            })
            .body(content)
            .into_opener(Dummy, HashMap::default())
            .copy_section_to(10, &mut output)
            .should()
            .be_ok();

        String::from_utf8(output)
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string(content);
    }
}
