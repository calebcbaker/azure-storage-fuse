use crate::{
    new_request, AuthenticatedRequest, HeadRequest, MapErrToUnknown, ReadRequest, ReadSectionRequest, Response, SessionPropertiesExt,
    StreamError, StreamResult,
};
use chrono::DateTime;
use http::Method;
use rslex_core::SessionProperties;
use std::str::FromStr;

/// A tool class handling building http request to work with Azure Blob Service.
#[derive(Clone, Debug)]
pub(super) struct RequestBuilder {
    uri: String,
}

impl RequestBuilder {
    pub fn new<S: Into<String>>(uri: S) -> StreamResult<RequestBuilder> {
        let uri = uri.into();
        let uri_result = http::Uri::from_str(&uri);

        if uri_result.is_ok() && (uri.starts_with("http://") || uri.starts_with("https://")) {
            Ok(RequestBuilder { uri })
        } else if uri_result.is_err() {
            Err(StreamError::InvalidInput {
                message: "Invalid HTTP/HTTPS URL.".to_string(),
                source: Some(std::sync::Arc::new(uri_result.unwrap_err())),
            })
        } else {
            Err(StreamError::InvalidInput {
                message: "Invalid HTTP/HTTPS URL.".to_string(),
                source: None,
            })
        }
    }
}

impl HeadRequest for RequestBuilder {
    fn head(&self) -> AuthenticatedRequest {
        let request = new_request()
            .method(Method::HEAD)
            .uri(&self.uri)
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.into()
    }

    fn parse_response(response: Response, session_properties: &mut SessionProperties) -> StreamResult<()> {
        let headers = response.headers();
        let size: u64 = headers
            .get("Content-Length")
            .ok_or(StreamError::Unknown("Content-Length missing from HTTP header".to_owned(), None))?
            .to_str()
            .map_err_to_unknown()?
            .parse::<u64>()
            .map_err_to_unknown()?;
        session_properties.set_size(size);

        if let Some(modified_time) = headers
            .get("Last-Modified")
            .map::<StreamResult<_>, _>(|s| {
                Ok(DateTime::parse_from_rfc2822(s.to_str().map_err_to_unknown()?)
                    .map_err_to_unknown()?
                    .into())
            })
            .transpose()?
        {
            session_properties.set_modified_time(modified_time);
        }

        let is_seekable = match headers
            .get("Accept-Ranges")
            .map::<StreamResult<_>, _>(|s| Ok(s.to_str().map_err_to_unknown()?))
            .transpose()?
        {
            Some("bytes") => true,
            _ => false,
        };
        session_properties.set_is_seekable(is_seekable);

        Ok(())
    }

    /// default to non-seekable for http streams
    fn default_is_seekable() -> bool {
        false
    }
}

impl ReadSectionRequest for RequestBuilder {
    fn read_section(&self, offset: u64, length: u64) -> AuthenticatedRequest {
        assert!(
            length > 0,
            "[http_stream_handler::request_builder::ReadSectionRequest::read_section] Attempt to send range request of a zero length"
        );
        let request = new_request()
            .uri(&self.uri)
            .header("Range", format!("bytes={}-{}", offset, offset + length - 1))
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.into()
    }
}

impl ReadRequest for RequestBuilder {
    fn read(&self) -> AuthenticatedRequest {
        let request = new_request()
            .uri(&self.uri)
            .body(Vec::<u8>::default())
            .expect("create request should succeed");

        request.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_client::Method;
    use fluent_assertions::*;

    #[test]
    fn new_request_builder_with_invalid_http_return_invalid_uri() {
        let uri = "abc://somesite/somefile.csv";
        RequestBuilder::new(uri).should().be_err().with_value(StreamError::InvalidInput {
            message: "Invalid HTTP/HTTPS URL.".to_string(),
            source: None,
        });
    }

    #[test]
    fn new_request_builder_with_invalid_uri_chars_returns_invalid_input_error() {
        let uri = "!!!^^^";
        RequestBuilder::new(uri).should().be_err().with_value(StreamError::InvalidInput {
            message: "Invalid HTTP/HTTPS URL.".to_string(),
            source: None,
        });
    }

    #[test]
    fn head_request() {
        let uri = "https://somesite/somepath/somefile.csv";
        let builder = RequestBuilder::new(uri).expect("creating RequestBuilder should succeed");

        builder
            .head()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|request| {
                request.method().should().be(&Method::HEAD);
                request.uri().should().equal_string("https://somesite/somepath/somefile.csv");
            });
    }

    #[test]
    fn read_section_will_return_get_request() {
        let uri = "https://somesite/somepath/somefile.csv";

        RequestBuilder::new(uri)
            .expect("creating RequestBuilder should succeed")
            .read_section(10, 20)
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result.uri().should().equal_string("https://somesite/somepath/somefile.csv");
                result.headers()["Range"].to_str().should().be_ok().with_value("bytes=10-29");
            });
    }

    #[test]
    #[should_panic]
    fn read_section_for_zero_length_should_panic() {
        let uri = "https://somesite/somepath/somefile.csv";

        // this should panic
        let _ = RequestBuilder::new(uri)
            .expect("creating RequestBuilder should succeed")
            .read_section(10, 0)
            .into_request();
    }

    #[test]
    fn read_will_return_get_request() {
        let uri = "https://somesite/somepath/somefile.csv";

        RequestBuilder::new(uri)
            .expect("build request builder should succeed")
            .read()
            .into_request()
            .should()
            .be_ok()
            .which_value()
            .should()
            .pass(|result| {
                result.method().should().be(&Method::GET);
                result.uri().should().equal_string("https://somesite/somepath/somefile.csv");
            });
    }
}
