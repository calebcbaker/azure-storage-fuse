mod request_builder;
mod stream_handler;

use self::stream_handler::HttpStreamHandler;
use crate::{create_http_client, StreamHandler};
use rslex_core::file_io::StreamError;
use std::{fmt, fmt::Display};

/// The type of the [`StreamHandler`](crate::StreamHandler) that can read from a http stream.
pub const HANDLER_TYPE: &str = "Http";

pub enum HttpUriScheme {
    Http,
    Https,
}

impl Display for HttpUriScheme {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            HttpUriScheme::Http => write!(f, "http"),
            HttpUriScheme::Https => write!(f, "https"),
        }
    }
}

/// Creates a [`StreamHandler`] that can read from a http stream.
///
/// # Example
///
/// ``` rust, no_run
/// use rslex_core::{file_io::{StreamAccessor, StreamHandler}, SyncRecord};
/// use rslex_http_stream::{create_http_handler, HttpUriScheme};
/// use std::collections::HashMap;
/// use std::convert::TryFrom;
/// use std::io::SeekFrom;
///
/// let handler = create_http_handler(HttpUriScheme::Http).unwrap();
///
/// // place holder. Not used in this scenario
/// let stream_accessor = StreamAccessor::default();
/// let arguments = SyncRecord::empty();
///
/// // this will return an iterator of result items
/// let uri = "https://somesite.com/somepath/somefile.csv";
/// let result = handler.find_streams(uri, arguments.parse().unwrap(), &stream_accessor)
///     .expect("search streams failed");
///
/// // print out the resource id of all items, in this case,
/// // the result should only contain one stream
/// for si in result.iter() {
///     println!("{}", si.unwrap().resource_id());
/// }
///
/// // or create an opener to access data. below will open a seekable stream,
/// // and read from it
/// let opener = handler.get_opener(uri, arguments.parse().unwrap(), &HashMap::default(), &stream_accessor)
///     .expect("get opener failed");
/// let mut seekable = opener.try_as_seekable().unwrap().open_seekable()
///     .expect("create seekable read failred");
/// seekable.seek(SeekFrom::Start(10)).expect("seek failed");
///
/// let mut buffer = [0; 100];
/// // read content to buffer
/// seekable.read(&mut buffer).expect("read content failed");
/// ```
pub fn create(uri_scheme: HttpUriScheme) -> Result<impl StreamHandler, StreamError> {
    Ok(HttpStreamHandler::new(create_http_client()?, uri_scheme))
}

#[cfg(test)]
#[cfg(feature = "component_tests")]
mod component_tests {
    use super::*;
    use crate::SessionPropertiesExt;
    use fluent_assertions::*;
    use lazy_static::lazy_static;
    use rslex_core::{file_io::StreamAccessor, SessionProperties, SyncRecord};

    lazy_static! {
        static ref STREAM_ACCESSOR: StreamAccessor = StreamAccessor::default();
    }

    #[test]
    fn list_dir_and_download_section() {
        let uri = "https://dprepdata.blob.core.windows.net/test/Sample-Spreadsheet-10-rows.csv";
        let handler = create(HttpUriScheme::Https).unwrap();

        let result = handler
            .find_streams(uri, SyncRecord::empty().parse().unwrap(), &STREAM_ACCESSOR)
            .unwrap();

        for si in result.iter() {
            println!("{:#?}", si.unwrap().resource_id());
        }

        let mut output = vec![0; 4];
        let mut session_properties = SessionProperties::new();
        session_properties.set_is_seekable(true);
        let opener = handler
            .get_opener(uri, SyncRecord::empty().parse().unwrap(), &session_properties, &STREAM_ACCESSOR)
            .unwrap();

        let seekable_opener = opener.try_as_seekable().unwrap();
        seekable_opener.copy_section_to(9, &mut output).unwrap();

        String::from_utf8(output)
            .should()
            .be_ok()
            .which_value()
            .should()
            .equal_string("Base");
    }
}
