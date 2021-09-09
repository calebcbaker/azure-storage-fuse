mod file_dto;
mod request_builder;
mod searcher;
mod stream_handler;

use self::stream_handler::FileStreamHandler;
use crate::credential::access_token::ScopedAccessTokenResolver;
use rslex_core::file_io::{StreamError, StreamHandler};
use rslex_http_stream::create_http_client;
use std::sync::Arc;

/// The type of the [`StreamHandler`](crate::StreamHandler) that can read from Azure File Share.
pub const HANDLER_TYPE: &str = "AzureFileStorage";

/// Creates a [`StreamHandler`] that can read from Azure File Storage.
///
///
/// # Example
///
/// ``` rust, no_run
/// use rslex_core::{file_io::{StreamAccessor, StreamHandler}, sync_record};
/// use rslex_azure_storage::create_file_handler;
/// use std::collections::HashMap;
/// use std::convert::TryFrom;
/// use std::io::SeekFrom;
///
/// let handler = create_file_handler().unwrap();
///
/// // use sas credential to access storage
/// let sas = "your&sas&token";
/// let arguments = sync_record!("sas" => sas);
///
/// // place holder. Not used in this scenario
/// let stream_accessor = StreamAccessor::default();
///
/// // this will return an iterator of result items
/// let uri = "https://youraccount.file.core.windows.net/someshare/*.csv";
/// let result = handler.find_streams(uri, arguments.parse().unwrap(), &stream_accessor)
///     .expect("search streams failed");
///
/// // print out the resource id of all items
/// for si in result.iter() {
///     println!("{}", si.unwrap().resource_id());
/// }
///
/// // or create an opener to access data. below will open a seekable stream,
/// // and read from it
/// let uri = "https://youraccount.file.core.windows.net/someshare/somefile.csv";
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
///
/// Azure File Storage also support account key credential
///
/// ``` rust, no_run
/// use rslex_core::sync_record;
///
/// // use account credential
/// let account_key = "<your-account-key>";
/// let arguments = sync_record!{ "accountKey" => account_key };
/// ```
pub fn create() -> Result<impl StreamHandler, StreamError> {
    let client = create_http_client()?;
    Ok(FileStreamHandler::new(client))
}

pub fn create_with_token_resolver(access_token_resolver: Arc<dyn ScopedAccessTokenResolver>) -> Result<impl StreamHandler, StreamError> {
    let client = create_http_client()?;
    Ok(FileStreamHandler::new(client).with_access_token_resolver(access_token_resolver))
}

#[cfg(test)]
#[cfg(feature = "component_tests")]
mod component_tests {
    use lazy_static::lazy_static;
    use rslex_core::file_io::StreamAccessor;

    lazy_static! {
        static ref STREAM_ACCESSOR: StreamAccessor = StreamAccessor::default();
    }

    // TODO: Re-enable after getting auth from env, Task: 839659
    // #[test]
    // fn list_dir_and_download_section() {
    //     init_log();
    //
    //     let uri = "https://dataprepe2etest.file.core.windows.net/test/*";
    //     //let sync_record = sas();
    //     let sync_record = account_key();
    //     let handler = create();
    //
    //     let result = handler.find_streams(uri, sync_record.parse().unwrap(), &STREAM_ACCESSOR).unwrap();
    //
    //     for si in result.iter() {
    //         println!("{:#?}", si.unwrap().resource_id());
    //     }
    //
    //     let uri = "https://dataprepe2etest.file.core.windows.net/test/input/crime0-10.csv";
    //     let handler = create();
    //     let mut output = vec![0; 11];
    //     let opener = handler
    //         .get_opener(uri, sync_record.parse().unwrap(), &HashMap::default(), &STREAM_ACCESSOR)
    //         .unwrap();
    //
    //     let seekable_opener = opener.try_as_seekable().unwrap();
    //     seekable_opener.copy_section_to(3, &mut output).unwrap();
    //
    //     let result = String::from_utf8(output).unwrap();
    //     assert_that(&result.as_str()).is_equal_to("Case Number");
    // }
    //
    // #[test]
    // #[ignore]
    // fn file_list_big_dir() {
    //     init_log();
    //     let start = Utc::now();
    //     //let uri = "https://dataprepe2etest.file.core.windows.net/test/input/*.csv";
    //     //let uri = "https://dataprepe2etest.file.core.windows.net/test/input/crime0-10.csv";
    //     let uri = "https://dataprepe2etest.file.core.windows.net/test/";
    //     let sync_record = sas();
    //     let handler = create();
    //     let mut count = 0;
    //
    //     let result = handler.find_streams(uri, sync_record.parse().unwrap(), &STREAM_ACCESSOR).unwrap();
    //
    //     for si in result.iter() {
    //         match si {
    //             Ok(r) => {
    //                 println!("[{}]{:#?}", count, r.resource_id());
    //                 count += 1;
    //             },
    //             Err(e) => {
    //                 log::error!("{:?}", e);
    //             },
    //         }
    //     }
    //
    //     log::info!("count={}", count);
    //
    //     let end = Utc::now();
    //
    //     log::info!("time={}", (end - start));
    // }
}
