mod blob_dto;
mod destination;
mod public_blob;
mod request_builder;
mod searcher;
mod stream_handler;

use self::stream_handler::BlobStreamHandler;
use crate::credential::{access_token::ScopedAccessTokenResolver, CredentialInput};
use rslex_core::file_io::{DestinationError, DestinationHandler, StreamError, StreamHandler};
use rslex_http_stream::create_http_client;
use std::{fmt, fmt::Display, sync::Arc};

/// The type of the [`StreamHandler`](crate::StreamHandler) that can read from Azure Blob Storage.
pub const HANDLER_TYPE: &str = "AzureBlobStorage";

// see [https://docs.microsoft.com/en-us/rest/api/storageservices/put-block]
// max block size = 4M before, 100M after version 2016-05-31, and 4000M after version 2019-12-12
// we pick 100M here
const MAX_BLOCK_SIZE: usize = 100_000_000;
const MAX_BLOCK_COUNT: usize = 50_000;

pub enum BlobUriScheme {
    Wasb,
    Wasbs,
}

impl Display for BlobUriScheme {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BlobUriScheme::Wasb => write!(f, "wasb"),
            BlobUriScheme::Wasbs => write!(f, "wasbs"),
        }
    }
}

/// Creates a [`StreamHandler`] that can read from Azure Blob Storage.
///
/// # Example
///
/// ``` rust, no_run
/// use rslex_core::{file_io::{StreamAccessor, StreamHandler}, sync_record};
/// use rslex_azure_storage::create_blob_handler;
/// use std::{io::SeekFrom, collections::HashMap};
/// use rslex_azure_storage::BlobUriScheme;
///
/// let handler = create_blob_handler(BlobUriScheme::Wasbs).unwrap();
///
/// // use sas credential to access storage
/// let sas = "your&sas&token";
/// let arguments = sync_record!("sas" => sas);
///
/// // place holder. Not used in this scenario
/// let stream_accessor = StreamAccessor::default();
///
/// // this will return an iterator of result items
/// let uri = "https://youraccount.blob.core.windows.net/somecontainer/*.csv";
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
/// let uri = "https://youraccount.blob.core.windows.net/somecontainer/somefile.csv";
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
/// Azure Blob Storage also support account key credential
///
/// ``` rust, no_run
/// use rslex_core::sync_record;
///
/// // use account credential
/// let account_key = "<your-account-key>";
/// let arguments = sync_record!{ "accountKey" => account_key };
/// ```
///
/// or service principal credential
///
/// ``` rust, no_run
/// use rslex_core::sync_record;
/// use serde_json::json;
///
/// // use service principal credential
/// let resource_url = "https://storage.azure.com/".to_owned();
/// let authority_url = "https://login.microsoftonline.com".to_owned();
/// let tenant_id = "<your-tenant-id>".to_owned();
/// let client_id = "<your-client-id>".to_owned();
/// let client_secret = "<your-client-secret>".to_owned();
/// let json = json!({
///     "type": "servicePrincipal",
///     "resourceUrl": resource_url,
///     "authorityUrl": authority_url,
///     "tenantId": tenant_id,
///     "clientId": client_id,
///     "clientSecret": client_secret})
///     .to_string();
/// let arguments = sync_record!{ "credential" => json };
/// ```
pub fn create(uri_scheme: BlobUriScheme) -> Result<impl StreamHandler, StreamError> {
    let client = create_http_client()?;
    Ok(BlobStreamHandler::new(client, uri_scheme))
}

pub fn create_destination() -> Result<impl DestinationHandler<Arguments = CredentialInput>, DestinationError> {
    let client = create_http_client()?;
    Ok(BlobStreamHandler::new(client, BlobUriScheme::Wasbs))
}

pub fn create_with_token_resolver(
    access_token_resolver: Arc<dyn ScopedAccessTokenResolver>,
    uri_scheme: BlobUriScheme,
) -> Result<impl StreamHandler, StreamError> {
    let client = create_http_client()?;
    Ok(BlobStreamHandler::new(client, uri_scheme).with_access_token_resolver(access_token_resolver))
}

pub fn create_destination_with_token_resolver(
    access_token_resolver: Arc<dyn ScopedAccessTokenResolver>,
) -> Result<impl DestinationHandler<Arguments = CredentialInput>, DestinationError> {
    let client = create_http_client()?;
    Ok(BlobStreamHandler::new(client, BlobUriScheme::Wasbs).with_access_token_resolver(access_token_resolver))
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
    //     let uri = "https://dataprepe2etest.blob.core.windows.net/test/*";
    //     let sync_record = account_key();
    //     let handler = create();
    //
    //     let result = handler.find_streams(uri, sync_record.parse().unwrap(), &STREAM_ACCESSOR).unwrap();
    //
    //     for si in result.iter() {
    //         println!("{:#?}", si.unwrap().resource_id());
    //     }
    //
    //     let uri = "https://dataprepe2etest.blob.core.windows.net/test/crime0-10.csv";
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
    // fn blob_list_big_dir() {
    //     init_log();
    //     let start = Utc::now();
    //     let uri = "https://dataprepe2etest.blob.core.windows.net/test/*/*d*/*.csv";
    //     let sync_record = account_key();
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

    // #[test]
    // fn upload_to_file() {
    //     let uri = "https://dataprepe2etest.blob.core.windows.net/test/try_upload/";
    //     let client = create_http_client();
    //     let handler = BlobStreamHandler::new(client);
    //     // [SuppressMessage("Microsoft.Security", "CS002:SecretInNextLine", Justification="test account, will remove later")]
    //     let credential = ;
    //
    //     let result = handler.get_destination(uri.into(), credential).unwrap();
    //     let mut writer = result.try_open_parallel_writer("20201012", 256, 64, 4).unwrap().unwrap();
    //
    //     for i in 0u8..4 {
    //         let writer_cloned = writer.get_block_writer();
    //         thread::spawn(move || {
    //             let content = vec![i.to_string().as_bytes()[0]; 64];
    //
    //             println!("enter work {}", i);
    //             writer_cloned.write_block(i as usize, content);
    //             println!("exit work {}", i);
    //         });
    //     }
    //
    //     println!("wait");
    //     writer.wait_for_completion();
    //     println!("wait done");
    //     writer.get_block_writer().completion_status().should().be_completed();
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::access_token::fake_access_token_resolver::FakeAccessTokenResolver;

    #[test]
    fn handler_constrution_should_be_successful() {
        let _stream_handler = create(BlobUriScheme::Wasbs);
        let _destination_handler = create_destination();
        let _stream_handler_with_token_resolver =
            create_with_token_resolver(Arc::new(FakeAccessTokenResolver::new()), BlobUriScheme::Wasbs);
        let _destination_handler_with_token_resolver = create_destination_with_token_resolver(Arc::new(FakeAccessTokenResolver::new()));
    }
}
