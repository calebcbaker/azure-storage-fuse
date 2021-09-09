mod destination;
mod path_dto;
mod request_builder;
mod searcher;
mod stream_handler;

use self::stream_handler::ADLSGen2StreamHandler;
use crate::credential::access_token::ScopedAccessTokenResolver;
use rslex_core::file_io::{DestinationError, DestinationHandler, StreamError, StreamHandler};
use rslex_http_stream::create_http_client;
use std::sync::Arc;

/// The type of the [`StreamHandler`](crate::StreamHandler) that can read from Azure Data Lake Gen 2.
pub const HANDLER_TYPE: &str = "ADLSGen2";

/// Creates a [`StreamHandler`] that can read from Azure Data Lake Gen 2.
///
/// # Example
///
/// ``` rust, no_run
/// use rslex_core::{file_io::{StreamAccessor, StreamHandler}, sync_record};
/// use rslex_azure_storage::create_adls_gen2_handler;
/// use std::collections::HashMap;
/// use std::convert::{TryFrom, TryInto};
/// use std::io::SeekFrom;
///
/// let handler = create_adls_gen2_handler().unwrap();
///
/// // use sas credential to access storage
/// let sas = "your&sas&token";
/// let arguments = sync_record!{ "sas" => sas };
///
/// // place holder. Not used in this scenario
/// let stream_accessor = StreamAccessor::default();
///
/// // this will return an iterator of result items
/// let uri = "https://youraccount.dfs.core.windows.net/somefilesystem/*.csv";
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
/// let uri = "https://youraccount.dfs.core.windows.net/somefilesystem/somefile.csv";
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
pub fn create() -> Result<impl StreamHandler, StreamError> {
    Ok(ADLSGen2StreamHandler::new(create_http_client()?))
}

pub fn create_destination() -> Result<impl DestinationHandler, DestinationError> {
    Ok(ADLSGen2StreamHandler::new(create_http_client()?))
}

pub fn create_with_token_resolver(access_token_resolver: Arc<dyn ScopedAccessTokenResolver>) -> Result<impl StreamHandler, StreamError> {
    let client = create_http_client()?;
    Ok(ADLSGen2StreamHandler::new(client).with_access_token_resolver(access_token_resolver))
}

pub fn create_destination_with_token_resolver(
    access_token_resolver: Arc<dyn ScopedAccessTokenResolver>,
) -> Result<impl DestinationHandler, DestinationError> {
    let client = create_http_client()?;
    Ok(ADLSGen2StreamHandler::new(client).with_access_token_resolver(access_token_resolver))
}

#[cfg(test)]
#[cfg(feature = "component_tests")]
mod component_tests {
    // use lazy_static::lazy_static;
    // lazy_static! {
    //     static ref STREAM_ACCESSOR: StreamAccessor = StreamAccessor::default();
    // }

    // TODO: Re-enable after getting auth from env, Task: 839659
    // #[test]
    // fn adls_gen2_list_dir_and_download_section() {
    //     init_log();
    //     let start = Utc::now();
    //     let arguments = service_principal();
    //     let handler = create();
    //
    //     let uri = "https://adlsgen2datapreptest.dfs.core.windows.net/datapreptest/input/*.csv";
    //
    //     let result = handler.find_streams(uri, arguments.parse().unwrap(), &STREAM_ACCESSOR).unwrap();
    //
    //     for si in result.iter().map(|r| r.unwrap()) {
    //         println!("{:#?}", si.resource_id());
    //     }
    //
    //     let end = Utc::now();
    //
    //     log::info!("time={}", (end - start));
    //
    //     let uri = "https://adlsgen2datapreptest.dfs.core.windows.net/datapreptest/people.csv";
    //     let opener = handler
    //         .get_opener(uri, arguments.parse().unwrap(), &HashMap::default(), &STREAM_ACCESSOR)
    //         .unwrap();
    //     let mut output = vec![0; 5];
    //
    //     let seekable_opener = opener.try_as_seekable().unwrap();
    //     seekable_opener.copy_section_to(12, &mut output).unwrap();
    //
    //     let result = String::from_utf8(output).unwrap();
    //     assert_that(&result.as_str()).is_equal_to("email");
    //     println!("{}", result);
    // }
    //
    // #[test]
    // #[ignore]
    // fn adls_gen2_list_big_dir() {
    //     init_log();
    //     let start = Utc::now();
    //     let arguments = service_principal();
    //     let handler = create();
    //     let mut count = 0;
    //
    //     let uri = "https://adlsgen2datapreptest.dfs.core.windows.net/testfiles/**/*.csv";
    //
    //     let result = handler.find_streams(uri, arguments.parse().unwrap(), &STREAM_ACCESSOR).unwrap();
    //
    //     for si in result.iter() {
    //         match si {
    //             Ok(r) => {
    //                 println!("[{}]{:#?}", count, r.resource_id());
    //                 count += 1;
    //             },
    //             Err(e) => {
    //                 log::error!("Error: {:?}", e);
    //             },
    //         }
    //     }
    //     log::info!("count={}", count);
    //
    //     let end = Utc::now();
    //
    //     log::info!("time={}", (end - start));
    // }
    // #[test]
    // fn adls_gen2_parallel_upload() {
    //     use crate::{adls_gen2_stream_handler::stream_handler::ADLSGen2StreamHandler, credential::CredentialInput};
    //     use fluent_assertions::*;
    //     use rslex_core::{
    //         file_io::{DestinationHandler, IfDestinationExists},
    //         test_helper::*,
    //     };
    //     use rslex_http_stream::create_http_client;
    //     use std::thread;
    //
    //     let base_path = "https://adlsgen2datapreptest.dfs.core.windows.net/datapreptest/upload/";
    //     let credential = CredentialInput::Sas {token: "".to_string()};
    //     let client = create_http_client();
    //     let handler = ADLSGen2StreamHandler::new(client);
    //     let destination = handler
    //         .get_destination(base_path.into(), credential, IfDestinationExists::MergeWithOverwrite)
    //         .unwrap();
    //     let mut writer = destination.try_open_parallel_writer("20201106", 256, 64, 4).unwrap().unwrap();
    //
    //     for i in 0u8..4 {
    //         let writer_cloned = writer.get_block_writer();
    //         thread::spawn(move || {
    //             let content = vec![i.to_string().as_bytes()[0]; 64];
    //
    //             println!("enter work {}", i);
    //             writer_cloned.write_binary(i as usize, &content).should().be_ok();
    //             println!("exit work {}", i);
    //         });
    //     }
    //
    //     println!("wait");
    //     writer.wait_for_completion().should().be_ok();
    //     println!("wait done");
    //     writer.completion_status().should().be_completed();
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::access_token::fake_access_token_resolver::FakeAccessTokenResolver;

    #[test]
    fn handler_constrution_should_be_successful() {
        let _stream_handler = create();
        let _destination_handler = create_destination();
        let _stream_handler_with_token_resolver = create_with_token_resolver(Arc::new(FakeAccessTokenResolver::new()));
        let _destination_handler_with_token_resolver = create_destination_with_token_resolver(Arc::new(FakeAccessTokenResolver::new()));
    }
}
