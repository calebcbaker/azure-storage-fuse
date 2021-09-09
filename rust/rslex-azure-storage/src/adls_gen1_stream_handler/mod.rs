mod destination;
mod file_dto;
mod request_builder;
mod retry_strategy;
mod searcher;
mod stream_handler;

pub use self::stream_handler::ADLSGen1StreamHandler;
use crate::credential::access_token::ScopedAccessTokenResolver;
use retry_strategy::AdlsGen1RetryCondition;
use rslex_core::file_io::{DestinationError, DestinationHandler, StreamError};
use rslex_http_stream::HttpClientBuilder;
use std::sync::Arc;

/// The type of the [`StreamHandler`](crate::StreamHandler) that can read from Azure Data Lake Gen 1.
pub const HANDLER_TYPE: &str = "AzureDataLakeStorage";

/// Creates a [`StreamHandler`] that can read from Azure Blob Storage.
///
/// # Example
///
/// ```rust,no_run
/// use rslex_core::{file_io::{StreamAccessor, StreamHandler}, sync_record};
/// use rslex_azure_storage::create_adls_gen1_handler;
/// use std::collections::HashMap;
/// use std::convert::{TryFrom, TryInto};
/// use serde_json::json;
/// use std::io::SeekFrom;
///
/// let handler = create_adls_gen1_handler().unwrap();
///
/// // use service principal credential to access your ADLS Gen1 storage
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
///
/// // place holder. Not used in this scenario
/// let stream_accessor = StreamAccessor::default();
///
/// // the following will search the uri pattern, and returning all the matching items
/// let uri = "adl://youraccount.azuredatalakestore.net/somefolder/*.csv";
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
/// let uri = "adl://youraccount.azuredatalakestore.net/somefolder/somefile.csv";
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
pub fn create() -> Result<ADLSGen1StreamHandler, StreamError> {
    Ok(ADLSGen1StreamHandler::new(
        HttpClientBuilder::with_retry_condition(AdlsGen1RetryCondition::new()).build()?,
    ))
}

pub fn create_destination() -> Result<impl DestinationHandler, StreamError> {
    Ok(ADLSGen1StreamHandler::new(
        HttpClientBuilder::with_retry_condition(AdlsGen1RetryCondition::new()).build()?,
    ))
}

pub fn create_with_token_resolver(access_token_resolver: Arc<dyn ScopedAccessTokenResolver>) -> Result<ADLSGen1StreamHandler, StreamError> {
    let client = HttpClientBuilder::with_retry_condition(AdlsGen1RetryCondition::new()).build()?;
    Ok(ADLSGen1StreamHandler::new(client).with_access_token_resolver(access_token_resolver))
}

pub fn create_destination_with_token_resolver(
    access_token_resolver: Arc<dyn ScopedAccessTokenResolver>,
) -> Result<impl DestinationHandler, DestinationError> {
    let client = HttpClientBuilder::with_retry_condition(AdlsGen1RetryCondition::new()).build()?;
    Ok(ADLSGen1StreamHandler::new(client).with_access_token_resolver(access_token_resolver))
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
    // fn adls_gen1_list_dir_and_download_section() {
    //     use super::*;
    //     use chrono::Utc;
    //
    //     init_log();
    //     let start = Utc::now();
    //
    //     //let uri = "adl://dataprepe2e.azuredatalakestore.net/input/*";
    //     let uri = "adl://dataprepe2e.azuredatalakestore.net/*/*365*/*";
    //     let arguments = service_principal();
    //     let handler = create();
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
    //     let uri = "adl://dataprepe2e.azuredatalakestore.net/PP0716.MUIDFLT33.txt";
    //     let opener = handler
    //         .get_opener(uri, arguments.parse().unwrap(), &HashMap::default(), &STREAM_ACCESSOR)
    //         .unwrap();
    //     let mut output = vec![0; 11];
    //     let seekable_opener = opener.try_as_seekable().unwrap();
    //     seekable_opener.copy_section_to(3, &mut output).unwrap();
    //
    //     let result = String::from_utf8(output).unwrap();
    //     assert_that(&result.as_str()).is_equal_to("EmptyFlight");
    // }

    // #[test]
    // fn adls_gen1_parallel_upload() {
    //     use super::*;
    //     use crate::credential::CredentialInput;
    //     use fluent_assertions::*;
    //     use rslex_core::test_helper::*;
    //     use std::thread;
    //
    //     let uri = "adl://dataprepe2e.azuredatalakestore.net/input/";
    //     let http_client = rslex_http_stream::create_http_client();
    //     let handler = ADLSGen1StreamHandler::new(http_client);
    //     let credential = CredentialInput::ServicePrincipal {
    //     };
    //     let destination = handler.get_destination(uri.into(), credential).unwrap();
    //     println!("start");
    //     let mut parallel_writer = destination
    //         .try_open_parallel_writer("adls_gen1_parallel_upload.txt", 256, 64, 1)
    //         .unwrap()
    //         .unwrap();
    //
    //     println!("start uploading...");
    //
    //     for i in 0usize..4 {
    //         let writer_cloned = parallel_writer.get_block_writer();
    //         thread::spawn(move || {
    //             let content = vec![i.to_string().as_bytes()[0]; 64];
    //             let mut buffer = writer_cloned.get_block_buffer(i);
    //             buffer.copy_from_slice(content.as_ref());
    //
    //             println!("enter work {}", i);
    //             writer_cloned.write_block(i, buffer);
    //             println!("exit work {}", i);
    //         });
    //     }
    //
    //     println!("wait");
    //     parallel_writer.wait_for_completion();
    //     println!("wait done");
    //     parallel_writer.completion_status().should().be_completed();
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
