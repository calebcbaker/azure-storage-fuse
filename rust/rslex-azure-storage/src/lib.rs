mod adls_gen1_stream_handler;
mod adls_gen2_stream_handler;
mod blob_stream_handler;
mod continuation_token_iterator;
pub mod credential;
mod file_stream_handler;

#[cfg(any(feature = "fake_access_token_resolver", test))]
pub use self::credential::access_token::fake_access_token_resolver::FakeAccessTokenResolver;

pub use self::{
    adls_gen1_stream_handler::{
        create as create_adls_gen1_handler, create_destination as create_adls_gen1_destination_handler,
        create_destination_with_token_resolver as create_adls_gen1_destination_handler_with_token_resolver,
        create_with_token_resolver as create_adls_gen1_handler_with_token_resolver, HANDLER_TYPE as ADLS_GEN1_HANDLER_TYPE,
        ADLSGen1StreamHandler,
    },
    adls_gen2_stream_handler::{
        create as create_adls_gen2_handler, create_destination as create_adls_gen2_destination_handler,
        create_destination_with_token_resolver as create_adls_gen2_destination_handler_with_token_resolver,
        create_with_token_resolver as create_adls_gen2_handler_with_token_resolver, HANDLER_TYPE as ADLS_GEN2_HANDLER_TYPE,
    },
    blob_stream_handler::{
        create as create_blob_handler, create_destination as create_blob_destination_handler,
        create_destination_with_token_resolver as create_blob_destination_handler_with_token_resolver,
        create_with_token_resolver as create_blob_handler_with_token_resolver, BlobUriScheme, HANDLER_TYPE as BLOB_HANDLER_TYPE,
    },
    file_stream_handler::{
        create as create_file_handler, create_with_token_resolver as create_file_handler_with_token_resolver,
        HANDLER_TYPE as FILE_HANDLER_TYPE,
    },
};
