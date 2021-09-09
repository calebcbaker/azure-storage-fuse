
use std::{sync::Arc};

use std::ptr;

use rslex_azure_storage::{create_adls_gen1_handler as create, create_adls_gen1_handler_with_token_resolver as create_with_token_resolver, credential::{
    access_token::{ScopedAccessTokenResolver},
},
ADLSGen1StreamHandler
};

pub struct BlobfuseScopedAccessTokenResolver {
    data: Arc<dyn ScopedAccessTokenResolver>
  }

#[no_mangle]
pub unsafe extern "C" fn create_adls_gen1_handler() -> *mut ADLSGen1StreamHandler {
    match create() {
      Ok(handler) => Box::into_raw(Box::new(handler)),
      Err(_e) => ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn create_adls_gen1_destination_handler_with_token_resolver(
  access_token_resolver: *mut BlobfuseScopedAccessTokenResolver) -> *mut ADLSGen1StreamHandler {
    assert!(!access_token_resolver.is_null());
    match create_with_token_resolver(Box::from_raw(access_token_resolver).data) {
      Ok(handler) => Box::into_raw(Box::new(handler)),
      Err(_e) => ptr::null_mut(),
    }
}