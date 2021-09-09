use crate::{
    destination::{destination::CreateDirectoryRequest, AppendWriteRequest, ParallelWriteRequest, RemoveRequest},
    new_request, AuthenticatedRequest, Method, Request, RequestWithCredential,
};
use fluent_assertions::*;
use rslex_core::{StreamInfo, SyncRecord};
use std::sync::Arc;

pub(super) struct RequestBuilder(pub String);

impl RequestBuilder {
    pub fn verify_parallel_create(&self, request: &Request) {
        request
            .uri()
            .should()
            .equal_string(format!("{}/ParallelWriteRequest/create/", self.0));
        request.method().should().be(&Method::PUT);
        request.body().should().equal_binary(b"");
    }

    pub fn verify_parallel_write_block(&self, request: &Request, block_idx: usize, position: usize, data: impl AsRef<[u8]>) {
        request
            .uri()
            .should()
            .equal_string(format!("{}/ParallelWriteRequest/write_block/{}/{}", self.0, block_idx, position));
        request.method().should().be(&Method::PUT);
        request.body().should().equal_binary(data.as_ref());
    }

    pub fn verify_parallel_complete(&self, request: &Request, block_count: usize, position: usize) {
        request
            .uri()
            .should()
            .equal_string(format!("{}/ParallelWriteRequest/complete/{}/{}", self.0, block_count, position));
        request.method().should().be(&Method::PUT);
        request.body().should().equal_binary(b"");
    }

    pub fn verify_append_create(&self, request: &Request) {
        request
            .uri()
            .should()
            .equal_string(format!("{}/AppendWriteRequest/create/", self.0));
        request.method().should().be(&Method::PUT);
        request.body().should().equal_binary(b"");
    }

    pub fn verify_append_write(&self, request: &Request, position: usize, data: impl AsRef<[u8]>) {
        request
            .uri()
            .should()
            .equal_string(format!("{}/AppendWriteRequest/write/{}", self.0, position));
        request.method().should().be(&Method::PUT);
        request.body().should().equal_binary(data.as_ref());
    }

    pub fn verify_append_flush(&self, request: &Request, position: usize) {
        request
            .uri()
            .should()
            .equal_string(format!("{}/AppendWriteRequest/flush/{}", self.0, position));
        request.method().should().be(&Method::PUT);
        request.body().should().equal_binary(b"");
    }

    pub fn verify_remove(&self, request: &Request) {
        request.uri().should().equal_string(format!("{}/RemoveRequest/remove/", self.0));
        request.method().should().be(&Method::PUT);
        request.body().should().equal_binary(b"");
    }
}

impl ParallelWriteRequest for RequestBuilder {
    fn create(&self) -> AuthenticatedRequest {
        new_request()
            .uri(format!("{}/ParallelWriteRequest/create/", self.0))
            .method(Method::PUT)
            .body::<Vec<u8>>(vec![])
            .unwrap()
            .with_credential(Arc::new(()))
    }

    fn write_block(&self, block_idx: usize, position: usize, data: &[u8]) -> AuthenticatedRequest {
        new_request()
            .uri(format!("{}/ParallelWriteRequest/write_block/{}/{}", self.0, block_idx, position))
            .method(Method::PUT)
            .body(data.to_vec())
            .unwrap()
            .with_credential(Arc::new(()))
    }

    fn complete(&self, block_count: usize, position: usize) -> AuthenticatedRequest {
        new_request()
            .uri(format!("{}/ParallelWriteRequest/complete/{}/{}", self.0, block_count, position))
            .method(Method::PUT)
            .body(vec![])
            .unwrap()
            .with_credential(Arc::new(()))
    }

    fn stream_info(&self) -> StreamInfo {
        StreamInfo::new("DUMMY_HANDLER", &self.0, SyncRecord::empty())
    }

    fn max_block_size(&self) -> usize {
        1024
    }

    fn max_block_count(&self) -> usize {
        100
    }
}

impl AppendWriteRequest for RequestBuilder {
    fn create(&self) -> AuthenticatedRequest {
        new_request()
            .uri(format!("{}/AppendWriteRequest/create/", self.0))
            .method(Method::PUT)
            .body(vec![])
            .unwrap()
            .with_credential(Arc::new(()))
    }

    fn write(&self, position: usize, data: &[u8]) -> AuthenticatedRequest {
        new_request()
            .uri(format!("{}/AppendWriteRequest/write/{}", self.0, position))
            .method(Method::PUT)
            .body(data.to_vec())
            .unwrap()
            .with_credential(Arc::new(()))
    }

    fn flush(&self, position: usize) -> Option<AuthenticatedRequest> {
        Some(
            new_request()
                .uri(format!("{}/AppendWriteRequest/flush/{}", self.0, position))
                .method(Method::PUT)
                .body(vec![])
                .unwrap()
                .with_credential(Arc::new(())),
        )
    }
}

impl CreateDirectoryRequest for RequestBuilder {
    fn create_directory(&self) -> AuthenticatedRequest {
        new_request()
            .uri(format!("{}/AppendWriteRequest/create_dir/", self.0))
            .method(Method::PUT)
            .body(vec![])
            .unwrap()
            .with_credential(Arc::new(()))
    }
}

impl RemoveRequest for RequestBuilder {
    fn remove(&self) -> AuthenticatedRequest {
        new_request()
            .uri(format!("{}/RemoveRequest/remove/", self.0))
            .method(Method::PUT)
            .body(vec![])
            .unwrap()
            .with_credential(Arc::new(()))
    }

    fn remove_directory(&self) -> AuthenticatedRequest {
        self.remove()
    }
}
