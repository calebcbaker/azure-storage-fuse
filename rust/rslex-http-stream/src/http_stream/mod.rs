mod opener;
mod seekable_read;
mod unseekable_read;

use crate::{http_stream::opener::HttpStreamOpener, AuthenticatedRequest, HttpClient, Response};
use rslex_core::{
    file_io::{StreamOpener, StreamResult},
    SessionProperties,
};
use std::sync::Arc;

pub trait HeadRequest {
    fn head(&self) -> AuthenticatedRequest;

    fn parse_response(response: Response, _session_properties: &mut SessionProperties) -> StreamResult<()>;

    fn default_is_seekable() -> bool;
}

pub use seekable_read::ReadSectionRequest;
pub use unseekable_read::ReadRequest;

pub fn create(
    request_builder: impl HeadRequest + ReadRequest + ReadSectionRequest + Clone + Send + Sync + 'static,
    http_client: Arc<dyn HttpClient>,
    session_properties: SessionProperties,
) -> impl StreamOpener {
    HttpStreamOpener::new(request_builder, http_client, session_properties)
}
