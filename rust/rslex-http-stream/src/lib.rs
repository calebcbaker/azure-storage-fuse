//! A utility crate to write HTTP-based [`StreamHandler`]. It provides:
//! - [`HttpClient`]: HttpClient library provides both sync & async operations.
//! - [`AsyncSearch`]: Util trait which enables implementing incremental search over remote file system.
//! - [`HttpStreamOpener`]: An implementation of [`StreamOpener`], wrapping common bahaviors of HTTP-based stream handler.
//! - [`create_http_handler`]: StreamHandler for public HTTP stream.
//! - [`FakeHttpClient`]: enabled by ``` features = ["fake_http_client"] ```, provide a faked HttpClient for testing.
#![allow(incomplete_features)]
#![feature(box_syntax)]
#![feature(async_closure)]
#![feature(specialization)]

pub use rslex_core::session_properties_ext::SessionPropertiesExt;
use rslex_core::{file_io::*, *};

#[cfg(any(feature = "fake_http_client", test))]
pub use self::http_client::{FakeCredential, FakeHttpClient};
pub use self::{
    destination::{AppendWriteRequest, CreateDirectoryRequest, DestinationBuilder, ParallelWriteRequest, RemoveRequest},
    encoded_url::EncodedUrl,
    glob_pattern::{AsyncSearch, SearchContext},
    http_client::{
        create as create_http_client, new_request, ApplyCredential, AsyncResponse, AsyncResponseExt, AuthenticatedRequest, HttpClient,
        HttpClientBuilder, HttpClientCreationError, HttpError, Method, Request, RequestWithCredential, Response, ResponseExt, Spawn,
        SpawnBlocking, Wait,
    },
    http_stream::{create as create_stream_opener, HeadRequest, ReadRequest, ReadSectionRequest},
    http_stream_handler::{create as create_http_handler, HttpUriScheme, HANDLER_TYPE as HTTP_HANDLER_TYPE},
    retry::{http_client_retry::DefaultHttpRetryCondition, RetryCondition, RetryStrategy},
};

mod destination;
mod encoded_url;
mod glob_pattern;
mod http_client;
mod http_stream;
mod http_stream_handler;
mod retry;
