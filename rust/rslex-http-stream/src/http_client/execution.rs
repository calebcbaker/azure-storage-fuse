use super::http_error::HttpError;
use lazy_static::lazy_static;
use std::{
    future::Future,
    result::Result,
    sync::{
        mpsc::{channel, RecvError, RecvTimeoutError},
        Arc,
    },
    time::Duration,
};
use tokio::{runtime::Runtime, task::JoinHandle};
use tracing::Instrument;

lazy_static! {
    static ref RUN_TIME: Runtime = Runtime::new().unwrap();
}

/// Spawn will submit a future to runtime. As most of
/// The http_client library will host a tokio runtime.
/// [`Spawn`], [`SpawnBlocking`], [`Wait`] provides three interfaces to interact with the runtime.
/// - [`Spawn`] - submit an async function (future) to runtime.
/// - [`SpawnBlocking`] - wraps a blocking function to a async one. This enables calling a blocking function
///     inside an async function.
/// - [`Wait`] - blocking calling an async function. This enables calling an async function inside a blocking function.
///
/// Wait will wait for a Future to complete, effective calling a async
/// function in a blocking way. Behind the scene, it will submit the
/// future to runtime, setting up a channel to communicate between future
/// and blocking thread, wake up with blocking thread with channel.
///
/// Note: as wait use a channel to communicate between async logic and blocking function,
/// there is a chance of channel throw RecvError. So Wait can only be called on async function
/// whose return type is ```Result<_, E: From<RecvError>>```
///
/// ## Example
/// ```
/// use rslex_http_stream::Wait;
/// use std::sync::mpsc::RecvError;
///
/// let s = async { Ok::<_, RecvError>(1 + 1) }.wait();
/// ```
pub trait Wait {
    type Output;
    fn wait(self) -> Result<Self::Output, RecvError>;

    fn wait_timeout(self, timeout: Duration) -> Result<Self::Output, RecvTimeoutError>;
}

impl<F> Wait for F
where
    F: Future + Send + 'static,
    <F as Future>::Output: Send + 'static,
{
    type Output = <F as Future>::Output;

    fn wait(self) -> Result<Self::Output, RecvError> {
        let (sender, receiver) = channel();

        RUN_TIME.spawn(
            async move {
                let result = self.await;
                sender.send(result).expect("send should succeed");
                drop(sender);
            }
            .instrument(tracing::trace_span!(parent: &tracing::Span::current(), "wait")),
        );

        receiver.recv()
    }

    fn wait_timeout(self, timeout: Duration) -> Result<Self::Output, RecvTimeoutError> {
        let (sender, receiver) = channel();

        RUN_TIME.spawn(
            async move {
                let result = self.await;
                sender.send(result).expect("send should succeed");
                drop(sender);
            }
            .instrument(tracing::info_span!(parent: &tracing::Span::current(), "wait_timeout")),
        );

        receiver.recv_timeout(timeout)
    }
}

impl From<RecvError> for HttpError {
    fn from(e: RecvError) -> Self {
        HttpError {
            is_connect: false,
            boxed_error: Arc::new(e),
        }
    }
}

/// Spawn will submit a future to runtime. As most of
/// The http_client library will host a tokio runtime.
/// [`Spawn`], [`SpawnBlocking`], [`Wait`] provides three interfaces to interact with the runtime.
/// - [`Spawn`] - submit an async function (future) to runtime.
/// - [`SpawnBlocking`] - wraps a blocking function to a async one. This enables calling a blocking function
///     inside an async function.
/// - [`Wait`] - blocking calling an async function. This enables calling an async function inside a blocking function.
///
/// ## Example
/// ```
/// use rslex_http_stream::Spawn;
///
/// let handle = async { 1 + 1 }.spawn(); // handle is a JoinHandle, you can await on it
/// ```
pub trait Spawn {
    type Output;
    fn spawn(self) -> JoinHandle<Self::Output>;
}

impl<F> Spawn for F
where
    F: Future + Send + 'static,
    <F as Future>::Output: Send,
{
    type Output = <F as Future>::Output;

    fn spawn(self) -> JoinHandle<Self::Output> {
        RUN_TIME.spawn(self)
    }
}

/// Spawn will submit a future to runtime. As most of
/// The http_client library will host a tokio runtime.
/// [`Spawn`], [`SpawnBlocking`], [`Wait`] provides three interfaces to interact with the runtime.
/// - [`Spawn`] - submit an async function (future) to runtime.
/// - [`SpawnBlocking`] - wraps a blocking function to a async one. This enables calling a blocking function
///     inside an async function.
/// - [`Wait`] - blocking calling an async function. This enables calling an async function inside a blocking function.
///
/// SpawnBlocking will submit a blocking function to runtime, give
/// back a async handle to call in an async function.
/// async function MUST be non-blocking (i.e., no lock, blocking etc.),
/// to call a blocking function from async, spawn_blocking the blocking
/// function (which will start a new thread to run the blocking function),
/// await for the handle.
///
/// ## Example
/// ``` no_run
/// use rslex_http_stream::SpawnBlocking;
/// use std::{thread::sleep, time::Duration};
///
/// fn blocking() {
///     // this a blocking function
///     sleep(Duration::new(60, 0));
/// }
///
/// let handle = blocking.spawn_blocking(); // JoinHandle you can await in an async context
/// ```
pub trait SpawnBlocking {
    type Output;
    fn spawn_blocking(self) -> JoinHandle<Self::Output>;
}

impl<F, R> SpawnBlocking for F
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    type Output = R;

    fn spawn_blocking(self) -> JoinHandle<R> {
        tokio::task::spawn_blocking(self)
    }
}
