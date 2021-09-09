//! `tracing-sensitive` provides tools to tag types as containing [`SensitiveData`](crate::SensitiveData) and 'scrub'
//! the `Display` and `Debug` representations of these types when being processed by a [`Layer`].
//!
//! # Examples
//!
//! ```rust
//! use tracing_sensitive::{AsSensitive, ScrubSensitiveLayer, SensitiveData};
//! # use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
//! use tracing::debug;
//!
//! let stdout_layer = tracing_subscriber::fmt::layer().without_time().with_ansi(false);
//! let scrubbed_layer = ScrubSensitiveLayer::new(stdout_layer);
//! tracing_subscriber::registry().with(scrubbed_layer).init();
//!
//! // This event will look like: "DEBUG: Logging Initialized! [REDACTED]"
//! debug!("{} {}", "Logging Initialized!", "don't look at me...".as_sensitive());
//! ```
//!
//! [`Layer`]: https://docs.rs/tracing-subscriber/0.2.7/tracing_subscriber/layer/trait.Layer.html

#[cfg(feature = "layer")]
mod layer;
#[cfg(feature = "layer")]
pub use layer::ScrubSensitiveLayer;

use std::{
    cell::RefCell,
    fmt::{Debug, Display},
    ops::Deref,
};

thread_local! {
    /// Thread Local bool which determines whether SensitiveData types should be scrubbed when being formatted.
    pub static SCRUB_SENSITIVE: RefCell<bool> = RefCell::new(false);
}

pub fn scrub(v: &dyn Debug) -> &dyn Debug {
    if self::SCRUB_SENSITIVE.with(|v| *v.borrow()) {
        &"[REDACTED]"
    } else {
        v
    }
}

/// Wrapper struct for any `Sized` type that represents data which can only be formatted in certain contexts.
///
/// [`SensitiveData`](crate::SensitiveData) implements `Debug` and `Display` to use `T`s implementation,
/// *unless* [`SCRUB_SENSITIVE`](crate::SCRUB_SENSITIVE) is `true` in which case it formats as `"[REDACTED]"`.
pub struct SensitiveData<T>(T)
where T: Sized;

impl<T: Sized> SensitiveData<T> {
    pub fn new(data: T) -> Self {
        SensitiveData(data)
    }
}

pub trait SensitiveDisplay: Display {
    fn sens_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[REDACTED]")
    }
}

/// Any type which implements `Display` gets the default implementation of [`SensitiveDisplay`](crate::SensitiveDisplay).
impl<T> SensitiveDisplay for T where T: Display {}

pub trait SensitiveDebug: Debug {
    fn sens_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", "[REDACTED]")
    }
}

// TODO: Make this `#[derive()]`able, remove blanket over T, https://msdata.visualstudio.com/Vienna/_workitems/edit/814094
/// Any type which implements `Debug` gets the default implementation of [`SensitiveDebug`](crate::SensitiveDebug).
impl<T> SensitiveDebug for T where T: Debug {}

impl<T: Sized + SensitiveDebug> Debug for SensitiveData<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self::SCRUB_SENSITIVE.with(|v| *v.borrow()) {
            self.0.sens_fmt(f)
        } else if f.alternate() {
            write!(f, "{:#?}", self.0)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl<T: Sized + SensitiveDisplay> Display for SensitiveData<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self::SCRUB_SENSITIVE.with(|v| *v.borrow()) {
            self.0.sens_fmt(f)
        } else {
            write!(f, "{}", self.0)
        }
    }
}

pub trait AsSensitive: Sized {
    fn as_sensitive(self) -> SensitiveData<Self> {
        SensitiveData::new(self)
    }

    fn as_sensitive_ref(&self) -> SensitiveData<&Self> {
        SensitiveData::new(&self)
    }
}

impl<T> AsSensitive for T where T: Sized {}

impl<T> Deref for SensitiveData<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "layer")]
#[cfg(test)]
mod tests {
    use super::*;
    use lazy_static::lazy_static;
    use std::sync::{Mutex, MutexGuard, TryLockError};
    use tracing::debug;
    use tracing_subscriber::layer::SubscriberExt;

    /// Lifted from `tracing-subscriber::fmt` tests module.
    pub(crate) struct MockWriter<'a> {
        buf: &'a Mutex<Vec<u8>>,
    }

    impl<'a> MockWriter<'a> {
        pub(crate) fn new(buf: &'a Mutex<Vec<u8>>) -> Self {
            Self { buf }
        }

        pub(crate) fn map_error<Guard>(err: TryLockError<Guard>) -> std::io::Error {
            match err {
                TryLockError::WouldBlock => std::io::Error::from(std::io::ErrorKind::WouldBlock),
                TryLockError::Poisoned(_) => std::io::Error::from(std::io::ErrorKind::Other),
            }
        }

        pub(crate) fn buf(&self) -> std::io::Result<MutexGuard<'a, Vec<u8>>> {
            self.buf.try_lock().map_err(Self::map_error)
        }
    }

    impl<'a> std::io::Write for MockWriter<'a> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.buf()?.write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.buf()?.flush()
        }
    }

    #[test]
    fn scrub_sensitive_layer_only_scrubs_sensitive_data() {
        lazy_static! {
            static ref BUF: Mutex<Vec<u8>> = Mutex::new(vec![]);
        }

        let make_buf_writer = || MockWriter::new(&BUF);
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(make_buf_writer)
            .without_time()
            .with_ansi(false);
        // NOTE: fmt_layer wrapped in ScrubSensitiveLayer here.
        let subscriber = tracing_subscriber::registry().with(ScrubSensitiveLayer::new(fmt_layer));
        // Execute event macros in context of Subscriber.
        let dispatch = tracing::Dispatch::from(subscriber);
        tracing::dispatcher::with_default(&dispatch, || {
            debug!(normal_data = %"HI I'M DATA!", sensitive_data = %"don't look at me...".as_sensitive(), "{:?}", "sensitive_msg_arg".as_sensitive());
        });

        let expected = format!(
            "DEBUG {}: {}\n",
            module_path!(),
            "\"[REDACTED]\" normal_data=HI I'M DATA! sensitive_data=[REDACTED]"
        );
        let actual = String::from_utf8(BUF.try_lock().unwrap().to_vec()).unwrap();
        assert!(actual.contains(expected.as_str()));
    }

    #[test]
    fn sensitive_data_not_scrubbed_when_no_scrub_layer() {
        lazy_static! {
            static ref BUF: Mutex<Vec<u8>> = Mutex::new(vec![]);
        }

        let make_buf_writer = || MockWriter::new(&BUF);
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(make_buf_writer)
            .without_time()
            .with_ansi(false);
        // NOTE: fmt_layer NOT wrapped in ScrubSensitiveLayer here.
        let subscriber = tracing_subscriber::registry().with(fmt_layer);
        // Execute event macros in context of Subscriber.
        let dispatch = tracing::Dispatch::from(subscriber);
        tracing::dispatcher::with_default(&dispatch, || {
            debug!(normal_data = %"HI I'M DATA!", sensitive_data = %"don't look at me...".as_sensitive(), "{:?}", "sensitive_msg_arg".as_sensitive());
        });

        let expected = format!(
            "DEBUG {}: {}\n",
            module_path!(),
            "\"sensitive_msg_arg\" normal_data=HI I'M DATA! sensitive_data=don't look at me..."
        );
        let actual = String::from_utf8(BUF.try_lock().unwrap().to_vec()).unwrap();
        assert!(actual.contains(expected.as_str()));
    }
}
