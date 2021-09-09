//! [`ScrubSensitiveLayer`](crate::layer::ScrubSensitiveLayer) wraps a [`tracing::Layer`] and ensures
//! [`SensitiveData`](crate::SensitiveData) will scrub themselves when being formatted via `Debug` or `Display`.
//!
//! [`tracing::Layer`]: https://docs.rs/tracing-subscriber/0.2.7/tracing_subscriber/layer/trait.Layer.html

use crate::SCRUB_SENSITIVE;
use std::{any::TypeId, cell::RefCell, marker::PhantomData, thread::LocalKey};
use tracing::{span, Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};

/// Wraps a [`Layer`] and sets [`SCRUB_SENSITIVE=true`](crate::SCRUB_SENSITIVE) before forwarding any method calls.
///
/// Sets `SCRUB_SENSITIVE=false` before returning from any method.
///
/// [`Layer`]: https://docs.rs/tracing-subscriber/0.2.7/tracing_subscriber/layer/trait.Layer.html
pub struct ScrubSensitiveLayer<I, S> {
    scrub_sensitive: &'static LocalKey<RefCell<bool>>,
    inner: I,
    _s: PhantomData<fn(S)>,
}

impl<S: Subscriber, I: Layer<S> + Sized> ScrubSensitiveLayer<I, S> {
    pub fn new(inner: I) -> Self {
        ScrubSensitiveLayer {
            scrub_sensitive: &SCRUB_SENSITIVE,
            inner,
            _s: PhantomData,
        }
    }
}

impl<S: Subscriber, I: Layer<S>> Layer<S> for ScrubSensitiveLayer<I, S> {
    fn new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        self.scrub_sensitive.with(|v| *v.borrow_mut() = true);
        self.inner.new_span(attrs, id, ctx);
        self.scrub_sensitive.with(|v| *v.borrow_mut() = false);
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        self.scrub_sensitive.with(|v| *v.borrow_mut() = true);
        self.inner.on_record(id, values, ctx);
        self.scrub_sensitive.with(|v| *v.borrow_mut() = false);
    }

    fn on_follows_from(&self, id: &span::Id, follows: &span::Id, ctx: Context<S>) {
        self.scrub_sensitive.with(|v| *v.borrow_mut() = true);
        self.inner.on_follows_from(id, follows, ctx);
        self.scrub_sensitive.with(|v| *v.borrow_mut() = false);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        self.scrub_sensitive.with(|v| *v.borrow_mut() = true);
        self.inner.on_event(event, ctx);
        self.scrub_sensitive.with(|v| *v.borrow_mut() = false);
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        self.scrub_sensitive.with(|v| *v.borrow_mut() = true);
        self.inner.on_close(id, ctx);
        self.scrub_sensitive.with(|v| *v.borrow_mut() = false);
    }

    unsafe fn downcast_raw(&self, id: TypeId) -> Option<*const ()> {
        self.inner.downcast_raw(id)
    }
}
