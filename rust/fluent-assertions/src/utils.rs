//! Utility classes to help write customized Assertions trait.
use crate::{Assertions, Should};
use std::fmt::Debug;

impl<T> Should<T> for AndConstraint<T> {
    fn should(self) -> Assertions<T> {
        self.subject.should()
    }
}

/// A class returned by assertion functions to enable the `and()` clause.
pub struct AndConstraint<T> {
    subject: T,
}

impl<T> AndConstraint<T> {
    pub fn new(value: T) -> Self {
        AndConstraint { subject: value }
    }

    pub fn and(self) -> T {
        self.subject
    }
}

/// A class returned by assertion functions to enable the `and()` and `which_value()` clause.
pub struct AndWhichValueConstraint<A, W> {
    subject: A,
    value: W,
}

impl<A, W> AndWhichValueConstraint<A, W> {
    pub fn new(subject: A, value: W) -> Self {
        AndWhichValueConstraint { subject, value }
    }

    pub fn and(self) -> A {
        self.subject
    }

    pub fn which_value(self) -> W {
        self.value
    }
}

/// A test trait to print some debug message for any type. It's inspired by this [post](https://www.reddit.com/r/rust/comments/6poulm/tip_print_a_t_without_requiring_t_debug/).
/// If the type implementing `Debug`, it will output the Debug message. Otherwise it will output the type name.
pub trait DebugMessage {
    fn debug_message(&self) -> String;
}

impl<T> DebugMessage for T {
    default fn debug_message(&self) -> String {
        format!("({})", std::any::type_name::<T>())
    }
}

impl<T: Debug> DebugMessage for T {
    default fn debug_message(&self) -> String {
        format!("{:?}", self)
    }
}

/// A helper class to generate consistent assertion failure messages.
pub struct AssertionFailure {
    messages: Vec<String>,
}

impl AssertionFailure {
    pub fn new(description: impl Into<String>) -> Self {
        AssertionFailure {
            messages: vec![description.into()],
        }
    }

    pub fn with_message(mut self, prompt: &str, message: impl AsRef<str>) -> Self {
        self.messages.push(format!("{}:\n  {}", prompt, message.as_ref()));
        self
    }

    pub fn expected<T>(self, expected: T) -> Self {
        self.with_message("expected", expected.debug_message())
    }

    pub fn expecting(self, expecting: impl AsRef<str>) -> Self {
        self.with_message("expecting", expecting.as_ref())
    }

    pub fn but(self, but: impl AsRef<str>) -> Self {
        self.with_message("but", but.as_ref())
    }

    pub fn but_was<T>(self, actual: T) -> Self {
        self.with_message("but was", actual.debug_message())
    }

    pub fn subject<T>(self, value: T) -> Self {
        self.with_message("subject", value.debug_message())
    }

    pub fn fail(self) {
        panic!("{}", self.messages.join("\n"));
    }
}

#[macro_export]
/// Convevient macro to assert that with specified expression, when executed will
/// panic with a message ([`String`] or &[`str`]), which pattern matches specified message.
/// See [`FnAssertions`](crate::FnAssertions) for details.
///
/// ### Examples
/// ```
/// use fluent_assertions::*;
///
/// should_fail_with_message!(
///     {
///         panic!("hello");
///     },
///     "hello"
/// );
/// ```
macro_rules! should_fail_with_message {
    ($expression:expr, $message:expr) => {{
        use $crate::FnAssertions;
        (|| $expression).should().panic().with_message($message);
    }};
}
