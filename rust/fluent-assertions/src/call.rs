use crate::{
    utils::{AssertionFailure, DebugMessage},
    Assertions,
};
use regex::{escape, Regex};
use std::{any::Any, panic, panic::UnwindSafe};

/// Making assertion on panic cause.
pub struct PanicCauseConstraint {
    cause: Box<dyn Any + Send>,
}

fn normalize_string(input: String) -> String {
    let space = Regex::new("(?s)\\s+").unwrap();
    space.replace_all(&input, " ").trim().to_string()
}

impl PanicCauseConstraint {
    /// Panic cause should be a String, and matching pattern.
    pub fn with_message(self, pattern: impl AsRef<str>) -> Self {
        let message = if let Some(m) = self.cause.downcast_ref::<String>() {
            m.to_owned()
        } else if let Some(m) = self.cause.downcast_ref::<&str>() {
            m.to_string()
        } else {
            AssertionFailure::new("should().panic().with_message() expecting panic cause to be String but failed")
                .subject(self.cause)
                .fail();

            unreachable!()
        };
        let message = normalize_string(message);

        let reg_pattern = format!("(?s){}", escape(pattern.as_ref()).replace("\\*", ".*").replace("\\?", "."));
        let reg_pattern = normalize_string(reg_pattern);

        let regex = Regex::new(&reg_pattern).unwrap();

        if !regex.is_match(&message) {
            AssertionFailure::new("should().panic().with_message() expectation failed for panic message")
                .expecting(format!("message matching pattern <{}>", pattern.as_ref()))
                .but_was(message)
                .fail();
        }

        self
    }

    pub fn which(self) -> Box<dyn Any + Send> {
        self.cause
    }
}

/// Assertions for closures implementing [`FnOnce()`](std::ops::FnOnce) + [`UnwindSafe`](std::panic::UnwindSafe).
///
/// ### Examples
/// ```
/// use fluent_assertions::*;
///
/// // test closure should panic
/// let subject = || {
///     panic!(format!("this is the message"));
/// };
/// subject.should().panic();
///
/// // use with_message() clause to test closure panic with string message (&str or String)
/// // and pattern match the error message
/// let subject = || {
///     panic!("this is the message");
/// };
/// subject.should().panic().with_message("this is the message");
///
/// // with_message() normalize space characters
/// let subject = || {
///     panic!("this \t is\n the                   message");
/// };
/// subject.should().panic().with_message("this is the message");
///
/// // with_message() support glob patterns
/// let subject = || {
///     panic!("hello world");
/// };
/// subject.should().panic().with_message("h*w???d");
///
/// // should_panic_with_message! macro provide a syntactic sugar to test panic message from an expression
/// should_fail_with_message!(
///     {
///         panic!("hello");
///     },
///     "hello"
/// );
///
/// // if closure panic with no string message, use which() clause to test the value of Cause.
/// // which() clause returns Box<dyn Any + Send>
/// let subject = || {
///     panic!(13u32);
/// };
/// subject.should().panic().which().should().be_of_type::<u32>().with_value(13u32);
/// ```
pub trait FnAssertions {
    fn panic(self) -> PanicCauseConstraint;
}

fn run_fn<F, R>(f: F) -> Result<R, Box<dyn Any + Send + 'static>>
where F: FnOnce() -> R + UnwindSafe {
    panic::set_hook(Box::new(|p| {
        log::debug!("panic occurred: {:?}", p.payload().type_id());
    }));

    let result = panic::catch_unwind(|| f());

    let _ = panic::take_hook();

    result
}

impl<F, R> FnAssertions for Assertions<F>
where F: FnOnce() -> R + UnwindSafe
{
    fn panic(self) -> PanicCauseConstraint {
        let result = run_fn(self.into_inner());

        match result {
            Ok(r) => {
                AssertionFailure::new(format!(
                    "should().panic() expecting function call to panic, but succeed with returned value <{}>",
                    r.debug_message()
                ))
                .fail();

                unreachable!()
            },
            Err(cause) => PanicCauseConstraint { cause },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use call::FnAssertions;

    #[test]
    fn should_panic_with_messageas_string_matches_pattern() {
        (|| {
            panic!("this is the message");
        })
        .should()
        .panic()
        .with_message("mes*ge");
    }

    #[test]
    fn should_panic_with_message_as_str_matches_pattern() {
        (|| {
            panic!("this is the message");
        })
        .should()
        .panic()
        .with_message("mes*ge");
    }

    #[test]
    fn pattern_match_will_normalize_spaces() {
        (|| {
            panic!("this \nis  the \t message");
        })
        .should()
        .panic()
        .with_message("this is the message ");
    }

    #[test]
    fn should_panic_which_assertion() {
        (|| {
            std::panic::panic_any(42u32);
        })
        .should()
        .panic()
        .which()
        .should()
        .be_of_type::<u32>()
        .with_value(42);
    }
}
