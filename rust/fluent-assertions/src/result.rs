use super::*;
use crate::utils::{AndConstraint, AssertionFailure, DebugMessage};
use std::{borrow::Borrow, marker::PhantomData};

pub struct IsOk;
pub struct IsErr;

#[allow(dead_code)]
pub struct ResultConstraint<T, E, R: Borrow<Result<T, E>>, OE> {
    subject: R,
    phantom_t: PhantomData<T>,
    phantom_r: PhantomData<E>,
    ok_or_err: OE,
}

impl<T, E, R: Borrow<Result<T, E>>, OE> ResultConstraint<T, E, R, OE> {
    fn new(subject: R, ok_or_err: OE) -> Self {
        ResultConstraint {
            subject,
            phantom_t: PhantomData,
            phantom_r: PhantomData,
            ok_or_err,
        }
    }

    pub fn and(self) -> R {
        self.subject
    }
}

impl<T: PartialEq, E, R: Borrow<Result<T, E>>> ResultConstraint<T, E, R, IsOk> {
    pub fn with_value(self, value: impl Borrow<T>) -> AndConstraint<R> {
        if !self.subject.borrow().as_ref().ok().unwrap().eq(value.borrow()) {
            AssertionFailure::new(format!(
                "should().be_ok().with_value() expecting subject to be\n  Ok({})\nbut failed with",
                value.borrow().debug_message()
            ))
            .subject(&self.subject)
            .fail();
        }

        AndConstraint::new(self.subject)
    }
}

impl<T, E, R: Borrow<Result<T, E>>> ResultConstraint<T, E, R, IsOk> {
    pub fn with_value_that(self, pred: impl FnOnce(&T) -> bool) -> AndConstraint<R> {
        let value = self.subject.borrow().as_ref().ok().unwrap();
        if !pred(value) {
            AssertionFailure::new(
                "should().be_ok().with_value_that() expecting subject to be Result::Ok with inner value matching condition but failed",
            )
            .subject(&self.subject)
            .fail();
        }

        AndConstraint::new(self.subject)
    }
}

impl<T, E> ResultConstraint<T, E, Result<T, E>, IsOk> {
    pub fn which_value(self) -> T {
        self.subject.ok().unwrap()
    }
}

impl<'r, T, E> ResultConstraint<T, E, &'r Result<T, E>, IsOk> {
    pub fn which_value(self) -> &'r T {
        self.subject.as_ref().ok().unwrap()
    }
}

impl<T, E: PartialEq, R: Borrow<Result<T, E>>> ResultConstraint<T, E, R, IsErr> {
    pub fn with_value(self, value: impl Borrow<E>) -> AndConstraint<R> {
        if !self.subject.borrow().as_ref().err().unwrap().eq(value.borrow()) {
            AssertionFailure::new(format!(
                "should().be_ok().with_value() expecting subject to be Result::Err({}) but failed",
                value.borrow().debug_message()
            ))
            .subject(&self.subject)
            .fail();
        }

        AndConstraint::new(self.subject)
    }
}

impl<T, E, R: Borrow<Result<T, E>>> ResultConstraint<T, E, R, IsErr> {
    pub fn with_value_that(self, pred: impl FnOnce(&E) -> bool) -> AndConstraint<R> {
        let value = self.subject.borrow().as_ref().err().unwrap();
        if !pred(value) {
            AssertionFailure::new(
                "should().be_err().with_value_that() expecting subject to be Result::Err with inner value match condition but failed",
            )
            .subject(&self.subject)
            .fail();
        }

        AndConstraint::new(self.subject)
    }
}

impl<T, E> ResultConstraint<T, E, Result<T, E>, IsErr> {
    pub fn which_value(self) -> E {
        self.subject.err().unwrap()
    }
}

impl<'r, T, E> ResultConstraint<T, E, &'r Result<T, E>, IsErr> {
    pub fn which_value(self) -> &'r E {
        self.subject.as_ref().err().unwrap()
    }
}

/// Assertions for types implementing [`Borrow`](std::borrow::Borrow)<[`Result`]>.
///
/// ### Examples
/// ```
/// use fluent_assertions::*;
///
/// // test subject to be Ok(_)
/// Result::Ok::<usize, usize>(1).should().be_ok();
///
/// // use with_value_that() clause to test inner value
/// Result::Ok::<usize, usize>(1).should().be_ok().with_value_that(|v| v < &3);
///
/// // or use with_value() clause if T implementing PartialEq
/// Result::Ok::<bool, ()>(true).should().be_ok().with_value(true);
///
/// // can also use which_value() clause to start a new test statement for inner value
/// Result::Ok::<String, ()>(String::from("Hello"))
///     .should()
///     .be_ok()
///     .which_value()
///     .should()
///     .start_with("H");
///
/// // if subject is reference, which_value() returns reference as well
/// Result::Ok::<u32, ()>(14).to_ref().should().be_ok().which_value().should().be(&14);
///
/// // same syntax for Err as well
/// Result::Err::<usize, usize>(1).should().be_err();
/// Result::Err::<usize, usize>(3).should().be_err().with_value_that(|v| v > &2);
/// Result::Err::<(), bool>(true).should().be_err().with_value(true);
/// Result::Err::<(), String>(String::from("Hello"))
///     .should()
///     .be_err()
///     .which_value()
///     .should()
///     .start_with("H");
/// Result::Err::<(), u32>(14).to_ref().should().be_err().which_value().should().be(&14);
/// ```
pub trait ResultAssertions<T, E, I: Borrow<Result<T, E>>> {
    fn be_ok(self) -> ResultConstraint<T, E, I, IsOk>;
    fn be_err(self) -> ResultConstraint<T, E, I, IsErr>;
}

impl<T, E, R: Borrow<Result<T, E>>> ResultAssertions<T, E, R> for Assertions<R> {
    fn be_ok(self) -> ResultConstraint<T, E, R, IsOk> {
        match self.subject().borrow() {
            Ok(_) => ResultConstraint::new(self.into_inner(), IsOk),
            Err(_) => {
                AssertionFailure::new("should().be_ok() expecting subject to be Result::Ok but failed")
                    .subject(self.subject())
                    .fail();

                unreachable!();
            },
        }
    }

    fn be_err(self) -> ResultConstraint<T, E, R, IsErr> {
        match self.subject().borrow() {
            Err(_) => ResultConstraint::new(self.into_inner(), IsErr),
            Ok(_) => {
                AssertionFailure::new("should().be_err() expecting subject to be Result::Err but failed")
                    .subject(self.subject())
                    .fail();

                unreachable!();
            },
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::*;

    #[test]
    fn should_not_panic_if_result_is_expected_to_be_ok_and_is() {
        let result: Result<&str, &str> = Ok("Hello");

        result.should().be_ok();
    }

    #[test]
    fn should_panic_if_result_is_expected_to_be_ok_and_is_not() {
        let result: Result<&str, &str> = Err("Oh no");

        should_fail_with_message!(result.should().be_ok(), "expecting subject to be Result::Ok but failed");
    }

    #[test]
    fn should_return_unwrapped_value_if_subject_is_ok() {
        let result: Result<&str, &str> = Ok("Hello");

        result.should().be_ok().with_value(&"Hello");
    }

    #[test]
    fn should_panic_if_be_ok_with_value_not_match_value() {
        let result: Result<&str, &str> = Ok("Hello");

        should_fail_with_message!(
            result.should().be_ok().with_value(&"world"),
            "expecting subject to be\n Ok(\"world\")\nbut failed"
        );
    }

    #[test]
    fn should_return_unwrapped_value_if_subject_is_ok_with_which_value() {
        let result: Result<&str, &str> = Ok("Hello");

        result.should().be_ok().which_value().should().be("Hello");
        (&result).should().be_ok().which_value().should().be(&"Hello");
    }

    #[test]
    fn subject_is_ok_with_value_that_should_succeed_if_condition_matches() {
        let result: Result<&str, &str> = Ok("Hello");

        result.should().be_ok().with_value_that(|v| v == &"Hello");
    }

    #[test]
    fn subject_is_ok_with_value_that_should_panic_if_condition_not_matches() {
        let result: Result<&str, &str> = Ok("Hello");

        should_fail_with_message!(
            result.should().be_ok().with_value_that(|v| v == &"world"),
            "expecting subject to be Result::Ok with inner value matching condition but failed"
        );
    }

    #[test]
    fn should_not_panic_if_result_is_expected_to_be_error_and_is() {
        let result: Result<&str, &str> = Err("Oh no");
        result.should().be_err();
    }

    #[test]
    fn should_panic_if_result_is_expected_to_be_error_and_is_not() {
        let result: Result<&str, &str> = Ok("Hello");

        should_fail_with_message!(result.should().be_err(), "expecting subject to be Result::Err but failed");
    }

    #[test]
    fn should_return_unwrapped_value_if_subject_is_err() {
        let result: Result<&str, &str> = Err("Hello");
        result.should().be_err().with_value(&"Hello");
    }

    #[test]
    fn be_err_with_value_should_panic_if_err_value_not_match() {
        let result: Result<&str, &str> = Err("Hello");
        should_fail_with_message!(
            result.should().be_err().with_value(&"world"),
            "expecting subject to be Result::Err(\"world\") but failed"
        );
    }

    #[test]
    fn be_err_which_value_should_return_err_value() {
        let result: Result<&str, &str> = Err("Hello");
        result.should().be_err().which_value().should().be("Hello");
        (&result).should().be_err().which_value().should().be(&"Hello");
    }

    #[test]
    fn be_err_with_value_that_should_not_panic_if_value_matches_condition() {
        let result: Result<&str, &str> = Err("Hello");
        result.should().be_err().with_value_that(|v| v == &"Hello");
    }

    #[test]
    fn be_err_with_value_that_should_panic_if_value_not_matches_condition() {
        let result: Result<&str, &str> = Err("Hello");
        should_fail_with_message!(
            result.should().be_err().with_value_that(|v| v == &"world"),
            "expecting subject to be Result::Err with inner value match condition but failed"
        );
    }
}
