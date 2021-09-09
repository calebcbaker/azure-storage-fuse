use crate::{
    utils::{AndConstraint, AssertionFailure},
    Assertions,
};
use std::borrow::Borrow;

/// Assertions for types implementing [`Borrow`](std::borrow::Borrow)<[`bool`]>.
///
/// ### Examples
/// ```
/// use fluent_assertions::*;
///
/// // test subject to be true
/// true.should().be_true();
///
/// // or false
/// false.should().be_false();
/// ```
pub trait BooleanAssertions<T> {
    fn be_true(self) -> AndConstraint<T>;
    fn be_false(self) -> AndConstraint<T>;
}

impl<T: Borrow<bool>> BooleanAssertions<T> for Assertions<T> {
    fn be_true(self) -> AndConstraint<T> {
        let subject = self.into_inner();
        if !subject.borrow() {
            AssertionFailure::new("should().be_true() expectation failed")
                .expected(true)
                .but_was(false)
                .fail();
        }

        AndConstraint::new(subject)
    }

    fn be_false(self) -> AndConstraint<T> {
        let subject = self.into_inner();
        if *subject.borrow() {
            AssertionFailure::new("should().be_false() expectation failed")
                .expected(false)
                .but_was(true)
                .fail();
        }

        AndConstraint::new(subject)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    pub fn should_not_panic_if_value_is_expected_to_be_true_and_is() {
        true.should().be_true();
    }

    #[test]
    pub fn should_be_true_works_for_borrowed_forms() {
        (&true).should().be_true();
        (&mut true).should().be_true();
    }

    #[test]
    pub fn should_panic_if_value_is_expected_to_be_true_and_is_not() {
        should_fail_with_message!(false.should().be_true(), "expected: true*but was: false");
    }

    #[test]
    pub fn should_not_panic_if_value_is_expected_to_be_false_and_is() {
        false.should().be_false();
    }

    #[test]
    pub fn should_panic_if_value_is_expected_to_be_false_and_is_not() {
        should_fail_with_message!(true.should().be_false(), "expected: false*but was: true");
    }
}
