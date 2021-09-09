use crate::{
    utils::{AndConstraint, AssertionFailure, DebugMessage},
    Assertions,
};
use std::{borrow::Borrow, cmp::PartialEq, marker::PhantomData};

pub struct SomeConstraint<T, I: Borrow<Option<T>>> {
    subject: I,
    phantom: PhantomData<T>,
}

impl<T, I: Borrow<Option<T>>> SomeConstraint<T, I> {
    pub fn and(self) -> I {
        self.subject
    }

    pub fn with_value_that(self, pred: impl FnOnce(&T) -> bool) -> AndConstraint<I> {
        let value = self.subject.borrow().as_ref().unwrap();
        if !pred(value) {
            AssertionFailure::new(format!(
                "should().be_some().with_value_that() expecting subject <{}> to contain value matching condition but failed",
                self.subject.debug_message()
            ))
            .fail();
        }
        AndConstraint::new(self.subject)
    }
}

impl<T: PartialEq, I: Borrow<Option<T>>> SomeConstraint<T, I> {
    pub fn with_value<E: Borrow<T>>(self, value: E) -> AndConstraint<I> {
        let subject_value = self.subject.borrow().as_ref().unwrap();
        if !subject_value.eq(value.borrow()) {
            AssertionFailure::new(format!(
                "should().be_some().with_value() expecting subject <{}> to contain value <{}> but failed",
                self.subject.debug_message(),
                value.debug_message()
            ))
            .fail();
        }

        AndConstraint::new(self.subject)
    }
}

impl<T> SomeConstraint<T, Option<T>> {
    pub fn which_value(self) -> T {
        self.subject.unwrap()
    }
}

impl<'o, T> SomeConstraint<T, &'o Option<T>> {
    pub fn which_value(self) -> &'o T {
        self.subject.as_ref().unwrap()
    }
}

/// Assertions for types implementing [`Borrow`](std::borrow::Borrow)<[`Option`]>.
///
/// ### Examples
/// ```
/// use fluent_assertions::*;
///
/// // test option to be Some(_)
/// Some(1).should().be_some();
///
/// // use clause with_value_test() to test inner value
/// Some("Hello World").should().be_some().with_value_that(|v| v.starts_with("H"));
///
/// // or use with_value() clause, if inner value implementing PartialEq
/// Some(42).should().be_some().with_value(42);
///
/// // can also use which_value() clause to start another test statement
/// Some(String::from("World"))
///     .should()
///     .be_some()
///     .which_value()
///     .should()
///     .start_with("W");
///
/// // if subject is a reference, which_value() is also a reference
/// (&Some(3)).should().be_some().which_value().should().be(&3);
///
/// // test option to be None
/// None::<u32>.should().be_none();
/// ```
pub trait OptionAssertions<T, I: Borrow<Option<T>>> {
    fn be_some(self) -> SomeConstraint<T, I>;
    fn be_none(self) -> AndConstraint<I>;
}

impl<T, I: Borrow<Option<T>>> OptionAssertions<T, I> for Assertions<I> {
    fn be_some(self) -> SomeConstraint<T, I> {
        match self.subject().borrow() {
            Some(_) => SomeConstraint {
                subject: self.into_inner(),
                phantom: PhantomData,
            },
            None => {
                AssertionFailure::new("should().be_some() expecting subject <Option::None> to be Option::Some but failed").fail();

                unreachable!();
            },
        }
    }

    fn be_none(self) -> AndConstraint<I> {
        if let Some(val) = self.subject().borrow() {
            AssertionFailure::new(format!(
                "should().be_none() expecting subject <{}> to be Option::None but failed",
                Some(val).debug_message()
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }
}

#[cfg(test)]
mod tests {

    use crate::*;

    #[test]
    fn should_not_panic_if_option_is_expected_to_contain_value_and_does() {
        let option = Some("Hello");
        option.should().be_some();
    }

    #[test]
    fn should_panic_if_option_is_expected_to_contain_value_and_does_not() {
        let option: Option<&str> = None;

        should_fail_with_message!(
            option.should().be_some(),
            "expecting subject <Option::None> to be Option::Some but failed"
        );
    }

    #[test]
    fn should_be_able_to_unwrap_option_if_some() {
        let option = Some("Hello");
        option
            .should()
            .be_some()
            .with_value(&"Hello")
            .and()
            .should()
            .be_some()
            .with_value_that(|v| v == &"Hello")
            .and()
            .should()
            .be_some()
            .which_value()
            .should()
            .be(&"Hello");
    }

    #[test]
    fn should_be_some_works_for_ref_option() {
        let option = Some("Hello");
        (&option).should().be_some().which_value().should().be(&"Hello");
    }

    #[test]
    fn should_panic_if_be_some_with_value_expectation_fail() {
        should_fail_with_message!(
            Some("Hello").should().be_some().with_value(&"world"),
            "expecting subject <Some(\"Hello\")> to contain value <\"world\"> but failed"
        );

        should_fail_with_message!(
            Some("Hello").should().be_some().which_value().should().be(&"world"),
            "expected: \"world\"*but was: \"Hello\""
        );

        should_fail_with_message!(
            Some("Hello").should().be_some().with_value_that(|v| v == &"world"),
            "expecting subject <Some(\"Hello\")> to contain value matching condition but failed"
        );
    }

    #[test]
    fn should_not_panic_if_option_is_empty() {
        let option: Option<&str> = None;
        option.should().be_none();
    }

    #[test]
    fn should_panic_if_option_is_not_empty_but_was_expected_as_empty() {
        let option = Some("Hello");

        should_fail_with_message!(
            option.should().be_none(),
            "expecting subject <Some(\"Hello\")> to be Option::None but failed"
        );
    }
}
