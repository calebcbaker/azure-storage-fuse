use crate::utils::{AndConstraint, AssertionFailure, DebugMessage};
use std::{borrow::Borrow, cmp::PartialEq};

/// Trait to start assertion. It has a blanket implementation on any type `T`.
/// Calling [`Should::should`] will return [`Assertions`]<`T`> to enable testing.
pub trait Should<T> {
    fn should(self) -> Assertions<T>;
}

impl<T> Should<T> for T {
    fn should(self) -> Assertions<T> {
        Assertions { subject: self }
    }
}

/// A convenient trait to convert from owned to ref in fluent syntax.
/// [`Should::should()`] will move the object, while in some case the subject cannot be moved. Call [`ToRef::to_ref()`] to assert on reference.
/// This is not different from directly retrieve reference by `&`. But it's useful in a fluent syntax like below:
/// ```
/// use fluent_assertions::*;
///
/// // in this case you want to test second value of inner tuple. You cannot directly
/// // call which_value().1.should() because it will move the value, call to_ref() to
/// // retrieve a reference to test, without break the fluent syntax
/// Ok::<_, ()>((3, 4, 5)).should().be_ok().which_value().1.to_ref().should().be(&4);
///
/// // compare with
/// (&Ok::<_, ()>((3, 4, 5)).should().be_ok().which_value().1).should().be(&4);
/// ```
pub trait ToRef<T: ?Sized> {
    fn to_ref(&self) -> &T;
}

impl<T: ?Sized> ToRef<T> for T {
    fn to_ref(&self) -> &T {
        self
    }
}

/// A symbolic class to representing *assertable* subject. Created by
/// calling [`Should`] on subject to be tested. [`Assertions`] contains
/// some basic assertions functions, but most assertions need additional trait
/// enabled on conditioned types. E.g., use [`ResultAssertions`](crate::ResultAssertions) to apply assertions
/// for all types implementing [`Borrow`](std::borrow::Borrow)<[`Result`]>.
///
/// ```Assertions``` class also enables extension point. One could create a customized
/// assertion function by defining a customer trait, and implementing the customer trait
/// on [`Assertions`] with `T` being target types.
///
/// ### Examples
/// ```
/// use fluent_assertions::*;
///
/// // test subject meeting condition specified by a predicate
/// "hello".should().satisfy(|x| x == &"hello");
///
/// // sometimes subject has multiple sub-fields to be tested, in this case,
/// // use pass() assertion, which takes an closure on subject
/// let subject = (3, 4, 5);
/// subject.should().pass(|s| {
///     s.0.to_ref().should().be(&3);
///     s.1.to_ref().should().be(&4);
///     s.2.to_ref().should().be(&5);
/// });
///
/// // the above case is not different from the following form
/// let subject = (3, 4, 5);
/// subject.0.to_ref().should().be(&3);
/// subject.1.to_ref().should().be(&4);
/// subject.2.to_ref().should().be(&5);
///
/// // but it will be useful if the subject was a result of previous test, like in the following case
/// Ok::<_, ()>((3, 4, 5)).should().be_ok().which_value().should().pass(|s| {
///     s.0.to_ref().should().be(&3);
///     s.1.to_ref().should().be(&4);
///     s.2.to_ref().should().be(&5);
/// });
///
/// // test subject value directly if subject implements PartialEq
/// 5.should().be(5);
///
/// // or not equal
/// 4.should().not_be(5);
///
/// // if subject implements PartialEq<F> where F is a different type,
/// // could also use the following form to test equality
/// String::from("abc").should().equal("abc");
/// ```
pub struct Assertions<T> {
    subject: T,
}

impl<T> Assertions<T> {
    /// return the reference to subject
    pub fn subject(&self) -> &T {
        &self.subject
    }

    pub fn into_inner(self) -> T {
        self.subject
    }

    pub fn satisfy<F>(self, matching_function: F) -> AndConstraint<T>
    where F: Fn(&T) -> bool {
        let subject = self.subject();

        if !matching_function(subject) {
            AssertionFailure::new(format!(
                "should().satisfy() expectation failed for value <{}>",
                subject.debug_message()
            ))
            .fail();
        }

        AndConstraint::new(self.subject)
    }

    pub fn pass<F>(self, checking_function: F) -> AndConstraint<T>
    where F: Fn(&T) {
        let subject = self.subject();

        checking_function(subject);

        AndConstraint::new(self.subject)
    }

    pub fn equal<E>(self, expected: &E) -> AndConstraint<T>
    where
        T: PartialEq<E>,
        E: ?Sized,
    {
        if !self.subject().borrow().eq(expected) {
            AssertionFailure::new("should().equal() expectation failed")
                .expected(expected)
                .but_was(self.subject())
                .fail();
        }

        AndConstraint::new(self.into_inner())
    }
}

impl<T> Assertions<T>
where T: PartialEq
{
    pub fn be<E: Borrow<T>>(self, expected: E) -> AndConstraint<T> {
        let borrowed_expected = expected.borrow();

        if !self.subject().eq(borrowed_expected) {
            AssertionFailure::new("should().be() expectation failed")
                .expected(borrowed_expected)
                .but_was(self.subject())
                .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    pub fn not_be<E: Borrow<T>>(self, expected: E) -> AndConstraint<T> {
        let borrowed_expected = expected.borrow();

        if self.subject().eq(borrowed_expected) {
            AssertionFailure::new(format!(
                "should().not_be() expecting value <{}> not be <{}> but failed",
                self.subject.debug_message(),
                borrowed_expected.debug_message()
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
    fn should_satisfy_succeed() {
        3.should().satisfy(|i| *i + 1 == 4);
    }

    #[test]
    fn should_satisfy_failed() {
        should_fail_with_message!(3.should().satisfy(|i| *i + 1 == 3), "expectation failed for value <3>");
    }

    #[test]
    fn should_pass_succeed() {
        (2, 3).should().pass(|i| {
            i.0.should().be(2);
            i.1.should().be(3);
        });
    }

    #[test]
    fn should_pass_fail() {
        should_fail_with_message!(
            (2, 3).should().pass(|i| {
                i.0.should().be(3);
            }),
            "expected: 3 but was: 2"
        );
    }

    #[test]
    fn should_be_succeed() {
        3.should().be(3);
    }

    #[test]
    fn should_be_fail() {
        should_fail_with_message!(3.should().be(4), "expected: 4*but was: 3");
    }

    #[test]
    fn should_not_be_succeed() {
        3.should().not_be(4);
    }

    #[test]
    fn should_not_be_fail() {
        should_fail_with_message!(3.should().not_be(3), "expecting value <3> not be <3> but failed");
    }

    #[test]
    fn and_contraint_succeed() {
        3.should().not_be(4).and().should().satisfy(|x| x + 1 == 4);
    }

    #[test]
    fn should_be_support_multiple_borrow_forms() {
        1.should().be(1);
        1.should().be(&mut 1);
        1.should().be(&1);
    }

    #[test]
    fn should_not_be_support_multiple_borrow_forms() {
        1.should().not_be(2);
        1.should().not_be(&mut 2);
        1.should().not_be(&2);
    }
}
