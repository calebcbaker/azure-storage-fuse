use crate::{
    utils::{AndConstraint, AssertionFailure, DebugMessage},
    Assertions,
};
use num::Float;
use std::{borrow::Borrow, cmp::PartialOrd};

/// Assertions for types implementing [`PartialOrd`](std::cmp::PartialOrd).
///
/// ### Examples
/// ```
/// use fluent_assertions::*;
///
/// // test subject < target value
/// 1.should().be_less_than(2);
///
/// // test subject <= target value
/// "One".should().be_less_than_or_equal_to("Two");
///
/// // test subject > target value
/// 5f32.should().be_greater_than(3f32);
///
/// // test subject >= target value
/// 33u8.should().be_greater_than_or_equal_to(33u8);
/// ```
pub trait OrderedAssertions<T>
where T: PartialOrd
{
    fn be_less_than<E: Borrow<T>>(self, other: E) -> AndConstraint<T>;
    fn be_less_than_or_equal_to<E: Borrow<T>>(self, other: E) -> AndConstraint<T>;
    fn be_greater_than<E: Borrow<T>>(self, other: E) -> AndConstraint<T>;
    fn be_greater_than_or_equal_to<E: Borrow<T>>(self, other: E) -> AndConstraint<T>;
}

impl<T> OrderedAssertions<T> for Assertions<T>
where T: PartialOrd
{
    fn be_less_than<E: Borrow<T>>(self, other: E) -> AndConstraint<T> {
        let subject = self.subject();
        let borrowed_other = other.borrow();

        if subject >= borrowed_other {
            AssertionFailure::new(format!(
                "should().be_less_than() expecting subject <{}> to be less than <{}> but failed",
                subject.debug_message(),
                borrowed_other.debug_message()
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn be_less_than_or_equal_to<E: Borrow<T>>(self, other: E) -> AndConstraint<T> {
        let subject = self.subject();
        let borrowed_other = other.borrow();

        if subject > borrowed_other {
            AssertionFailure::new(format!(
                "should().be_less_than_or_equal_to() expecting subject <{}> to be less than or equal to <{}> but failed",
                subject.debug_message(),
                borrowed_other.debug_message()
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn be_greater_than<E: Borrow<T>>(self, other: E) -> AndConstraint<T> {
        let subject = self.subject();
        let borrowed_other = other.borrow();

        if subject <= borrowed_other {
            AssertionFailure::new(format!(
                "should().be_greater_than() expecting subject <{}> to be greater than <{}> but failed",
                subject.debug_message(),
                borrowed_other.debug_message()
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn be_greater_than_or_equal_to<E: Borrow<T>>(self, other: E) -> AndConstraint<T> {
        let subject = self.subject();
        let borrowed_other = other.borrow();

        if subject < borrowed_other {
            AssertionFailure::new(format!(
                "should().be_greater_than_or_equal_to() expecting subject <{}> to be greater than or equal to <{}> but failed",
                subject.debug_message(),
                borrowed_other.debug_message()
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }
}

/// Assertions for types implementing [`Float`](num::Float) trait.
///
/// ### Examples
/// ```
/// use fluent_assertions::*;
///
/// 2.0f64.should().be_close_to(2.0f64, 0.01f64);
/// ```
pub trait FloatAssertions<T: Float> {
    fn be_close_to<E: Borrow<T>, O: Borrow<T>>(self, expected: E, tolerance: O) -> AndConstraint<T>;
}

impl<'s, T: Float> FloatAssertions<T> for Assertions<T> {
    fn be_close_to<E: Borrow<T>, O: Borrow<T>>(self, expected: E, tolerance: O) -> AndConstraint<T> {
        let subject = self.subject();
        let borrowed_expected = expected.borrow();
        let borrowed_tolerance = tolerance.borrow();

        let difference = (*subject - *borrowed_expected).abs();

        if !subject.is_finite() || difference > borrowed_tolerance.abs() {
            AssertionFailure::new(format!(
                "should().be_close_to() expecting subject <{}> close to <{}> (tolerance of <{}>) but failed",
                subject.debug_message(),
                borrowed_expected.debug_message(),
                borrowed_tolerance.debug_message()
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
    fn is_less_than_should_allow_multiple_borrow_forms() {
        1.should()
            .be_less_than(2)
            .and()
            .should()
            .be_less_than(&mut 2)
            .and()
            .should()
            .be_less_than(&2);
    }

    #[test]
    fn should_panic_if_value_is_greater_than_expected() {
        should_fail_with_message!(3.should().be_less_than(2), "expecting subject <3> to be less than <2> but failed");
    }

    #[test]
    fn is_less_than_or_equal_to_should_allow_multiple_borrow_forms() {
        2.should()
            .be_less_than_or_equal_to(2)
            .and()
            .should()
            .be_less_than_or_equal_to(&mut 2)
            .and()
            .should()
            .be_less_than_or_equal_to(&2);
    }

    #[test]
    fn should_panic_if_value_is_greater_than_or_not_equal_to_expected() {
        should_fail_with_message!(
            3.should().be_less_than_or_equal_to(2),
            "expecting subject <3> to be less than or equal to <2> but failed"
        );
    }

    #[test]
    fn is_greater_than_should_allow_multiple_borrow_forms() {
        3.should()
            .be_greater_than(2)
            .and()
            .should()
            .be_greater_than(&mut 2)
            .and()
            .should()
            .be_greater_than(&2);
    }

    #[test]
    fn should_panic_if_value_is_less_than_expected() {
        should_fail_with_message!(
            2.should().be_greater_than(3),
            "expecting subject <2> to be greater than <3> but failed"
        );
    }

    #[test]
    fn is_greater_than_or_equal_to_should_allow_multiple_borrow_forms() {
        3.should()
            .be_greater_than_or_equal_to(3)
            .and()
            .should()
            .be_greater_than_or_equal_to(&mut 3)
            .and()
            .should()
            .be_greater_than_or_equal_to(&3);
    }

    #[test]
    fn should_panic_if_value_is_less_than_or_not_equal_to_expected() {
        should_fail_with_message!(
            2.should().be_greater_than_or_equal_to(3),
            "expecting subject <2> to be greater than or equal to <3> but failed"
        );
    }

    #[test]
    fn is_close_to_should_allow_multiple_borrow_forms() {
        2.0f64
            .should()
            .be_close_to(2.0f64, 0.01f64)
            .and()
            .should()
            .be_close_to(&mut 2.0f64, 0.01f64)
            .and()
            .should()
            .be_close_to(&2.0f64, 0.01f64);
    }

    #[test]
    fn should_panic_if_float_is_not_close_to() {
        should_fail_with_message!(
            2.0f64.should().be_close_to(1.0f64, 0.01f64),
            "expecting subject <2.0> close to <1.0> (tolerance of <0.01>) but failed"
        );
    }

    #[test]
    fn should_panic_if_float_is_nan() {
        should_fail_with_message!(
            f64::NAN.should().be_close_to(1.0f64, 0.01f64),
            "expecting subject <NaN> close to <1.0> (tolerance of <0.01>) but failed"
        );
    }

    #[test]
    fn should_panic_if_float_is_infinity() {
        should_fail_with_message!(
            f64::INFINITY.should().be_close_to(1.0f64, 0.01f64),
            "expecting subject <inf> close to <1.0> (tolerance of <0.01>) but failed"
        );
    }

    #[test]
    fn should_panic_if_float_is_negative_infinity() {
        should_fail_with_message!(
            f64::NEG_INFINITY.should().be_close_to(1.0f64, 0.01f64),
            "expecting subject <-inf> close to <1.0> (tolerance of <0.01>) but failed"
        );
    }
}
