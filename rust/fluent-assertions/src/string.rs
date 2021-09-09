use crate::{
    utils::{AndConstraint, AssertionFailure},
    Assertions,
};
use regex::Regex;

/// Assertions for types implementing [`ToString`] trait.
///
/// ### Examples
/// ```
/// use fluent_assertions::*;
///
/// // test subject.to_string() equal the target string
/// "Hello".should().equal_string("Hello");
///
/// // test subject starting with specified string
/// "Hello".should().start_with("H");
///
/// // test subject ending with specified string
/// "Hello".should().end_with("o");
///
/// // test subject containing specified string
/// "Hello".should().contain("ell");
///
/// // test subject to be an empty string
/// "".should().be_empty_string();
///
/// // test subject matches a regex pattern
/// "abcd".should().match_regex("^a.*d$");
/// ```
pub trait StringAssertions<S> {
    fn equal_string<E: AsRef<str>>(self, expected: E) -> AndConstraint<S>;
    fn start_with<E: AsRef<str>>(self, expected: E) -> AndConstraint<S>;
    fn end_with<E: AsRef<str>>(self, expected: E) -> AndConstraint<S>;
    fn contain<E: AsRef<str>>(self, expected: E) -> AndConstraint<S>;
    fn be_empty_string(self) -> AndConstraint<S>;
    fn match_regex<E: AsRef<str>>(self, expected: E) -> AndConstraint<S>;
}

impl<S: ToString> StringAssertions<S> for Assertions<S> {
    fn equal_string<E: AsRef<str>>(self, expected: E) -> AndConstraint<S> {
        let actual = self.subject().to_string();
        let expected = expected.as_ref();

        if !actual.eq(expected) {
            AssertionFailure::new("should().equal_string() expectation failed")
                .expected(expected)
                .but_was(actual)
                .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn start_with<E: AsRef<str>>(self, expected: E) -> AndConstraint<S> {
        let subject = self.subject().to_string();

        let expected = expected.as_ref();

        if !subject.starts_with(expected) {
            AssertionFailure::new(format!(
                "should().start_with() expecting subject <{}> to start with <{}> but failed",
                &subject, expected
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn end_with<E: AsRef<str>>(self, expected: E) -> AndConstraint<S> {
        let subject = self.subject().to_string();

        let expected = expected.as_ref();

        if !subject.ends_with(expected) {
            AssertionFailure::new(format!(
                "should().start_with() expecting subject <{}> to end with <{}> but failed",
                &subject, expected
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn contain<E: AsRef<str>>(self, expected: E) -> AndConstraint<S> {
        let subject = self.subject().to_string();

        let expected = expected.as_ref();

        if !subject.contains(expected) {
            AssertionFailure::new(format!(
                "should().start_with() expecting subject <{}> to contain <{}> but failed",
                &subject, expected
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn be_empty_string(self) -> AndConstraint<S> {
        let subject = self.subject().to_string();

        if !subject.is_empty() {
            AssertionFailure::new(format!(
                "should().be_empty() expecting subject <{}> to be an empty string but failed",
                subject
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn match_regex<E: AsRef<str>>(self, expected: E) -> AndConstraint<S> {
        let regex = Regex::new(expected.as_ref()).expect("failed to create regex from pattern");
        let subject = self.subject().to_string();

        if !regex.is_match(&subject) {
            AssertionFailure::new(format!(
                "should().match_regex() expecting subject <{}> to match regex <{}> but failed",
                regex, subject
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
    fn str_should_equal_str_with_different_forms() {
        "abc".should().equal_string("abc");
        "abc".should().equal_string(&"abc");
        "abc".should().equal_string("abc".to_owned());
    }

    #[test]
    fn other_string_types_should_equal_str() {
        String::from("abc").should().equal_string("abc");
        (&String::from("abc")).should().equal_string("abc");
    }

    #[test]
    fn str_should_match_regex() {
        "abcde".should().match_regex(".*cd.*");
    }

    #[test]
    fn should_not_panic_if_str_starts_with_value() {
        let value = "Hello";
        value.should().start_with("H").and().should().start_with("H".to_owned());
    }

    #[test]
    fn should_panic_if_str_does_not_start_with_value() {
        let value = "Hello";
        should_fail_with_message!(
            value.should().start_with("A"),
            "expecting subject <Hello> to start with <A> but failed"
        );
    }

    #[test]
    fn should_not_panic_if_str_ends_with_value() {
        let value = "Hello";
        value.should().end_with("o").and().should().end_with("o".to_owned());
    }

    #[test]
    fn should_panic_if_str_does_not_end_with_value() {
        let value = "Hello";

        should_fail_with_message!(value.should().end_with("A"), "expecting subject <Hello> to end with <A> but failed");
    }

    #[test]
    fn should_not_panic_if_str_contains_value() {
        let value = "Hello";
        value.should().contain("l").and().should().contain("l".to_owned());
    }

    #[test]
    fn should_panic_if_str_does_not_contain_value() {
        let value = "Hello";
        should_fail_with_message!(value.should().contain("A"), "expecting subject <Hello> to contain <A> but failed");
    }

    #[test]
    fn should_not_panic_if_str_is_empty() {
        let value = "";
        value.should().be_empty_string();
    }

    #[test]
    fn should_panic_if_str_is_not_empty() {
        let value = "Hello";
        should_fail_with_message!(
            value.should().be_empty_string(),
            "expecting subject <Hello> to be an empty string but failed"
        );
    }

    #[test]
    fn should_allow_multiple_borrow_forms_for_string() {
        let value = "Hello".to_owned();
        (&value)
            .should()
            .start_with("H")
            .and()
            .should()
            .start_with(&"H")
            .and()
            .should()
            .start_with(&mut "H")
            .and()
            .should()
            .start_with("H".to_owned());

        (&value)
            .should()
            .end_with("o")
            .and()
            .should()
            .end_with(&"o")
            .and()
            .should()
            .end_with(&mut "o")
            .and()
            .should()
            .end_with("o".to_owned());

        (&value)
            .should()
            .contain("l")
            .and()
            .should()
            .contain(&"l")
            .and()
            .should()
            .contain(&mut "l")
            .and()
            .should()
            .contain("l".to_owned());
    }
}
