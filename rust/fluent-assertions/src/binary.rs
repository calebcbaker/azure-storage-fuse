use crate::{
    utils::{AndConstraint, AssertionFailure},
    Assertions,
};

/// Assertions for types implementing [`AsRef`](std::convert::AsRef)<[\[u8\]](std::u8)>.
///
/// ### Examples
/// ```
/// use fluent_assertions::*;
///
/// // test subject equals binary array
/// vec![b'a', b'b', b'c'].should().equal_binary(b"abc");
///
/// // and not equal
/// "Hello World".as_bytes().should().not_equal_binary(b"hello world");
/// ```
pub trait BinaryAssertions<T> {
    fn equal_binary<E: AsRef<[u8]>>(self, expected: E) -> AndConstraint<T>;

    fn not_equal_binary<E: AsRef<[u8]>>(self, expected: E) -> AndConstraint<T>;
}

impl<T: AsRef<[u8]>> BinaryAssertions<T> for Assertions<T> {
    fn equal_binary<E: AsRef<[u8]>>(self, expected: E) -> AndConstraint<T> {
        if self.subject().as_ref() != expected.as_ref() {
            AssertionFailure::new("should().equal_binary() expectation failed")
                .expected(expected)
                .but_was(self.subject())
                .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn not_equal_binary<E: AsRef<[u8]>>(self, expected: E) -> AndConstraint<T> {
        if self.subject().as_ref() == expected.as_ref() {
            AssertionFailure::new("should().not_equal_binary() expecting subject to match expected but failed")
                .subject(self.subject())
                .fail();
        }

        AndConstraint::new(self.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn binary_vec_could_match_binary() {
        vec![b'a', b'b', b'c'].should().equal_binary(b"abc");
    }

    #[test]
    fn binary_vec_panic_if_not_match_expected() {
        should_fail_with_message!(
            vec![b'a', b'b', b'c'].should().equal_binary(b"abcd"),
            "expected: [97, 98, 99, 100] but was: [97, 98, 99]"
        );
    }

    #[test]
    fn binary_vec_not_equal_binary_should_not_panic_if_not_match() {
        vec![b'a', b'b', b'c'].should().not_equal_binary(b"abcd");
    }

    #[test]
    fn binary_vec_not_equal_binary_should_panic_if_match() {
        should_fail_with_message!(
            vec![b'a', b'b', b'c'].should().not_equal_binary(b"abc"),
            "expecting subject to match expected but failed"
        );
    }
}
