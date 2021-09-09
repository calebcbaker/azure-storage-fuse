use crate::{
    utils::{AndConstraint, AssertionFailure, DebugMessage},
    Assertions,
};
use std::{borrow::Borrow, collections::HashMap, hash::Hash, marker::PhantomData};

pub struct ContainKeyConstraint<T, K, V, E> {
    subject: T,
    expected_key: E,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
}

impl<T, K, V, E> ContainKeyConstraint<T, K, V, E>
where
    T: Borrow<HashMap<K, V>>,
    K: Hash + Eq,
    V: PartialEq,
    E: Borrow<K>,
{
    fn new(subject: T, expected_key: E) -> Self {
        ContainKeyConstraint {
            subject,
            expected_key,
            phantom_key: PhantomData,
            phantom_value: PhantomData,
        }
    }

    pub fn and(self) -> T {
        self.subject
    }

    pub fn with_value_that(self, pred: impl FnOnce(&V) -> bool) -> AndConstraint<T> {
        let value = self.subject.borrow().get(self.expected_key.borrow()).unwrap();

        if !pred(value) {
            AssertionFailure::new(format!(
                "should().contain_key().with_which_value() expectation failed with value <{}>",
                value.debug_message()
            ))
            .fail();
        }

        AndConstraint::new(self.subject)
    }
}

impl<'t, K, V, E> ContainKeyConstraint<&'t HashMap<K, V>, K, V, E>
where
    K: Hash + Eq,
    V: PartialEq,
    E: Borrow<K>,
{
    pub fn which_value(self) -> &'t V {
        self.subject.get(self.expected_key.borrow()).unwrap()
    }
}

impl<K, V, E> ContainKeyConstraint<HashMap<K, V>, K, V, E>
where
    K: Hash + Eq,
    V: PartialEq,
    E: Borrow<K>,
{
    pub fn which_value(mut self) -> V {
        self.subject.remove(self.expected_key.borrow()).unwrap()
    }
}

/// Assertions for types implementing [`Borrow`](std::borrow::Borrow)<[`HashMap`](std::collections::HashMap)>.
///
/// ## Examples
/// ```
/// use fluent_assertions::*;
/// use std::collections::HashMap;
///
/// // test hashmap containing a specified key, use which_value() clause to
/// // test the corresponding value
/// let mut test_map = HashMap::new();
/// test_map.insert("hello", "hi");
/// test_map.should().contain_key("hello").which_value().should().be("hi");
///
/// // test hashmap not contains key
/// let mut test_map = HashMap::new();
/// test_map.insert("hello", "hi");
/// test_map.should().not_contain_key("hey");
///
/// // test hashmap containing a specified key-value pair
/// let mut test_map = HashMap::new();
/// test_map.insert("hello", "hi");
/// test_map.should().contain_entry("hello", "hi");
///
/// // not containing key-value
/// let mut test_map = HashMap::new();
/// test_map.insert("hello", "hi");
/// test_map.should().not_contain_entry("hello", "hey");
/// ```
pub trait HashMapAssertions<K, V, T> {
    fn contain_key<E: Borrow<K>>(self, expected_key: E) -> ContainKeyConstraint<T, K, V, E>;
    fn not_contain_key<E: Borrow<K>>(self, expected_key: E) -> AndConstraint<T>;
    fn contain_entry<E: Borrow<K>, F: Borrow<V>>(self, expected_key: E, expected_value: F) -> AndConstraint<T>;
    fn not_contain_entry<E: Borrow<K>, F: Borrow<V>>(self, expected_key: E, expected_value: F) -> AndConstraint<T>;
}

impl<K, V, T> HashMapAssertions<K, V, T> for Assertions<T>
where
    T: Borrow<HashMap<K, V>>,
    K: Hash + Eq,
    V: PartialEq,
{
    fn contain_key<E: Borrow<K>>(self, expected_key: E) -> ContainKeyConstraint<T, K, V, E> {
        let hm = self.into_inner();

        if !hm.borrow().contains_key(expected_key.borrow()) {
            AssertionFailure::new(format!(
                "should().contain_key() expecting HashMap contain key <{}> but failed",
                expected_key.debug_message()
            ))
            .with_message(
                "actual keys",
                hm.borrow().keys().map(|k| k.debug_message()).collect::<Vec<_>>().join(","),
            )
            .subject(hm.borrow())
            .fail();
        }

        ContainKeyConstraint::new(hm, expected_key)
    }

    fn not_contain_key<E: Borrow<K>>(self, expected_key: E) -> AndConstraint<T> {
        let hm = self.into_inner();

        if hm.borrow().contains_key(expected_key.borrow()) {
            AssertionFailure::new(format!(
                "should().contain_key() expecting HashMap not contain key <{}> but failed",
                expected_key.debug_message()
            ))
            .subject(hm.borrow())
            .fail();
        };

        AndConstraint::new(hm)
    }

    fn contain_entry<E: Borrow<K>, F: Borrow<V>>(self, expected_key: E, expected_value: F) -> AndConstraint<T> {
        let hm = self.into_inner();
        let borrowed_expected_key = expected_key.borrow();
        let borrowed_expected_value = expected_value.borrow();

        if let Some(value) = hm.borrow().get(borrowed_expected_key) {
            if !value.eq(borrowed_expected_value) {
                AssertionFailure::new("should().contain_entry() expectation failed")
                    .expecting(format!(
                        "HashMap contains entry {}:{}",
                        borrowed_expected_key.debug_message(),
                        borrowed_expected_value.debug_message()
                    ))
                    .but(format!(
                        "key {} has different value {}",
                        borrowed_expected_key.debug_message(),
                        value.debug_message()
                    ))
                    .subject(hm.borrow())
                    .fail();
            }
        } else {
            AssertionFailure::new("should().contain_entry() expectation failed")
                .expecting(format!(
                    "HashMap to contain entry {}:{}",
                    borrowed_expected_key.debug_message(),
                    borrowed_expected_value.debug_message()
                ))
                .but(format!("key {} not found", borrowed_expected_key.debug_message()))
                .with_message(
                    "actual keys",
                    hm.borrow().keys().map(|k| k.debug_message()).collect::<Vec<_>>().join(","),
                )
                .subject(hm.borrow())
                .fail();
        }

        AndConstraint::new(hm)
    }

    fn not_contain_entry<E: Borrow<K>, F: Borrow<V>>(self, expected_key: E, expected_value: F) -> AndConstraint<T> {
        let hm = self.into_inner();
        let borrowed_expected_key = expected_key.borrow();
        let borrowed_expected_value = expected_value.borrow();

        if let Some(value) = hm.borrow().get(borrowed_expected_key) {
            if value.eq(borrowed_expected_value) {
                AssertionFailure::new("should().not_contain_entry() expectation failed")
                    .expecting(format!(
                        "HashMap does not contain entry {}:{}",
                        borrowed_expected_key.debug_message(),
                        borrowed_expected_value.debug_message()
                    ))
                    .subject(hm.borrow())
                    .fail();
            }
        }

        AndConstraint::new(hm)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use std::collections::HashMap;

    #[test]
    fn should_not_panic_if_hashmap_length_matches_expected() {
        let mut test_map = HashMap::new();
        test_map.insert(1, 1);
        test_map.insert(2, 2);

        test_map.should().have_length(2).and().should().contain_key(2);
    }

    #[test]
    fn have_length_work_for_reference() {
        let mut test_map = HashMap::new();
        test_map.insert(1, 1);
        test_map.insert(2, 2);

        (&test_map).should().have_length(2).and().should().contain_key(2);
        (&mut test_map).should().have_length(2);
    }

    #[test]
    fn should_panic_if_hashmap_length_does_not_match_expected() {
        let mut test_map = HashMap::new();
        test_map.insert(1, 1);
        test_map.insert(2, 2);

        should_fail_with_message!(test_map.should().have_length(1), "expected: 1*but was: 2");
    }

    #[test]
    fn should_not_panic_if_hashmap_was_expected_to_be_empty_and_is() {
        let test_map: HashMap<u8, u8> = HashMap::new();
        test_map.should().be_empty();
    }

    #[test]
    fn should_panic_if_hashmap_was_expected_to_be_empty_and_is_not() {
        let mut test_map = HashMap::new();
        test_map.insert(1, 1);

        should_fail_with_message!(test_map.should().be_empty(), "expecting subject empty but having length 1");
    }

    #[test]
    fn contains_key_should_allow_multiple_borrow_forms() {
        let mut test_map = HashMap::new();
        test_map.insert("hello", "hi");

        test_map
            .should()
            .contain_key("hello")
            .and()
            .should()
            .contain_key(&"hello")
            .and()
            .should()
            .contain_key(&mut "hello");
    }

    #[test]
    fn should_not_panic_if_hashmap_contains_key() {
        let mut test_map = HashMap::new();
        test_map.insert("hello", "hi");

        (&test_map).should().contain_key("hello");
        (&mut test_map).should().contain_key("hello");
        test_map.should().contain_key("hello");
    }

    #[test]
    fn should_not_panic_if_hashmap_does_not_contain_key() {
        let mut test_map = HashMap::new();
        test_map.insert("hi", "hi");
        test_map.insert("hey", "hey");

        should_fail_with_message!(
            test_map.should().contain_key(&"hello"),
            "expecting HashMap contain key <\"hello\"> but failed"
        );
    }

    #[test]
    fn should_be_able_to_chain_value_from_contains_key() {
        let mut test_map = HashMap::new();
        test_map.insert("hello", "hi");

        (&test_map).should().contain_key("hello").which_value().should().be(&"hi");
        test_map.should().contain_key("hello").which_value().should().be("hi");
    }

    #[test]
    fn does_not_contain_key_should_allow_multiple_borrow_forms() {
        let mut test_map = HashMap::new();
        test_map.insert("hello", "hi");

        test_map
            .should()
            .not_contain_key("hey")
            .and()
            .should()
            .not_contain_key(&"hey")
            .and()
            .should()
            .not_contain_key(&mut "hey");
    }

    #[test]
    fn should_panic_if_hashmap_does_contain_key_when_not_expected() {
        let mut test_map = HashMap::new();
        test_map.insert("hello", "hi");

        should_fail_with_message!(
            test_map.should().not_contain_key("hello"),
            "expecting HashMap not contain key <\"hello\"> but failed"
        );
    }

    #[test]
    fn with_which_value_chain_value_from_contains_key() {
        let mut test_map = HashMap::new();
        test_map.insert("hello", "hi");

        (&test_map).should().contain_key("hello").with_value_that(|v| v == &"hi");
        test_map.should().contain_key("hello").with_value_that(|v| v == &"hi");
    }

    #[test]
    fn with_which_value_chain_value_from_contains_key_panic_when_not_expected() {
        let mut test_map = HashMap::new();
        test_map.insert("hello", "hi");

        should_fail_with_message!(
            test_map.should().contain_key("hello").with_value_that(|v| v == &"abc"),
            "expectation failed with value <\"hi\">"
        );
    }

    #[test]
    fn contains_entry_should_allow_multiple_borrow_forms() {
        let mut test_map = HashMap::new();
        test_map.insert("hello", "hi");

        test_map
            .should()
            .contain_entry("hello", "hi")
            .and()
            .should()
            .contain_entry(&mut "hello", &mut "hi")
            .and()
            .should()
            .contain_entry("hello", &mut "hi")
            .and()
            .should()
            .contain_entry(&"hello", &"hi");
    }

    #[test]
    fn should_panic_if_hashmap_contains_entry_without_key() {
        let mut test_map = HashMap::new();
        test_map.insert("hello", "hi");

        should_fail_with_message!(test_map.should().contain_entry(&"hey", &"hi"), "key \"hey\" not found");
    }

    #[test]
    fn should_panic_if_hashmap_contains_entry_with_different_value() {
        let mut test_map = HashMap::new();
        test_map.insert("hi", "hello");

        should_fail_with_message!(
            test_map.should().contain_entry(&"hi", &"hey"),
            "key \"hi\" has different value \"hello\""
        );
    }

    #[test]
    fn should_not_panic_if_hashmap_does_not_contain_entry_if_expected() {
        let mut test_map = HashMap::new();
        test_map.insert("hello", "hi");

        test_map.should().not_contain_entry(&"hey", &"hi");
    }

    #[test]
    fn should_not_panic_if_hashmap_contains_entry_with_different_value_if_expected() {
        let mut test_map = HashMap::new();
        test_map.insert("hi", "hello");

        test_map.should().not_contain_entry(&"hi", &"hey");
    }

    #[test]
    fn should_panic_if_hashmap_contains_entry_if_not_expected() {
        let mut test_map = HashMap::new();
        test_map.insert("hello", "hi");

        should_fail_with_message!(
            test_map.should().not_contain_entry(&"hello", &"hi"),
            "expecting: HashMap does not contain entry \"hello\":\"hi\"*subject: {\"hello\": \"hi\"}"
        );
    }
}
