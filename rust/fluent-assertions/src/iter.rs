use crate::{
    result::{IsErr, IsOk},
    utils::{AssertionFailure, DebugMessage},
    Assertions, Should,
};
use std::{borrow::Borrow, cmp::PartialEq, collections::HashMap, hash::Hash};

/// Assertions for types implementing [`IntoIterator`] trait.
///
/// ### Examples
/// ```
/// use fluent_assertions::*;
/// use std::collections::HashMap;
///
/// // test subject containing item that meet condition
/// vec![1, 2, 3].should().contain_item_that(|v| *v > 1);
///
/// // use for_times() clause to test how many items found meeting condition
/// vec![1, 2, 3].should().contain_item_that(|v| *v > 1).for_times(2);
///
/// // test subject not containing any item that meet condition
/// vec![1, 2, 3].should().not_contain_item_that(|v| *v > 4);
///
/// // test subject to contain only one item
/// vec![3].should().be_single();
///
/// // then use with_value() clause to verify the value.
/// // this will only work if value type implementing PartialEq.
/// vec![3].should().be_single().with_value(3);
///
/// // or use which_value() clause to start a new test statement for the value
/// vec![3].should().be_single().which_value().should().be_less_than(4);
///
/// // if Iterator::Item is reference, which_value() returns reference as well
/// vec![3].iter().should().be_single().which_value().should().be(&3);
///
/// // test subject to have specified length
/// vec![1, 2, 3].should().have_length(3);
///
/// // Note: hashmaps are iterators so they can also use IterAssertions.
/// // for hashmap Iterator::Item = (K, V)
/// let mut subject = HashMap::new();
/// subject.insert(12, "abc");
/// subject.should().be_single().which_value().0.should().be(12);
///
/// // test subject to be empty
/// "".chars().should().be_empty();
/// ```
pub trait IterAssertions<T, I>
where I: IntoIterator<Item = T>
{
    fn contain_item_that<F: Fn(&T) -> bool>(self, pred: F) -> IterConstraint<T, I, CountExtension>;
    fn not_contain_item_that<F: Fn(&T) -> bool>(self, pred: F) -> IterConstraint<T, I, ()>;

    fn be_single(self) -> IterConstraint<T, I, SingleExtension>;
    fn have_length(self, expected: usize) -> IterConstraint<T, I, ()>;
    fn be_empty(self) -> IterConstraint<T, I, ()>;
}

/// Additional assertions for type implementing [`IntoIterator`], when
/// [`IntoIterator::Item`] implementing [`PartialEq`].
///
/// ### Examples
/// ```
/// use fluent_assertions::{PartialEqIterAssertions, *};
///
/// // test subject containing a specific value
/// vec![1, 2, 3].should().contain(2);
///
/// // same as contain_item_that(), can use for_times() clause to test number of occurence
/// vec![1, 2, 2, 3].should().contain(2).for_times(2);
///
/// // test subject containing all the specified items
/// // NOTE: duplicate items are not deduped. If a target item appears multiple times,
/// // it's supposed to appear in subject for that many times.
/// vec![1, 2, 2, 3, 3, 3].should().contain_all_of(vec![2, 2, 3]);
///
/// // test subject not to contain a specific item
/// vec![1, 2, 3].should().not_contain(4);
///
/// // test subject equal a target iterator
/// vec![1, 2, 3].should().equal_iterator(vec![1, 2, 3]);
/// ```
pub trait PartialEqIterAssertions<T, I>
where I: IntoIterator<Item = T>
{
    fn contain<E: Borrow<T>>(self, expected_value: E) -> IterConstraint<T, I, CountExtension>;
    fn contain_all_of<E>(self, expected_values_iter: E) -> IterConstraint<T, I, ()>
    where
        E: IntoIterator,
        <E as IntoIterator>::Item: Borrow<T>;
    fn not_contain<E: Borrow<T>>(self, expected_value: E) -> IterConstraint<T, I, ()>;
    fn equal_iterator<E>(self, expected_iter: E) -> IterConstraint<T, I, ()>
    where
        E: IntoIterator,
        <E as IntoIterator>::Item: Borrow<T>;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// IterContraint - tool class to enable and() chain and addition assertions
//
// Design consideration: IntoIterator will eat the subject when being consumed, this will break the and() chain.
// There is no easy solution. But we could work around to make several most useful scenario work.
// For other scenarios, the and() will not be available.
// - for Vec<> and HashMap<>, we will keep the items and rebuild the subject later;
// - for &Vec and &HashMap, we will clone the reference, and return later as subject.
pub struct IterConstraint<T, I: IntoIterator<Item = T>, E> {
    vec: Vec<T>,
    extension: E,
    cloned_subject: Option<I>,
}

// for default case, we will consume the iterator and keep the items in an owned Vec
impl<T, I: IntoIterator<Item = T>> From<I> for IterConstraint<T, I, ()> {
    default fn from(iter: I) -> Self {
        IterConstraint {
            vec: iter.into_iter().collect(),
            extension: (),
            cloned_subject: None,
        }
    }
}

// if subject implements Clone, we will clone the subject (&Vec and &HashMap fits here)
impl<'v, I: IntoIterator<Item = T> + Clone, T> From<I> for IterConstraint<T, I, ()> {
    fn from(iter: I) -> Self {
        let cloned_iter = iter.clone();
        IterConstraint {
            vec: iter.into_iter().collect(),
            extension: (),
            cloned_subject: Some(cloned_iter),
        }
    }
}

// when and() is called, restore &Vec from cloned subject
impl<'v, T, E> From<IterConstraint<&'v T, &'v Vec<T>, E>> for &'v Vec<T> {
    fn from(value: IterConstraint<&'v T, &'v Vec<T>, E>) -> Self {
        value.cloned_subject.unwrap()
    }
}

// when and() is called, rebuild Vec
impl<T, E> From<IterConstraint<T, Vec<T>, E>> for Vec<T> {
    fn from(value: IterConstraint<T, Vec<T>, E>) -> Self {
        value.vec
    }
}

// when and() is called, rebuild HashMap
impl<K: Hash + Eq, V: PartialEq, E> From<IterConstraint<(K, V), HashMap<K, V>, E>> for HashMap<K, V> {
    fn from(value: IterConstraint<(K, V), HashMap<K, V>, E>) -> Self {
        value.vec.into_iter().collect()
    }
}

// when and() is called, restore &HashMap from cloned subject
impl<'h, K: Hash + Eq, V: PartialEq, E> From<IterConstraint<(&'h K, &'h V), &'h HashMap<K, V>, E>> for &'h HashMap<K, V> {
    fn from(value: IterConstraint<(&'h K, &'h V), &'h HashMap<K, V>, E>) -> Self {
        value.cloned_subject.unwrap()
    }
}

impl<T, I: IntoIterator<Item = T>> IterConstraint<T, I, ()> {
    fn iter(&self) -> impl Iterator<Item = &T> {
        self.vec.iter()
    }

    fn with_extension<E>(self, extension: E) -> IterConstraint<T, I, E> {
        IterConstraint {
            vec: self.vec,
            extension,
            cloned_subject: self.cloned_subject,
        }
    }
}

impl<T, I: From<IterConstraint<T, I, E>> + IntoIterator<Item = T>, E> IterConstraint<T, I, E> {
    pub fn and(self) -> I {
        self.into()
    }
}

pub struct CountExtension {
    expecting: String,
    count: usize,
}

impl<T, I: IntoIterator<Item = T>> IterConstraint<T, I, CountExtension> {
    pub fn for_times(self, n: usize) {
        if self.extension.count != n {
            AssertionFailure::new("should().contain().for_times() expectation failed")
                .expecting(self.extension.expecting.replace("__n__", &n.to_string()))
                .but(format!("item appear {} times", self.extension.count))
                .subject(&self.vec)
                .fail();
        }
    }
}

pub struct SingleExtension;

impl<T, I: IntoIterator<Item = T>> IterConstraint<T, I, SingleExtension> {
    pub fn which_value(mut self) -> T {
        assert_eq!(self.vec.len(), 1);
        self.vec.remove(0)
    }
}

impl<T: PartialEq, I: IntoIterator<Item = T>> IterConstraint<T, I, SingleExtension> {
    pub fn with_value<E: Borrow<T>>(self, expected: E) -> IterConstraint<T, I, ()> {
        assert_eq!(self.vec.len(), 1);
        self.vec.get(0).unwrap().should().be(expected.borrow());

        IterConstraint {
            vec: self.vec,
            extension: (),
            cloned_subject: self.cloned_subject,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////

impl<T, I> IterAssertions<T, I> for Assertions<I>
where I: IntoIterator<Item = T>
{
    fn contain_item_that<F: Fn(&T) -> bool>(self, matching_fn: F) -> IterConstraint<T, I, CountExtension> {
        let ic = IterConstraint::from(self.into_inner());

        let count = ic.iter().filter(|v| matching_fn(*v)).count();

        if count == 0 {
            AssertionFailure::new("should().contain() expecting Iter contain item that matching condition but not found")
                .subject(&ic.vec)
                .fail();
        }

        ic.with_extension(CountExtension {
            expecting: format!("Iter contain item that matching condition for __n__ times"),
            count,
        })
    }

    fn not_contain_item_that<F: Fn(&T) -> bool>(self, pred: F) -> IterConstraint<T, I, ()> {
        let ic = IterConstraint::from(self.into_inner());

        let count = ic.iter().filter(|v| pred(*v)).count();

        if count != 0 {
            AssertionFailure::new("should().contain() expecting Iter not contain item matching condition but found")
                .subject(&ic.vec)
                .fail();
        }

        ic
    }

    fn be_single(self) -> IterConstraint<T, I, SingleExtension> {
        let ic = IterConstraint::from(self.into_inner());

        if ic.vec.len() != 1 {
            AssertionFailure::new(format!(
                "should().be_single() expecting subject to have single element but it has {}",
                ic.vec.len()
            ))
            .subject(&ic.vec)
            .fail();
        }

        ic.with_extension(SingleExtension)
    }

    fn have_length(self, expected: usize) -> IterConstraint<T, I, ()> {
        let ic = IterConstraint::from(self.into_inner());

        if ic.vec.len() != expected {
            AssertionFailure::new("should().have_length() expectation failed")
                .expected(expected)
                .but_was(ic.vec.len())
                .subject(&ic.vec)
                .fail();
        }

        ic
    }

    fn be_empty(self) -> IterConstraint<T, I, ()> {
        let ic = IterConstraint::from(self.into_inner());

        if !ic.vec.is_empty() {
            AssertionFailure::new(format!(
                "should().be_empty() expecting subject empty but having length {}",
                ic.vec.len()
            ))
            .subject(&ic.vec)
            .fail();
        }

        ic
    }
}

impl<T, I> PartialEqIterAssertions<T, I> for Assertions<I>
where
    T: PartialEq,
    I: IntoIterator<Item = T>,
{
    fn contain<E: Borrow<T>>(self, expected_value: E) -> IterConstraint<T, I, CountExtension> {
        let ic = IterConstraint::from(self.into_inner());

        let count = ic.iter().filter(|v| *v == expected_value.borrow()).count();

        if count == 0 {
            AssertionFailure::new(format!(
                "should().contain() expecting Iter contain item <{}> but not found",
                expected_value.debug_message()
            ))
            .subject(&ic.vec)
            .fail();
        }

        ic.with_extension(CountExtension {
            expecting: format!("Iter contain item <{}> for __n__ times", expected_value.debug_message()),
            count,
        })
    }

    fn contain_all_of<E>(self, expected_values_iter: E) -> IterConstraint<T, I, ()>
    where
        E: IntoIterator,
        <E as IntoIterator>::Item: Borrow<T>,
    {
        let ic = IterConstraint::from(self.into_inner());

        let actual_values: Vec<_> = ic.iter().collect();
        let expected_values: Vec<_> = expected_values_iter.into_iter().collect();

        let mut matched_indexes = vec![];
        let mut matched_indexes_holder = vec![];

        let mut matched_values = vec![];
        let mut unmatched_values = vec![];

        'outer: for expected in expected_values.iter() {
            matched_indexes.append(&mut matched_indexes_holder);

            for (index, actual) in actual_values.iter().enumerate().filter(|&(i, _)| !matched_indexes.contains(&i)) {
                if expected.borrow().eq(actual) {
                    matched_indexes_holder.push(index);
                    matched_values.push(expected);
                    continue 'outer;
                }
            }

            unmatched_values.push(expected);
        }

        if !unmatched_values.is_empty() {
            AssertionFailure::new("should().contain_all_of() expectation failed")
                .expecting(format!("subject contain all values in {}", expected_values.debug_message()))
                .but(format!("{} not found", unmatched_values.debug_message()))
                .with_message("found values", matched_values.debug_message())
                .subject(actual_values)
                .fail();
        }

        ic
    }

    fn not_contain<E: Borrow<T>>(self, expected_value: E) -> IterConstraint<T, I, ()> {
        let ic = IterConstraint::from(self.into_inner());

        let count = ic.iter().filter(|v| *v == expected_value.borrow()).count();

        if count != 0 {
            AssertionFailure::new(format!(
                "should().contain() expecting Iter not contain item <{}> but found",
                expected_value.debug_message()
            ))
            .subject(&ic.vec)
            .fail();
        }

        ic
    }

    fn equal_iterator<E>(self, expected_iter: E) -> IterConstraint<T, I, ()>
    where
        E: IntoIterator,
        <E as IntoIterator>::Item: Borrow<T>,
    {
        let ic = IterConstraint::from(self.into_inner());

        {
            let mut actual_iter = ic.iter();
            let expected_vec: Vec<_> = expected_iter.into_iter().collect();
            let mut expected_iter = expected_vec.iter();

            let mut read_subject = vec![];
            let mut read_expected = vec![];

            loop {
                match (actual_iter.next(), expected_iter.next()) {
                    (Some(actual), Some(expected)) => {
                        if !actual.eq(expected.borrow()) {
                            AssertionFailure::new("should().equal_iterator() expectation failed")
                                .expecting(format!("subject equals iterator <{}>", expected_vec.debug_message()))
                                .but(format!(
                                    "element not match at position {}, expecting <{}> while actual <{}>",
                                    read_expected.len(),
                                    expected.debug_message(),
                                    actual.debug_message()
                                ))
                                .with_message("read_expected", read_expected.debug_message())
                                .with_message("read_actual", read_subject.debug_message())
                                .subject(&ic.vec)
                                .fail();

                            unreachable!();
                        }

                        read_subject.push(actual);
                        read_expected.push(expected);
                    },
                    (Some(actual), None) => {
                        AssertionFailure::new("should().equal_iterator() expectation failed")
                            .expecting(format!("subject equals iterator <{}>", expected_vec.debug_message()))
                            .but(format!(
                                "element not match at position {}, expecting iter complete while actual <{}>",
                                read_expected.len(),
                                actual.debug_message(),
                            ))
                            .with_message("read_expected", read_expected.debug_message())
                            .with_message("read_actual", read_subject.debug_message())
                            .subject(&ic.vec)
                            .fail();

                        unreachable!();
                    },
                    (None, Some(expected)) => {
                        AssertionFailure::new("should().equal_iterator() expectation failed")
                            .expecting(format!("subject equals iterator <{}>", expected_vec.debug_message()))
                            .but(format!(
                                "element not match at position {}, expecting <{}> while actual iter complete",
                                read_expected.len(),
                                expected.debug_message(),
                            ))
                            .with_message("read_expected", read_expected.debug_message())
                            .with_message("read_actual", read_subject.debug_message())
                            .subject(&ic.vec)
                            .fail();

                        unreachable!();
                    },
                    (None, None) => {
                        break;
                    },
                }
            }
        }

        ic
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Additional assertions for type implementing [`IntoIterator`], when
/// [`IntoIterator::Item`] = [`Result`].
///
/// ### Examples
/// ```
/// use fluent_assertions::*;
///
/// // test all the items in subjects are Ok
/// vec![Ok::<_, ()>(3), Ok(4), Ok(5)].should().all_be_ok();
///
/// // use which_inner_values() clause to start a new statement testing inner values
/// vec![Ok::<_, ()>(3), Ok(4), Ok(5)]
///     .should()
///     .all_be_ok()
///     .which_inner_values()
///     .should()
///     .equal_iterator(&[3, 4, 5]);
///
/// // test subject contain at least one Err
/// vec![Ok::<usize, bool>(3), Err(true)].should().contain_err();
///
/// // use which_value() clause to continue test the error value
/// // NOTE: if subject contain multiple Err, the first one will be taken
/// vec![Ok(3), Err(true)].should().contain_err().which_value().should().be_true();
///
/// // or use with_value_that() clause to test the value with a predicator
/// vec![Ok(3), Err("Hello".to_owned())]
///     .should()
///     .contain_err()
///     .with_value_that(|e| e.starts_with("H"));
///
/// // or use with_value() clause to verify the Err value directly, if Err
/// // type implements PartialEq
/// vec![Ok(3u32), Err(5u32)].should().contain_err().with_value(5u32);
/// ```
pub trait ResultIterAssertions<T, E, R, I>
where
    R: Borrow<Result<T, E>>,
    I: IntoIterator<Item = R>,
{
    fn all_be_ok(self) -> IterConstraint<R, I, IsOk>;

    fn contain_err(self) -> IterConstraint<R, I, IsErr>;
}

impl<T, E, I> IterConstraint<Result<T, E>, I, IsOk>
where I: IntoIterator<Item = Result<T, E>>
{
    pub fn which_inner_values(self) -> Vec<T> {
        self.vec.into_iter().map(|r| r.ok().unwrap()).collect()
    }
}

impl<T, E, I> IterConstraint<Result<T, E>, I, IsErr>
where I: IntoIterator<Item = Result<T, E>>
{
    pub fn which_value(self) -> E {
        for r in self.vec {
            if let Err(e) = r {
                return e;
            }
        }

        panic!()
    }

    pub fn with_value_that(self, pred: impl Fn(&E) -> bool) {
        for r in self.vec.iter() {
            if let Err(e) = r {
                if pred(e) {
                    return;
                } else {
                    AssertionFailure::new(
                        "should().contain_err().with_value_that() expecting subject to contain Err which value meet condition",
                    )
                    .but(format!(
                        "found Err in subject, first value <{}> failed to meet condition",
                        e.debug_message()
                    ))
                    .subject(&self.vec)
                    .fail();
                }
            }
        }

        panic!()
    }
}

impl<T, E, I> IterConstraint<Result<T, E>, I, IsErr>
where
    I: IntoIterator<Item = Result<T, E>>,
    E: PartialEq,
{
    pub fn with_value(self, expected: impl Borrow<E>) {
        for r in self.vec.iter() {
            if let Err(e) = r {
                if e.eq(expected.borrow()) {
                    return;
                } else {
                    AssertionFailure::new(format!(
                        "should().contain_err().with_value() expecting subject to contain Err with value <{}>",
                        expected.debug_message()
                    ))
                    .but(format!("found Err in subject, the first one is <{}>", e.debug_message()))
                    .subject(&self.vec)
                    .fail();
                }
            }
        }

        panic!()
    }
}

impl<T, E, I, R> ResultIterAssertions<T, E, R, I> for Assertions<I>
where
    R: Borrow<Result<T, E>>,
    I: IntoIterator<Item = R>,
{
    fn all_be_ok(self) -> IterConstraint<R, I, IsOk> {
        let ic = IterConstraint::from(self.into_inner());

        let mut ok_values = vec![];
        for (i, r) in ic.iter().enumerate() {
            match r.borrow() {
                Ok(o) => ok_values.push(o),
                Err(e) => {
                    AssertionFailure::new("should().all_be_ok() expectation failed")
                        .expecting("all items in subject are Result::Ok")
                        .but(format!("{}-th item is Result::Err({})", i, e.debug_message()))
                        .with_message("matched items", ok_values.debug_message())
                        .subject(&ic.vec)
                        .fail();
                },
            }
        }

        ic.with_extension(IsOk)
    }

    fn contain_err(self) -> IterConstraint<R, I, IsErr> {
        let ic = IterConstraint::from(self.into_inner());

        if ic.iter().find(|r| (*r).borrow().is_err()).is_none() {
            AssertionFailure::new("should().contain_err() expecting subject to contain Err but failed")
                .subject(ic.iter())
                .fail();
        }

        ic.with_extension(IsErr)
    }
}

#[cfg(test)]
mod tests {

    use crate::*;
    use std::collections::LinkedList;

    #[test]
    fn contains_should_allow_for_multiple_borrow_types_for_intoiter() {
        vec![1, 2, 3]
            .should()
            .contain(2)
            .and()
            .should()
            .contain(&2)
            .and()
            .should()
            .contain(&mut 2);
    }

    #[test]
    fn ref_vec_could_chain_with_and() {
        (&vec![1, 2, 3]).should().contain(&2).and().should().not_contain(&4);
    }

    #[test]
    fn contains_for_times() {
        vec![1, 2, 2].should().contain(2).for_times(2);
    }

    #[test]
    fn contain_item_should_not_panic_if_condition_meets() {
        vec![1, 2, 3].should().contain_item_that(|v| *v > 1).for_times(2);
    }

    #[test]
    fn should_panic_if_vec_does_not_contain_value() {
        let test_vec = vec![1, 2, 3];
        should_fail_with_message!(test_vec.should().contain(5), "expecting Iter contain item <5> but not found");
    }

    #[test]
    fn should_not_panic_if_vec_does_not_contain_value_if_expected() {
        let test_vec = vec![1, 2, 3];
        test_vec.should().not_contain(4);
    }

    #[test]
    fn should_panic_if_vec_does_contain_value_and_expected_not_to() {
        let test_vec = vec![1, 2, 3];
        should_fail_with_message!(test_vec.should().not_contain(2), "expecting Iter not contain item <2> but found");
    }

    #[test]
    fn should_not_panic_if_iterable_contains_value() {
        let mut test_into_iter = LinkedList::new();
        test_into_iter.push_back(1);
        test_into_iter.push_back(2);
        test_into_iter.push_back(3);

        test_into_iter.should().contain(2);
    }

    #[test]
    fn should_not_panic_if_iterable_contains_all_expected_values() {
        let mut test_into_iter = LinkedList::new();
        test_into_iter.push_back(1);
        test_into_iter.push_back(2);
        test_into_iter.push_back(3);

        test_into_iter.should().contain_all_of(vec![2, 3]);
    }

    #[test]
    fn should_panic_if_iterable_does_not_contain_all_expected_values() {
        let mut test_into_iter = LinkedList::new();
        test_into_iter.push_back(1);
        test_into_iter.push_back(2);
        test_into_iter.push_back(3);

        should_fail_with_message!(test_into_iter.should().contain_all_of(vec![1, 6]), "[6] not found");
    }

    #[test]
    fn should_not_panic_if_iterator_contains_all_expected_values() {
        let test_vec = vec![1, 2, 3];
        test_vec.iter().should().contain_all_of(vec![&2, &3]);
    }

    #[test]
    fn should_panic_if_iterator_does_not_contain_all_expected_values_exactly() {
        let test_vec = vec![1, 2, 3];

        should_fail_with_message!(test_vec.should().contain_all_of(&vec![1, 1, 3]), "[1] not found");
    }

    #[test]
    fn should_not_panic_if_iteratable_equals_expected_iterator() {
        let expected_vec = vec![1, 2, 3];
        let test_vec = vec![1, 2, 3];

        test_vec.should().equal_iterator(expected_vec);
    }

    #[test]
    fn should_panic_if_iteratable_does_not_equal_expected_iterator() {
        let expected_vec = vec![1, 2, 4];
        let test_vec = vec![1, 2, 3];

        should_fail_with_message!(
            test_vec.should().equal_iterator(expected_vec),
            "element not match at position 2, expecting <4> while actual <3>"
        );
    }

    #[test]
    fn should_not_panic_if_iterator_does_not_contain_value_if_expected() {
        let test_vec = vec![1, 2, 3];
        test_vec.should().not_contain_item_that(|v| *v > 4);
    }

    #[test]
    fn should_not_panic_if_vec_length_matches_expected() {
        let test_vec = vec![1, 2, 3];
        test_vec.should().have_length(3);
    }

    #[test]
    fn should_panic_if_vec_length_does_not_match_expected() {
        let test_vec = vec![1, 2, 3];

        should_fail_with_message!(test_vec.should().have_length(1), "expected: 1*but was: 3");
    }

    #[test]
    fn should_not_panic_if_vec_was_expected_to_be_empty_and_is() {
        let test_vec: Vec<u8> = vec![];
        test_vec.should().be_empty();
    }

    #[test]
    fn should_panic_if_vec_was_expected_to_be_empty_and_is_not() {
        should_fail_with_message!(vec![1].should().be_empty(), "expecting subject empty but having length 1");
    }

    #[test]
    fn should_be_single_which_value() {
        vec![1].should().be_single().which_value().should().be(1);
    }

    #[test]
    fn should_be_single_with_value() {
        vec![1].should().be_single().with_value(1);
    }

    #[test]
    fn should_be_single_panic_when_not_single() {
        should_fail_with_message!(
            vec![1, 2].should().be_single(),
            "expecting subject to have single element but it has 2"
        );
    }

    #[test]
    fn all_be_ok_should_not_panic_if_iter_all_be_ok() {
        vec![Ok::<_, ()>(1), Ok(2), Ok(3)].should().all_be_ok();
    }

    #[test]
    fn all_be_ok_should_not_panic_if_iter_all_be_ok_and() {
        vec![Ok::<_, ()>(1), Ok(2), Ok(3)]
            .should()
            .all_be_ok()
            .and()
            .should()
            .have_length(3);
    }

    #[test]
    fn all_be_ok_should_not_panic_if_iter_all_be_ok_with_inner_values_matches() {
        vec![Ok::<_, ()>(1), Ok(2), Ok(3)]
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&[1, 2, 3]);
    }

    #[test]
    fn all_be_ok_should_panic_if_not_all_be_ok() {
        should_fail_with_message!(
            vec![Ok::<_, ()>(1), Err(()), Ok(3)].should().all_be_ok(),
            "1-th item is Result::Err"
        );
    }

    #[test]
    fn contain_err_should_not_panic_if_err_exists() {
        vec![Ok::<u32, u32>(1), Err(2), Ok(3)].should().contain_err();
    }

    #[test]
    fn contain_err_should_panic_if_err_not_exists() {
        should_fail_with_message!(
            vec![Ok::<u32, u32>(1), Ok(2), Ok(3)].should().contain_err(),
            "expecting subject to contain Err but failed"
        );
    }

    #[test]
    fn contain_err_which_value_returns_first_error_value() {
        vec![Ok::<u32, u32>(1), Err(2), Ok(3)]
            .should()
            .contain_err()
            .which_value()
            .should()
            .be(2);
    }

    #[test]
    fn contain_err_with_value_should_not_panic_if_first_value_matches() {
        vec![Ok::<u32, u32>(1), Err(2), Err(3)].should().contain_err().with_value(2);
    }

    #[test]
    fn contain_err_with_value_should_panic_if_first_value_mismatches() {
        should_fail_with_message!(
            vec![Ok::<u32, u32>(1), Err(2), Err(3)].should().contain_err().with_value(3),
            "expecting subject to contain Err with value <3> but: found Err in subject, the first one is <2>"
        );
    }

    #[test]
    fn contain_err_with_value_that_should_not_panic_if_first_value_meet_condition() {
        vec![Ok::<u32, u32>(1), Err(2), Err(3)]
            .should()
            .contain_err()
            .with_value_that(|e| *e > 1);
    }

    #[test]
    fn contain_err_with_value_that_should_panic_if_first_value_not_meet_condition() {
        should_fail_with_message!(vec![Ok::<u32, u32>(1), Err(2), Err(3)]
            .should()
            .contain_err()
            .with_value_that(|e| *e > 3), "expecting subject to contain Err which value meet condition but: found Err in subject, first value <2> failed to meet condition");
    }
}
