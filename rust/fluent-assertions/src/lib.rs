//! # Fluent Assertions Testing in Rust
//!
//! FluentAssertions is a testing framework designed to make your assertions read like plain English.
//! This allows you to more easily expose the intent of your test, rather than having it shrouded by
//! assertions which work, but are opaque on their meaning. It's inspired by the same name [project](https://fluentassertions.com/) in C#.
//!
//! Methods available to assert with are dependent upon the type of the subject under test. There is also
//! an extension point to write customized asserts on specific types.
//!
//! ## Usage
//!
//! To quickly start using assertions, `use` the fluent_assertions root module:
//!
//! ```rust
//! use fluent_assertions::*;
//! ```
//!
//! ## Write Test with Fluent-Assertions
//!
//! Now that we have something to test, we need to actually start asserting on it. The first part
//! to that is to call the `should()` function.
//!
//! ```rust
//! use fluent_assertions::*;
//!
//! #[test]
//! pub fn should_be_the_correct_string() {
//!     let subject = "Hello World!";
//!
//!     // this call will consume subject
//!     subject.should().start_with("H");
//! }
//! ```
//!
//! But for the purpose of exploration, let's break the actual value. We'll change "Hello World!"
//! to be "ello World!".
//!
//! ```rust
//! use fluent_assertions::*;
//!
//! #[test]
//! pub fn should_be_the_correct_string() {
//!     let subject = "ello World!";
//!
//!     subject.should().start_with("H");
//! }
//! ```
//!
//! This time, we see that the test fails, and we also get some output from our assertion to tell
//! us what it was, and what it was expected to be:
//!
//! ```bash
//!     should().start_with() expecting subject <ello> to start with <H> but failed
//! ```
//!
//! Great! So we've just encountered a failing test. This particular case is quite easy to fix up
//! (just add the letter 'H' back to the start of the `String`), but we can also see that the panic
//! message tells us enough information to work that out as well.
//!
//! ## Basic Use Cases
//!
//! Start testing by calling [`Should::should`] on the subject followed by assertions like below.
//!
//! ```
//! use fluent_assertions::*;
//!
//! let subject = 3 + 5;
//! subject.should().be(8);
//! ```
//!
//! You can use `and()` clause with any assertions to continue with a new assertion on the subject, like below.
//!
//! ```
//! use fluent_assertions::*;
//!
//! let subject = "Hello world";
//! subject
//!     .should()
//!     .start_with("H")
//!     .and()
//!     .should()
//!     .end_with("d")
//!     .and()
//!     .len()
//!     .should()
//!     .be(11);
//! ```
//!
//! Many assertions provide additional clause for more detailed testing. e.g.
//!
//! ```
//! use fluent_assertions::*;
//!
//! let subject = vec![1, 2, 2, 3, 4];
//! subject.should().contain(2).for_times(2);
//! ```
//!
//! Some times you want to test multiple fields of a subject. One way is to use multiple `should()` statement one on each field.
//! You can also use [`Assertions::pass`] for this.
//!
//! ```
//! use fluent_assertions::*;
//!
//! let subject = (1, 2, 3, 4, 5);
//! subject.should().pass(|s| {
//!     s.0.should().be(&1);
//!     s.2.should().be(&3);
//! });
//! ```
//!
//! **Note**: [`Should::should`] will consume the subject. If you want to continue using the subject, you could pass in a reference.
//!
//! ```
//! use fluent_assertions::*;
//!
//! let subject = String::from("Hello world");
//! (&subject).should().start_with("H");
//! ```
//!
//! There is also a helper trait [`ToRef`] to enable reference retrieval in fluent syntax, like below:
//!
//! ```
//! use fluent_assertions::*;
//!
//! // in this case you want to test second value of inner tuple. You cannot directly
//! // call which_value().1.should() because it will move the value, call to_ref() to
//! // retrieve a reference to test, without break the fluent syntax
//! Ok::<_, ()>((3, 4, 5)).should().be_ok().which_value().1.to_ref().should().be(&4);
//!
//! // compare with
//! (&Ok::<_, ()>((3, 4, 5)).should().be_ok().which_value().1).should().be(&4);
//! ```
//!
//! ## More examples
//!
//! Additional assertions are provided for different data structure. Some examples below.
//! For more details refer to the each `*Assertions` trait.
//!
//! ```rust
//! use fluent_assertions::*;
//! use std::collections::HashMap;
//!
//! // use be and sastisfy to assert the value of subject
//! // use and() to chain multiple assertions
//! let subject = "hello";
//! subject
//!     .should()
//!     .be("hello")
//!     .and()
//!     .should()
//!     .satisfy(|s| s.starts_with("h"))
//!     .and()
//!     .is_empty()
//!     .should()
//!     .be_false();
//!
//! // working with boolean
//! let subject = true;
//! subject.should().be_true();
//!
//! // working with iterators
//! vec![1, 2, 3].should().have_length(3).and().should().contain(2);
//! vec![2].should().be_single().which_value().should().be(2);
//!
//! // working with numbers
//! 3.should().be_greater_than(2).and().should().be_less_than_or_equal_to(3);
//!
//! // working with options
//! Some(5).should().be_some().with_value(5);
//!
//! // working with results
//! Ok::<_, ()>(8).should().be_ok().which_value().should().be_greater_than(5);
//! Err::<(), _>("hello").should().be_err().with_value_that(|v| v.starts_with("h"));
//!
//! // working with hashmap
//! let mut subject = HashMap::new();
//! subject.insert(3, "three");
//! subject.should().contain_key(3).which_value().should().start_with("t");
//!
//! // working with string
//! "hello".should().contain("ll").and().should().end_with("o");
//!
//! // working with closures
//! (|| panic!("test".to_owned())).should().panic().with_message("test");
//!
//! // working with enum types
//! // in this case, you need to add #[derive(EnumAssertions)] on the enum type,
//! // the derive macro here will create a trait MyEnumAssertions in the same namespace
//! // and same visibility. Use the assertions in your test to enable it.
//! #[derive(EnumAssertions)]
//! pub enum MyEnum {
//!     First,
//!     Second(String),
//!     Third(u32, u32, bool),
//!     Fourth { apple: f32, banana: u64, orange: String },
//! }
//!
//! // if your tests are in different module, include 'use MyEnumAssertions'
//! MyEnum::Second("Hello".to_string())
//!     .should()
//!     .be_second()
//!     .which_value()
//!     .should()
//!     .start_with("He");
//! ```
#![allow(incomplete_features)]
#![feature(specialization)]
#![feature(box_syntax)]
#![feature(error_iter)]

/// Assertions on `enum` types. Variants of `enum` types cannot be retrieved
/// dynamically. This derive macro enables building a dedicated Assertions
/// class for the specific `enum` type.
///
/// ### Examples
/// ```
/// pub mod my_enum {
///     use fluent_assertions::EnumAssertions;
///
///     // the derive macro here will create a trait MyEnumAssertions in the same namespace
///     // and same visibility. Use the assertions in your test to enable it.
///     #[derive(EnumAssertions)]
///     pub enum MyEnum {
///         First,
///         Second(String),
///         Third(u32, u32, bool),
///         Fourth { apple: f32, banana: u64, orange: String },
///     }
/// }
///
/// use fluent_assertions::*;
/// use my_enum::{MyEnum, MyEnumAssertions};
///
/// // test the enum is of a specified variant
/// MyEnum::First.should().be_first();
///
/// // use which_value() clause to test the embedded value of the variant
/// MyEnum::Second("Hello".to_string())
///     .should()
///     .be_second()
///     .which_value()
///     .should()
///     .start_with("He");
///
/// // when there are multiple embedded values, which_value() returns a tuple
/// MyEnum::Third(3, 5, true)
///     .should()
///     .be_third()
///     .which_value()
///     .1
///     .should()
///     .be_greater_than(4);
///
/// // when embeded value is a anonymous struct, which_value() returns a struct with same schema
/// MyEnum::Fourth {
///     apple: 2f32,
///     banana: 43,
///     orange: "World".to_string(),
/// }
/// .should()
/// .be_fourth()
/// .which_value()
/// .banana
/// .should()
/// .be(43);
/// ```
pub use fluent_assertions_derive::EnumAssertions;

pub use any::AnyAssertions;
pub use binary::BinaryAssertions;
pub use boolean::BooleanAssertions;
pub use call::FnAssertions;
pub use hashmap::HashMapAssertions;
pub use iter::{IterAssertions, PartialEqIterAssertions, ResultIterAssertions};
pub use numeric::{FloatAssertions, OrderedAssertions};
pub use option::OptionAssertions;
pub use path::PathAssertions;
pub use result::ResultAssertions;
pub use should::{Assertions, Should, ToRef};
pub use string::StringAssertions;

#[macro_use]
pub mod utils;
mod any;
mod binary;
mod boolean;
mod call;
#[cfg(test)]
mod enum_tests;
mod hashmap;
mod iter;
mod numeric;
mod option;
mod path;
mod result;
mod should;
mod string;
