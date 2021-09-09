# Fluent Assertions in Rust

FluentAssertions is a testing framework designed to make your assertions read like plain English.
This allows you to more easily expose the intent of your test, rather than having it shrouded by
assertions which work, but are opaque on their meaning. It's inspired by the same name [project](https://fluentassertions.com/) in C#.

Methods available to assert with are dependent upon the type of the subject under test. There is also
an extension point to write customized asserts on specific types.

- [Quick Start](#quick-start)
  - [Write Test with Fluent-Assertions](#write-test-with-fluent-assertions)
  - [Basic Use Cases](#basic-use-cases)
  - [More examples](#more-examples)
- [Advanced](#advanced)
  - [General Assertions](#general-assertions)
  - [Working with Any](#working-with-any)
  - [Working with Binary](#working-with-binary)
  - [Working with Boolean](#working-with-boolean)
  - [Working with Number](#working-with-number)
  - [Working with Closure](#working-with-closure)
  - [Working with Hashmap](#working-with-hashmap)
  - [Working with Iterator](#working-with-iterator)
  - [Working with Option](#working-with-option)
  - [Working with Path](#working-with-path)
  - [Working with Result](#working-with-result)
  - [Working with String](#working-with-string)
  - [Working with enum](#working-with-enum)

## Quick Start

Add this to your `Cargo.toml`:

```toml
[dependencies]
fluent-assertions = "0.2.0"
```

To quickly start using assertions:
```rust
use fluent_assertions::*;
```

### Write Test with Fluent-Assertions

Now that we have something to test, we need to actually start asserting on it. The first part
to that is to call the `should()` function.

```rust
use fluent_assertions::*;

#[test]
pub fn should_be_the_correct_string() {
    let subject = "Hello World!";

    // this call will consume subject
    subject.should().start_with("H");
}
```

But for the purpose of exploration, let's break the actual value. We'll change "Hello World!"
to be "ello World!".

```rust
use fluent_assertions::*;

#[test]
pub fn should_be_the_correct_string() {
    let subject = "ello World!";

    subject.should().start_with("H");
}
```

This time, we see that the test fails, and we also get some output from our assertion to tell
us what it was, and what it was expected to be:

```bash
    should().start_with() expecting subject <ello> to start with <H> but failed
```

Great! So we've just encountered a failing test. This particular case is quite easy to fix up
(just add the letter 'H' back to the start of the `String`), but we can also see that the panic
message tells us enough information to work that out as well.

### Basic Use Cases

Start testing by calling `should()` on the subject followed by assertions like below.

```rust
use fluent_assertions::*;

let subject = 3 + 5;
subject.should().be(8);
```

You can use `and()` clause with any assertions to continue with a new assertion on the subject, like below.

```rust
use fluent_assertions::*;

let subject = "Hello world";
subject
    .should()
    .start_with("H")
    .and()
    .should()
    .end_with("d")
    .and()
    .len()
    .should()
    .be(11);
```

Many assertions provide additional clause for more detailed testing. e.g.

```rust
use fluent_assertions::*;

let subject = vec![1, 2, 2, 3, 4];
subject.should().contain(2).for_times(2);
```

Some times you want to test multiple fields of a subject. One way is to use multiple `should()` statement one on each field.
You can also use `pass()` for this.

```rust
use fluent_assertions::*;

let subject = (1, 2, 3, 4, 5);
subject.should().pass(|s| {
    s.0.should().be(&1);
    s.2.should().be(&3);
});
```

**Note**: `should()` will consume the subject. If you want to continue using the subject, you could pass in a reference.

```rust
use fluent_assertions::*;

let subject = String::from("Hello world");
(&subject).should().start_with("H");
```

There is also a helper function `to_ref()` to enable reference retrieval in fluent syntax, like below:

```rust
use fluent_assertions::*;

// in this case you want to test second value of inner tuple. You cannot directly
// call which_value().1.should() because it will move the value, call to_ref() to
// retrieve a reference to test, without break the fluent syntax
Ok::<_, ()>((3, 4, 5)).should().be_ok().which_value().1.to_ref().should().be(&4);

// compare with
(&Ok::<_, ()>((3, 4, 5)).should().be_ok().which_value().1).should().be(&4);
```

### More examples

```rust
use fluent_assertions::*;
use std::collections::HashMap;

// use be and sastisfy to assert the value of subject
// use and() to chain multiple assertions
let subject = "hello";
subject
    .should()
    .be("hello")
    .and()
    .should()
    .satisfy(|s| s.starts_with("h"))
    .and()
    .is_empty()
    .should()
    .be_false();

// working with boolean
let subject = true;
subject.should().be_true();

// working with iterators
vec![1, 2, 3].should().have_length(3).and().should().contain(2);
vec![2].should().be_single().which_value().should().be(2);

// working with numbers
3.should().be_greater_than(2).and().should().be_less_than_or_equal_to(3);

// working with options
Some(5).should().be_some().with_value(5);

// working with results
Ok::<_, ()>(8).should().be_ok().which_value().should().be_greater_than(5);
Err::<(), _>("hello").should().be_err().with_value_that(|v| v.starts_with("h"));

// working with hashmap
let mut subject = HashMap::new();
subject.insert(3, "three");
subject.should().contain_key(3).which_value().should().start_with("t");

// working with string
"hello".should().contain("ll").and().should().end_with("o");

// working with closures
(|| panic!("test".to_owned())).should().panic().with_message("test");

// working with enum types
// in this case, you need to add #[derive(EnumAssertions)] on the enum type,
// the derive macro here will create a trait MyEnumAssertions in the same namespace
// and same visibility. Use the assertions in your test to enable it.
#[derive(EnumAssertions)]
pub enum MyEnum {
    First,
    Second(String),
    Third(u32, u32, bool),
    Fourth { apple: f32, banana: u64, orange: String },
}

// if your tests are in different module, include 'use MyEnumAssertions'
MyEnum::Second("Hello".to_string())
    .should()
    .be_second()
    .which_value()
    .should()
    .start_with("He");
```

## Advanced

### General Assertions

```rust
use fluent_assertions::*;

// test subject meeting condition specified by a predicate
"hello".should().satisfy(|x| x == &"hello");

// sometimes subject has multiple sub-fields to be tested, in this case,
// use pass() assertion, which takes an closure on subject
let subject = (3, 4, 5);
subject.should().pass(|s| {
    s.0.to_ref().should().be(&3);
    s.1.to_ref().should().be(&4);
    s.2.to_ref().should().be(&5);
});

// the above case is not different from the following form
let subject = (3, 4, 5);
subject.0.to_ref().should().be(&3);
subject.1.to_ref().should().be(&4);
subject.2.to_ref().should().be(&5);

// but it will be useful if the subject was a result of previous test, like in the following case
Ok::<_, ()>((3, 4, 5)).should().be_ok().which_value().should().pass(|s| {
    s.0.to_ref().should().be(&3);
    s.1.to_ref().should().be(&4);
    s.2.to_ref().should().be(&5);
});

// test subject value directly if subject implements PartialEq
5.should().be(5);

// or not equal
4.should().not_be(5);

// if subject implements PartialEq<F> where F is a different type,
// could also use the following form to test equality
String::from("abc").should().equal("abc");
```


### Working with Any

```rust
use fluent_assertions::*;
use std::any::Any;

// use be_of_type::<T>() to test type of dyn Any
let v: Box<dyn Any> = Box::new(3u32);
v.should().be_of_type::<u32>();

// use which_value() clause to test actual value
let v: Box<dyn Any> = Box::new("Hello World");
v.should().be_of_type::<&str>().which_value().should().start_with("H");

// can also use with_value() clause to test the value if value implements PartialEq
let v: Box<dyn Any> = Box::new(42usize);
v.should().be_of_type::<usize>().with_value(42);

// or use the with_value_that() clause which taks a predicate
let v: Box<dyn Any> = Box::new(42usize);
v.should().be_of_type::<usize>().with_value_that(|v| *v > 40);
```

### Working with Binary

```rust
use fluent_assertions::*;

// test subject equals binary array
vec![b'a', b'b', b'c'].should().equal_binary(b"abc");

// and not equal
"Hello World".as_bytes().should().not_equal_binary(b"hello world");
```

### Working with Boolean

```rust
use fluent_assertions::*;

// test subject to be true
true.should().be_true();

// or false
false.should().be_false();
```

### Working with Number

```rust
use fluent_assertions::*;

// test subject < target value
1.should().be_less_than(2);

// test subject <= target value
"One".should().be_less_than_or_equal_to("Two");

// test subject > target value
5f32.should().be_greater_than(3f32);

// test subject >= target value
33u8.should().be_greater_than_or_equal_to(33u8);

2.0f64.should().be_close_to(2.0f64, 0.01f64);
```

### Working with Closure

```rust
use fluent_assertions::*;

// test closure should panic
let subject = || {
    panic!(format!("this is the message"));
};
subject.should().panic();

// use with_message() clause to test closure panic with string message (&str or String)
// and pattern match the error message
let subject = || {
    panic!("this is the message");
};
subject.should().panic().with_message("this is the message");

// with_message() normalize space characters
let subject = || {
    panic!("this \t is\n the                   message");
};
subject.should().panic().with_message("this is the message");

// with_message() support grab patterns
let subject = || {
    panic!("hello world");
};
subject.should().panic().with_message("h*w???d");

// should_panic_with_message! macro provide a syntax suger to test panic message from an expression
should_fail_with_message!(
    {
        panic!("hello");
    },
    "hello"
);

// if closure panic not with a string message, use which() clause to test the value.
// which() clause returns Box<dyn Any + Send>
let subject = || {
    panic!(13u32);
};
subject.should().panic().which().should().be_of_type::<u32>().with_value(13u32);
```

### Working with Hashmap

```rust
use fluent_assertions::*;
use std::collections::HashMap;

// test hashmap containing a specified key, use which_value() clause to
// test the corresponding value
let mut test_map = HashMap::new();
test_map.insert("hello", "hi");
test_map.should().contain_key("hello").which_value().should().be("hi");

// test hashmap not contains key
let mut test_map = HashMap::new();
test_map.insert("hello", "hi");
test_map.should().not_contain_key("hey");

// test hashmap containing a specified key-value pair
let mut test_map = HashMap::new();
test_map.insert("hello", "hi");
test_map.should().contain_entry("hello", "hi");

// not containing key-value
let mut test_map = HashMap::new();
test_map.insert("hello", "hi");
test_map.should().not_contain_entry("hello", "hey");

// Note: hashmaps are iterators, so assertions on Iterator also work
// which_value() below returns tuple (K, V)
let mut subject = HashMap::new();
subject.insert(12, "abc");
subject.should().be_single().which_value().0.should().be(12);
```

### Working with Iterator

```rust
use fluent_assertions::*;
use std::collections::HashMap;

// test subject containing item that meet condition
vec![1, 2, 3].should().contain_item_that(|v| *v > 1);

// use for_times() clause to test how many items found meeting condition
vec![1, 2, 3].should().contain_item_that(|v| *v > 1).for_times(2);

// test subject not containing any item that meet condition
vec![1, 2, 3].should().not_contain_item_that(|v| *v > 4);

// test subject containing a specific value (if Item implementing PartialEq)
vec![1, 2, 3].should().contain(2);

// same as contain_item_that(), can use for_times() clause to test number of occurence
vec![1, 2, 2, 3].should().contain(2).for_times(2);

// test subject containing all the specified items
// NOTE: duplicate items are not deduped. If a target item appears multiple times,
// it's supposed to appear in subject for that many times.
vec![1, 2, 2, 3, 3, 3].should().contain_all_of(vec![2, 2, 3]);

// test subject not to contain a specific item
vec![1, 2, 3].should().not_contain(4);

// test subject to contain only one item
vec![3].should().be_single();

// then use with_value() clause to verify the value.
// this will only work if value type implementing PartialEq.
vec![3].should().be_single().with_value(3);

// or use which_value() clause to start a new test statement for the value
vec![3].should().be_single().which_value().should().be_less_than(4);

// if Iterator::Item is reference, which_value() returns reference as well
vec![3].iter().should().be_single().which_value().should().be(&3);

// test subject to have specified length
vec![1, 2, 3].should().have_length(3);

// test subject to be empty
"".chars().should().be_empty();

// test subject equal a target iterator
vec![1, 2, 3].should().equal_iterator(vec![1, 2, 3]);

// if Item = Result, test all the items in subjects are Ok
vec![Ok::<_, ()>(3), Ok(4), Ok(5)].should().all_be_ok();

// use which_inner_values() clause to start a new statement testing inner values
vec![Ok::<_, ()>(3), Ok(4), Ok(5)]
    .should()
    .all_be_ok()
    .which_inner_values()
    .should()
    .equal_iterator(&[3, 4, 5]);

// test subject contain at least one Err
vec![Ok::<usize, bool>(3), Err(true)].should().contain_err();

// use which_value() clause to continue test the error value
// NOTE: if subject contain multiple Err, the first one will be taken
vec![Ok(3), Err(true)].should().contain_err().which_value().should().be_true();

// or use with_value_that() clause to test the value with a predicator
vec![Ok(3), Err("Hello".to_owned())]
    .should()
    .contain_err()
    .with_value_that(|e| e.starts_with("H"));

// or use with_value() clause to verify the Err value directly, if Err
// type implements PartialEq
vec![Ok(3u32), Err(5u32)].should().contain_err().with_value(5u32);
```

### Working with Option

```rust
use fluent_assertions::*;

// test option to be Some(_)
Some(1).should().be_some();

// use clause with_value_test() to test inner value
Some("Hello World").should().be_some().with_value_that(|v| v.starts_with("H"));

// or use with_value() clause, if inner value implementing PartialEq
Some(42).should().be_some().with_value(42);

// can also use which_value() clause to start another test statement
Some(String::from("World"))
    .should()
    .be_some()
    .which_value()
    .should()
    .start_with("W");

// if subject is a reference, which_value() is also a reference
(&Some(3)).should().be_some().which_value().should().be(&3);

// test option to be None
None::<u32>.should().be_none();
```

### Working with Path

```rust
use fluent_assertions::*;
use std::path::Path;

// test subject path exists
Path::new("/tmp/file").should().exist();

// test path not exists
Path::new("/tmp/file").should().not_exist();

// test path points to a file
Path::new("/tmp/file").should().be_a_file();

// test path points to a directory
Path::new("/tmp/dir/").should().be_a_directory();

// test path has a valid filename
Path::new("/tmp/file").should().have_file_name(&"file");
```

### Working with Result

```rust
use fluent_assertions::*;

// test subject to be Ok(_)
Result::Ok::<usize, usize>(1).should().be_ok();

// use with_value_that() clause to test inner value
Result::Ok::<usize, usize>(1).should().be_ok().with_value_that(|v| v < &3);

// or use with_value() clause if T implementing PartialEq
Result::Ok::<bool, ()>(true).should().be_ok().with_value(true);

// can also use which_value() clause to start a new test statement for inner value
Result::Ok::<String, ()>(String::from("Hello"))
    .should()
    .be_ok()
    .which_value()
    .should()
    .start_with("H");

// if subject is reference, which_value() returns reference as well
Result::Ok::<u32, ()>(14).to_ref().should().be_ok().which_value().should().be(&14);

// same syntax for Err as well
Result::Err::<usize, usize>(1).should().be_err();
Result::Err::<usize, usize>(3).should().be_err().with_value_that(|v| v > &2);
Result::Err::<(), bool>(true).should().be_err().with_value(true);
Result::Err::<(), String>(String::from("Hello"))
    .should()
    .be_err()
    .which_value()
    .should()
    .start_with("H");
Result::Err::<(), u32>(14).to_ref().should().be_err().which_value().should().be(&14);
```

### Working with String

```rust
use fluent_assertions::*;

// test subject.to_string() equal the target string
"Hello".should().equal_string("Hello");

// test subject starting with specified string
"Hello".should().start_with("H");

// test subject ending with specified string
"Hello".should().end_with("o");

// test subject containing specified string
"Hello".should().contain("ell");

// test subject to be an empty string
"".should().be_empty_string();

// test subject matches a regex pattern
"abcd".should().match_regex("^a.*d$");
```

### Working with enum

```rust
pub mod my_enum {
    use fluent_assertions::EnumAssertions;

    // the derive macro here will create a trait MyEnumAssertions in the same namespace
    // and same visibility. Use the assertions in your test to enable it.
    #[derive(EnumAssertions)]
    pub enum MyEnum {
        First,
        Second(String),
        Third(u32, u32, bool),
        Fourth { apple: f32, banana: u64, orange: String },
    }
}

use fluent_assertions::*;
use my_enum::{MyEnum, MyEnumAssertions};

// test the enum is of a specified variant
MyEnum::First.should().be_first();

// use which_value() clause to test the embedded value of the variant
MyEnum::Second("Hello".to_string())
    .should()
    .be_second()
    .which_value()
    .should()
    .start_with("He");

// when there are multiple embedded values, which_value() returns a tuple
MyEnum::Third(3, 5, true)
    .should()
    .be_third()
    .which_value()
    .1
    .should()
    .be_greater_than(4);

// when embeded value is a anonymous struct, which_value() returns a struct with same schema
MyEnum::Fourth {
    apple: 2f32,
    banana: 43,
    orange: "World".to_string(),
}
.should()
.be_fourth()
.which_value()
.banana
.should()
.be(43);
```
