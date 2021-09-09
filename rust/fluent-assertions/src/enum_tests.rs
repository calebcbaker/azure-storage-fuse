use crate::*;

extern crate self as fluent_assertions;

#[derive(EnumAssertions, Debug)]
enum TestOne {
    A,
    B,
}

#[test]
fn enum_should_be_variant_should_not_panic_if_matches() {
    TestOne::A.should().be_a();
}

#[test]
fn enum_should_be_variant_should_panic_if_not_matches() {
    should_fail_with_message!(TestOne::B.should().be_a(), "expecting subject to be TestOne::A but failed");
}

#[test]
fn enum_should_be_variant_and_return_raw_value() {
    TestOne::A.should().be_a().and().should().be_a();
}

#[derive(EnumAssertions, Debug)]
enum TestTwo {
    A(u32),
    B(String, bool, f64),
    C { c1: String, c2: bool, c3: f32 },
}

#[test]
fn variant_with_single_unnamed_field_which_value_returns_inner_value() {
    TestTwo::A(5).should().be_a().which_value().should().be(5);
}

#[test]
fn variant_with_multiple_unnamed_field_which_value_returns_tuple() {
    TestTwo::B("Hello".to_owned(), true, 42.0)
        .should()
        .be_b()
        .which_value()
        .0
        .should()
        .start_with("H");
}

#[test]
fn variant_with_named_field_which_value_returns_struct() {
    TestTwo::C {
        c1: "Hello".to_owned(),
        c2: true,
        c3: 42.0,
    }
    .should()
    .be_c()
    .which_value()
    .c1
    .should()
    .start_with("H");
}

#[test]
fn variant_with_single_unnamed_field_ref_which_value_returns_ref_inner_value() {
    (&TestTwo::A(5)).should().be_a().which_value().should().be(&5);
}

#[test]
fn variant_with_multiple_unnamed_field_ref_which_value_returns_ref_tuple() {
    (&TestTwo::B("Hello".to_owned(), true, 42.0))
        .should()
        .be_b()
        .which_value()
        .1
        .should()
        .be(&true);
}

#[test]
fn variant_with_named_field_ref_which_value_returns_struct_ref() {
    (&TestTwo::C {
        c1: "Hello".to_owned(),
        c2: true,
        c3: 42.0,
    })
        .should()
        .be_c()
        .which_value()
        .c3
        .should()
        .be(&42.0);
}
