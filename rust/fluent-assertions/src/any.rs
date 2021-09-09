use crate::{
    utils::{AssertionFailure, DebugMessage},
    Assertions,
};
use std::{
    any::{type_name, Any, TypeId},
    borrow::Borrow,
    marker::PhantomData,
    sync::Arc,
};

////////////////////////////////////////////////////////
// a helper trait to include any types that can get &dyn Any
pub trait ToAny {
    fn to_any(&self) -> &dyn Any;
}

impl ToAny for Box<dyn Any> {
    fn to_any(&self) -> &dyn Any {
        self.as_ref()
    }
}

impl ToAny for Box<dyn Any + Send> {
    fn to_any(&self) -> &dyn Any {
        self.as_ref()
    }
}

impl ToAny for Box<dyn Any + Send + Sync> {
    fn to_any(&self) -> &dyn Any {
        self.as_ref()
    }
}

impl ToAny for Arc<dyn Any> {
    fn to_any(&self) -> &dyn Any {
        self.as_ref()
    }
}

impl ToAny for Arc<dyn Any + Send> {
    fn to_any(&self) -> &dyn Any {
        self.as_ref()
    }
}

impl ToAny for Arc<dyn Any + Send + Sync> {
    fn to_any(&self) -> &dyn Any {
        self.as_ref()
    }
}

impl ToAny for &dyn Any {
    fn to_any(&self) -> &dyn Any {
        *self
    }
}

impl ToAny for &(dyn Any + Send) {
    fn to_any(&self) -> &dyn Any {
        *self
    }
}

impl ToAny for &(dyn Any + Send + Sync) {
    fn to_any(&self) -> &dyn Any {
        *self
    }
}

///////////////////////////////////////////////////
pub struct AnyConstraint<S, T: Any> {
    subject: S,
    phantom: PhantomData<T>,
}

impl<S, T: Any> AnyConstraint<S, T> {
    fn new(subject: S) -> Self {
        AnyConstraint {
            subject,
            phantom: PhantomData,
        }
    }

    pub fn and(self) -> S {
        self.subject
    }
}

impl<S: ToAny, T: Any> AnyConstraint<S, T> {
    pub fn which_value(&self) -> &T {
        self.subject.to_any().downcast_ref::<T>().unwrap()
    }

    pub fn with_value_that(&self, pred: impl FnOnce(&T) -> bool) {
        let value = self.subject.to_any().downcast_ref::<T>().unwrap();
        if !pred(value) {
            AssertionFailure::new(format!(
                "should().be_of_type().with_value_that() expecting subject to be of type <{}> and meet condition",
                type_name::<T>()
            ))
            .but(format!("subject type matches but fails the condition"))
            .fail();
        }
    }
}

impl<S: ToAny, T: Any + PartialEq> AnyConstraint<S, T> {
    pub fn with_value<Q: Borrow<T>>(&self, expected: Q) {
        let value = self.subject.to_any().downcast_ref::<T>().unwrap();
        if !value.eq(expected.borrow()) {
            AssertionFailure::new(format!(
                "should().be_of_type().with_value() expecting subject to be of type <{}> and value <{}>",
                type_name::<T>(),
                expected.debug_message()
            ))
            .but(format!("subject type matches but has different value <{}>", value.debug_message()))
            .fail();
        }
    }
}

/// Assertions for types which could retrive a [`Any`](std::any::Any) reference, like [`Box`](std::boxed::Box)<[`Any`](std::any::Any)>
/// , or [`Arc`](std::sync::Arc)<[`Any`](std::any::Any)+[`Send`](std::marker::Send)>, or `&(dyn`[`Any`](std::any::Any)+[`Send`](std::marker::Send)+[`Sync`](std::marker::Sync)`)`.
///
/// ## Examples
/// ```rust
/// use fluent_assertions::*;
/// use std::any::Any;
///
/// // use be_of_type::<T>() to test type of dyn Any
/// let v: Box<dyn Any> = Box::new(3u32);
/// v.should().be_of_type::<u32>();
///
/// // use which_value() clause to test actual value
/// let v: Box<dyn Any> = Box::new("Hello World");
/// v.should().be_of_type::<&str>().which_value().should().start_with("H");
///
/// // can also use with_value() clause to test the value if value implements PartialEq
/// let v: Box<dyn Any> = Box::new(42usize);
/// v.should().be_of_type::<usize>().with_value(42);
///
/// // or use the with_value_that() clause which taks a predicate
/// let v: Box<dyn Any> = Box::new(42usize);
/// v.should().be_of_type::<usize>().with_value_that(|v| *v > 40);
/// ```
pub trait AnyAssertions<S> {
    fn be_of_type<T: Any>(self) -> AnyConstraint<S, T>;
}

impl<S: ToAny> AnyAssertions<S> for Assertions<S> {
    fn be_of_type<T: Any>(self) -> AnyConstraint<S, T> {
        if !self.subject().to_any().is::<T>() {
            AssertionFailure::new(format!(
                "should().be_of_type() expecting subject to be of type <{}> but failed",
                type_name::<T>()
            ))
            .expecting(format!("TypeId to be {:?}", TypeId::of::<T>()))
            .but_was(self.subject().to_any().type_id())
            .fail();
        }

        AnyConstraint::new(self.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use std::{any::Any, sync::Arc};

    #[test]
    fn any_be_of_type_should_not_panic_if_type_matches() {
        let v: Box<dyn Any> = Box::new(3u32);

        v.should().be_of_type::<u32>();
    }

    #[test]
    fn any_be_of_type_should_panic_if_type_mismatches() {
        should_fail_with_message!(
            {
                let v: Box<dyn Any> = Box::new(3u32);
                v.should().be_of_type::<&str>()
            },
            "expecting subject to be of type <&str> but failed"
        );
    }

    #[test]
    fn any_be_of_type_should_not_panic_if_type_matches_which_value_returns_ref_value() {
        let v: Box<dyn Any> = Box::new(3u32);

        v.should().be_of_type::<u32>().which_value().should().be(&3);
    }

    #[test]
    fn any_be_of_type_should_not_panic_if_type_matches_with_value() {
        let v: Box<dyn Any> = Box::new(3u32);

        v.should().be_of_type::<u32>().with_value(3u32);
    }

    #[test]
    fn any_be_of_type_should_not_panic_if_type_matches_with_value_that() {
        let v: Box<dyn Any> = Box::new(3u32);

        v.should().be_of_type::<u32>().with_value_that(|f| *f > 2);
    }

    #[test]
    fn any_be_of_type_should_panic_if_type_matches_but_value_mismatch() {
        should_fail_with_message!(
            {
                let v: Box<dyn Any> = Box::new(3u32);
                v.should().be_of_type::<u32>().with_value(4u32)
            },
            "expecting subject to be of type <u32> and value <4> but: subject type matches but has different value <3>"
        );
    }

    #[test]
    fn any_be_of_type_should_panic_if_type_matches_with_value_that_failed() {
        should_fail_with_message!(
            {
                let v: Box<dyn Any> = Box::new(3u32);

                v.should().be_of_type::<u32>().with_value_that(|f| *f > 5);
            },
            "expecting subject to be of type <u32> and meet condition but: subject type matches but fails the condition"
        );
    }

    #[test]
    fn multiple_any_types_work() {
        let box_any: Box<dyn Any> = Box::new(3u32);
        let box_any_send: Box<dyn Any + Send> = Box::new(3u32);
        let box_any_send_sync: Box<dyn Any + Send + Sync> = Box::new(3u32);
        let ref_any: &dyn Any = box_any.as_ref();
        let ref_any_send: &(dyn Any + Send) = box_any_send.as_ref();
        let ref_any_send_sync: &(dyn Any + Send + Sync) = box_any_send_sync.as_ref();
        let arc_any: Arc<dyn Any> = Arc::new(3u32);
        let arc_any_send: Arc<dyn Any + Send> = Arc::new(3u32);
        let arc_any_send_sync: Arc<dyn Any + Send + Sync> = Arc::new(3u32);

        ref_any.should().be_of_type::<u32>().which_value().should().be(&3);
        ref_any_send.should().be_of_type::<u32>().which_value().should().be(&3);
        ref_any_send_sync.should().be_of_type::<u32>().which_value().should().be(&3);
        box_any.should().be_of_type::<u32>().which_value().should().be(&3);
        box_any_send.should().be_of_type::<u32>().which_value().should().be(&3);
        box_any_send_sync.should().be_of_type::<u32>().which_value().should().be(&3);
        arc_any.should().be_of_type::<u32>().which_value().should().be(&3);
        arc_any_send.should().be_of_type::<u32>().which_value().should().be(&3);
        arc_any_send_sync.should().be_of_type::<u32>().which_value().should().be(&3);
    }
}
