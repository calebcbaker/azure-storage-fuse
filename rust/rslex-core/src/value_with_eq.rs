use crate::{Record, SyncRecord, SyncValue, Value};
use num::Float;
use std::{
    hash::{Hash, Hasher},
    mem,
    mem::discriminant,
};

// masks for the parts of the IEEE 754 float
const SIGN_MASK: u64 = 0x8000000000000000u64;
const EXP_MASK: u64 = 0x7ff0000000000000u64;
const MAN_MASK: u64 = 0x000fffffffffffffu64;

// canonical raw bit patterns (for hashing)
const CANONICAL_NAN_BITS: u64 = 0x7ff8000000000000u64;
const CANONICAL_ZERO_BITS: u64 = 0x0u64;

#[inline]
fn hash_float<F: Float, H: Hasher>(f: &F, state: &mut H) {
    raw_double_bits(f).hash(state);
}

#[inline]
fn raw_double_bits<F: Float>(f: &F) -> u64 {
    if f.is_nan() {
        return CANONICAL_NAN_BITS;
    }

    let (man, exp, sign) = f.integer_decode();
    if man == 0 {
        return CANONICAL_ZERO_BITS;
    }

    let exp_u64 = unsafe { mem::transmute::<i16, u16>(exp) } as u64;
    let sign_u64 = if sign > 0 { 1u64 } else { 0u64 };
    (man & MAN_MASK) | ((exp_u64 << 52) & EXP_MASK) | ((sign_u64 << 63) & SIGN_MASK)
}

macro_rules! impl_value_eq {
    (
        $name:ident,
        $value_type:ident,
        $value_eq_fn:ident,
        $value_hash_fn:ident,
        $record_type:ident,
        $record_eq_fn:ident,
        $record_hash_fn:ident
    ) => {
        #[derive(Clone, Debug)]
        pub struct $name {
            pub inner_value: $value_type,
        }

        impl From<$value_type> for $name {
            fn from(value: $value_type) -> $name {
                $name { inner_value: value }
            }
        }

        impl Eq for $name {}

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                $value_eq_fn(&self.inner_value, &other.inner_value)
            }
        }

        impl Hash for $name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                $value_hash_fn(&self.inner_value, state);
            }
        }

        fn $value_eq_fn(lhs: &$value_type, rhs: &$value_type) -> bool {
            if discriminant(lhs) != discriminant(rhs) {
                false
            } else {
                match (lhs, rhs) {
                    ($value_type::Float64(l), $value_type::Float64(r)) => {
                        if l.is_nan() && r.is_nan() {
                            return true;
                        }
                        l.eq(r)
                    },
                    ($value_type::List(l), $value_type::List(r)) => {
                        if l.len() != r.len() {
                            false
                        } else {
                            l.iter().zip(r.iter()).all(|(v1, v2)| $value_eq_fn(v1, v2))
                        }
                    },
                    ($value_type::Record(l), $value_type::Record(r)) => $record_eq_fn(l.as_ref(), r.as_ref()),
                    (l, r) => l.eq(r),
                }
            }
        }

        fn $value_hash_fn<H: Hasher>(value: &$value_type, state: &mut H) {
            let discriminant = discriminant(value);
            discriminant.hash(state);

            match value {
                $value_type::Null => {},
                $value_type::Boolean(v) => v.hash(state),
                $value_type::Int64(v) => hash_float(&(*v as f64), state),
                $value_type::Float64(v) => hash_float(v, state),
                $value_type::String(v) => v.hash(state),
                $value_type::DateTime(v) => v.hash(state),
                $value_type::Binary(v) => v.hash(state),
                $value_type::List(v) => v.iter().for_each(|v| {
                    $value_hash_fn(v, state);
                }),
                $value_type::Record(v) => $record_hash_fn(v.as_ref(), state),
                $value_type::Error(v) => format!("{:?}", v).hash(state),
                $value_type::StreamInfo(v) => format!("{:?}", v).hash(state),
            }
        }

        pub fn $record_eq_fn(lhs: &$record_type, rhs: &$record_type) -> bool {
            if lhs.schema() != rhs.schema() {
                false
            } else {
                lhs.iter().zip(rhs.iter()).all(|(l, r)| $value_eq_fn(l, r))
            }
        }

        pub fn $record_hash_fn<H: Hasher>(record: &$record_type, state: &mut H) {
            record.schema().iter().for_each(|v| v.hash(state));
            record.iter().for_each(|v| $value_hash_fn(v, state));
        }
    };
}

impl_value_eq!(ValueWithEq, Value, value_eq, hash_value, Record, record_eq, hash_record);
impl_value_eq!(
    SyncValueWithEq,
    SyncValue,
    sync_value_eq,
    hash_sync_value,
    SyncRecord,
    sync_record_eq,
    hash_sync_record
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{record, StreamInfo, SyncRecord};
    use chrono::{DateTime, NaiveDateTime, Utc};
    use std::{collections::hash_map::DefaultHasher, rc::Rc};
    use tendril::ByteTendril;

    #[test]
    fn test_equality_for_null_values() {
        let value1 = ValueWithEq::from(Value::Null);
        let value2 = ValueWithEq::from(Value::Null);
        assert_eq!(value1, value2);
    }

    #[test]
    fn test_equality_for_boolean_values() {
        let value1 = ValueWithEq::from(Value::Boolean(true));
        let value2 = ValueWithEq::from(Value::Boolean(true));
        assert_eq!(value1, value2);

        let value2 = ValueWithEq::from(Value::Boolean(false));
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_equality_for_int_values() {
        let value1 = ValueWithEq::from(Value::Int64(12345));
        let value2 = ValueWithEq::from(Value::Int64(12345));
        assert_eq!(value1, value2);

        let value2 = ValueWithEq::from(Value::Int64(12346));
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_equality_for_float_values() {
        let value1 = ValueWithEq::from(Value::Float64(1.5));
        let value2 = ValueWithEq::from(Value::Float64(1.5));
        assert_eq!(value1, value2);

        let value2 = ValueWithEq::from(Value::Float64(-1.5));
        assert_ne!(value1, value2);

        let value2 = ValueWithEq::from(Value::Float64(1.5 + f64::EPSILON));
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_equality_for_float_nan() {
        let value1 = ValueWithEq::from(Value::Float64(f64::NAN));
        let value2 = ValueWithEq::from(Value::Float64(f64::NAN));
        assert_eq!(value1, value2);

        let value2 = ValueWithEq::from(Value::Float64(-1.5));
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_equality_for_float_infinity() {
        let value1 = ValueWithEq::from(Value::Float64(f64::INFINITY));
        let value2 = ValueWithEq::from(Value::Float64(f64::INFINITY));
        assert_eq!(value1, value2);

        let value3 = ValueWithEq::from(Value::Float64(f64::NEG_INFINITY));
        let value4 = ValueWithEq::from(Value::Float64(f64::NEG_INFINITY));
        assert_eq!(value3, value4);

        let value5 = ValueWithEq::from(Value::Float64(-1.5));
        assert_ne!(value1, value3);
        assert_ne!(value1, value5);
    }

    #[test]
    fn test_equality_for_string_values() {
        let value1 = ValueWithEq::from(Value::from("hello there"));
        let value2 = ValueWithEq::from(Value::from("hello there"));
        assert_eq!(value1, value2);

        let value2 = ValueWithEq::from(Value::from("Hello there"));
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_equality_for_datetime_values() {
        let datetime = NaiveDateTime::from_timestamp(1000000, 0);

        let value1 = ValueWithEq::from(Value::from(DateTime::from_utc(datetime, Utc)));
        let value2 = ValueWithEq::from(Value::from(DateTime::from_utc(datetime, Utc)));
        assert_eq!(value1, value2);

        let datetime = NaiveDateTime::from_timestamp(1000000, 1);
        let value2 = ValueWithEq::from(Value::from(DateTime::from_utc(datetime, Utc)));
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_equality_for_binary_values() {
        let value1 = ValueWithEq::from(Value::Binary(ByteTendril::from_slice(b"Hello world!")));
        let value2 = ValueWithEq::from(Value::Binary(ByteTendril::from_slice(b"Hello world!")));
        assert_eq!(value1, value2);

        let value2 = ValueWithEq::from(Value::Binary(ByteTendril::from_slice(b"Hello world")));
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_equality_for_list_values() {
        let value1 = ValueWithEq::from(Value::List(Box::new(vec![
            Value::from(1),
            Value::from(3),
            Value::Binary(ByteTendril::from_slice(b"hello")),
        ])));
        let value2 = ValueWithEq::from(Value::List(Box::new(vec![
            Value::from(1),
            Value::from(3),
            Value::Binary(ByteTendril::from_slice(b"hello")),
        ])));
        assert_eq!(value1, value2);

        let value2 = ValueWithEq::from(Value::List(Box::new(vec![
            Value::from(1),
            Value::from(3),
            Value::Binary(ByteTendril::from_slice(b"world")),
        ])));
        assert_ne!(value1, value2);

        let value2 = ValueWithEq::from(Value::List(Box::new(vec![
            Value::from(1),
            Value::from(3.0),
            Value::Binary(ByteTendril::from_slice(b"hello")),
        ])));
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_equality_for_record_values() {
        let value1 = ValueWithEq::from(Value::Record(Box::new(record! {
            "A" => vec![vec![1, 2]],
            "B" => vec![Value::Null, Value::from("hello"), Value::Binary(ByteTendril::from_slice(b"Hello"))],
        })));
        let value2 = ValueWithEq::from(Value::Record(Box::new(record! {
            "A" => vec![vec![1, 2]],
            "B" => vec![Value::Null, Value::from("hello"), Value::Binary(ByteTendril::from_slice(b"Hello"))],
        })));
        assert_eq!(value1, value2);

        let value2 = ValueWithEq::from(Value::Record(Box::new(record! {
            "A" => vec![vec![1, 2]],
            "C" => vec![Value::Null, Value::from("hello"), Value::Binary(ByteTendril::from_slice(b"Hello"))],
        })));
        assert_ne!(value1, value2);

        let value2 = ValueWithEq::from(Value::Record(Box::new(record! {
            "A" => vec![vec![1, 2]],
            "B" => vec![Value::from("null"), Value::from("hello"), Value::Binary(ByteTendril::from_slice(b"Hello"))],
        })));
        assert_ne!(value1, value2);

        let value2 = ValueWithEq::from(Value::Record(Box::new(record! {
            "A" => vec![vec![1, 2]],
            "B" => vec![Value::Null, Value::from("hello"), Value::Binary(ByteTendril::from_slice(b"world"))],
        })));
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_float_int_equality_in_record() {
        let first = ValueWithEq::from(Value::from(record! { "A" => 1 }));
        let second = ValueWithEq::from(Value::from(record! { "A" => 1.0 }));
        assert_ne!(first, second);
    }

    #[test]
    fn test_equality_for_stream_info_values() {
        let value1 = ValueWithEq::from(Value::StreamInfo(Rc::new(StreamInfo::new(
            "Handler",
            "Resource",
            SyncRecord::empty(),
        ))));
        let value2 = ValueWithEq::from(Value::StreamInfo(Rc::new(StreamInfo::new(
            "Handler",
            "Resource",
            SyncRecord::empty(),
        ))));
        assert_eq!(value1, value2);

        let value2 = ValueWithEq::from(Value::StreamInfo(Rc::new(StreamInfo::new(
            "Handler",
            "Resource2",
            SyncRecord::empty(),
        ))));
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_equality_between_different_value_types() {
        let value1 = ValueWithEq::from(Value::Null);
        let value2 = ValueWithEq::from(Value::from("Null"));
        assert_ne!(value1, value2);

        let value1 = ValueWithEq::from(Value::from("Hello world!"));
        let value2 = ValueWithEq::from(Value::Binary(ByteTendril::from_slice(b"Hello world!")));
        assert_ne!(value1, value2);

        let value1 = ValueWithEq::from(Value::from(123i64));
        let value2 = ValueWithEq::from(Value::from(123f64));
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_different_hash_for_int_and_float() {
        let value1 = ValueWithEq::from(Value::from(123i64));
        let mut hasher = DefaultHasher::new();
        value1.hash(&mut hasher);
        let hash1 = hasher.finish();

        let value2 = ValueWithEq::from(Value::from(123f64));
        let mut hasher = DefaultHasher::new();
        value2.hash(&mut hasher);
        let hash2 = hasher.finish();

        assert_ne!(value1, value2);
        assert_ne!(hash1, hash2);
    }
}
