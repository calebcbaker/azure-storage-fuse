use super::{ErrorValue, Record, StreamInfo, SyncErrorValue, SyncRecord, SyncRecordSchema};
use crate::{FieldSelector, FieldSelectorBuilder};
use chrono::prelude::*;
#[cfg(any(feature = "test_helper", test))]
use fluent_assertions::EnumAssertions;
use itertools::Itertools;
use std::{
    cmp::{Ordering, PartialEq},
    collections::{hash_map::RandomState, HashMap},
    convert::TryFrom,
    fmt::{Display, Error, Formatter},
    rc::Rc,
    sync::Arc,
};
use strum_macros::{Display as StrumDisplay, EnumDiscriminants};
use tendril::{ByteTendril, StrTendril};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Error)]
#[error("An error happened casting value {actual} to type {expected}.")]
pub struct ValueCastError {
    pub expected: ValueKind,
    pub actual: SyncValue,
}

/// A dynamic type wrapper around fields in row-oriented data.
///
/// This object is not thread-safe. For a thread-safe version, see [`SyncValue`].
/// When possible, prefer to use this version for lower overhead.
#[cfg_attr(any(feature = "test_helper", test), derive(EnumAssertions))]
#[derive(Clone, Debug, EnumDiscriminants)]
#[strum_discriminants(name(ValueKind), derive(StrumDisplay))]
#[repr(u8)]
pub enum Value {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(StrTendril),
    DateTime(DateTime<Utc>),
    Binary(ByteTendril),
    #[allow(clippy::box_vec)]
    List(Box<Vec<Value>>),
    Record(Box<Record>),
    Error(Box<ErrorValue>),
    StreamInfo(Rc<StreamInfo>),
}

impl Value {
    /// Produces a json-like string representation of this Value.
    /// TODO: put proper escape of string and rename this to to_json
    pub fn to_json_like_string(&self) -> String {
        match self {
            Value::String(s) => format!("\"{}\"", s.replace('"', "\\\"")),
            Value::Boolean(v) => {
                if *v {
                    "true".to_owned()
                } else {
                    "false".to_owned()
                }
            },
            Value::Record(r) => r.to_json_like_string(),
            Value::Null => "null".to_owned(),
            v => v.to_string(),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Boolean(l), Value::Boolean(r)) => l.eq(r),
            (Value::Int64(l), Value::Int64(r)) => l.eq(r),
            (Value::Float64(l), Value::Float64(r)) => l.eq(r),
            (Value::String(l), Value::String(r)) => l.eq(r),
            (Value::DateTime(l), Value::DateTime(r)) => l.eq(r),
            (Value::Binary(l), Value::Binary(r)) => l.eq(r),
            (Value::List(l), Value::List(r)) => l.eq(r),
            (Value::Record(l), Value::Record(r)) => l.eq(r),
            (Value::Error(l), Value::Error(r)) => l.eq(r),
            (Value::StreamInfo(l), Value::StreamInfo(r)) => l.eq(&r),

            // Mixed types that could have the same numeric value.
            // i64s have more precision, so equality in f64's number
            // space means they *may* be the same in long's number space.
            (Value::Int64(i), Value::Float64(f)) => *f == (*i as f64) && *i == (*f as i64),
            (Value::Float64(f), Value::Int64(i)) => *f == (*i as f64) && *i == (*f as i64),

            (l, r) => {
                debug_assert!(
                    ValueKind::from(l) as i8 != ValueKind::from(r) as i8,
                    "Are you missing a Value kind pair in PartialEq implementation?"
                );
                false
            },
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Float64(f), _) | (_, Value::Float64(f)) => {
                if f.is_nan() {
                    return None; // NaN is never comparable. even to other kinds to be consistent.
                }
            },
            _ => {},
        }
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Value::Boolean(l), Value::Boolean(r)) => l.partial_cmp(r),
            (Value::Int64(left), Value::Int64(right)) => left.partial_cmp(right),
            (Value::Float64(left), Value::Float64(right)) => left.partial_cmp(right),
            (Value::String(left), Value::String(right)) => left.partial_cmp(right),
            (Value::DateTime(left), Value::DateTime(right)) => left.partial_cmp(right),
            (Value::Binary(l), Value::Binary(r)) => l.partial_cmp(r),
            (Value::List(l), Value::List(r)) => l.partial_cmp(r),
            (Value::Record(l), Value::Record(r)) => l.partial_cmp(&r),
            (Value::Error(l), Value::Error(r)) => l.partial_cmp(&r),
            (Value::StreamInfo(l), Value::StreamInfo(r)) => l.partial_cmp(&r),

            (Value::Int64(i), Value::Float64(f)) => {
                // i64s have more precision, so equality in f64's number
                // space means they *may* be the same in i64's number space.
                let result = (*i as f64).partial_cmp(f);
                if result != Some(Ordering::Equal) {
                    result
                } else {
                    i.partial_cmp(&(*f as i64))
                }
            },
            (Value::Float64(f), Value::Int64(i)) => {
                // i64s have more precision, so equality in f64's number
                // space means they *may* be the same in i64's number space.
                let result = f.partial_cmp(&(*i as f64));
                if result != Some(Ordering::Equal) {
                    result
                } else {
                    (*f as i64).partial_cmp(i)
                }
            },
            (left, right) => {
                let difference = ValueKind::from(left) as i8 - ValueKind::from(right) as i8;
                debug_assert!(difference != 0, "Missing compare for same kind.");
                difference.partial_cmp(&0)
            },
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Value::Null => f.write_str("Null"),
            Value::Boolean(v) => {
                if *v {
                    f.write_str("True")
                } else {
                    f.write_str("False")
                }
            },
            Value::Int64(v) => f.write_str(v.to_string().as_str()),
            Value::Float64(v) => f.write_str(v.to_string().as_str()),
            Value::String(v) => f.write_str(v.to_string().as_str()),
            Value::DateTime(v) => f.write_str(v.to_rfc3339().as_str()),
            Value::Binary(v) => f.write_fmt(format_args!("[{} bytes]", v.len())),
            Value::List(v) => f.write_fmt(format_args!("[{}]", v.iter().map(|v| v.to_json_like_string()).join(", ").as_str())),
            Value::Record(v) => f.write_str(v.to_string().as_str()),
            Value::Error(v) => f.write_str(v.to_string().as_str()),
            Value::StreamInfo(v) => f.write_str(v.to_string().as_str()),
        }
    }
}

/// A dynamic type wrapper around fields in row-oriented data.
///
/// This is the thread-safe version of [`Value`]. When possible, prefer to use the
/// non-thread-safe version for lower overhead.
#[derive(Clone, Debug, EnumDiscriminants)]
#[strum_discriminants(name(SyncValueKind))]
#[repr(u8)]
pub enum SyncValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    DateTime(DateTime<Utc>),
    Binary(Box<[u8]>),
    #[allow(clippy::box_vec)]
    List(Box<Vec<SyncValue>>),
    Record(Box<SyncRecord>),
    Error(Box<SyncErrorValue>),
    StreamInfo(Arc<StreamInfo>),
}

impl SyncValue {
    /// Produces a json-like string representation of this Value.
    ///
    /// For most values, this returns the same as `to_string`. However, String, Binary, and DateTime
    /// values will have their contents quoted. Note that the string is not guaranteed to be valid-JSON.
    pub fn to_json_like_string(&self) -> String {
        match self {
            SyncValue::String(s) => format!("\"{}\"", s.replace('"', "\\\"")),
            v => v.to_string(),
        }
    }
}

impl Display for SyncValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            SyncValue::Null => f.write_str("Null"),
            SyncValue::Boolean(v) => {
                if *v {
                    f.write_str("True")
                } else {
                    f.write_str("False")
                }
            },
            SyncValue::Int64(v) => f.write_str(v.to_string().as_str()),
            SyncValue::Float64(v) => f.write_str(v.to_string().as_str()),
            SyncValue::String(v) => f.write_str(v.to_string().as_str()),
            SyncValue::DateTime(v) => f.write_str(v.to_rfc3339().as_str()),
            SyncValue::Binary(v) => f.write_fmt(format_args!("[{} bytes]", v.len())),
            SyncValue::List(v) => f.write_fmt(format_args!("[{}]", v.iter().map(|v| v.to_json_like_string()).join(", ").as_str())),
            SyncValue::Record(v) => f.write_str(v.to_string().as_str()),
            SyncValue::Error(v) => f.write_str(v.to_string().as_str()),
            SyncValue::StreamInfo(v) => f.write_str(v.to_string().as_str()),
        }
    }
}

impl From<SyncValue> for Value {
    fn from(value: SyncValue) -> Value {
        match value {
            SyncValue::Null => Value::Null,
            SyncValue::Boolean(v) => Value::Boolean(v),
            SyncValue::Int64(v) => Value::Int64(v),
            SyncValue::Float64(v) => Value::Float64(v),
            SyncValue::String(v) => Value::String(StrTendril::from(v)),
            SyncValue::DateTime(v) => Value::DateTime(v),
            SyncValue::Binary(v) => Value::Binary(ByteTendril::from_slice(&v)),
            SyncValue::List(l) => Value::List(Box::new(l.into_iter().map(Value::from).collect_vec())),
            SyncValue::Record(v) => Value::Record(Box::new(Record::from(*v))),
            SyncValue::Error(v) => Value::Error(Box::new(ErrorValue::from(*v))),
            SyncValue::StreamInfo(v) => Value::StreamInfo(match Arc::try_unwrap(v) {
                Ok(v) => Rc::new(v),
                Err(v) => Rc::new(v.as_ref().clone()),
            }),
        }
    }
}

impl From<&SyncValue> for Value {
    fn from(value: &SyncValue) -> Self {
        match value {
            SyncValue::Null => Value::Null,
            SyncValue::Boolean(v) => Value::Boolean(*v),
            SyncValue::Int64(v) => Value::Int64(*v),
            SyncValue::Float64(v) => Value::Float64(*v),
            SyncValue::String(v) => Value::String(StrTendril::from(v.as_str())),
            SyncValue::DateTime(v) => Value::DateTime(*v),
            SyncValue::Binary(v) => Value::Binary(ByteTendril::from_slice(v)),
            SyncValue::List(l) => Value::List(Box::new(l.iter().map(Value::from).collect_vec())),
            SyncValue::Record(v) => Value::Record(Box::new(Record::from(v.as_ref()))),
            SyncValue::Error(v) => Value::Error(Box::new(ErrorValue::from(v.as_ref().clone()))),
            SyncValue::StreamInfo(v) => Value::StreamInfo(Rc::new(v.as_ref().clone())),
        }
    }
}

impl From<Value> for SyncValue {
    fn from(value: Value) -> SyncValue {
        match value {
            Value::Null => SyncValue::Null,
            Value::Boolean(v) => SyncValue::Boolean(v),
            Value::Int64(v) => SyncValue::Int64(v),
            Value::Float64(v) => SyncValue::Float64(v),
            Value::String(v) => SyncValue::String(v.to_string()),
            Value::DateTime(v) => SyncValue::DateTime(v),
            Value::Binary(v) => SyncValue::Binary(v.to_vec().into_boxed_slice()),
            Value::List(l) => SyncValue::List(Box::new(l.into_iter().map(SyncValue::from).collect_vec())),
            Value::Record(v) => SyncValue::Record(Box::new(SyncRecord::from(*v))),
            Value::Error(v) => SyncValue::Error(Box::new(SyncErrorValue::from(*v))),
            Value::StreamInfo(v) => SyncValue::StreamInfo(match Rc::try_unwrap(v) {
                Ok(v) => Arc::new(v),
                Err(v) => Arc::new(v.as_ref().clone()),
            }),
        }
    }
}

impl From<&Value> for SyncValue {
    fn from(value: &Value) -> SyncValue {
        match value {
            Value::Null => SyncValue::Null,
            Value::Boolean(v) => SyncValue::Boolean(*v),
            Value::Int64(v) => SyncValue::Int64(*v),
            Value::Float64(v) => SyncValue::Float64(*v),
            Value::String(v) => SyncValue::String(v.to_string()),
            Value::DateTime(v) => SyncValue::DateTime(*v),
            Value::Binary(v) => SyncValue::Binary(v.to_vec().into_boxed_slice()),
            Value::List(l) => SyncValue::List(Box::new(l.iter().map(SyncValue::from).collect_vec())),
            Value::Record(v) => SyncValue::Record(Box::new(SyncRecord::from(v.as_ref()))),
            Value::Error(v) => SyncValue::Error(Box::new(SyncErrorValue::from(v.as_ref().clone()))),
            Value::StreamInfo(v) => SyncValue::StreamInfo(Arc::new(v.as_ref().clone())),
        }
    }
}

impl PartialEq for SyncValue {
    fn eq(&self, other: &Self) -> bool {
        // This must match eq for Value.
        match (self, other) {
            (SyncValue::Null, SyncValue::Null) => true,
            (SyncValue::Boolean(l), SyncValue::Boolean(r)) => l.eq(r),
            (SyncValue::Int64(l), SyncValue::Int64(r)) => l.eq(r),
            (SyncValue::Float64(l), SyncValue::Float64(r)) => l.eq(r),
            (SyncValue::String(l), SyncValue::String(r)) => l.eq(r),
            (SyncValue::DateTime(l), SyncValue::DateTime(r)) => l.eq(r),
            (SyncValue::Binary(l), SyncValue::Binary(r)) => l.eq(r),
            (SyncValue::List(l), SyncValue::List(r)) => l.eq(r),
            (SyncValue::Record(l), SyncValue::Record(r)) => l.eq(r),
            (SyncValue::Error(l), SyncValue::Error(r)) => l.eq(r),
            (SyncValue::StreamInfo(l), SyncValue::StreamInfo(r)) => l.eq(&r),

            // Mixed types that could have the same numeric value.
            // i64s have more precision, so equality in f64's number
            // space means they *may* be the same in long's number space.
            (SyncValue::Int64(i), SyncValue::Float64(f)) => *f == (*i as f64) && *i == (*f as i64),
            (SyncValue::Float64(f), SyncValue::Int64(i)) => *f == (*i as f64) && *i == (*f as i64),

            (l, r) => {
                debug_assert!(
                    SyncValueKind::from(l) as i8 != SyncValueKind::from(r) as i8,
                    "Are you missing a Value kind pair in PartialEq implementation?"
                );
                false
            },
        }
    }
}

impl PartialEq<SyncValue> for Value {
    fn eq(&self, other: &SyncValue) -> bool {
        // This must match eq for Value.
        match (self, other) {
            (Value::Null, SyncValue::Null) => true,
            (Value::Boolean(l), SyncValue::Boolean(r)) => l.eq(r),
            (Value::Int64(l), SyncValue::Int64(r)) => l.eq(r),
            (Value::Float64(l), SyncValue::Float64(r)) => l.eq(r),
            (Value::String(l), SyncValue::String(r)) => l.as_ref().eq(r.as_str()),
            (Value::DateTime(l), SyncValue::DateTime(r)) => l.eq(r),
            (Value::Binary(l), SyncValue::Binary(r)) => l.as_ref() == r.as_ref(),
            (Value::List(l), SyncValue::List(r)) => l.as_ref() == r.as_ref(),
            (Value::Record(l), SyncValue::Record(r)) => l.as_ref() == r.as_ref(),
            (Value::Error(l), SyncValue::Error(r)) => l.as_ref() == r.as_ref(),
            (Value::StreamInfo(l), SyncValue::StreamInfo(r)) => l.as_ref() == r.as_ref(),

            // Mixed types that could have the same numeric value.
            // i64s have more precision, so equality in f64's number
            // space means they *may* be the same in long's number space.
            (Value::Int64(i), SyncValue::Float64(f)) => *f == (*i as f64) && *i == (*f as i64),
            (Value::Float64(f), SyncValue::Int64(i)) => *f == (*i as f64) && *i == (*f as i64),

            (l, r) => {
                debug_assert!(
                    ValueKind::from(l) as i8 != SyncValueKind::from(r) as i8,
                    "Are you missing a Value kind pair in PartialEq implementation?"
                );
                false
            },
        }
    }
}

impl PartialEq<Value> for SyncValue {
    fn eq(&self, other: &Value) -> bool {
        other == self
    }
}

impl PartialOrd for SyncValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Must agree with Value.partial_cmp
        match (self, other) {
            (SyncValue::Float64(f), _) | (_, SyncValue::Float64(f)) => {
                if f.is_nan() {
                    return None; // NaN is never comparable. even to other kinds to be consistent.
                }
            },
            _ => {},
        }
        match (self, other) {
            (SyncValue::Null, SyncValue::Null) => Some(Ordering::Equal),
            (SyncValue::Null, _) => Some(Ordering::Less),
            (_, SyncValue::Null) => Some(Ordering::Greater),
            (SyncValue::Boolean(l), SyncValue::Boolean(r)) => l.partial_cmp(r),
            (SyncValue::Int64(left), SyncValue::Int64(right)) => left.partial_cmp(right),
            (SyncValue::Float64(left), SyncValue::Float64(right)) => left.partial_cmp(right),
            (SyncValue::String(left), SyncValue::String(right)) => left.partial_cmp(right),
            (SyncValue::DateTime(left), SyncValue::DateTime(right)) => left.partial_cmp(right),
            (SyncValue::Binary(l), SyncValue::Binary(r)) => l.partial_cmp(r),
            (SyncValue::List(l), SyncValue::List(r)) => l.partial_cmp(r),
            (SyncValue::Record(l), SyncValue::Record(r)) => l.partial_cmp(&r),
            (SyncValue::Error(l), SyncValue::Error(r)) => l.partial_cmp(&r),
            (SyncValue::StreamInfo(l), SyncValue::StreamInfo(r)) => l.partial_cmp(&r),

            (SyncValue::Int64(i), SyncValue::Float64(f)) => {
                // i64s have more precision, so equality in f64's number
                // space means they *may* be the same in i64's number space.
                let result = (*i as f64).partial_cmp(f);
                if result != Some(Ordering::Equal) {
                    result
                } else {
                    i.partial_cmp(&(*f as i64))
                }
            },
            (SyncValue::Float64(f), SyncValue::Int64(i)) => {
                // i64s have more precision, so equality in f64's number
                // space means they *may* be the same in i64's number space.
                let result = f.partial_cmp(&(*i as f64));
                if result != Some(Ordering::Equal) {
                    result
                } else {
                    (*f as i64).partial_cmp(i)
                }
            },
            (left, right) => {
                let difference = SyncValueKind::from(left) as i8 - SyncValueKind::from(right) as i8;
                debug_assert!(difference != 0, "Missing compare for same kind.");
                difference.partial_cmp(&0)
            },
        }
    }
}

macro_rules! impl_from {
    ($value_kind:tt, $value_type:ty, $sync_value_type:ty) => {
        impl From<$value_type> for Value {
            fn from(value: $value_type) -> Value {
                Value::$value_kind(value)
            }
        }

        impl From<$sync_value_type> for SyncValue {
            fn from(value: $sync_value_type) -> SyncValue {
                SyncValue::$value_kind(value)
            }
        }

        impl TryFrom<Value> for $value_type {
            type Error = ValueCastError;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                if let Value::$value_kind(v) = value {
                    Ok(v)
                } else {
                    Err(ValueCastError {
                        expected: ValueKind::$value_kind,
                        actual: value.into(),
                    })
                }
            }
        }

        impl<'a> TryFrom<&'a Value> for &'a $value_type {
            type Error = ValueCastError;

            fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
                if let Value::$value_kind(v) = value {
                    Ok(v)
                } else {
                    Err(ValueCastError {
                        expected: ValueKind::$value_kind,
                        actual: value.into(),
                    })
                }
            }
        }

        impl TryFrom<SyncValue> for $sync_value_type {
            type Error = ValueCastError;

            fn try_from(value: SyncValue) -> Result<Self, Self::Error> {
                if let SyncValue::$value_kind(v) = value {
                    Ok(v)
                } else {
                    Err(ValueCastError {
                        expected: ValueKind::$value_kind,
                        actual: value.into(),
                    })
                }
            }
        }

        impl TryFrom<&SyncValue> for $sync_value_type {
            type Error = ValueCastError;

            fn try_from(value: &SyncValue) -> Result<Self, Self::Error> {
                <$sync_value_type>::try_from(value.clone())
            }
        }
    };
}

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Value::Null
    }
}

impl From<()> for SyncValue {
    fn from(_: ()) -> Self {
        SyncValue::Null
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(StrTendril::from(value))
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(StrTendril::from(value))
    }
}

impl TryFrom<Value> for String {
    type Error = ValueCastError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        StrTendril::try_from(value).map(|v| v.to_string())
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Value::Binary(ByteTendril::from_slice(value))
    }
}

impl From<&[u8]> for SyncValue {
    fn from(value: &[u8]) -> Self {
        SyncValue::Binary(value.into())
    }
}

impl From<&str> for SyncValue {
    fn from(value: &str) -> Self {
        SyncValue::String(value.to_string())
    }
}

impl<T: Into<SyncValue>> From<Vec<T>> for SyncValue {
    fn from(value: Vec<T>) -> Self {
        SyncValue::List(Box::new(value.into_iter().map(|v| v.into()).collect_vec()))
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(value: Vec<T>) -> Self {
        Value::List(Box::new(value.into_iter().map(|v| v.into()).collect_vec()))
    }
}

impl TryFrom<&SyncValue> for SyncRecord {
    type Error = ValueCastError;

    fn try_from(value: &SyncValue) -> Result<Self, Self::Error> {
        if let SyncValue::Record(r) = value {
            Ok((**r).clone())
        } else {
            Err(ValueCastError {
                expected: ValueKind::Record,
                actual: value.clone(),
            })
        }
    }
}

impl<T> From<Option<T>> for Value
where Value: From<T>
{
    fn from(opt: Option<T>) -> Self {
        opt.map_or(Value::Null, Value::from)
    }
}

impl<T> From<Option<T>> for SyncValue
where SyncValue: From<T>
{
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => SyncValue::from(v),
            None => SyncValue::Null,
        }
    }
}

impl<S, T> From<HashMap<S, T>> for SyncRecord
where
    SyncValue: From<T>,
    S: Into<String> + Ord,
{
    fn from(map: HashMap<S, T, RandomState>) -> Self {
        let mut values = vec![];
        let mut keys = vec![];
        // HashMap doesn't keep the order, and the iter order is random
        // this is not a perfect solution (to sort the key) but at least
        // give determinism
        for (key, value) in map.into_iter().map(|(k, v)| (k.into(), v.into())).sorted_by_key(|kv| kv.0.clone()) {
            keys.push(key.into());
            values.push(value);
        }

        SyncRecord::new(values, SyncRecordSchema::new(keys).unwrap())
    }
}

impl_from!(Boolean, bool, bool);
impl_from!(String, StrTendril, String);
impl_from!(Int64, i64, i64);
impl_from!(Float64, f64, f64);
impl_from!(DateTime, DateTime<Utc>, DateTime<Utc>);
impl_from!(Binary, ByteTendril, Box<[u8]>);
impl_from!(Error, Box<ErrorValue>, Box<SyncErrorValue>);
impl_from!(Record, Box<Record>, Box<SyncRecord>);
impl_from!(List, Box<Vec<Value>>, Box<Vec<SyncValue>>);
impl_from!(StreamInfo, Rc<StreamInfo>, Arc<StreamInfo>);

macro_rules! impl_from_box {
    ($value_type:ty, $sync_value_type:ty) => {
        impl From<$value_type> for Value {
            fn from(value: $value_type) -> Value {
                Value::from(Box::new(value))
            }
        }

        impl From<$sync_value_type> for SyncValue {
            fn from(value: $sync_value_type) -> SyncValue {
                SyncValue::from(Box::new(value))
            }
        }

        impl TryFrom<Value> for $value_type {
            type Error = ValueCastError;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                Box::<$value_type>::try_from(value).map(|b| (*b).clone())
            }
        }

        impl TryFrom<SyncValue> for $sync_value_type {
            type Error = ValueCastError;

            fn try_from(value: SyncValue) -> Result<Self, Self::Error> {
                Box::<$sync_value_type>::try_from(value).map(|b| (*b).clone())
            }
        }
    };
}

impl_from_box!(ErrorValue, SyncErrorValue);
impl_from_box!(Record, SyncRecord);

impl From<StreamInfo> for Value {
    fn from(stream_info: StreamInfo) -> Self {
        Value::StreamInfo(Rc::new(stream_info))
    }
}

impl From<StreamInfo> for SyncValue {
    fn from(stream_info: StreamInfo) -> Self {
        SyncValue::StreamInfo(Arc::new(stream_info))
    }
}

pub trait ValueFunction: Sized {
    type Builder: ValueFunctionBuilder<Function = Self>;

    fn call(&mut self, args: &[Value]) -> Value;
}

pub trait ValueFunctionBuilder: Sized + Send + Sync {
    type Function: ValueFunction<Builder = Self>;

    fn build(&self) -> Self::Function;
}

pub trait RecordFunction: Sized {
    type Builder: RecordFunctionBuilder<Function = Self>;

    fn call(&mut self, record: &Record) -> Value;
}

pub trait RecordFunctionBuilder: Sized + Send + Sync {
    type Function: RecordFunction<Builder = Self>;

    fn build(&self) -> Self::Function;
}

impl<T: FnMut(&[Value]) -> Value + Clone + Send + Sync + 'static> ValueFunction for T {
    type Builder = T;

    fn call(&mut self, args: &[Value]) -> Value {
        self(args)
    }
}

impl<T: FnMut(&[Value]) -> Value + Clone + Send + Sync + 'static> ValueFunctionBuilder for T {
    type Function = T;

    fn build(&self) -> Self {
        self.clone()
    }
}

impl<T: FnMut(&Record) -> Value + Clone + Send + Sync + 'static> RecordFunction for T {
    type Builder = T;

    fn call(&mut self, record: &Record) -> Value {
        self(record)
    }
}

impl<T: FnMut(&Record) -> Value + Clone + Send + Sync + 'static> RecordFunctionBuilder for T {
    type Function = T;

    fn build(&self) -> Self {
        self.clone()
    }
}

pub fn create_record_function_with_field_selector<T: FnMut(&mut dyn FieldSelector, &Record) -> Value + Clone + Send + Sync + 'static>(
    selector: Box<dyn FieldSelector>,
    function: T,
) -> impl RecordFunctionBuilder {
    struct Builder<TInner: FnMut(&mut dyn FieldSelector, &Record) -> Value + Clone + Send + Sync + 'static> {
        builder: Arc<dyn FieldSelectorBuilder>,
        function: TInner,
    }

    impl<TInner: FnMut(&mut dyn FieldSelector, &Record) -> Value + Clone + Send + Sync + 'static> RecordFunctionBuilder for Builder<TInner> {
        type Function = Fn<TInner>;

        fn build(&self) -> Self::Function {
            Fn {
                selector: self.builder.build(),
                function: self.function.clone(),
            }
        }
    }

    struct Fn<TInner: FnMut(&mut dyn FieldSelector, &Record) -> Value + Clone + Send + Sync + 'static> {
        selector: Box<dyn FieldSelector>,
        function: TInner,
    }

    impl<TInner: FnMut(&mut dyn FieldSelector, &Record) -> Value + Clone + Send + Sync + 'static> RecordFunction for Fn<TInner> {
        type Builder = Builder<TInner>;

        fn call(&mut self, record: &Record) -> Value {
            (self.function)(self.selector.as_mut(), record)
        }
    }

    Builder {
        builder: selector.to_builder(),
        function,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{RecordSchema, SyncRecordSchema},
        *,
    };

    #[test]
    fn to_json_like_string_returns_correct_string() {
        assert_eq!(Value::Boolean(true).to_json_like_string(), "true");
        assert_eq!(Value::Boolean(false).to_json_like_string(), "false");
        assert_eq!(Value::Null.to_json_like_string(), "null");

        assert_eq!(
            Value::Record(Box::new(Record::new(
                vec![Value::Int64(1), Value::Int64(2)],
                RecordSchema::try_from(vec!["A", "B"]).unwrap()
            )))
            .to_json_like_string(),
            "{\"A\": 1, \"B\": 2}"
        );
    }

    #[test]
    fn size_of_is_24_bytes() {
        assert_eq!(std::mem::size_of::<Value>(), 24)
    }

    #[test]
    fn eq_str_same_contents_different_tendrils_returns_true() {
        let first = Value::String(StrTendril::from("value"));
        let second = Value::String(StrTendril::from("value"));
        assert_eq!(first, second);
        // SyncValue eq should be equivalent.
        assert_eq!(SyncValue::from(first), SyncValue::from(second));
    }

    #[test]
    fn eq_str_same_tendril_returns_true() {
        let instance = StrTendril::from("value");
        let first = Value::String(instance.clone());
        let second = Value::String(instance);
        assert_eq!(first, second);
        // SyncValue eq should be equivalent.
        assert_eq!(SyncValue::from(first), SyncValue::from(second));
    }

    #[test]
    fn eq_str_different_values_returns_false() {
        let first = Value::String(StrTendril::from("value1"));
        let second = Value::String(StrTendril::from("value2"));
        assert_ne!(first, second);
        // SyncValue eq should be equivalent.
        assert_ne!(SyncValue::from(first), SyncValue::from(second));
    }

    //noinspection DuplicatedCode
    #[test]
    fn eq_for_same_values_returns_true() {
        assert_eq!(Value::Null, Value::Null);
        assert_eq!(Value::Boolean(true), Value::Boolean(true));
        assert_eq!(Value::Int64(1), Value::Int64(1));
        assert_eq!(Value::Float64(1.0), Value::Float64(1.0));
        assert_eq!(Value::String(StrTendril::from("asd")), Value::String(StrTendril::from("asd")));
        assert_eq!(
            Value::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 35)),
            Value::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 35))
        );
        assert_eq!(
            Value::Binary(ByteTendril::from_slice(&[1, 2, 3])),
            Value::Binary(ByteTendril::from_slice(&[1, 2, 3]))
        );
        assert_eq!(
            Value::List(Box::new(vec![Value::Null, Value::Int64(2)])),
            Value::List(Box::new(vec![Value::Null, Value::Int64(2)]))
        );
        assert_eq!(
            Value::Record(Box::new(Record::new(
                vec![Value::Int64(2), Value::Float64(1.2)],
                RecordSchema::try_from(vec!["A", "B"]).unwrap()
            ))),
            Value::Record(Box::new(Record::new(
                vec![Value::Int64(2), Value::Float64(1.2)],
                RecordSchema::try_from(vec!["A", "B"]).unwrap()
            )))
        );
        assert_eq!(
            Value::Error(Box::new(ErrorValue::new("Code", Value::Int64(2)))),
            Value::Error(Box::new(ErrorValue::new("Code", Value::Int64(2))))
        );
        assert_eq!(
            Value::StreamInfo(Rc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty()))),
            Value::StreamInfo(Rc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty())))
        );
    }

    #[test]
    fn eq_for_same_sync_values_returns_true() {
        assert_eq!(SyncValue::Null, SyncValue::Null);
        assert_eq!(SyncValue::Boolean(true), SyncValue::Boolean(true));
        assert_eq!(SyncValue::Int64(1), SyncValue::Int64(1));
        assert_eq!(SyncValue::Float64(1.0), SyncValue::Float64(1.0));
        assert_eq!(SyncValue::String(String::from("asd")), SyncValue::String(String::from("asd")));
        assert_eq!(
            SyncValue::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 35)),
            SyncValue::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 35))
        );
        assert_eq!(
            SyncValue::Binary(vec![1, 2, 3].into_boxed_slice()),
            SyncValue::Binary(vec![1, 2, 3].into_boxed_slice())
        );
        assert_eq!(
            SyncValue::List(Box::new(vec![SyncValue::Null, SyncValue::Int64(2)])),
            SyncValue::List(Box::new(vec![SyncValue::Null, SyncValue::Int64(2)]))
        );
        assert_eq!(
            SyncValue::Record(Box::new(SyncRecord::new(
                vec![SyncValue::Int64(2), SyncValue::Float64(1.2)],
                SyncRecordSchema::try_from(vec!["A", "B"]).unwrap()
            ))),
            SyncValue::Record(Box::new(SyncRecord::new(
                vec![SyncValue::Int64(2), SyncValue::Float64(1.2)],
                SyncRecordSchema::try_from(vec!["A", "B"]).unwrap()
            )))
        );
        assert_eq!(
            SyncValue::Error(Box::new(SyncErrorValue::new("Code".into(), SyncValue::Int64(2)))),
            SyncValue::Error(Box::new(SyncErrorValue::new("Code".into(), SyncValue::Int64(2))))
        );
        assert_eq!(
            SyncValue::StreamInfo(Arc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty()))),
            SyncValue::StreamInfo(Arc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty())))
        );
    }

    #[test]
    fn eq_for_same_numeric_values_returns_true() {
        assert_eq!(Value::Int64(1), Value::Float64(1.0));
        assert_eq!(Value::Float64(2.0), Value::Int64(2));
    }

    #[test]
    fn eq_for_same_numeric_sync_values_returns_true() {
        assert_eq!(SyncValue::Int64(1), SyncValue::Float64(1.0));
        assert_eq!(SyncValue::Float64(2.0), SyncValue::Int64(2));
    }

    #[test]
    fn eq_for_nan_values_compares_returns_false() {
        // The partial part of PartialEq
        assert_ne!(Value::Int64(1), Value::Float64(f64::NAN));
        assert_ne!(Value::Float64(1.0), Value::Float64(f64::NAN));
        assert_ne!(Value::Float64(f64::NAN), Value::Float64(1.0));
        assert_ne!(Value::Float64(f64::NAN), Value::Float64(f64::NAN)); // Surprising IEEE defined.
    }

    #[test]
    fn eq_for_nan_sync_values_compares_returns_false() {
        // The partial part of PartialEq
        assert_ne!(SyncValue::Int64(1), SyncValue::Float64(f64::NAN));
        assert_ne!(SyncValue::Float64(1.0), SyncValue::Float64(f64::NAN));
        assert_ne!(SyncValue::Float64(f64::NAN), SyncValue::Float64(1.0));
        assert_ne!(SyncValue::Float64(f64::NAN), SyncValue::Float64(f64::NAN)); // Surprising IEEE defined.
    }

    #[test]
    fn eq_for_mixed_type_number_values_compares_with_long_precision() {
        assert_ne!(Value::Int64(1), Value::Float64(1.000000000000001));
        assert_ne!(Value::Float64(1.000000000000001), Value::Int64(1));

        // i64 has more digits of precision than f64. Eq needs to consider full precision.
        // To be consistent with PartialOrd eq must be correct for large integers.
        // For int values with more digits than f64's digits of precision, f64 can only
        // represent some values. For example, f64 can represent both these values:
        // 9223372036854774784
        // 9223372036854775807 (the max value for i64)
        // but none of the 1022 values between them. These f64 gaps appear for the majority of
        // the i64's range of values, although the size of the gaps varies.
        // We consider values equal only if both types represent the same int.
        // Here the test code literals look to be the same in the not equal cases, but these
        // are values that f64 cannot represent, so when the f64 literals in this test are
        // converted, it is to one of the nearest values f64 can represent and hence is not
        // equal to integer made from the same literal digits.
        assert_ne!(Value::Int64(9223372036854774785), Value::Float64(9223372036854774785.0));
        assert_ne!(Value::Float64(9223372036854774785.0), Value::Int64(9223372036854774785));

        assert_eq!(Value::Int64(9223372036854774784), Value::Float64(9223372036854774784.0));
        assert_eq!(Value::Float64(9223372036854774784.0), Value::Int64(9223372036854774784));
    }

    #[test]
    fn eq_for_mixed_type_number_sync_values_compares_with_long_precision() {
        assert_ne!(SyncValue::Int64(1), SyncValue::Float64(1.000000000000001));
        assert_ne!(SyncValue::Float64(1.000000000000001), SyncValue::Int64(1));

        // i64 has more digits of precision than f64. Eq needs to consider full precision.
        // To be consistent with PartialOrd eq must be correct for large integers.
        // For int values with more digits than f64's digits of precision, f64 can only
        // represent some values. For example, f64 can represent both these values:
        // 9223372036854774784
        // 9223372036854775807 (the max value for i64)
        // but none of the 1022 values between them. These f64 gaps appear for the majority of
        // the i64's range of values, although the size of the gaps varies.
        // We consider values equal only if both types represent the same int.
        // Here the test code literals look to be the same in the not equal cases, but these
        // are values that f64 cannot represent, so when the f64 literals in this test are
        // converted, it is to one of the nearest values f64 can represent and hence is not
        // equal to integer made from the same literal digits.
        assert_ne!(SyncValue::Int64(9223372036854774785), SyncValue::Float64(9223372036854774785.0));
        assert_ne!(SyncValue::Float64(9223372036854774785.0), SyncValue::Int64(9223372036854774785));

        assert_eq!(SyncValue::Int64(9223372036854774784), SyncValue::Float64(9223372036854774784.0));
        assert_eq!(SyncValue::Float64(9223372036854774784.0), SyncValue::Int64(9223372036854774784));
    }

    //noinspection DuplicatedCode
    #[test]
    fn eq_for_different_values_returns_false() {
        assert_ne!(Value::Boolean(true), Value::Boolean(false));
        assert_ne!(Value::Int64(1), Value::Int64(2));
        assert_ne!(Value::Float64(1.0), Value::Float64(2.0));
        assert_ne!(Value::String(StrTendril::from("asd")), Value::String(StrTendril::from("asdf")));
        assert_ne!(
            Value::DateTime(Utc.ymd(2020, 1, 1).and_hms(3, 30, 35)),
            Value::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 35))
        );
        assert_ne!(
            Value::Binary(ByteTendril::from_slice(&[1, 4, 3])),
            Value::Binary(ByteTendril::from_slice(&[1, 2, 3]))
        );
        assert_ne!(
            Value::List(Box::new(vec![Value::Null, Value::Int64(3)])),
            Value::List(Box::new(vec![Value::Null, Value::Int64(2)]))
        );
        assert_ne!(
            Value::Record(Box::new(Record::new(
                vec![Value::Int64(3), Value::Float64(1.2)],
                RecordSchema::try_from(vec!["A", "B"]).unwrap()
            ))),
            Value::Record(Box::new(Record::new(
                vec![Value::Int64(2), Value::Float64(1.2)],
                RecordSchema::try_from(vec!["A", "B"]).unwrap()
            )))
        );
        assert_ne!(
            Value::Error(Box::new(ErrorValue::new("Code", Value::Int64(2)))),
            Value::Error(Box::new(ErrorValue::new("Code", Value::Int64(3))))
        );
        assert_ne!(
            Value::StreamInfo(Rc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty()))),
            Value::StreamInfo(Rc::new(StreamInfo::new("Handler2", "Resource", SyncRecord::empty())))
        );
    }

    //noinspection DuplicatedCode
    #[test]
    fn eq_for_different_sync_values_returns_false() {
        assert_ne!(SyncValue::Boolean(true), SyncValue::Boolean(false));
        assert_ne!(SyncValue::Int64(1), SyncValue::Int64(2));
        assert_ne!(SyncValue::Float64(1.0), SyncValue::Float64(2.0));
        assert_ne!(SyncValue::String(String::from("asd")), SyncValue::String(String::from("asdf")));
        assert_ne!(
            SyncValue::DateTime(Utc.ymd(2020, 1, 1).and_hms(3, 30, 35)),
            SyncValue::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 35))
        );
        assert_ne!(
            SyncValue::Binary(vec![1, 4, 3].into_boxed_slice()),
            SyncValue::Binary(vec![1, 2, 3].into_boxed_slice())
        );
        assert_ne!(
            SyncValue::List(Box::new(vec![SyncValue::Null, SyncValue::Int64(3)])),
            SyncValue::List(Box::new(vec![SyncValue::Null, SyncValue::Int64(2)]))
        );
        assert_ne!(
            SyncValue::Record(Box::new(SyncRecord::new(
                vec![SyncValue::Int64(3), SyncValue::Float64(1.2)],
                SyncRecordSchema::try_from(vec!["A", "B"]).unwrap()
            ))),
            SyncValue::Record(Box::new(SyncRecord::new(
                vec![SyncValue::Int64(2), SyncValue::Float64(1.2)],
                SyncRecordSchema::try_from(vec!["A", "B"]).unwrap()
            )))
        );
        assert_ne!(
            SyncValue::Error(Box::new(SyncErrorValue::new("Code".into(), SyncValue::Int64(2)))),
            SyncValue::Error(Box::new(SyncErrorValue::new("Code".into(), SyncValue::Int64(3))))
        );
        assert_ne!(
            SyncValue::StreamInfo(Arc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty()))),
            SyncValue::StreamInfo(Arc::new(StreamInfo::new("Handler2", "Resource", SyncRecord::empty())))
        );
    }

    //noinspection DuplicatedCode
    #[test]
    fn eq_value_and_sync_value_for_same_returns_true() {
        assert_eq!(Value::Null, SyncValue::Null);
        assert_eq!(Value::Boolean(true), SyncValue::Boolean(true));
        assert_eq!(Value::Int64(1), SyncValue::Int64(1));
        assert_eq!(Value::Float64(1.0), SyncValue::Float64(1.0));
        assert_eq!(Value::String(StrTendril::from("asd")), SyncValue::String(String::from("asd")));
        assert_eq!(
            Value::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 35)),
            SyncValue::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 35))
        );
        assert_eq!(
            Value::Binary(ByteTendril::from_slice(&[1, 2, 3])),
            SyncValue::Binary(vec![1, 2, 3].into_boxed_slice())
        );
        assert_eq!(
            Value::List(Box::new(vec![Value::Null, Value::Int64(2)])),
            SyncValue::List(Box::new(vec![SyncValue::Null, SyncValue::Int64(2)]))
        );
        assert_eq!(
            Value::Record(Box::new(Record::new(
                vec![Value::Int64(2), Value::Float64(1.2)],
                RecordSchema::try_from(vec!["A", "B"]).unwrap()
            ))),
            SyncValue::Record(Box::new(SyncRecord::new(
                vec![SyncValue::Int64(2), SyncValue::Float64(1.2)],
                SyncRecordSchema::try_from(vec!["A", "B"]).unwrap()
            )))
        );
        assert_eq!(
            Value::Error(Box::new(ErrorValue::new("Code", Value::Int64(2)))),
            SyncValue::Error(Box::new(SyncErrorValue::new("Code".into(), SyncValue::Int64(2))))
        );
        assert_eq!(
            Value::StreamInfo(Rc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty()))),
            SyncValue::StreamInfo(Arc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty())))
        );
    }

    //noinspection DuplicatedCode
    #[test]
    fn eq_value_and_sync_value_for_different_returns_false() {
        assert_ne!(Value::Boolean(true), SyncValue::Boolean(false));
        assert_ne!(Value::Int64(1), SyncValue::Int64(2));
        assert_ne!(Value::Float64(1.0), SyncValue::Float64(2.0));
        assert_ne!(Value::String(StrTendril::from("asd")), SyncValue::String(String::from("asde")));
        assert_ne!(
            Value::DateTime(Utc.ymd(2020, 1, 1).and_hms(3, 30, 35)),
            SyncValue::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 35))
        );
        assert_ne!(
            Value::Binary(ByteTendril::from_slice(&[1, 2, 3])),
            SyncValue::Binary(vec![1, 3, 3].into_boxed_slice())
        );
        assert_ne!(
            Value::List(Box::new(vec![Value::Null, Value::Int64(4)])),
            SyncValue::List(Box::new(vec![SyncValue::Null, SyncValue::Int64(2)]))
        );
        assert_ne!(
            Value::Record(Box::new(Record::new(
                vec![Value::Int64(5), Value::Float64(1.2)],
                RecordSchema::try_from(vec!["A", "B"]).unwrap()
            ))),
            SyncValue::Record(Box::new(SyncRecord::new(
                vec![SyncValue::Int64(2), SyncValue::Float64(1.2)],
                SyncRecordSchema::try_from(vec!["A", "B"]).unwrap()
            )))
        );
        assert_ne!(
            Value::Error(Box::new(ErrorValue::new("Code3", Value::Int64(2)))),
            SyncValue::Error(Box::new(SyncErrorValue::new("Code".into(), SyncValue::Int64(2))))
        );
        assert_ne!(
            Value::StreamInfo(Rc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty()))),
            SyncValue::StreamInfo(Arc::new(StreamInfo::new("Handler2", "Resource", SyncRecord::empty())))
        );
    }

    fn test_sync_value_from_value(value: Value) {
        let sync_value = SyncValue::from(value.clone());
        assert_eq!(value, sync_value);
    }

    #[test]
    fn sync_value_from_value_returns_correct_value() {
        test_sync_value_from_value(Value::Null);
        test_sync_value_from_value(Value::Int64(2));
        test_sync_value_from_value(Value::Float64(2.0));
        test_sync_value_from_value(Value::String(StrTendril::from("asd")));
        test_sync_value_from_value(Value::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 9)));
        test_sync_value_from_value(Value::Binary(ByteTendril::from_slice(&[1, 2, 3])));
        test_sync_value_from_value(Value::List(Box::new(vec![Value::Int64(2), Value::Int64(3)])));
        test_sync_value_from_value(Value::Record(Box::new(Record::empty())));
        test_sync_value_from_value(Value::Error(Box::new(ErrorValue::new("Code", Value::Int64(2)))));
        test_sync_value_from_value(Value::StreamInfo(Rc::new(StreamInfo::new(
            "Handler",
            "Resource",
            SyncRecord::empty(),
        ))));
    }

    fn test_value_from_sync_value(sync_value: SyncValue) {
        let value = Value::from(sync_value.clone());
        assert_eq!(sync_value, value);
    }

    #[test]
    fn value_from_sync_value_returns_correct_value() {
        test_value_from_sync_value(SyncValue::Null);
        test_value_from_sync_value(SyncValue::Int64(2));
        test_value_from_sync_value(SyncValue::Float64(2.0));
        test_value_from_sync_value(SyncValue::String(String::from("asd")));
        test_value_from_sync_value(SyncValue::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 9)));
        test_value_from_sync_value(SyncValue::Binary(vec![1, 2, 3].into_boxed_slice()));
        test_value_from_sync_value(SyncValue::List(Box::new(vec![SyncValue::Int64(2), SyncValue::Int64(3)])));
        test_value_from_sync_value(SyncValue::Record(Box::new(SyncRecord::empty())));
        test_value_from_sync_value(SyncValue::Error(Box::new(SyncErrorValue::new("Code".into(), SyncValue::Int64(2)))));
        test_value_from_sync_value(SyncValue::StreamInfo(Arc::new(StreamInfo::new(
            "Handler",
            "Resource",
            SyncRecord::empty(),
        ))));
    }

    #[test]
    fn value_to_string_returns_expected() {
        assert_eq!(Value::Null.to_string(), "Null");
        assert_eq!(Value::Boolean(true).to_string(), "True");
        assert_eq!(Value::Int64(1).to_string(), "1");
        assert_eq!(Value::Float64(1.2).to_string(), "1.2");
        assert_eq!(Value::from("asd").to_string(), "asd");
        assert_eq!(
            Value::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 9)).to_string(),
            "2019-01-01T03:30:09+00:00"
        );
        assert_eq!(Value::Binary(ByteTendril::from(&[1u8, 2, 3, 4] as &[u8])).to_string(), "[4 bytes]");
        assert_eq!(
            Value::List(Box::new(vec![Value::Int64(1), Value::from("asd")])).to_string(),
            "[1, \"asd\"]"
        );
        assert_eq!(
            Value::Record(Box::new(Record::new(
                vec![Value::Int64(1)],
                RecordSchema::try_from(vec!["A"]).unwrap()
            )))
            .to_string(),
            "{A: 1}"
        );
        assert_eq!(
            Value::Error(Box::new(ErrorValue::new("ErrorCode", Value::Int64(1)))).to_string(),
            "{ErrorCode: \"ErrorCode\", SourceValue: 1, Details: None}"
        );
        assert_eq!(
            Value::StreamInfo(Rc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty()))).to_string(),
            "{Handler: \"Handler\", ResourceId: \"Resource\", Arguments: {}}"
        );
    }

    #[test]
    fn sync_value_to_string_returns_expected() {
        assert_eq!(SyncValue::Null.to_string(), "Null");
        assert_eq!(SyncValue::Boolean(true).to_string(), "True");
        assert_eq!(SyncValue::Int64(1).to_string(), "1");
        assert_eq!(SyncValue::Float64(1.2).to_string(), "1.2");
        assert_eq!(SyncValue::String("asd".to_string()).to_string(), "asd");
        assert_eq!(
            SyncValue::DateTime(Utc.ymd(2019, 1, 1).and_hms(3, 30, 9)).to_string(),
            "2019-01-01T03:30:09+00:00"
        );
        assert_eq!(SyncValue::Binary(vec![1u8, 2, 3, 4].into_boxed_slice()).to_string(), "[4 bytes]");
        assert_eq!(
            SyncValue::List(Box::new(vec![SyncValue::Int64(1), SyncValue::String("asd".to_string())])).to_string(),
            "[1, \"asd\"]"
        );
        assert_eq!(
            SyncValue::Record(Box::new(SyncRecord::new(
                vec![SyncValue::Int64(1)],
                SyncRecordSchema::try_from(vec!["A"]).unwrap()
            )))
            .to_string(),
            "{A: 1}"
        );
        assert_eq!(
            SyncValue::Error(Box::new(SyncErrorValue::new("ErrorCode".into(), SyncValue::Int64(1)))).to_string(),
            "{ErrorCode: \"ErrorCode\", SourceValue: 1, Details: None}"
        );
        assert_eq!(
            SyncValue::StreamInfo(Arc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty()))).to_string(),
            "{Handler: \"Handler\", ResourceId: \"Resource\", Arguments: {}}"
        );
    }

    #[test]
    fn validate_integer_value_comparison_less() {
        let value1 = Value::from(1);
        let value2 = Value::from(2);

        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Less);
    }

    #[test]
    fn validate_integer_value_comparison_greater() {
        let value1 = Value::from(100);
        let value2 = Value::from(20);

        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Greater);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Greater);
    }

    #[test]
    fn validate_floating_value_comparison_less() {
        let value1 = Value::from(1.0);
        let value2 = Value::from(2.0);

        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Less);
    }

    #[test]
    fn validate_floating_value_comparison_greater() {
        let value1 = Value::from(10.6);
        let value2 = Value::from(3.0);

        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Greater);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Greater);
    }

    #[test]
    fn validate_mixed_integer_value_comparison() {
        let value1 = Value::from(1);
        let value2 = Value::from(2.0);
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Less);

        let value1 = Value::from(1.0);
        let value2 = Value::from(2);
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Less);

        let value1 = Value::from(1.0);
        let value2 = Value::from(1);
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Equal);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Equal);

        let value1 = Value::from(1);
        let value2 = Value::from(1.0);
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Equal);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Equal);
    }

    #[test]
    fn validate_nan_value_comparison() {
        // NaN values are the partial part of partial_cmp.
        let value1 = Value::from(1);
        let value2 = Value::from(f64::NAN);
        let order = Value::partial_cmp(&value1, &value2);
        assert_eq!(order, None);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2));
        assert_eq!(order, None);

        let value2 = Value::from(f64::NAN);
        let value1 = Value::from(1);
        let order = Value::partial_cmp(&value1, &value2);
        assert_eq!(order, None);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2));
        assert_eq!(order, None);

        let value2 = Value::from(f64::NAN);
        let value1 = Value::from(1.0);
        let order = Value::partial_cmp(&value1, &value2);
        assert_eq!(order, None);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2));
        assert_eq!(order, None);

        let value1 = Value::from(1.0);
        let value2 = Value::from(f64::NAN);
        let order = Value::partial_cmp(&value1, &value2);
        assert_eq!(order, None);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2));
        assert_eq!(order, None);

        let value1 = Value::from(f64::NAN);
        let value2 = Value::from(f64::NAN);
        let order = Value::partial_cmp(&value1, &value2);
        assert_eq!(order, None);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2));
        assert_eq!(order, None);
    }

    #[test]
    fn validate_mixed_number_uses_all_precision() {
        // i64 has more digits of precision than f64. Eq needs to consider full precision.
        // To give correct ordering PartialOrd eq must be correct for large integers.
        // For int values with more digits than f64's digits of precision, f64 can only
        // represent some values. For example, f64 can represent both these values:
        // 9223372036854774784
        // 9223372036854775807 (the max value for i64)
        // but none of the 1022 values between them. These f64 gaps appear for the majority of
        // the i64's range of values, although the size of the gaps varies.
        // We only consider values equal only if both types represent the same int.
        // Here the test code literals look to be the same in the not equal cases, but these
        // are values that f64 cannot represent, so when the f64 literals in this test are
        // converted, it is to one of the nearest values f64 can represent and hence is not
        // equal to integer made from the same literal digits.

        let value1 = Value::from(1);
        let value2 = Value::from(1.000000000000001);
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = Value::partial_cmp(&value2, &value1).unwrap();
        assert_eq!(order, Ordering::Greater);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1.clone()), &SyncValue::from(value2.clone())).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = SyncValue::partial_cmp(&SyncValue::from(value2), &SyncValue::from(value1)).unwrap();
        assert_eq!(order, Ordering::Greater);

        let value1 = Value::from(9223372036854774784);
        let value2 = Value::from(9223372036854774784.0);
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Equal);
        let order = Value::partial_cmp(&value2, &value1).unwrap();
        assert_eq!(order, Ordering::Equal);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1.clone()), &SyncValue::from(value2.clone())).unwrap();
        assert_eq!(order, Ordering::Equal);
        let order = SyncValue::partial_cmp(&SyncValue::from(value2), &SyncValue::from(value1)).unwrap();
        assert_eq!(order, Ordering::Equal);

        let value1 = Value::from(9223372036854774783);
        let value2 = Value::from(9223372036854774783.0); // literal will convert to ...784.0
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = Value::partial_cmp(&value2, &value1).unwrap();
        assert_eq!(order, Ordering::Greater);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1.clone()), &SyncValue::from(value2.clone())).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = SyncValue::partial_cmp(&SyncValue::from(value2), &SyncValue::from(value1)).unwrap();
        assert_eq!(order, Ordering::Greater);

        let value1 = Value::from(9223372036854774785);
        let value2 = Value::from(9223372036854774785.0); // literal will convert to ...784.0
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Greater);
        let order = Value::partial_cmp(&value2, &value1).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1.clone()), &SyncValue::from(value2.clone())).unwrap();
        assert_eq!(order, Ordering::Greater);
        let order = SyncValue::partial_cmp(&SyncValue::from(value2), &SyncValue::from(value1)).unwrap();
        assert_eq!(order, Ordering::Less);
    }

    #[test]
    fn validate_integer_less_than_string() {
        let value1 = Value::from(1);
        let value2 = Value::from("Lets have this");
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Less);
    }

    #[test]
    fn validate_float_less_than_string() {
        let value1 = Value::from(1.0);
        let value2 = Value::from("Lets have this");
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Less);
    }

    #[test]
    fn validate_boolean_less_than_string() {
        let value1 = Value::from(true);
        let value2 = Value::from("test_string");
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Less);
    }

    #[test]
    fn validate_string_less_than_datetime() {
        let datetime = NaiveDateTime::from_timestamp(1000000 / 1000, (1000000 % 1000 * 1_000_000) as u32);
        let value1 = Value::from("test");
        let value2 = Value::from(DateTime::from_utc(datetime, Utc));
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Less);
    }

    #[test]
    fn validate_string_less_than_list() {
        let value1 = Value::from("test");
        let value2 = Value::from(Value::List(Box::new(vec![Value::Int64(1), Value::Int64(2)])));
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Less);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Less);
    }

    #[test]
    fn validate_equal_for_strings() {
        let value1 = Value::from("test");
        let value2 = Value::from("test");
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Equal);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Equal);
    }

    #[test]
    fn validate_equal_for_integers() {
        let value1 = Value::from(100);
        let value2 = Value::from(100);
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Equal);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Equal);
    }

    #[test]
    fn validate_equal_for_float() {
        let value1 = Value::from(20.4);
        let value2 = Value::from(20.4);
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Equal);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Equal);
    }

    #[test]
    fn validate_equal_for_datetime() {
        let datetime = NaiveDateTime::from_timestamp(1000000 / 1000, (1000000 % 1000 * 1_000_000) as u32);
        let value1 = Value::from(DateTime::from_utc(datetime, Utc));
        let value2 = Value::from(DateTime::from_utc(datetime, Utc));
        let order = Value::partial_cmp(&value1, &value2).unwrap();
        assert_eq!(order, Ordering::Equal);
        let order = SyncValue::partial_cmp(&SyncValue::from(value1), &SyncValue::from(value2)).unwrap();
        assert_eq!(order, Ordering::Equal);
    }

    #[test]
    fn validate_try_from_string() {
        let str = "My test string!";
        let str_value = Value::from(str);
        assert_eq!(str, String::try_from(str_value).unwrap());
    }

    #[test]
    fn validate_try_from_f64() {
        let f = 3.14;
        let f_value = Value::from(f);
        assert_eq!(f, f64::try_from(f_value).unwrap());
    }

    #[test]
    fn validate_try_from_list() {
        let list = Box::new(vec![Value::from(3), Value::from(1), Value::from(4)]);
        let list_value = Value::from(list.clone());
        assert_eq!(list, Box::try_from(list_value).unwrap());
    }
}
