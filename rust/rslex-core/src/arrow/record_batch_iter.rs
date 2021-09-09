use crate::{
    arrow::consts::{HEADER_ERROR_VALUE, HEADER_STREAM_INFO},
    values_buffer_pool::ValuesBufferPool,
    ErrorValue, ExecutionResult, Record, RecordIterator, RecordSchema, StreamInfo, SyncRecord, Value,
};
use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
        Float64Array, Int16Array, Int16DictionaryArray, Int32Array, Int32DictionaryArray, Int64Array, Int64DictionaryArray, Int8Array,
        Int8DictionaryArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, StringArray, StructArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
        UInt16DictionaryArray, UInt32Array, UInt32DictionaryArray, UInt64Array, UInt64DictionaryArray, UInt8Array, UInt8DictionaryArray,
    },
    datatypes::{DataType, DateUnit, TimeUnit},
    record_batch::RecordBatch,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use std::convert::{TryFrom, TryInto};

struct ColumnToValueErrorCodes {
    float16: &'static str,
    time32: &'static str,
    time64: &'static str,
    interval: &'static str,
    duration: &'static str,
    union: &'static str,
    dictionary: &'static str,
    decimal: &'static str,
}

impl ColumnToValueErrorCodes {
    pub fn new() -> ColumnToValueErrorCodes {
        ColumnToValueErrorCodes {
            float16: "Microsoft.DPrep.Arrow.Float16NotSupported",
            time32: "Microsoft.DPrep.Arrow.Time32NotSupported",
            time64: "Microsoft.DPrep.Arrow.Time64NotSupported",
            interval: "Microsoft.DPrep.Arrow.IntervalNotSupported",
            duration: "Microsoft.DPrep.Arrow.DurationNotSupported",
            union: "Microsoft.DPrep.Arrow.UnionNotSupported",
            dictionary: "Microsoft.DPrep.Arrow.DictionaryNotSupported",
            decimal: "Microsoft.DPrep.Arrow.DecimalNotSupported",
        }
    }
}

struct ValueFromColumnConverter {
    column: ArrayRef,
    null_count: usize,
    error_codes: ColumnToValueErrorCodes,

    // context if the column is a Record column
    output_record_schema: RecordSchema,
    nested_record_values_pool: ValuesBufferPool,

    // child converters if the column is a complex column (List or Struct)
    fields_converters: Vec<ValueFromColumnConverter>,
}

impl ValueFromColumnConverter {
    fn len(&self) -> usize {
        self.column.len()
    }

    fn value_from_column(&mut self, value_index: usize) -> Value {
        macro_rules! read_array {
            ($array_type:ty) => {{
                Value::from(self.column.as_any().downcast_ref::<$array_type>().unwrap().value(value_index))
            }};

            ($array_type:ty, $as_type:ty) => {{
                Value::from(self.column.as_any().downcast_ref::<$array_type>().unwrap().value(value_index) as $as_type)
            }};

            ($array_type:ty, $convert:expr) => {{
                Value::from($convert(
                    self.column.as_any().downcast_ref::<$array_type>().unwrap().value(value_index),
                ))
            }};
        }

        macro_rules! read_list_array {
            ($list_type:tt, $length_fn:expr) => {{
                debug_assert_eq!(self.fields_converters.len(), 1);

                let list_array: &$list_type = self.column.as_any().downcast_ref::<$list_type>().unwrap();
                let offset = list_array.value_offset(value_index) as usize;
                let element_count = $length_fn(&list_array, value_index) as usize;
                let mut values = Vec::with_capacity(element_count);
                #[allow(clippy::needless_range_loop)]
                for i in 0..element_count {
                    values.push(self.fields_converters[0].value_from_column(offset + i));
                }
                Value::from(Box::new(values))
            }};
        }

        macro_rules! read_dictionary_array {
            ($array_type:ty) => {{
                debug_assert_eq!(self.fields_converters.len(), 1);
                let dict_array = self.column.as_any().downcast_ref::<$array_type>().unwrap();
                let key = dict_array.keys().value(value_index);

                self.fields_converters[0].value_from_column(key as usize)
            }};
        }

        macro_rules! error_code {
            ($code:tt) => {{
                Value::Error(Box::new(ErrorValue::new(self.error_codes.$code.clone(), Value::Null)))
            }};
        }

        if self.null_count > 0 && self.column.is_null(value_index) {
            Value::Null
        } else {
            match self.column.data_type() {
                DataType::Null => Value::Null,
                DataType::Boolean => read_array!(BooleanArray),
                DataType::Binary => read_array!(BinaryArray),
                DataType::Int8 => read_array!(Int8Array, i64),
                DataType::Int16 => read_array!(Int16Array, i64),
                DataType::Int32 => read_array!(Int32Array, i64),
                DataType::Int64 => read_array!(Int64Array, i64),
                DataType::UInt8 => read_array!(UInt8Array, i64),
                DataType::UInt16 => read_array!(UInt16Array, i64),
                DataType::UInt32 => read_array!(UInt32Array, i64),
                DataType::UInt64 => read_array!(UInt64Array, i64),
                DataType::Float16 => error_code!(float16),
                DataType::Float32 => read_array!(Float32Array, f64),
                DataType::Float64 => read_array!(Float64Array),
                DataType::Timestamp(TimeUnit::Second, _) => read_array!(TimestampSecondArray, |seconds| {
                    let datetime = NaiveDateTime::from_timestamp(seconds, 0);
                    DateTime::from_utc(datetime, Utc)
                }),
                DataType::Timestamp(TimeUnit::Millisecond, _) => read_array!(TimestampMillisecondArray, |milliseconds| {
                    let datetime = NaiveDateTime::from_timestamp(milliseconds / 1000, (milliseconds % 1000 * 1_000_000) as u32);
                    DateTime::from_utc(datetime, Utc)
                }),
                DataType::Timestamp(TimeUnit::Microsecond, _) => read_array!(TimestampMicrosecondArray, |microseconds| {
                    let datetime = NaiveDateTime::from_timestamp(microseconds / 1_000_000, (microseconds % 1_000_000 * 1_000) as u32);
                    DateTime::from_utc(datetime, Utc)
                }),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => read_array!(TimestampNanosecondArray, |nanoseconds| {
                    let datetime = NaiveDateTime::from_timestamp(nanoseconds / 1_000_000_000, (nanoseconds % 1_000_000_000) as u32);
                    DateTime::from_utc(datetime, Utc)
                }),
                DataType::Date32(DateUnit::Day) => read_array!(Date32Array, |days| {
                    let datetime = NaiveDateTime::from_timestamp((days * 86_400) as i64, 0);
                    DateTime::from_utc(datetime, Utc)
                }),
                DataType::Date32(DateUnit::Millisecond) => read_array!(Date32Array, |milliseconds| {
                    let datetime = NaiveDateTime::from_timestamp((milliseconds / 1000) as i64, (milliseconds % 1000 * 1_000_000) as u32);
                    DateTime::from_utc(datetime, Utc)
                }),
                DataType::Date64(DateUnit::Day) => read_array!(Date64Array, |days| {
                    let datetime = NaiveDateTime::from_timestamp((days * 86_400) as i64, 0);
                    DateTime::from_utc(datetime, Utc)
                }),
                DataType::Date64(DateUnit::Millisecond) => read_array!(Date64Array, |milliseconds| {
                    let datetime = NaiveDateTime::from_timestamp((milliseconds / 1000) as i64, (milliseconds % 1000 * 1_000_000) as u32);
                    DateTime::from_utc(datetime, Utc)
                }),
                DataType::Time32(_) => error_code!(time32),
                DataType::Time64(_) => error_code!(time64),
                DataType::Interval(_) => error_code!(interval),
                DataType::Utf8 => read_array!(StringArray),
                DataType::LargeUtf8 => read_array!(LargeStringArray),
                DataType::LargeBinary => read_array!(LargeBinaryArray),
                DataType::FixedSizeBinary(_) => read_array!(FixedSizeBinaryArray),
                DataType::List(_) => read_list_array!(ListArray, |array: &ListArray, value_index: usize| array.value_length(value_index)),
                DataType::FixedSizeList(_, _) => {
                    read_list_array!(FixedSizeListArray, |array: &FixedSizeListArray, _: usize| { array.value_length() })
                },
                DataType::LargeList(_) => read_list_array!(LargeListArray, |array: &LargeListArray, value_index: usize| array
                    .value_length(value_index)),
                // serialize stream info as StructArray
                // to differentiate from other Record, use a special dummy column to tag it
                DataType::Struct(fields) if fields.get(0).map(|f| f.name().as_str()) == Some(HEADER_STREAM_INFO) => {
                    debug_assert_eq!(fields.len(), 4);
                    debug_assert_eq!(self.fields_converters.len(), 4);

                    // TODO: StreamInfo now only take & 'static str or String
                    // so in this case, we have to duplicate the handler string
                    // consider changing to Arc<String>
                    let handler: String = self.fields_converters[1].value_from_column(value_index).try_into().unwrap();
                    let resource_id: String = self.fields_converters[2].value_from_column(value_index).try_into().unwrap();
                    let arguments = match self.fields_converters[3].value_from_column(value_index) {
                        Value::Null => SyncRecord::empty(),
                        Value::Record(r) => SyncRecord::from(*r),
                        _ => panic!("Unexpected value when reading from StreamInfo column."),
                    };

                    StreamInfo::new(handler, resource_id, arguments).into()
                },
                // serialize errorValue as struct
                DataType::Struct(fields) if fields.get(0).map(|f| f.name().as_str()) == Some(HEADER_ERROR_VALUE) => {
                    debug_assert_eq!(fields.len(), 4);
                    debug_assert_eq!(self.fields_converters.len(), 4);

                    let error_code: String = self.fields_converters[1].value_from_column(value_index).try_into().unwrap();
                    let source_value = self.fields_converters[2].value_from_column(value_index);
                    let error_value = match self.fields_converters[3].value_from_column(value_index) {
                        Value::Null => ErrorValue::new(error_code, source_value),
                        Value::Record(r) => ErrorValue::new_with_details(error_code, source_value, (*r).clone()),
                        _ => panic!("Unexpected value when reading from Error column."),
                    };

                    Value::Error(Box::new(error_value))
                },
                DataType::Struct(_) => {
                    let mut values = self.nested_record_values_pool.get_buffer(self.fields_converters.len());
                    for i in 0..self.fields_converters.len() {
                        values[i] = self.fields_converters[i].value_from_column(value_index);
                    }
                    Value::Record(Box::new(Record::new(values, self.output_record_schema.clone())))
                },
                DataType::Duration(_) => error_code!(duration),
                DataType::Union(_) => error_code!(union),
                DataType::Decimal(_, _) => error_code!(decimal),
                DataType::Dictionary(box DataType::UInt8, _) => read_dictionary_array!(UInt8DictionaryArray),
                DataType::Dictionary(box DataType::UInt16, _) => read_dictionary_array!(UInt16DictionaryArray),
                DataType::Dictionary(box DataType::UInt32, _) => read_dictionary_array!(UInt32DictionaryArray),
                DataType::Dictionary(box DataType::UInt64, _) => read_dictionary_array!(UInt64DictionaryArray),
                DataType::Dictionary(box DataType::Int8, _) => read_dictionary_array!(Int8DictionaryArray),
                DataType::Dictionary(box DataType::Int16, _) => read_dictionary_array!(Int16DictionaryArray),
                DataType::Dictionary(box DataType::Int32, _) => read_dictionary_array!(Int32DictionaryArray),
                DataType::Dictionary(box DataType::Int64, _) => read_dictionary_array!(Int64DictionaryArray),
                DataType::Dictionary(_, _) => error_code!(dictionary),
            }
        }
    }
}

impl From<ArrayRef> for ValueFromColumnConverter {
    fn from(array: ArrayRef) -> Self {
        macro_rules! with_list_array {
            ($array_type:ty) => {{
                let complex_array = array.as_any().downcast_ref::<$array_type>().unwrap();
                (RecordSchema::empty(), vec![Self::from(complex_array.values().clone())])
            }};
        }

        let (schema, child_converters) = match array.data_type() {
            DataType::Struct(fields) => {
                let schema = RecordSchema::try_from(fields.iter().map(|f| f.name()).collect_vec()).unwrap();
                let struct_array: &StructArray = array.as_any().downcast_ref::<StructArray>().unwrap();
                let converters = fields
                    .iter()
                    .enumerate()
                    .map(|(i, _)| Self::from(struct_array.column(i).clone()))
                    .collect_vec();
                (schema, converters)
            },
            DataType::List(_) => with_list_array!(ListArray),
            DataType::FixedSizeList(_, _) => with_list_array!(FixedSizeListArray),
            DataType::LargeList(_) => with_list_array!(LargeListArray),
            DataType::Dictionary(box DataType::UInt8, _) => with_list_array!(UInt8DictionaryArray),
            DataType::Dictionary(box DataType::UInt16, _) => with_list_array!(UInt16DictionaryArray),
            DataType::Dictionary(box DataType::UInt32, _) => with_list_array!(UInt32DictionaryArray),
            DataType::Dictionary(box DataType::UInt64, _) => with_list_array!(UInt64DictionaryArray),
            DataType::Dictionary(box DataType::Int8, _) => with_list_array!(Int8DictionaryArray),
            DataType::Dictionary(box DataType::Int16, _) => with_list_array!(Int16DictionaryArray),
            DataType::Dictionary(box DataType::Int32, _) => with_list_array!(Int32DictionaryArray),
            DataType::Dictionary(box DataType::Int64, _) => with_list_array!(Int64DictionaryArray),
            _ => (RecordSchema::empty(), Vec::new()),
        };

        let null_count = array.null_count();
        ValueFromColumnConverter {
            column: array,
            null_count,
            nested_record_values_pool: ValuesBufferPool::new(),
            output_record_schema: schema,
            fields_converters: child_converters,
            error_codes: ColumnToValueErrorCodes::new(),
        }
    }
}

pub struct ArrayValueIter {
    current_index: usize,
    value_from_array_converter: ValueFromColumnConverter,
}

impl ArrayValueIter {
    pub fn new(array: ArrayRef) -> ArrayValueIter {
        ArrayValueIter {
            current_index: 0,
            value_from_array_converter: array.into(),
        }
    }
}

impl Iterator for ArrayValueIter {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.value_from_array_converter.len() {
            None
        } else {
            let value = self.value_from_array_converter.value_from_column(self.current_index);
            self.current_index += 1;
            Some(value)
        }
    }
}

impl From<ArrayRef> for ArrayValueIter {
    fn from(array: ArrayRef) -> Self {
        ArrayValueIter::new(array)
    }
}

/// An iterator of [`Records`](crate::Record) over a [`RecordBatch`](arrow::record_batch::RecordBatch).
pub struct RecordBatchIter {
    batch: RecordBatch,
    schema: RecordSchema,
    value_converters: Vec<ValueFromColumnConverter>,
    idx: Option<usize>,
    values_pool: ValuesBufferPool,
}

impl Iterator for RecordBatchIter {
    type Item = ExecutionResult<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.idx = self.idx.map_or(Some(0), |i| Some(i + 1));
        let i = self.idx.unwrap();
        if i < self.batch.num_rows() {
            let mut values_array = self.values_pool.get_buffer(self.batch.num_columns());
            for j in 0..values_array.len() {
                values_array[j] = self.value_converters[j].value_from_column(i);
            }

            Some(Ok(Record::new(values_array, self.schema.clone())))
        } else {
            None
        }
    }
}

impl RecordIterator for RecordBatchIter {
    fn try_as_record_batch(&mut self) -> Option<ExecutionResult<&RecordBatch>> {
        Some(Ok(&self.batch))
    }
}

impl From<RecordBatch> for RecordBatchIter {
    /// Creates an iterator of [`Records`](crate::Record) over a [`RecordBatch`](arrow::record_batch::RecordBatch).
    fn from(batch: RecordBatch) -> Self {
        let schema = RecordSchema::try_from(batch.schema().fields().iter().map(|f| f.name()).collect_vec()).unwrap();
        let value_converters = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, _)| ValueFromColumnConverter::from(batch.column(idx).clone()))
            .collect_vec();
        RecordBatchIter {
            batch,
            schema,
            value_converters,
            idx: None,
            values_pool: ValuesBufferPool::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record;
    use arrow::{
        array::{
            Array, BinaryBuilder, FixedSizeBinaryBuilder, FixedSizeListBuilder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
            LargeBinaryBuilder, LargeListBuilder, LargeStringBuilder, ListBuilder, NullArray, PrimitiveBuilder, StringBuilder,
            StringDictionaryBuilder, StructBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder, UnionBuilder,
        },
        datatypes::{
            ArrowPrimitiveType, Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
            DurationSecondType, Field, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, IntervalDayTimeType, IntervalYearMonthType,
            Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
            TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
        },
    };
    use fluent_assertions::*;
    use std::sync::Arc;
    use tendril::{ByteTendril, StrTendril};

    #[test]
    fn null_column_returns_null_values() {
        let length = 2;
        let column: Arc<dyn Array> = Arc::new(NullArray::new(length));

        ArrayValueIter::new(column)
            .should()
            .equal_iterator(std::iter::repeat(Value::Null).take(length));
    }

    fn test_nullable_column_iter_with_mapping<T: ArrowPrimitiveType>(values: &[Option<T::Native>], map_fn: fn(T::Native) -> Value) {
        let mut builder: PrimitiveBuilder<T> = PrimitiveBuilder::new(values.len());
        let values_to_add: Vec<_> = values.iter().map(|o| o.unwrap_or(T::default_value())).collect();
        let validity_values: Vec<_> = values.iter().map(Option::is_some).collect();
        builder.append_values(&values_to_add, &validity_values).unwrap();
        let column: Arc<dyn Array> = Arc::new(builder.finish());

        ArrayValueIter::new(column)
            .should()
            .equal_iterator(values.iter().cloned().map(|o| o.map_or(Value::Null, map_fn)));
    }

    fn test_column_iter_with_mapping<T: ArrowPrimitiveType>(values: &[T::Native], map_fn: fn(T::Native) -> Value) {
        test_nullable_column_iter_with_mapping::<T>(&values.iter().cloned().map(Option::Some).collect_vec(), map_fn);
    }

    fn test_nullable_column_iter<T: ArrowPrimitiveType>(values: &[Option<T::Native>])
    where Value: From<T::Native> {
        test_nullable_column_iter_with_mapping::<T>(&values, Value::from);
    }

    fn test_column_iter<T: ArrowPrimitiveType>(values: &[T::Native])
    where Value: From<T::Native> {
        test_nullable_column_iter_with_mapping::<T>(&values.iter().cloned().map(Option::Some).collect_vec(), Value::from);
    }

    #[test]
    fn i8_column_returns_correct_values() {
        test_column_iter_with_mapping::<Int8Type>(&[1, 2], |v| Value::Int64(v as i64));
    }

    #[test]
    fn nullable_i8_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<Int8Type>(&[Some(1), None, Some(2)], |v| Value::Int64(v as i64));
    }

    #[test]
    fn i16_column_returns_correct_values() {
        test_column_iter_with_mapping::<Int16Type>(&[1, 2], |v| Value::Int64(v as i64));
    }

    #[test]
    fn nullable_i16_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<Int16Type>(&[Some(1), None, Some(2)], |v| Value::Int64(v as i64));
    }

    #[test]
    fn i32_column_returns_correct_values() {
        test_column_iter_with_mapping::<Int32Type>(&[1, 2], |v| Value::Int64(v as i64));
    }

    #[test]
    fn nullable_i32_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<Int32Type>(&[Some(1), None, Some(2)], |v| Value::Int64(v as i64));
    }

    #[test]
    fn i64_column_returns_correct_values() {
        test_column_iter::<Int64Type>(&[123, 456]);
    }

    #[test]
    fn nullable_i64_column_returns_correct_values() {
        test_nullable_column_iter::<Int64Type>(&[Some(123), None, Some(456)]);
    }

    #[test]
    fn u8_column_returns_correct_values() {
        test_column_iter_with_mapping::<UInt8Type>(&[1, 2], |v| Value::Int64(v as i64));
    }

    #[test]
    fn nullable_u8_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<UInt8Type>(&[Some(1), None, Some(2)], |v| Value::Int64(v as i64));
    }

    #[test]
    fn u16_column_returns_correct_values() {
        test_column_iter_with_mapping::<UInt16Type>(&[1, 2], |v| Value::Int64(v as i64));
    }

    #[test]
    fn nullable_u16_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<UInt16Type>(&[Some(1), None, Some(2)], |v| Value::Int64(v as i64));
    }

    #[test]
    fn u32_column_returns_correct_values() {
        test_column_iter_with_mapping::<UInt32Type>(&[1, 2], |v| Value::Int64(v as i64));
    }

    #[test]
    fn nullable_u32_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<UInt32Type>(&[Some(1), None, Some(2)], |v| Value::Int64(v as i64));
    }

    #[test]
    fn u64_column_returns_correct_values() {
        test_column_iter_with_mapping::<UInt64Type>(&[123, 456], |v| Value::Int64(v as i64));
    }

    #[test]
    fn nullable_u64_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<UInt64Type>(&[Some(123), None, Some(456)], |v| Value::Int64(v as i64));
    }

    #[test]
    fn f64_column_returns_correct_values() {
        test_column_iter::<Float64Type>(&[1.123, 5.64]);
    }

    #[test]
    fn nullable_f64_column_returns_correct_values() {
        test_nullable_column_iter::<Float64Type>(&[Some(1.123), None, Some(5.64)]);
    }

    #[test]
    fn timestamp_seconds_column_returns_correct_values() {
        test_column_iter_with_mapping::<TimestampSecondType>(&[5, 10], |v| {
            Value::DateTime(chrono::DateTime::from_utc(chrono::NaiveDateTime::from_timestamp(v, 0), Utc))
        });
    }

    #[test]
    fn nullable_timestamp_seconds_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<TimestampSecondType>(&[Some(5), None, Some(10)], |v| {
            Value::DateTime(chrono::DateTime::from_utc(chrono::NaiveDateTime::from_timestamp(v, 0), Utc))
        });
    }

    #[test]
    fn timestamp_milliseconds_column_returns_correct_values() {
        test_column_iter_with_mapping::<TimestampMillisecondType>(&[5, 10], |v| {
            Value::DateTime(chrono::DateTime::from_utc(
                chrono::NaiveDateTime::from_timestamp(v / 1000, (v % 1000 * 1_000_000) as u32),
                Utc,
            ))
        });
    }

    #[test]
    fn nullable_timestamp_milliseconds_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<TimestampMillisecondType>(&[Some(5), None, Some(10)], |v| {
            Value::DateTime(chrono::DateTime::from_utc(
                chrono::NaiveDateTime::from_timestamp(v / 1000, (v % 1000 * 1_000_000) as u32),
                Utc,
            ))
        });
    }

    #[test]
    fn timestamp_microseconds_column_returns_correct_values() {
        test_column_iter_with_mapping::<TimestampMicrosecondType>(&[5, 10], |v| {
            Value::DateTime(chrono::DateTime::from_utc(
                chrono::NaiveDateTime::from_timestamp(v / 1_000_000, (v % 1_000_000 * 1_000) as u32),
                Utc,
            ))
        });
    }

    #[test]
    fn nullable_timestamp_microseconds_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<TimestampMicrosecondType>(&[Some(5), None, Some(10)], |v| {
            Value::DateTime(chrono::DateTime::from_utc(
                chrono::NaiveDateTime::from_timestamp(v / 1_000_000, (v % 1_000_000 * 1_000) as u32),
                Utc,
            ))
        });
    }

    #[test]
    fn timestamp_nanoseconds_column_returns_correct_values() {
        test_column_iter_with_mapping::<TimestampNanosecondType>(&[5, 10], |v| {
            Value::DateTime(chrono::DateTime::from_utc(
                chrono::NaiveDateTime::from_timestamp(v / 1_000_000_000, (v % 1_000_000_000) as u32),
                Utc,
            ))
        });
    }

    #[test]
    fn nullable_timestamp_nanoseconds_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<TimestampNanosecondType>(&[Some(5), None, Some(10)], |v| {
            Value::DateTime(chrono::DateTime::from_utc(
                chrono::NaiveDateTime::from_timestamp(v / 1_000_000_000, (v % 1_000_000_000) as u32),
                Utc,
            ))
        });
    }

    #[test]
    fn date_days_column_returns_correct_values() {
        test_column_iter_with_mapping::<Date32Type>(&[5, 10], |v| {
            Value::DateTime(chrono::DateTime::from_utc(
                chrono::NaiveDateTime::from_timestamp((v * 86_400) as i64, 0),
                Utc,
            ))
        });
    }

    #[test]
    fn nullable_date_days_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<Date32Type>(&[Some(5), None, Some(10)], |v| {
            Value::DateTime(chrono::DateTime::from_utc(
                chrono::NaiveDateTime::from_timestamp((v * 86_400) as i64, 0),
                Utc,
            ))
        });
    }

    #[test]
    fn date_milliseconds_column_returns_correct_values() {
        test_column_iter_with_mapping::<Date64Type>(&[5, 10], |v| {
            Value::DateTime(chrono::DateTime::from_utc(
                chrono::NaiveDateTime::from_timestamp((v / 1_000) as i64, (v % 1_000 * 1_000_000) as u32),
                Utc,
            ))
        });
    }

    #[test]
    fn nullable_date_milliseconds_column_returns_correct_values() {
        test_nullable_column_iter_with_mapping::<Date64Type>(&[Some(5), None, Some(10)], |v| {
            Value::DateTime(chrono::DateTime::from_utc(
                chrono::NaiveDateTime::from_timestamp((v / 1_000) as i64, (v % 1_000 * 1_000_000) as u32),
                Utc,
            ))
        });
    }

    #[test]
    fn time32_second_column_returns_error_value() {
        test_column_iter_with_mapping::<Time32SecondType>(&[1], |_| {
            Value::Error(Box::new(ErrorValue::new(ColumnToValueErrorCodes::new().time32, Value::Null)))
        });
    }

    #[test]
    fn time32_millisecond_column_returns_error_value() {
        test_column_iter_with_mapping::<Time32MillisecondType>(&[1], |_| {
            Value::Error(Box::new(ErrorValue::new(ColumnToValueErrorCodes::new().time32, Value::Null)))
        });
    }

    #[test]
    fn time64_microsecond_column_returns_error_value() {
        test_column_iter_with_mapping::<Time64MicrosecondType>(&[1], |_| {
            Value::Error(Box::new(ErrorValue::new(ColumnToValueErrorCodes::new().time64, Value::Null)))
        });
    }

    #[test]
    fn time64_nanosecond_column_returns_error_value() {
        test_column_iter_with_mapping::<Time64NanosecondType>(&[1], |_| {
            Value::Error(Box::new(ErrorValue::new(ColumnToValueErrorCodes::new().time64, Value::Null)))
        });
    }

    #[test]
    fn interval_daytime_column_returns_error_value() {
        test_column_iter_with_mapping::<IntervalDayTimeType>(&[1], |_| {
            Value::Error(Box::new(ErrorValue::new(ColumnToValueErrorCodes::new().interval, Value::Null)))
        });
    }

    #[test]
    fn interval_year_month_column_returns_error_value() {
        test_column_iter_with_mapping::<IntervalYearMonthType>(&[1], |_| {
            Value::Error(Box::new(ErrorValue::new(ColumnToValueErrorCodes::new().interval, Value::Null)))
        });
    }

    #[test]
    fn duration_second_column_returns_error_value() {
        test_column_iter_with_mapping::<DurationSecondType>(&[1], |_| {
            Value::Error(Box::new(ErrorValue::new(ColumnToValueErrorCodes::new().duration, Value::Null)))
        });
    }

    #[test]
    fn duration_millisecond_column_returns_error_value() {
        test_column_iter_with_mapping::<DurationMillisecondType>(&[1], |_| {
            Value::Error(Box::new(ErrorValue::new(ColumnToValueErrorCodes::new().duration, Value::Null)))
        });
    }

    #[test]
    fn duration_microsecond_column_returns_error_value() {
        test_column_iter_with_mapping::<DurationMicrosecondType>(&[1], |_| {
            Value::Error(Box::new(ErrorValue::new(ColumnToValueErrorCodes::new().duration, Value::Null)))
        });
    }

    #[test]
    fn duration_nanosecond_column_returns_error_value() {
        test_column_iter_with_mapping::<DurationNanosecondType>(&[1], |_| {
            Value::Error(Box::new(ErrorValue::new(ColumnToValueErrorCodes::new().duration, Value::Null)))
        });
    }

    macro_rules! test_list_column {
        ($builder:expr, $values:expr, $map_fn:expr) => {{
            let mut builder = $builder;
            for value in $values {
                builder.append_value(&value).unwrap();
            }
            let column: Arc<dyn Array> = Arc::new(builder.finish());
            let iter = ArrayValueIter::new(column);
            let expected_values = $values.into_iter().map($map_fn);
            iter.should().equal_iterator(expected_values);
        }};
    }

    #[test]
    fn binary_column_returns_expected_values() {
        test_list_column!(BinaryBuilder::new(2), vec![vec![1, 2], vec![3, 4]], |v: Vec<u8>| Value::Binary(
            ByteTendril::from(v.as_slice())
        ));
    }

    #[test]
    fn fixed_size_binary_column_returns_expected_values() {
        test_list_column!(FixedSizeBinaryBuilder::new(2, 2), vec![vec![1, 2], vec![3, 4]], |v: Vec<u8>| {
            Value::Binary(ByteTendril::from(v.as_slice()))
        });
    }

    #[test]
    fn large_binary_column_returns_expected_values() {
        test_list_column!(LargeBinaryBuilder::new(2), vec![vec![1, 2], vec![3, 4]], |v: Vec<u8>| {
            Value::Binary(ByteTendril::from(v.as_slice()))
        });
    }

    #[test]
    fn utf8_column_returns_expected_values() {
        test_list_column!(StringBuilder::new(2), vec!["ab", "cd"], |v: &'static str| {
            Value::String(StrTendril::from(v))
        });
    }

    #[test]
    fn large_utf8_column_returns_expected_values() {
        test_list_column!(LargeStringBuilder::new(2), vec!["ab", "cd"], |v: &'static str| {
            Value::String(StrTendril::from(v))
        });
    }

    #[test]
    fn list_column_returns_expected_values() {
        let mut builder = ListBuilder::new(UInt16Builder::new(2));
        builder.values().append_values(&[1, 2], &[true, true]).unwrap();
        builder.append(true).unwrap();
        builder.values().append_values(&[3, 4], &[true, true]).unwrap();
        builder.append(true).unwrap();
        let column: Arc<dyn Array> = Arc::new(builder.finish());

        ArrayValueIter::new(column).should().equal_iterator(vec![
            Value::List(Box::new(vec![Value::Int64(1), Value::Int64(2)])),
            Value::List(Box::new(vec![Value::Int64(3), Value::Int64(4)])),
        ]);
    }

    #[test]
    fn list_column_with_nested_nulls_returns_expected_values() {
        let mut builder = ListBuilder::new(UInt16Builder::new(2));
        builder.values().append_values(&[1, 0, 2], &[true, false, true]).unwrap();
        builder.append(true).unwrap();
        builder.values().append_values(&[3, 0, 4], &[true, false, true]).unwrap();
        builder.append(true).unwrap();
        let column: Arc<dyn Array> = Arc::new(builder.finish());

        ArrayValueIter::new(column).should().equal_iterator(vec![
            Value::List(Box::new(vec![Value::Int64(1), Value::Null, Value::Int64(2)])),
            Value::List(Box::new(vec![Value::Int64(3), Value::Null, Value::Int64(4)])),
        ]);
    }

    #[test]
    fn list_column_with_top_level_nulls_returns_expected_values() {
        let mut builder = ListBuilder::new(UInt16Builder::new(2));
        builder.values().append_values(&[1, 2], &[true, true]).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        let column: Arc<dyn Array> = Arc::new(builder.finish());

        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::List(Box::new(vec![Value::Int64(1), Value::Int64(2)])), Value::Null]);
    }

    #[test]
    fn fixed_size_list_column_returns_expected_values() {
        let mut builder = FixedSizeListBuilder::new(UInt16Builder::new(2), 2);
        builder.values().append_values(&[1, 2], &[true, true]).unwrap();
        builder.append(true).unwrap();
        let column: Arc<dyn Array> = Arc::new(builder.finish());

        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::List(Box::new(vec![Value::Int64(1), Value::Int64(2)]))]);
    }

    #[test]
    fn large_list_column_with_top_level_nulls_returns_expected_values() {
        let mut builder = LargeListBuilder::new(UInt16Builder::new(2));
        builder.values().append_values(&[1, 2], &[true, true]).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        let column: Arc<dyn Array> = Arc::new(builder.finish());

        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::List(Box::new(vec![Value::Int64(1), Value::Int64(2)])), Value::Null]);
    }

    #[test]
    fn struct_column_returns_correct_values() {
        let mut builder = StructBuilder::new(
            vec![
                Field::new("ColumnA", DataType::Int64, true),
                Field::new("ColumnB", DataType::Utf8, true),
            ],
            vec![Box::new(Int64Builder::new(2)), Box::new(StringBuilder::new(2))],
        );
        builder.field_builder::<Int64Builder>(0).unwrap().append_value(1).unwrap();
        builder.field_builder::<StringBuilder>(1).unwrap().append_value("value").unwrap();
        builder.append(true).unwrap();
        let column: Arc<dyn Array> = Arc::new(builder.finish());

        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::Record(Box::new(record!("ColumnA" => 1, "ColumnB" => "value")))]);
    }

    #[test]
    fn dense_union_column_returns_error() {
        let mut builder = UnionBuilder::new_dense(2);
        builder.append::<UInt8Type>("name", 2).unwrap();
        let column: Arc<dyn Array> = Arc::new(builder.build().unwrap());
        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::Error(Box::new(ErrorValue::new(
                ColumnToValueErrorCodes::new().union,
                Value::Null,
            )))]);
    }

    #[test]
    fn sparse_union_column_returns_error() {
        let mut builder = UnionBuilder::new_sparse(2);
        builder.append::<UInt8Type>("name", 2).unwrap();
        let column: Arc<dyn Array> = Arc::new(builder.build().unwrap());
        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::Error(Box::new(ErrorValue::new(
                ColumnToValueErrorCodes::new().union,
                Value::Null,
            )))]);
    }

    #[test]
    fn dictionary_column_with_strings_and_uint8() {
        let mut builder = StringDictionaryBuilder::new(UInt8Builder::new(2), StringBuilder::new(2));
        builder.append("a").unwrap();
        builder.append_null().unwrap();
        builder.append("a").unwrap();

        let column: Arc<dyn Array> = Arc::new(builder.finish());
        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::from("a"), Value::Null, Value::from("a")]);
    }

    #[test]
    fn dictionary_column_with_strings_and_uint16() {
        let mut builder = StringDictionaryBuilder::new(UInt16Builder::new(2), StringBuilder::new(2));
        builder.append("a").unwrap();
        builder.append_null().unwrap();
        builder.append("a").unwrap();

        let column: Arc<dyn Array> = Arc::new(builder.finish());
        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::from("a"), Value::Null, Value::from("a")]);
    }

    #[test]
    fn dictionary_column_with_strings_and_uint32() {
        let mut builder = StringDictionaryBuilder::new(UInt32Builder::new(2), StringBuilder::new(2));
        builder.append("a").unwrap();
        builder.append_null().unwrap();
        builder.append("a").unwrap();

        let column: Arc<dyn Array> = Arc::new(builder.finish());
        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::from("a"), Value::Null, Value::from("a")]);
    }

    #[test]
    fn dictionary_column_with_strings_and_uint64() {
        let mut builder = StringDictionaryBuilder::new(UInt64Builder::new(2), StringBuilder::new(2));
        builder.append("a").unwrap();
        builder.append_null().unwrap();
        builder.append("a").unwrap();

        let column: Arc<dyn Array> = Arc::new(builder.finish());
        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::from("a"), Value::Null, Value::from("a")]);
    }

    #[test]
    fn dictionary_column_with_strings_and_int8() {
        let mut builder = StringDictionaryBuilder::new(Int8Builder::new(2), StringBuilder::new(2));
        builder.append("a").unwrap();
        builder.append_null().unwrap();
        builder.append("a").unwrap();

        let column: Arc<dyn Array> = Arc::new(builder.finish());
        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::from("a"), Value::Null, Value::from("a")]);
    }

    #[test]
    fn dictionary_column_with_strings_and_int16() {
        let mut builder = StringDictionaryBuilder::new(Int16Builder::new(2), StringBuilder::new(2));
        builder.append("a").unwrap();
        builder.append_null().unwrap();
        builder.append("a").unwrap();

        let column: Arc<dyn Array> = Arc::new(builder.finish());
        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::from("a"), Value::Null, Value::from("a")]);
    }

    #[test]
    fn dictionary_column_with_strings_and_int32() {
        let mut builder = StringDictionaryBuilder::new(Int32Builder::new(2), StringBuilder::new(2));
        builder.append("a").unwrap();
        builder.append_null().unwrap();
        builder.append("a").unwrap();

        let column: Arc<dyn Array> = Arc::new(builder.finish());
        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::from("a"), Value::Null, Value::from("a")]);
    }

    #[test]
    fn dictionary_column_with_strings_and_int64() {
        let mut builder = StringDictionaryBuilder::new(Int64Builder::new(2), StringBuilder::new(2));
        builder.append("a").unwrap();
        builder.append_null().unwrap();
        builder.append("a").unwrap();

        let column: Arc<dyn Array> = Arc::new(builder.finish());
        ArrayValueIter::new(column)
            .should()
            .equal_iterator(vec![Value::from("a"), Value::Null, Value::from("a")]);
    }
}
