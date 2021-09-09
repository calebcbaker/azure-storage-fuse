use crate::{
    arrow::consts::{
        COLUMN_ARGUMENTS, COLUMN_ERROR_CODE, COLUMN_ERROR_DETAILS, COLUMN_HANDLER, COLUMN_RESOUCE_ID, COLUMN_SOURCE_VALUE,
        HEADER_ERROR_VALUE, HEADER_STREAM_INFO,
    },
    DataMaterializationError, ErrorValue, FieldSelector, MultiFieldSelector, OutOfRangeValueError, Record, RecordSchema, StreamInfo,
    SyncErrorValue, SyncValue, UnexpectedTypeError, Value, ValueKind,
};
use arrow::{
    array::{
        Array, ArrayBuilder, ArrayData, ArrayRef, BinaryArray, BooleanArray, BooleanBufferBuilder, BooleanBuilder, Float64Array,
        Float64Builder, Int64Array, Int64Builder, ListBuilder, NullArray, StringArray, StringDictionaryBuilder, StructArray,
        TimestampNanosecondBuilder, UInt32BufferBuilder, UInt8BufferBuilder, UInt8Builder,
    },
    datatypes::{DataType, Field, UInt8Type},
    error::ArrowError,
    record_batch::RecordBatch,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use std::{
    any::{Any, TypeId},
    cell::RefCell,
    cmp::max,
    collections::{HashMap, HashSet},
    marker::PhantomData,
    rc::Rc,
    sync::Arc,
};
use tendril::{ByteTendril, StrTendril};

/// How errors while materializing values should be handled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExceptionHandling {
    /// Replace the failing value with Null.
    Null,
    /// Fail execution and return the error.
    Fail,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorHandling {
    /// Replace the failing value with Null.
    Null,
    /// Fail execution and return the error.
    Fail,
    /// serialize the ErrorValue as Struct
    AsStruct,
}

#[derive(Clone, Debug)]
enum TargetColumn {
    StreamColumn(usize),
    NestedColumn(Rc<Vec<Arc<str>>>),
}

#[derive(Clone, Debug)]
pub struct StreamInfoCollector {
    target_column: TargetColumn,
    stream_columns: Rc<RefCell<Vec<(Vec<Arc<str>>, Vec<Option<Rc<StreamInfo>>>)>>>,
}

impl StreamInfoCollector {
    pub(self) fn for_stream_column(&self, column_name: Arc<str>) -> StreamInfoCollector {
        let mut stream_columns = self.stream_columns.borrow_mut();
        let column_path = match &self.target_column {
            TargetColumn::StreamColumn(_) => panic!(
                "[StreamInfoCollector::for_stream_column] for_stream_column called on a collector already targeting a stream column."
            ),
            TargetColumn::NestedColumn(path) => {
                let mut new_path = path.as_ref().clone();
                new_path.push(column_name);
                new_path
            },
        };

        let column_values = Vec::new();
        stream_columns.push((column_path, column_values));

        StreamInfoCollector {
            target_column: TargetColumn::StreamColumn(stream_columns.len() - 1),
            stream_columns: self.stream_columns.clone(),
        }
    }

    pub(self) fn for_nested_column(&self, column_name: Arc<str>) -> StreamInfoCollector {
        let column_path = match &self.target_column {
            TargetColumn::StreamColumn(_) => {
                panic!("[StreamInfoCollector::for_nested_column] Attempting to set up a nested column within a stream column.")
            },
            TargetColumn::NestedColumn(path) => {
                let mut column_path = path.as_ref().clone();
                column_path.push(column_name);
                column_path
            },
        };

        StreamInfoCollector {
            target_column: TargetColumn::NestedColumn(Rc::new(column_path)),
            stream_columns: self.stream_columns.clone(),
        }
    }

    pub(self) fn collect_value(&mut self, stream_info: Rc<StreamInfo>) {
        match self.target_column {
            TargetColumn::StreamColumn(idx) => self.stream_columns.borrow_mut()[idx].1.push(Some(stream_info)),
            _ => panic!("[StreamInfoCollector::collect_value] collect_value invoked without a target column. for_stream_column must be called before this can be used.")
        };
    }

    pub(self) fn collect_none(&mut self) {
        match self.target_column {
            TargetColumn::StreamColumn(idx) => self.stream_columns.borrow_mut()[idx].1.push(None),
            _ => panic!("[StreamInfoCollector::collect_value] collect_value invoked without a target column. for_stream_column must be called before this can be used.")
        };
    }

    pub fn take_collected_columns(&mut self) -> HashMap<Vec<Arc<str>>, Vec<Option<Rc<StreamInfo>>>> {
        self.stream_columns.take().into_iter().collect()
    }
}

impl Default for StreamInfoCollector {
    fn default() -> Self {
        StreamInfoCollector {
            target_column: TargetColumn::NestedColumn(Rc::new(Vec::new())),
            stream_columns: Default::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum StreamInfoHandling {
    /// only serialize the resource_id into a StringArray
    AsString,
    /// serialize the whole record into a StructArray
    AsStruct,
    NullAndCollect(StreamInfoCollector),
}

impl StreamInfoHandling {
    pub(self) fn for_nested_column(&self, column_name: Arc<str>) -> StreamInfoHandling {
        match self {
            StreamInfoHandling::NullAndCollect(collector) => StreamInfoHandling::NullAndCollect(collector.for_nested_column(column_name)),
            value => value.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RecordBatchBuilderOptions {
    pub mixed_type_handling: ExceptionHandling,
    pub out_of_range_datetime_handling: ExceptionHandling,
    pub error_handling: ErrorHandling,
    pub stream_info_handling: StreamInfoHandling,
}

impl RecordBatchBuilderOptions {
    pub(self) fn for_nested_column(&self, column_name: &Arc<str>) -> RecordBatchBuilderOptions {
        RecordBatchBuilderOptions {
            mixed_type_handling: self.mixed_type_handling,
            out_of_range_datetime_handling: self.out_of_range_datetime_handling,
            error_handling: self.error_handling,
            stream_info_handling: self.stream_info_handling.for_nested_column(column_name.clone()),
        }
    }

    pub fn with_mixed_type_handling(mut self, handling: ExceptionHandling) -> Self {
        self.mixed_type_handling = handling;
        self
    }

    pub fn with_out_of_range_datetime_handling(mut self, handling: ExceptionHandling) -> Self {
        self.out_of_range_datetime_handling = handling;
        self
    }

    pub fn with_error_handling(mut self, handling: ErrorHandling) -> Self {
        self.error_handling = handling;
        self
    }

    pub fn with_stream_info_handling(mut self, handling: StreamInfoHandling) -> Self {
        self.stream_info_handling = handling;
        self
    }
}

impl Default for RecordBatchBuilderOptions {
    fn default() -> Self {
        RecordBatchBuilderOptions {
            mixed_type_handling: ExceptionHandling::Fail,
            out_of_range_datetime_handling: ExceptionHandling::Fail,
            error_handling: ErrorHandling::Fail,
            stream_info_handling: StreamInfoHandling::AsString,
        }
    }
}

/// A helper class to build rslex::Value::String column and rslex::Value::Binary column.
/// Ideally, we should use the native StringBuilder and BinaryBuilder,
/// which will build StringArray(DataType::Utf8) and BinaryArray(DataType::Binary).
/// However, the current native implementation of StringBuilder and BinaryBuilder
/// were written on top of ListBuilder<UInt8Builder>, it builds a ListArray<UInt8>
/// then convert to StringArray/BinaryArray. With ListArray<UInt8>, the internal array
/// could contain null like [3, null, 5, 6], while for StringArray and BinaryArray it's
/// a non-scenario. So building list array pays some extra cost to build the bitmap array.
/// Bench shows the cost is significant.
/// As a workaround, we write our own StringBuilder and BinaryBuilder here, with exactly the same interface.
/// In case future optimization in native builders, we could switch back.
struct BinaryColumnBuilder<D: 'static + ?Sized + AsRef<[u8]>> {
    values_builder: UInt8BufferBuilder,
    offsets_builder: UInt32BufferBuilder,
    null_map_builder: BooleanBufferBuilder,
    null_count: usize,
    marker: PhantomData<D>,
}

impl<D: 'static + ?Sized + AsRef<[u8]>> BinaryColumnBuilder<D> {
    pub fn new(capacity: usize) -> Self {
        BinaryColumnBuilder {
            values_builder: UInt8BufferBuilder::new(capacity),
            offsets_builder: UInt32BufferBuilder::new(capacity),
            null_map_builder: BooleanBufferBuilder::new(capacity),
            null_count: 0,
            marker: PhantomData,
        }
    }

    fn data_type(&self) -> DataType {
        if TypeId::of::<str>() == TypeId::of::<D>() {
            DataType::Utf8
        } else if TypeId::of::<[u8]>() == TypeId::of::<D>() {
            DataType::Binary
        } else {
            panic!("[BinaryColumnBuilder::data_type()] Unexpected data type.")
        }
    }

    pub fn append_value(&mut self, value: &D) -> Result<(), ArrowError> {
        let offset = self.values_builder.len();
        self.values_builder.append_slice(value.as_ref());
        self.offsets_builder.append(offset as u32);
        self.null_map_builder.append(true);
        // We reserve here so that we can guarantee finish won't fail when appending the final offset.
        self.offsets_builder.reserve(1);
        Ok(())
    }

    pub fn append_null(&mut self) -> Result<(), ArrowError> {
        self.null_count += 1;
        self.offsets_builder.append(self.values_builder.len() as u32);
        self.null_map_builder.append(false);
        // We reserve here so that we can guarantee finish won't fail when appending the final offset.
        self.offsets_builder.reserve(1);
        Ok(())
    }
}

impl<D: 'static + ?Sized + AsRef<[u8]>> ArrayBuilder for BinaryColumnBuilder<D> {
    fn len(&self) -> usize {
        self.offsets_builder.len()
    }

    fn is_empty(&self) -> bool {
        self.offsets_builder.is_empty()
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        self.offsets_builder.append(self.values_builder.len() as u32);

        let array_data = ArrayData::builder(self.data_type())
            .len(self.offsets_builder.len() - 1)
            .add_buffer(self.offsets_builder.finish())
            .add_buffer(self.values_builder.finish())
            .null_bit_buffer(self.null_map_builder.finish())
            .build();

        if self.data_type() == DataType::Utf8 {
            Arc::new(StringArray::from(array_data))
        } else {
            Arc::new(BinaryArray::from(array_data))
        }
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

// if the native arrow builder has been optimized, should switch back to the native StringBuilder and BinaryBuilder
type StringBuilder = BinaryColumnBuilder<str>;
type BinaryBuilder = BinaryColumnBuilder<[u8]>;

/// An ArrayBuilder to build rslex::Value::Error array
struct ErrorAsStructBuilder {
    error_code_builder: StringBuilder,
    source_value_builder: Box<ArrowColumnBuilder>,
    error_details_builder: RecordColumnBuilder,
    bitmap_builder: BooleanBufferBuilder,
}

impl ErrorAsStructBuilder {
    pub fn new(options: RecordBatchBuilderOptions, capacity: usize) -> Self {
        let source_value_column_name: Arc<str> = COLUMN_SOURCE_VALUE.into();
        let source_value_options = options.for_nested_column(&source_value_column_name);
        let error_details_options = options.for_nested_column(&COLUMN_ERROR_DETAILS.into());
        ErrorAsStructBuilder {
            error_code_builder: StringBuilder::new(capacity),
            source_value_builder: Box::new(ArrowColumnBuilder::new(source_value_column_name, source_value_options, 0)),
            error_details_builder: RecordColumnBuilder::new(error_details_options, capacity),
            bitmap_builder: BooleanBufferBuilder::new(capacity),
        }
    }

    pub fn append_value(&mut self, error: &ErrorValue) -> Result<(), DataMaterializationError> {
        self.error_code_builder
            .append_value(error.error_code.as_ref())
            .map_err(map_arrow_error)?;
        // we don't have the parent record info here, put in a placeholder
        self.source_value_builder.add_value(&error.source_value, &Record::empty())?;
        match &error.error_details {
            Some(detail) => self.error_details_builder.append(detail)?,
            None => self.error_details_builder.append_null().map_err(map_arrow_error)?,
        };
        self.bitmap_builder.append(true);
        Ok(())
    }

    pub fn append_null(&mut self) -> Result<(), ArrowError> {
        self.error_code_builder.append_null()?;
        self.source_value_builder.append_null()?;
        self.error_details_builder.append_null()?;
        self.bitmap_builder.append(false);

        Ok(())
    }
}

impl ArrayBuilder for ErrorAsStructBuilder {
    fn len(&self) -> usize {
        self.error_code_builder.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn finish(&mut self) -> ArrayRef {
        let len = self.error_code_builder.len();
        debug_assert_eq!(self.source_value_builder.len(), len);
        debug_assert_eq!(self.error_details_builder.len(), len);

        let error_code_data = self.error_code_builder.finish();
        let source_value_data = self.source_value_builder.as_mut().finish();
        let error_details_data = self.error_details_builder.finish();

        let fields = vec![
            Field::new(HEADER_ERROR_VALUE, DataType::Null, true),
            Field::new(COLUMN_ERROR_CODE, DataType::Utf8, true),
            Field::new(COLUMN_SOURCE_VALUE, source_value_data.data_type().clone(), true),
            Field::new(COLUMN_ERROR_DETAILS, error_details_data.data_type().clone(), true),
        ];
        let child_data = vec![
            NullArray::new(len).data(),
            error_code_data.data(),
            source_value_data.data(),
            error_details_data.data(),
        ];

        let null_bit_buffer = self.bitmap_builder.finish();
        let null_count = len - null_bit_buffer.count_set_bits();
        let mut builder = ArrayData::builder(DataType::Struct(fields)).len(len).child_data(child_data);
        if null_count > 0 {
            builder = builder.null_bit_buffer(null_bit_buffer);
        }

        Arc::new(StructArray::from(builder.build()))
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

trait StreamInfoColumnBuilder: ArrayBuilder {
    fn append_value(&mut self, value: &Rc<StreamInfo>) -> Result<(), DataMaterializationError>;
    fn append_null(&mut self) -> Result<(), ArrowError>;
}

/// An ArrayBuilder to build rslex::Value::StreamInfo datatype array.
struct StreamInfoAsStructBuilder {
    handler_builder: StringDictionaryBuilder<UInt8Type>,
    resource_id_builder: StringBuilder,
    arguments_builder: RecordColumnBuilder,
    bitmap_builder: BooleanBufferBuilder,
}

impl StreamInfoColumnBuilder for StreamInfoAsStructBuilder {
    fn append_value(&mut self, value: &Rc<StreamInfo>) -> Result<(), DataMaterializationError> {
        let offset = self.resource_id_builder.len();
        debug_assert_eq!(self.arguments_builder.len(), offset);
        debug_assert_eq!(self.handler_builder.len(), offset);
        debug_assert_eq!(self.bitmap_builder.len(), offset);

        self.handler_builder.append(value.handler()).map_err(map_arrow_error)?;
        self.resource_id_builder
            .append_value(value.resource_id())
            .map_err(map_arrow_error)?;
        let record = Record::from(value.arguments());
        self.arguments_builder.append(&record)?;
        self.bitmap_builder.append(true);

        Ok(())
    }

    fn append_null(&mut self) -> Result<(), ArrowError> {
        let offset = self.resource_id_builder.len();
        debug_assert_eq!(self.arguments_builder.len(), offset);
        debug_assert_eq!(self.handler_builder.len(), offset);

        self.handler_builder.append_null()?;
        self.resource_id_builder.append_null()?;
        self.arguments_builder.append_null()?;
        self.bitmap_builder.append(false);

        Ok(())
    }
}

impl ArrayBuilder for StreamInfoAsStructBuilder {
    fn len(&self) -> usize {
        self.resource_id_builder.len()
    }

    fn is_empty(&self) -> bool {
        self.resource_id_builder.is_empty()
    }

    fn finish(&mut self) -> ArrayRef {
        let len = self.resource_id_builder.len();
        debug_assert_eq!(self.arguments_builder.len(), len);
        debug_assert_eq!(self.handler_builder.len(), len);
        let argument_data = self.arguments_builder.finish().data();
        let fields = vec![
            Field::new(HEADER_STREAM_INFO, DataType::Null, true),
            Field::new(COLUMN_HANDLER, DataType::Utf8, true),
            Field::new(COLUMN_RESOUCE_ID, DataType::Utf8, true),
            Field::new(COLUMN_ARGUMENTS, argument_data.data_type().clone(), true),
        ];
        let child_data = vec![
            NullArray::new(len).data(),
            self.handler_builder.finish().data(),
            self.resource_id_builder.finish().data(),
            argument_data,
        ];

        let null_bit_buffer = self.bitmap_builder.finish();
        let null_count = len - null_bit_buffer.count_set_bits();
        let mut builder = ArrayData::builder(DataType::Struct(fields)).len(len).child_data(child_data);
        if null_count > 0 {
            builder = builder.null_bit_buffer(null_bit_buffer);
        }

        Arc::new(StructArray::from(builder.build()))
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

struct StreamInfoAsStringBuilder {
    resource_id_builder: StringBuilder,
}

impl StreamInfoColumnBuilder for StreamInfoAsStringBuilder {
    fn append_value(&mut self, value: &Rc<StreamInfo>) -> Result<(), DataMaterializationError> {
        self.resource_id_builder.append_value(value.resource_id()).map_err(map_arrow_error)
    }

    fn append_null(&mut self) -> Result<(), ArrowError> {
        self.resource_id_builder.append_null()
    }
}

impl ArrayBuilder for StreamInfoAsStringBuilder {
    fn len(&self) -> usize {
        self.resource_id_builder.len()
    }

    fn is_empty(&self) -> bool {
        self.resource_id_builder.is_empty()
    }

    fn finish(&mut self) -> ArrayRef {
        self.resource_id_builder.finish()
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

struct CollectStreamInfoBuilder {
    collector: StreamInfoCollector,
    len: usize,
}

impl StreamInfoColumnBuilder for CollectStreamInfoBuilder {
    fn append_value(&mut self, value: &Rc<StreamInfo>) -> Result<(), DataMaterializationError> {
        self.len += 1;
        self.collector.collect_value(value.clone());
        Ok(())
    }

    fn append_null(&mut self) -> Result<(), ArrowError> {
        self.len += 1;
        self.collector.collect_none();
        Ok(())
    }
}

impl ArrayBuilder for CollectStreamInfoBuilder {
    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(NullArray::new(self.len))
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

fn create_stream_info_builder(
    column_name: Arc<str>,
    options: &RecordBatchBuilderOptions,
    capacity: usize,
) -> Box<dyn StreamInfoColumnBuilder> {
    match &options.stream_info_handling {
        StreamInfoHandling::AsString => Box::new(StreamInfoAsStringBuilder {
            resource_id_builder: StringBuilder::new(capacity),
        }),
        StreamInfoHandling::AsStruct => Box::new(StreamInfoAsStructBuilder {
            handler_builder: StringDictionaryBuilder::new(UInt8Builder::new(capacity), arrow::array::StringBuilder::new(1)),
            resource_id_builder: StringBuilder::new(capacity),
            arguments_builder: RecordColumnBuilder::new(options.for_nested_column(&column_name), capacity), /* TODO: Project Streaminfo handling for nested struct */
            bitmap_builder: BooleanBufferBuilder::new(capacity),
        }),
        StreamInfoHandling::NullAndCollect(collector) => Box::new(CollectStreamInfoBuilder {
            collector: collector.for_stream_column(column_name),
            len: 0,
        }),
    }
}

enum ColumnType {
    Null,
    Boolean(BooleanBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    DateTime(TimestampNanosecondBuilder),
    Binary(BinaryBuilder),
    List(ListColumnBuilder),
    Record(RecordColumnBuilder),
    StreamInfo(Box<dyn StreamInfoColumnBuilder>),
    Error(ErrorAsStructBuilder),
}

/// An ArrayBuilder to build an arbitrary rslex::Value array.
struct ArrowColumnBuilder {
    len: usize,
    pending_null_count: usize,
    column_name: Arc<str>,
    column_type: ColumnType,
    options: RecordBatchBuilderOptions,
}

// DateTimes will be materialized as nanosecond-resolution timestamps. These are our boundaries
// in second-resolution timestamps so we can safely add the nanosecond component.
const MAX_TIMESTAMP: i64 = 9_223_372_035;
const MIN_TIMESTAMP: i64 = -9_223_372_036;

lazy_static! {
    static ref MIN_DATETIME: SyncValue = SyncValue::DateTime(DateTime::from_utc(NaiveDateTime::from_timestamp(MIN_TIMESTAMP, 0), Utc));
    static ref MAX_DATETIME: SyncValue = SyncValue::DateTime(DateTime::from_utc(NaiveDateTime::from_timestamp(MAX_TIMESTAMP, 0), Utc));
}

pub fn map_arrow_error(error: ArrowError) -> DataMaterializationError {
    match error {
        ArrowError::MemoryError(_) => DataMaterializationError::MemoryError,
        _ => {
            tracing::error!("[map_arrow_error()] unexpected Arrow error.");
            // ArrowError doesn't implement the Error trait, so we can't box it.
            DataMaterializationError::ExternalError {
                message: error.to_string(),
                source: None,
            }
        },
    }
}

impl ArrowColumnBuilder {
    pub fn new(column_name: Arc<str>, options: RecordBatchBuilderOptions, null_fill_count: usize) -> ArrowColumnBuilder {
        ArrowColumnBuilder {
            len: 0,
            pending_null_count: null_fill_count,
            column_name,
            column_type: ColumnType::Null,
            options,
        }
    }

    fn init_builder(&mut self, kind: ValueKind) -> Result<(), ArrowError> {
        macro_rules! impl_init {
            ($builder_create:expr, $column_type:expr) => {{
                let mut builder = $builder_create(max(self.pending_null_count, 0));
                for _ in 0..self.pending_null_count {
                    builder.append_null()?;
                }
                self.column_type = $column_type(builder);
            }};
        }

        match kind {
            ValueKind::Boolean => impl_init!(BooleanArray::builder, ColumnType::Boolean),
            ValueKind::Int64 => impl_init!(Int64Array::builder, ColumnType::Int64),
            ValueKind::Float64 => impl_init!(Float64Array::builder, ColumnType::Float64),
            ValueKind::String => impl_init!(|c| StringBuilder::new(c), ColumnType::String),
            ValueKind::DateTime => impl_init!(TimestampNanosecondBuilder::new, ColumnType::DateTime),
            ValueKind::Binary => impl_init!(|c| BinaryBuilder::new(c), ColumnType::Binary),
            ValueKind::Record => impl_init!(
                |c| RecordColumnBuilder::new(self.options.for_nested_column(&self.column_name), c),
                ColumnType::Record
            ),
            ValueKind::List => impl_init!(
                |_| ListColumnBuilder::new(self.column_name.clone(), self.options.clone()),
                ColumnType::List
            ),
            ValueKind::StreamInfo => impl_init!(
                |c| create_stream_info_builder(self.column_name.clone(), &self.options, c),
                ColumnType::StreamInfo
            ),
            ValueKind::Error => impl_init!(|c| ErrorAsStructBuilder::new(self.options.clone(), c), ColumnType::Error),
            ValueKind::Null => panic!("Arrow columns should not be initialized as type Null."),
        }

        Ok(())
    }

    #[allow(clippy::cognitive_complexity)]
    pub fn add_value(&mut self, value: &Value, parent_record: &Record) -> Result<(), DataMaterializationError> {
        macro_rules! impl_add_value {
            ($value:tt, $kind:tt, $builder:expr, $value_extractor:expr) => {{
                self.len += 1;
                match $value {
                    Value::Null => $builder.append_null().map_err(map_arrow_error),
                    Value::Error(err) if ValueKind::$kind != ValueKind::Error => {
                        if self.options.error_handling == ErrorHandling::Null {
                            $builder.append_null().map_err(map_arrow_error)
                        } else {
                            Err(DataMaterializationError::UnexpectedErrorValue {
                                error: Box::new(SyncErrorValue::from(*err.clone())),
                                parent_record: parent_record.into(),
                            })
                        }
                    },
                    Value::$kind(v) => $value_extractor($builder, v),
                    _ => {
                        if self.options.mixed_type_handling == ExceptionHandling::Null {
                            $builder.append_null().map_err(map_arrow_error)
                        } else {
                            Err(DataMaterializationError::UnexpectedType(UnexpectedTypeError {
                                value: $value.clone().into(),
                                column: self.column_name.to_string(),
                                expected_type: ValueKind::$kind,
                            }))
                        }
                    },
                }
            }};
        }

        match &mut self.column_type {
            ColumnType::Null => match value {
                Value::Null => {
                    self.pending_null_count += 1;
                    Ok(())
                },
                Value::Error(err) => match self.options.error_handling {
                    ErrorHandling::Null => {
                        self.pending_null_count += 1;
                        Ok(())
                    },
                    ErrorHandling::Fail => Err(DataMaterializationError::UnexpectedErrorValue {
                        error: Box::new(SyncErrorValue::from(*err.clone())),
                        parent_record: parent_record.into(),
                    }),
                    ErrorHandling::AsStruct => {
                        self.init_builder(ValueKind::Error).map_err(map_arrow_error)?;
                        self.add_value(value, parent_record)
                    },
                },
                _ => {
                    self.init_builder(ValueKind::from(value)).map_err(map_arrow_error)?;
                    self.add_value(value, parent_record)
                },
            },
            ColumnType::Boolean(b) => impl_add_value!(value, Boolean, b, |b: &mut BooleanBuilder, v: &bool| b
                .append_value(*v)
                .map_err(map_arrow_error)),
            ColumnType::Int64(b) => impl_add_value!(value, Int64, b, |b: &mut Int64Builder, v: &i64| b
                .append_value(*v)
                .map_err(map_arrow_error)),
            ColumnType::Float64(b) => impl_add_value!(value, Float64, b, |b: &mut Float64Builder, v: &f64| b
                .append_value(*v)
                .map_err(map_arrow_error)),
            ColumnType::String(b) => impl_add_value!(value, String, b, |b: &mut StringBuilder, v: &StrTendril| b
                .append_value(v)
                .map_err(map_arrow_error)),
            ColumnType::DateTime(b) => {
                let out_of_range_datetime_handling = &self.options.out_of_range_datetime_handling;
                impl_add_value!(value, DateTime, b, |b: &mut TimestampNanosecondBuilder, v: &DateTime<Utc>| {
                    let timestamp = v.timestamp();
                    if timestamp > MAX_TIMESTAMP || timestamp < MIN_TIMESTAMP {
                        if *out_of_range_datetime_handling == ExceptionHandling::Null {
                            b.append_null().map_err(map_arrow_error)
                        } else {
                            Err(DataMaterializationError::OutOfRangeValue(OutOfRangeValueError {
                                value: SyncValue::DateTime(*v),
                                min: MIN_DATETIME.clone(),
                                max: MAX_DATETIME.clone(),
                            }))
                        }
                    } else {
                        let nanosecond_timestamp = timestamp * 1_000_000_000 + v.timestamp_subsec_nanos() as i64;
                        b.append_value(nanosecond_timestamp).map_err(map_arrow_error)
                    }
                })
            },
            ColumnType::Binary(b) => impl_add_value!(value, Binary, b, |b: &mut BinaryBuilder, v: &ByteTendril| b
                .append_value(&v)
                .map_err(map_arrow_error)),
            ColumnType::List(b) => impl_add_value!(value, List, b, |b: &mut ListColumnBuilder, v: &Vec<Value>| b
                .append(v, parent_record)),
            ColumnType::Record(b) => impl_add_value!(value, Record, b, |b: &mut RecordColumnBuilder, v: &Record| b.append(v)),
            ColumnType::StreamInfo(b) => impl_add_value!(
                value,
                StreamInfo,
                b,
                |b: &mut Box<dyn StreamInfoColumnBuilder>, v: &Rc<StreamInfo>| b.append_value(v)
            ),
            ColumnType::Error(b) => impl_add_value!(value, Error, b, |b: &mut ErrorAsStructBuilder, v: &ErrorValue| b.append_value(v)),
        }
    }

    pub fn append_null(&mut self) -> Result<(), ArrowError> {
        self.len += 1;
        match &mut self.column_type {
            ColumnType::Null => {
                self.pending_null_count += 1;
                Ok(())
            },
            ColumnType::Boolean(b) => b.append_null(),
            ColumnType::Int64(b) => b.append_null(),
            ColumnType::Float64(b) => b.append_null(),
            ColumnType::String(b) => b.append_null(),
            ColumnType::DateTime(b) => b.append_null(),
            ColumnType::Binary(b) => b.append_null(),
            ColumnType::Record(b) => b.append_null(),
            ColumnType::List(b) => b.append_null(),
            ColumnType::StreamInfo(b) => b.append_null(),
            ColumnType::Error(b) => b.append_null(),
        }
    }
}

impl ArrayBuilder for ArrowColumnBuilder {
    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        match &mut self.column_type {
            ColumnType::Null => Arc::new(NullArray::new(self.pending_null_count)),
            ColumnType::Boolean(b) => ArrayBuilder::finish(b),
            ColumnType::Int64(b) => ArrayBuilder::finish(b),
            ColumnType::Float64(b) => ArrayBuilder::finish(b),
            ColumnType::String(b) => ArrayBuilder::finish(b),
            ColumnType::DateTime(b) => ArrayBuilder::finish(b),
            ColumnType::Binary(b) => ArrayBuilder::finish(b),
            ColumnType::List(b) => ArrayBuilder::finish(b),
            ColumnType::Record(b) => ArrayBuilder::finish(b),
            ColumnType::StreamInfo(b) => ArrayBuilder::finish(b.as_mut()),
            ColumnType::Error(b) => ArrayBuilder::finish(b),
        }
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// An ArrayBuilder to build rslex::Value::List datatype array.
struct ListColumnBuilder {
    list_builder: Box<ListBuilder<ArrowColumnBuilder>>,
    count: usize,
}

impl ListColumnBuilder {
    pub fn new(column_name: Arc<str>, options: RecordBatchBuilderOptions) -> ListColumnBuilder {
        ListColumnBuilder {
            list_builder: Box::new(ListBuilder::new(ArrowColumnBuilder::new(column_name, options, 0))),
            count: 0,
        }
    }

    pub fn append(&mut self, list: &[Value], parent_record: &Record) -> Result<(), DataMaterializationError> {
        for item in list {
            self.list_builder.values().add_value(item, parent_record)?;
        }

        self.list_builder.append(true).map_err(map_arrow_error)
    }

    pub fn append_null(&mut self) -> Result<(), ArrowError> {
        self.list_builder.append(false)
    }
}

impl ArrayBuilder for ListColumnBuilder {
    fn len(&self) -> usize {
        self.count
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(self.list_builder.finish())
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// An ArrayBuilder to build rslex::Value::Record datatype array.
struct RecordColumnBuilder {
    columns: Vec<ArrowColumnBuilder>,
    bitmap_builder: BooleanBufferBuilder,
    column_names: HashSet<Arc<str>>,
    indices: Vec<Option<usize>>,
    last_schema_seen: RecordSchema,
    options: RecordBatchBuilderOptions,
    count: usize,
}

impl RecordColumnBuilder {
    pub fn new(options: RecordBatchBuilderOptions, capacity: usize) -> RecordColumnBuilder {
        RecordColumnBuilder {
            columns: vec![],
            bitmap_builder: BooleanBufferBuilder::new(capacity),
            column_names: Default::default(),
            indices: vec![],
            last_schema_seen: Default::default(),
            options,
            count: 0,
        }
    }

    pub fn append(&mut self, record: &Record) -> Result<(), DataMaterializationError> {
        if *record.schema() != self.last_schema_seen {
            self.last_schema_seen = record.schema().clone();
            for new_column in self.last_schema_seen.iter() {
                if !self.column_names.contains(new_column) {
                    self.column_names.insert(new_column.clone());
                    self.columns
                        .push(ArrowColumnBuilder::new(new_column.clone(), self.options.clone(), self.count));
                }
            }

            let mut field_selector = MultiFieldSelector::new(self.columns.iter().map(|c| c.column_name.clone()).collect());
            self.indices = field_selector.get_indices(&self.last_schema_seen).to_vec();
        }

        for i in 0..self.columns.len() {
            let value = if let Some(index) = self.indices[i] {
                &record[index]
            } else {
                &Value::Null
            };
            self.columns[i].add_value(value, record)?;
        }

        self.count += 1;
        self.bitmap_builder.append(true);
        Ok(())
    }

    pub fn append_null(&mut self) -> Result<(), ArrowError> {
        for column in &mut self.columns {
            column.append_null()?;
        }

        self.count += 1;
        self.bitmap_builder.append(false);
        Ok(())
    }
}

impl ArrayBuilder for RecordColumnBuilder {
    fn len(&self) -> usize {
        self.count
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        if self.count != 0 && self.columns.is_empty() {
            // in this case, all the record added are either null or empty
            // we cannot derive a schema, will fallback to a NullArray

            Arc::new(NullArray::new(self.count))
        } else {
            let child_data = self.columns.iter_mut().map(|b| b.finish().data()).collect_vec();
            let fields = self
                .columns
                .iter()
                .zip(child_data.iter())
                .map(|(builder, array)| Field::new(builder.column_name.as_ref(), array.data_type().clone(), true))
                .collect_vec();
            let null_bit_buffer = self.bitmap_builder.finish();
            let null_count = self.count - null_bit_buffer.count_set_bits();
            let mut builder = ArrayData::builder(DataType::Struct(fields)).len(self.count).child_data(child_data);
            if null_count > 0 {
                builder = builder.null_bit_buffer(null_bit_buffer);
            }

            self.count = 0;
            Arc::new(StructArray::from(builder.build()))
        }
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    #[cfg(not(tarpaulin_include))]
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Class to build a RecordBatch from batch of rslex::Record
pub struct RecordBatchBuilder {
    record_builder: RecordColumnBuilder,
}

impl RecordBatchBuilder {
    pub fn new(options: RecordBatchBuilderOptions) -> RecordBatchBuilder {
        RecordBatchBuilder {
            record_builder: RecordColumnBuilder::new(options, 0),
        }
    }

    pub fn append(&mut self, record: &Record) -> Result<(), DataMaterializationError> {
        self.record_builder.append(record)
    }

    pub fn finish(self) -> Result<RecordBatch, DataMaterializationError> {
        let mut builder = self.record_builder;
        if builder.columns.is_empty() {
            Err(DataMaterializationError::NoColumns {
                record_count: builder.count,
            })
        } else {
            let array = builder.finish();

            let struct_array: &StructArray = array.as_any().downcast_ref::<StructArray>().unwrap();
            Ok(RecordBatch::from(struct_array))
        }
    }
}

/// Enables conversion of Record iterators to RecordBatches.
pub trait IntoRecordBatch {
    /// Collect the records in an iterator into a RecordBatch.
    ///
    /// Arrow RecordBatches cannot contain multiple type and the range of DateTime values supported
    /// is smaller. The different parameters decide how to handle errors due to these differences.
    /// In addition, one must also specify how to handle any [`ErrorValues`](crate::ErrorValue) in the
    /// Dataset.
    fn collect_record_batch(self, options: RecordBatchBuilderOptions) -> Result<RecordBatch, DataMaterializationError>;
}

impl<T: IntoIterator<Item = Record>> IntoRecordBatch for T {
    #[tracing::instrument(err, skip(self))]
    fn collect_record_batch(self, options: RecordBatchBuilderOptions) -> Result<RecordBatch, DataMaterializationError> {
        let mut record_builder = RecordBatchBuilder::new(options);
        for record in self {
            record_builder.append(&record)?;
        }

        record_builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{record, test_helper::*, ErrorValue, SyncRecord};
    use chrono::{Duration, NaiveDateTime};
    use fluent_assertions::*;
    use itertools::Itertools;
    use std::convert::TryFrom;

    fn default_options() -> RecordBatchBuilderOptions {
        RecordBatchBuilderOptions {
            mixed_type_handling: ExceptionHandling::Null,
            out_of_range_datetime_handling: ExceptionHandling::Null,
            error_handling: ErrorHandling::Null,
            stream_info_handling: StreamInfoHandling::AsString,
        }
    }

    #[test]
    fn load_empty_records_returns_data_materialization_error() {
        Vec::new()
            .into_iter()
            .collect_record_batch(default_options())
            .should()
            .be_err()
            .which_value()
            .should()
            .equal(&DataMaterializationError::NoColumns { record_count: 0 });
    }

    fn records_from_single_column_option_results<T>(values: Vec<Option<T>>) -> Vec<Record>
    where Value: From<T> {
        values
            .into_iter()
            .map(|o| o.map_or(Value::Null, Value::from))
            .map(|v| Record::new(vec![v], RecordSchema::try_from(vec!["Column1"]).unwrap()))
            .collect_vec()
    }

    fn records_from_single_column_options<T>(values: Vec<Option<T>>) -> Vec<Record>
    where Value: From<T> {
        records_from_single_column_option_results(values.into_iter().collect_vec())
    }

    fn records_from_single_column_values<T>(values: Vec<T>) -> Vec<Record>
    where Value: From<T> {
        records_from_single_column_option_results(values.into_iter().map(Some).collect_vec())
    }

    fn test_single_column_values<T>(values: Vec<T>)
    where Value: From<T> {
        let data = records_from_single_column_values(values);

        data.clone()
            .collect_record_batch(default_options())
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(data);
    }

    fn test_single_column_options<T>(values: Vec<Option<T>>)
    where Value: From<T> {
        let data = records_from_single_column_options(values);

        data.clone()
            .collect_record_batch(default_options())
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(data);
    }

    #[test]
    fn load_records_with_int64() {
        test_single_column_values(vec![1, 2, 3, 4]);
    }

    #[test]
    fn load_records_with_int64_and_nulls() {
        test_single_column_options(vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn load_records_with_float64() {
        test_single_column_values(vec![1.3, 2.1, 3.5, 4.2]);
    }

    #[test]
    fn load_records_with_float64_and_nulls() {
        test_single_column_options(vec![Some(1.1), None, Some(3.3)]);
    }

    #[test]
    fn load_records_with_strings() {
        test_single_column_values(vec!["asd", "efg", "xyz"]);
    }

    #[test]
    fn load_records_with_strings_and_nulls() {
        test_single_column_options(vec![Some("asd"), None, Some("efh")]);
    }

    #[test]
    fn load_records_with_binary() {
        test_single_column_values(vec![
            ByteTendril::from(vec![0u8, 2].as_slice()),
            ByteTendril::from(vec![3u8, 4].as_slice()),
            ByteTendril::from(vec![5u8, 0].as_slice()),
        ]);
    }

    #[test]
    fn load_records_with_binary_and_nulls() {
        test_single_column_options(vec![
            Some(ByteTendril::from(vec![1u8, 2].as_slice())),
            None,
            Some(ByteTendril::from(vec![1u8, 2].as_slice())),
        ]);
    }

    #[test]
    fn load_records_with_datetimes() {
        test_single_column_values(vec![
            DateTime::parse_from_rfc3339("2020-01-01T12:30:00Z").unwrap().with_timezone(&Utc),
            DateTime::parse_from_rfc3339("2020-01-02T12:30:00Z").unwrap().with_timezone(&Utc),
            DateTime::parse_from_rfc3339("2020-01-03T12:30:00Z").unwrap().with_timezone(&Utc),
        ]);
    }

    #[test]
    fn load_records_with_datetimes_and_nulls() {
        test_single_column_options(vec![
            Some(DateTime::parse_from_rfc3339("2020-01-01T12:30:00Z").unwrap().with_timezone(&Utc)),
            None,
            Some(DateTime::parse_from_rfc3339("2020-01-01T12:30:00Z").unwrap().with_timezone(&Utc)),
        ]);
    }

    #[test]
    fn load_records_with_bools() {
        test_single_column_values(vec![true, false, false, true]);
    }

    #[test]
    fn load_records_with_bools_and_nulls() {
        test_single_column_options(vec![Some(true), None, Some(false), None]);
    }

    #[test]
    fn load_records_with_record_column() {
        let schema = RecordSchema::try_from(vec!["Column1", "Column2"]).unwrap();
        test_single_column_values(vec![
            Box::new(Record::new(vec![Value::from("asd"), Value::from(2)], schema.clone())),
            Box::new(Record::new(vec![Value::from("afg"), Value::from(3)], schema)),
        ]);
    }

    #[test]
    fn load_records_with_record_column_and_nulls() {
        let schema = RecordSchema::try_from(vec!["Column1", "Column2"]).unwrap();
        test_single_column_options(vec![
            Some(Box::new(Record::new(vec![Value::from("asd"), Value::from(2)], schema.clone()))),
            None,
            Some(Box::new(Record::new(vec![Value::from("afg"), Value::from(3)], schema))),
        ]);
    }

    #[test]
    fn load_records_with_list_column() {
        test_single_column_values(vec![
            Box::new(vec![Value::Int64(2), Value::Int64(3)]),
            Box::new(vec![Value::Int64(4), Value::Int64(5)]),
        ]);
    }

    #[test]
    fn load_records_with_list_column_and_nulls() {
        test_single_column_options(vec![
            Some(Box::new(vec![Value::Int64(2), Value::Int64(3)])),
            None,
            Some(Box::new(vec![Value::Int64(4), Value::Int64(5)])),
        ]);
    }

    #[test]
    fn load_records_with_streams_as_struct() {
        let records = vec![
            record! { "Column1" => StreamInfo::new("Handler", "Resource", SyncRecord::empty()) },
            record! { "Column1" => StreamInfo::new("Handler", "Resource2", SyncRecord::empty()) },
        ];

        records
            .clone()
            .collect_record_batch(default_options().with_stream_info_handling(StreamInfoHandling::AsStruct))
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(records);
    }

    #[test]
    fn load_records_with_streams_as_string() {
        let records = vec![
            record! { "Column1" => StreamInfo::new("Handler", "Resource", SyncRecord::empty()) },
            record! { "Column1" => StreamInfo::new("Handler", "Resource2", SyncRecord::empty()) },
        ];

        records
            .collect_record_batch(default_options())
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(vec![record! { "Column1" => "Resource" }, record! { "Column1" => "Resource2" }]);
    }

    #[test]
    fn load_records_with_streams_and_nulls_as_struct() {
        let records = vec![
            record! { "Column1" => StreamInfo::new("Handler", "Resource", SyncRecord::empty()) },
            record! { "Column1" => Value::Null },
            record! { "Column1" => StreamInfo::new("Handler", "Resource2", SyncRecord::empty()) },
        ];

        records
            .clone()
            .collect_record_batch(default_options().with_stream_info_handling(StreamInfoHandling::AsStruct))
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(records);
    }

    #[test]
    fn load_records_with_streams_and_nulls() {
        let records = vec![
            record! { "Column1" => StreamInfo::new("Handler", "Resource", SyncRecord::empty()) },
            record! { "Column1" => Value::Null },
            record! { "Column1" => StreamInfo::new("Handler", "Resource2", SyncRecord::empty()) },
        ];

        records
            .collect_record_batch(default_options())
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(vec![
                record! { "Column1" => "Resource" },
                record! { "Column1" => Value::Null },
                record! { "Column1" => "Resource2" },
            ]);
    }

    #[test]
    fn load_records_with_streams_and_collect_handling_column_in_record_batch_is_null() {
        let records = vec![
            record! { "Stream" => StreamInfo::new("Handler", "Resource", SyncRecord::empty())},
            record! { "Stream" => StreamInfo::new("Handler", "Resource", SyncRecord::empty())},
        ];

        records
            .collect_record_batch(default_options().with_stream_info_handling(StreamInfoHandling::NullAndCollect(Default::default())))
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(vec![record! { "Stream" => Value::Null }, record! { "Stream" => Value::Null }]);
    }

    #[test]
    fn load_records_with_streams_and_collect_handling_collector_contains_correct_stream_values() {
        let records = vec![
            record! { "Stream" => StreamInfo::new("Handler", "Resource1", SyncRecord::empty())},
            record! { "Stream" => StreamInfo::new("Handler", "Resource2", SyncRecord::empty())},
        ];

        let mut collector = StreamInfoCollector::default();
        records
            .collect_record_batch(default_options().with_stream_info_handling(StreamInfoHandling::NullAndCollect(collector.clone())))
            .unwrap();

        (&collector.take_collected_columns()[&vec!["Stream".into()]]).should().equal(&&vec![
            Some(Rc::new(StreamInfo::new("Handler", "Resource1", SyncRecord::empty()))),
            Some(Rc::new(StreamInfo::new("Handler", "Resource2", SyncRecord::empty()))),
        ]);
    }

    #[test]
    fn load_records_with_streams_and_nulls_and_collect_handling_collector_contains_correct_values() {
        let records = vec![
            record! { "Stream" => StreamInfo::new("Handler", "Resource1", SyncRecord::empty())},
            record! { "Stream" => Value::Null },
        ];

        let mut collector = StreamInfoCollector::default();
        records
            .collect_record_batch(default_options().with_stream_info_handling(StreamInfoHandling::NullAndCollect(collector.clone())))
            .unwrap();

        (&collector.take_collected_columns()[&vec!["Stream".into()]]).should().equal(&&vec![
            Some(Rc::new(StreamInfo::new("Handler", "Resource1", SyncRecord::empty()))),
            None,
        ]);
    }

    #[test]
    fn load_records_with_nested_stream_column_and_collect_handling_nested_column_is_null_column() {
        let records = vec![record! { "Parent" => record! { "Stream" => StreamInfo::new("Handler", "Resource", SyncRecord::empty()) } }];
        let collector = StreamInfoCollector::default();

        records
            .collect_record_batch(default_options().with_stream_info_handling(StreamInfoHandling::NullAndCollect(collector.clone())))
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(vec![record! { "Parent" => record! { "Stream" => Value::Null } }]);
    }

    #[test]
    fn load_records_with_nested_stream_column_and_collect_handling_collector_contains_correct_values() {
        let records = vec![record! { "Parent" => record! { "Stream" => StreamInfo::new("Handler", "Resource", SyncRecord::empty()) } }];
        let mut collector = StreamInfoCollector::default();

        records
            .collect_record_batch(default_options().with_stream_info_handling(StreamInfoHandling::NullAndCollect(collector.clone())))
            .unwrap();

        (&collector.take_collected_columns()[&vec!["Parent".into(), "Stream".into()]])
            .should()
            .equal(&&vec![Some(Rc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty())))]);
    }

    #[test]
    fn load_records_with_double_nested_stream_column_and_collect_handling_collector_contains_correct_values() {
        let records = vec![
            record! { "GrandParent" => record! { "Parent" => record! { "Stream" => StreamInfo::new("Handler", "Resource", SyncRecord::empty()) } }},
        ];
        let mut collector = StreamInfoCollector::default();

        records
            .collect_record_batch(default_options().with_stream_info_handling(StreamInfoHandling::NullAndCollect(collector.clone())))
            .unwrap();

        (&collector.take_collected_columns()[&vec!["GrandParent".into(), "Parent".into(), "Stream".into()]])
            .should()
            .equal(&&vec![Some(Rc::new(StreamInfo::new("Handler", "Resource", SyncRecord::empty())))]);
    }

    #[test]
    fn load_records_with_streams_and_mixed_types_and_collect_handling_collector_contains_none_for_mixed_type() {
        let records = vec![
            record! { "Stream" => StreamInfo::new("Handler", "Resource1", SyncRecord::empty())},
            record! { "Stream" => 2 },
        ];

        let mut collector = StreamInfoCollector::default();
        records
            .collect_record_batch(default_options().with_stream_info_handling(StreamInfoHandling::NullAndCollect(collector.clone())))
            .unwrap();

        (&collector.take_collected_columns()[&vec!["Stream".into()]]).should().equal(&&vec![
            Some(Rc::new(StreamInfo::new("Handler", "Resource1", SyncRecord::empty()))),
            None,
        ]);
    }

    #[test]
    fn load_records_with_streams_in_list_column_and_collect_handling_collector_contains_stream_values() {
        let first_list_streams = vec![Rc::new(StreamInfo::new("Handler", "Resource 1", SyncRecord::empty()))];
        let second_list_streams = vec![
            Rc::new(StreamInfo::new("Handler", "Resource 2", SyncRecord::empty())),
            Rc::new(StreamInfo::new("Handler", "Resource 3", SyncRecord::empty())),
        ];

        let records = vec![
            record! { "List" => Box::new(first_list_streams.iter().cloned().map(Value::StreamInfo).collect_vec()) },
            record! { "List" => Box::new(second_list_streams.iter().cloned().map(Value::StreamInfo).collect_vec()) },
        ];

        let mut collector = StreamInfoCollector::default();
        records
            .collect_record_batch(default_options().with_stream_info_handling(StreamInfoHandling::NullAndCollect(collector.clone())))
            .unwrap();

        (&collector.take_collected_columns()[&vec!["List".into()]]).should().equal(
            &&first_list_streams
                .into_iter()
                .chain(second_list_streams.into_iter())
                .map(Some)
                .collect_vec(),
        );
    }

    #[test]
    fn load_records_with_multiple_columns_same_schema() {
        let data = vec![
            record! { "Column1" => 2, "Column2" => "asd" },
            record! { "Column1" => 3, "Column2" => "efg" },
        ];

        data.clone()
            .collect_record_batch(default_options())
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(data);
    }

    #[test]
    fn load_records_with_varying_schemas() {
        let data = vec![record! { "Column1" => 2 }, record! { "Column2" => 3 }];

        data.collect_record_batch(default_options())
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(vec![
                record! {
                    "Column1" => 2,
                    "Column2" => Value::Null
                },
                record! {
                    "Column1" => Value::Null,
                    "Column2" => 3
                },
            ]);
    }

    #[test]
    fn load_records_with_column_that_is_all_null() {
        let data = vec![record! { "Column1" => Value::Null }, record! { "Column1" => Value::Null }];

        data.clone()
            .collect_record_batch(default_options())
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(data);
    }

    #[test]
    fn load_records_with_mixed_types_in_column_and_null_policy() {
        let data = vec![record! { "Column1" => 2 }, record! { "Column1" => "asd" }];

        data.collect_record_batch(default_options())
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(vec![record! { "Column1" => 2 }, record! { "Column1" => Value::Null }]);
    }

    #[test]
    fn load_records_with_mixed_types_in_column_and_error_policy() {
        let data = vec![record! { "Column1" => 2 }, record! { "Column1" => "asd" }];

        data.collect_record_batch(default_options().with_mixed_type_handling(ExceptionHandling::Fail))
            .should()
            .be_err()
            .which_value()
            .should()
            .be_unexpected_type()
            .which_value()
            .should()
            .pass(|err| {
                (&err.value).should().be(&SyncValue::from("asd"));
                err.column.as_str().should().equal_string("Column1");
                err.expected_type.should().be(ValueKind::Int64);
            });
    }

    //noinspection DuplicatedCode
    #[test]
    fn load_records_with_errors_and_null_error_policy() {
        let data = vec![
            record! { "Column1" => ErrorValue::new("Code", Value::Int64(2)) },
            record! { "Column1" => 2 },
        ];

        data.collect_record_batch(default_options().with_mixed_type_handling(ExceptionHandling::Fail))
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_num_results(2)
            .and()
            .column(0)
            .as_any()
            .should()
            .be_of_type::<Int64Array>()
            .which_value()
            .should()
            .pass(|column| {
                column.len().should().be(2);
                column.data().is_null(0).should().be_true();
                column.value(1).should().be(2);
            });
    }

    //noinspection DuplicatedCode
    #[test]
    fn load_records_with_error_in_first_record_and_fail_error_policy() {
        let data = vec![record! { "Column1" => ErrorValue::new("Code", Value::Int64(2)) }];

        data.collect_record_batch(
            default_options()
                .with_mixed_type_handling(ExceptionHandling::Fail)
                .with_error_handling(ErrorHandling::Fail),
        )
        .should()
        .be_err()
        .which_value()
        .should()
        .be_unexpected_error_value()
        .which_value()
        .error
        .should()
        .be(Box::new(SyncErrorValue::new("Code".into(), SyncValue::Int64(2))));
    }

    #[test]
    fn load_records_with_error_as_struct() {
        let records = vec![
            record! { "Column1" => ErrorValue::new("Code", Value::Int64(2)) },
            record! { "Column1" => ErrorValue::new_with_details("Code", Value::Int64(2), record!{ "a" => 1 }) },
        ];

        records
            .clone()
            .collect_record_batch(default_options().with_error_handling(ErrorHandling::AsStruct))
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(records);
    }

    #[test]
    fn load_records_with_error_and_null_as_struct() {
        let records = vec![
            record! { "Column1" => ErrorValue::new("Code", Value::Int64(2)) },
            record! { "Column1" => Value::Null },
            record! { "Column1" => ErrorValue::new_with_details("Code", Value::Int64(2), record!{ "a" => 1 }) },
        ];

        records
            .clone()
            .collect_record_batch(default_options().with_error_handling(ErrorHandling::AsStruct))
            .should()
            .be_ok()
            .which_value()
            .should()
            .contain_results_all_ok_with_records(records);
    }

    //noinspection DuplicatedCode
    #[test]
    fn load_records_with_error_not_in_first_record_and_fail_error_policy() {
        let data = vec![
            record! { "Column1" => 2 },
            record! { "Column1" => ErrorValue::new("Code", Value::Int64(2)) },
        ];

        data.collect_record_batch(
            default_options()
                .with_mixed_type_handling(ExceptionHandling::Fail)
                .with_error_handling(ErrorHandling::Fail),
        )
        .should()
        .be_err()
        .which_value()
        .should()
        .be_unexpected_error_value()
        .which_value()
        .error
        .should()
        .be(Box::new(SyncErrorValue::new("Code".into(), SyncValue::Int64(2))));
    }

    #[test]
    fn load_records_with_datetimes_outside_allowed_range_with_fail_error_policy_fails() {
        let data =
            vec![record! { "Column1" => DateTime::from_utc(NaiveDateTime::from_timestamp(MAX_TIMESTAMP, 0), Utc) + Duration::seconds(1) }];

        data.clone()
            .collect_record_batch(default_options().with_out_of_range_datetime_handling(ExceptionHandling::Fail))
            .should()
            .be_err()
            .which_value()
            .should()
            .be_out_of_range_value()
            .which_value()
            .value
            .should()
            .equal(&data[0][0]);
    }

    #[test]
    fn load_records_with_date_times_outside_allowed_range_with_null_error_policy_replaces_with_null() {
        let min_date_time = DateTime::from_utc(NaiveDateTime::from_timestamp(MIN_TIMESTAMP, 0), Utc);
        let data = vec![record! { "Column1" => min_date_time - Duration::seconds(1) }];

        data.collect_record_batch(default_options())
            .should()
            .be_ok()
            .which_value()
            .column(0)
            .is_null(0)
            .should()
            .be_true();
    }

    #[test]
    fn load_all_empty_records_returns_error() {
        let data = vec![Record::empty()];

        data.iter()
            .cloned()
            .collect_record_batch(default_options())
            .should()
            .be_err()
            .with_value(DataMaterializationError::NoColumns { record_count: 1 });
    }
}
