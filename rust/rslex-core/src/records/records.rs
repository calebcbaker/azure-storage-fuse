use crate::{values_buffer_pool::PooledValuesBuffer, SyncValue, Value};
use derivative::Derivative;
use itertools::Itertools;
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    fmt::{Debug, Display, Error, Formatter},
    ops::{Index, IndexMut},
    rc::Rc,
    sync::Arc,
};
use thiserror::Error;

/// Error returned when attempting to create a schema with duplicate fields.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("Field {:?} is duplicated in target schema.", .0)]
pub struct FieldNameConflict(pub String);

/// Error returned when attempting to retrieve the value of a field not present in the current schema.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("Requested field {:?} is missing from record schema {:?}", .field, .schema)]
pub struct ExpectedFieldMissing {
    pub field: String,
    pub schema: SyncRecordSchema,
}

#[derive(Clone, Debug, Derivative)]
#[derivative(Eq, PartialEq, PartialOrd)]
struct RecordSchemaData {
    pub(self) columns: Arc<Vec<Arc<str>>>,
    #[derivative(PartialEq = "ignore")]
    #[derivative(PartialOrd = "ignore")]
    pub(self) ordinals: Arc<HashMap<Arc<str>, usize>>,
}

lazy_static! {
    static ref EMPTY_SCHEMA_DATA: RecordSchemaData = RecordSchemaData {
        columns: Arc::new(Vec::new()),
        ordinals: Arc::new(HashMap::new()),
    };
}

impl RecordSchemaData {
    //noinspection DuplicatedCode
    pub fn new(columns: Vec<Arc<str>>) -> Result<RecordSchemaData, FieldNameConflict> {
        let mut ordinals = HashMap::new();
        for (idx, column) in columns.iter().enumerate() {
            if ordinals.insert(column.clone(), idx).is_some() {
                return Err(FieldNameConflict(column.as_ref().to_owned()));
            }
        }

        Ok(RecordSchemaData {
            columns: Arc::new(columns),
            ordinals: Arc::new(ordinals),
        })
    }

    pub fn empty() -> RecordSchemaData {
        EMPTY_SCHEMA_DATA.clone()
    }
}

/// A record schema, containing a list of column names.
///
/// This object is not thread-safe. For a thread-safe version, see [`SyncRecordSchema`].
/// When possible, prefer to use this version for lower overhead.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub struct RecordSchema {
    data: Rc<RecordSchemaData>,
}

macro_rules! impl_common_methods {
    () => {
        /// Returns the index at which values for the specified column can be found in an associated record.
        pub fn index_of(&self, column: &str) -> Option<usize> {
            self.data.ordinals.get(column).cloned()
        }

        /// Whether the schema is empty.
        pub fn is_empty(&self) -> bool {
            self.data.columns.is_empty()
        }

        /// Returns the number of columns in the schema.
        pub fn len(&self) -> usize {
            self.data.columns.len()
        }

        /// Returns an iterator to the columns in the schema.
        pub fn iter(&self) -> impl Iterator<Item = &Arc<str>> {
            self.data.columns.iter()
        }

        /// Returns a schema with a subset of the columns in this schema.
        pub fn slice(&self, offset: usize, count: usize) -> Self {
            let columns = self.data.columns.iter().skip(offset).take(count).cloned().collect_vec();
            Self::new(columns).unwrap()
        }

        /// Return a new schema with the specified field removed.
        ///
        /// This method is idempotent.
        pub fn delete_field(&self, field: &str) -> Self {
            let new_columns = self.iter().filter(|c| c.as_ref() != field).cloned().collect_vec();
            Self::new(new_columns).unwrap()
        }

        /// Creates a schema containing all the different columns in the input schemas.
        pub fn union_all<'a, T: Iterator<Item = &'a Self>>(schemas: T) -> Self {
            let mut columns_seen = HashSet::new();
            let mut columns = Vec::new();
            for schema in schemas {
                for column in schema.iter() {
                    if !columns_seen.contains(column) {
                        columns.push(column.clone());
                        columns_seen.insert(column.clone());
                    }
                }
            }

            Self::new(columns).unwrap()
        }

        /// Creates a schema from all the columns in the input schemas. If there are conflicts, this will fail.
        pub fn concat_all<'a, T: Iterator<Item = &'a Self>>(schemas: T) -> Result<Self, FieldNameConflict> {
            let mut columns = Vec::new();
            for schema in schemas {
                for column in schema.iter() {
                    columns.push(column.clone());
                }
            }

            Self::new(columns)
        }

        /// Creates a new schema by appending the columns in the specified schema to this one's.
        pub fn union(&self, other: &Self) -> Result<Self, FieldNameConflict> {
            if self == other {
                Ok(self.clone())
            } else if self.is_empty() {
                Ok(other.clone())
            } else if other.is_empty() {
                Ok(self.clone())
            } else {
                let ordinals = self.data.ordinals.clone();
                let mut new_columns = self.data.columns.as_ref().clone();
                for other_column in other.iter() {
                    if !ordinals.contains_key(other_column) {
                        new_columns.push(other_column.clone())
                    }
                }

                Self::new(new_columns)
            }
        }

        /// Creates a new schema by inserting the columns in the specified schema to this one's
        pub fn insert(&self, other: &Self, insertion_index: usize) -> Result<Self, FieldNameConflict> {
            if self.is_empty() {
                Ok(other.clone())
            } else if other.is_empty() {
                Ok(self.clone())
            } else if insertion_index == self.len() {
                Self::concat_all(vec![self, other].into_iter())
            } else {
                let mut columns = Vec::new();
                for (index, column) in self.iter().enumerate() {
                    if index == insertion_index {
                        for other_column in other.iter() {
                            columns.push(other_column.clone());
                        }
                    }

                    columns.push(column.clone());
                }

                Self::new(columns)
            }
        }
    };
}

macro_rules! impl_conversion_traits {
    ($schema_type:ty, $ptr_type:tt) => {
        impl<TItem: AsRef<str>> TryFrom<&Vec<TItem>> for $schema_type {
            type Error = FieldNameConflict;

            /// Creates a [`$schema_type`] from a Vec of str.
            ///
            /// This will clone every str into an owned string.
            fn try_from(vec: &Vec<TItem>) -> Result<Self, Self::Error> {
                let columns = vec.iter().map(|s| s.as_ref().to_string().into()).collect_vec();
                <$schema_type>::new(columns)
            }
        }

        impl<TItem: AsRef<str>> TryFrom<Vec<TItem>> for $schema_type {
            type Error = FieldNameConflict;

            /// Creates a [`$schema_type`] from a Vec of str.
            ///
            /// This will clone every str into an owned string.
            fn try_from(vec: Vec<TItem>) -> Result<Self, Self::Error> {
                <$schema_type>::try_from(&vec)
            }
        }
    };
}

impl RecordSchema {
    /// Creates a new empty schema.
    pub fn empty() -> RecordSchema {
        RecordSchema {
            data: Rc::new(RecordSchemaData::empty()),
        }
    }

    /// Creates a schema with the specified column names.
    pub fn new<T: Into<Vec<Arc<str>>>>(columns: T) -> Result<RecordSchema, FieldNameConflict> {
        Ok(RecordSchema {
            data: Rc::new(RecordSchemaData::new(columns.into())?),
        })
    }

    impl_common_methods!();
}

impl_conversion_traits!(RecordSchema, Rc);

impl Index<usize> for RecordSchema {
    type Output = str;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data.columns[index]
    }
}

impl Default for RecordSchema {
    fn default() -> Self {
        RecordSchema::empty()
    }
}

impl Display for RecordSchema {
    //noinspection DuplicatedCode
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.write_fmt(format_args!("[{}]", self.data.columns.iter().join(", ").as_str()))
    }
}

/// A record schema, containing a list of column names.
///
/// This is the thread-safe version of [`RecordSchema`]. When possible,
/// prefer to use the non-thread-safe version for lower overhead.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub struct SyncRecordSchema {
    pub(self) data: Arc<RecordSchemaData>,
}

impl SyncRecordSchema {
    /// Creates an empty schema.
    pub fn empty() -> SyncRecordSchema {
        SyncRecordSchema {
            data: Arc::new(RecordSchemaData::empty()),
        }
    }

    /// Creates a schema with the specified column names.
    pub fn new<T: Into<Vec<Arc<str>>>>(columns: T) -> Result<SyncRecordSchema, FieldNameConflict> {
        Ok(SyncRecordSchema {
            data: Arc::new(RecordSchemaData::new(columns.into())?),
        })
    }

    impl_common_methods!();
}

impl_conversion_traits!(SyncRecordSchema, Arc);

impl Index<usize> for SyncRecordSchema {
    type Output = str;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data.columns[index]
    }
}

impl Display for SyncRecordSchema {
    //noinspection DuplicatedCode
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.write_fmt(format_args!("[{}]", self.data.columns.iter().join(", ").as_str()))
    }
}

impl From<SyncRecordSchema> for RecordSchema {
    /// Creates a [`RecordSchema`] from a [`SyncRecordSchema`].
    ///
    /// This will attempt to take ownership of any column names for which there's a single outstanding reference.
    /// When there is more than one reference, this will clone the list of column names, including cloning each
    /// individual string and should be avoided in performance-sensitive code paths.
    fn from(schema: SyncRecordSchema) -> Self {
        RecordSchema {
            data: Rc::new((*schema.data).clone()),
        }
    }
}

impl From<&SyncRecordSchema> for RecordSchema {
    /// Creates a [`RecordSchema`] from a [`SyncRecordSchema`].
    ///
    /// This will clone the list of column names, including cloning each individual string and should be avoided
    /// in performance-sensitive code paths.
    fn from(schema: &SyncRecordSchema) -> Self {
        RecordSchema {
            data: Rc::new((*schema.data).clone()),
        }
    }
}

impl From<RecordSchema> for SyncRecordSchema {
    /// Creates a [`SyncRecordSchema`] from a [`RecordSchema`].
    ///
    /// This will attempt to take ownership of any column names for which there's a single outstanding reference.
    /// When there is more than one reference, this will clone the list of column names, including cloning each
    /// individual string and should be avoided in performance-sensitive code paths.
    fn from(schema: RecordSchema) -> Self {
        SyncRecordSchema {
            data: Arc::new((*schema.data).clone()),
        }
    }
}

impl From<&RecordSchema> for SyncRecordSchema {
    /// Creates a [`SyncRecordSchema`] from a [`RecordSchema`].
    ///
    /// This will clone the list of column names, including cloning each individual string and
    /// should be avoided in performance-sensitive code paths.
    fn from(schema: &RecordSchema) -> Self {
        SyncRecordSchema {
            data: Arc::new((*schema.data).clone()),
        }
    }
}

impl PartialEq<SyncRecordSchema> for RecordSchema {
    fn eq(&self, other: &SyncRecordSchema) -> bool {
        self.data.as_ref() == other.data.as_ref()
    }
}

impl PartialEq<RecordSchema> for SyncRecordSchema {
    fn eq(&self, other: &RecordSchema) -> bool {
        self.data.as_ref() == other.data.as_ref()
    }
}

impl<S: AsRef<str>> PartialEq<Vec<S>> for RecordSchema {
    fn eq(&self, other: &Vec<S>) -> bool {
        self.data.columns.len() == other.len() && self.data.columns.iter().zip_eq(other.iter()).all(|(l, r)| l.as_ref() == r.as_ref())
    }
}

/// A record of data, containing a sequence of fields identified by a schema, each containing a value.
///
/// This object is not thread-safe. For a thread-safe version, see [`SyncRecord`].
/// When possible, prefer to use this version for lower overhead.
#[derive(Clone, PartialEq, PartialOrd)]
pub struct Record {
    schema: RecordSchema,
    values: PooledValuesBuffer,
}

impl Record {
    /// Creates a new record with the specified values and schema.
    ///
    /// This call does not check that the schema and values match in length.
    pub fn new<T: Into<PooledValuesBuffer>>(values: T, schema: RecordSchema) -> Record {
        let values = values.into();
        debug_assert_eq!(values.len(), schema.len());

        Record { values, schema }
    }

    /// Creates an empty record.
    pub fn empty() -> Record {
        Record {
            values: PooledValuesBuffer::new_disconnected(),
            schema: RecordSchema::empty(),
        }
    }

    /// Retrieves the value for the specified field.
    ///
    /// This method will do a string-based lookup for the desired field on every invocation.
    /// If the same field will be retrieved from multiple records, use [`SingleFieldSelector`](crate::field_selectors::SingleFieldSelector)
    /// instead.
    pub fn get_value(&self, field: &str) -> Result<&Value, ExpectedFieldMissing> {
        self.schema.index_of(field).map_or_else(
            || {
                Err(ExpectedFieldMissing {
                    field: field.to_string(),
                    schema: self.schema.clone().into(),
                })
            },
            |i| Ok(&self.values[i]),
        )
    }

    /// Returns the record's schema.
    pub fn schema(&self) -> &RecordSchema {
        &self.schema
    }

    /// Returns an iterator to the values in the record.
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }

    /// Consumes the record and returns its schema and values.
    pub fn deconstruct(self) -> (RecordSchema, PooledValuesBuffer) {
        (self.schema, self.values)
    }

    /// Creates a new record by taking `count` elements starting from `offset`.
    pub fn slice(&self, offset: usize, count: usize) -> Record {
        let new_schema = self.schema.slice(offset, count);
        let new_values = self.iter().skip(offset).take(count).cloned().collect_vec();
        Record::new(new_values, new_schema)
    }
}

impl Index<usize> for Record {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl IndexMut<usize> for Record {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.values[index]
    }
}

macro_rules! impl_record_display {
    ($record_type:tt) => {
        impl Display for $record_type {
            fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
                f.write_str("{")?;
                for (idx, (field, value)) in self.schema.iter().zip(self.iter()).enumerate() {
                    if idx != 0 {
                        f.write_str(", ")?;
                    }

                    f.write_fmt(format_args!("{}: ", field))?;
                    f.write_str(value.to_string().as_str())?;
                }

                f.write_str("}")
            }
        }

        impl $record_type {
            pub fn to_json_like_string(&self) -> String {
                let mut result = "{".to_owned();
                for (idx, (field, value)) in self.schema.iter().zip(self.iter()).enumerate() {
                    if idx != 0 {
                        result.push_str(", ");
                    }

                    result.push_str(format!("\"{}\": ", field).as_str());
                    result.push_str(value.to_json_like_string().as_str());
                }

                result.push_str("}");
                result
            }
        }

        impl Debug for $record_type {
            fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
                write!(f, "{}", self)
            }
        }
    };
}

impl_record_display!(Record);

#[macro_export]
macro_rules! record {
    ($schema:expr, $($values:expr),*) => {{
        let values = vec![$($crate::Value::from($values),)*];
        $crate::Record::new(values, $schema)
    }};

    ($($key:expr => $value:expr,)+) => { record!($($key => $value),+) };

    ($($key:expr => $value:expr),*) => {
        {
            let mut keys = vec![];
            let mut values = vec![];
            $(
                keys.push($key.to_string().into());
                values.push($crate::Value::from($value));
            )*
            $crate::Record::new(values, $crate::RecordSchema::new(keys).unwrap())
        }
    };
}

/// A record of data, containing a sequence of fields identified by a schema, each containing a value.
///
/// This is the thread-safe version of [`Record`]. When possible,
/// prefer to use the non-thread-safe version for lower overhead.
#[derive(PartialEq, PartialOrd)]
pub struct SyncRecord {
    values: Vec<SyncValue>,
    schema: SyncRecordSchema,
}

impl SyncRecord {
    /// Creates a new record with the specified values and schema.
    ///
    /// This call does not check that the schema and values match in length.
    pub fn new<T: Into<Vec<SyncValue>>>(values: T, schema: SyncRecordSchema) -> SyncRecord {
        SyncRecord {
            values: values.into(),
            schema,
        }
    }

    /// Creates an empty record.
    pub fn empty() -> SyncRecord {
        SyncRecord {
            values: vec![],
            schema: SyncRecordSchema::empty(),
        }
    }

    /// Retrieves the value for the specified field.
    ///
    /// This method will do a string-based lookup for the desired field on every invocation.
    pub fn get_value(&self, field: &str) -> Result<&SyncValue, ExpectedFieldMissing> {
        self.schema.index_of(field).map_or_else(
            || {
                Err(ExpectedFieldMissing {
                    field: field.to_string(),
                    schema: self.schema.clone(),
                })
            },
            |i| Ok(&self.values[i]),
        )
    }

    /// Returns the record's schema.
    pub fn schema(&self) -> &SyncRecordSchema {
        &self.schema
    }

    /// Returns an iterator to the values in the record.
    pub fn iter(&self) -> impl Iterator<Item = &SyncValue> {
        self.values.iter()
    }

    /// Consumes the record and returns its schema and values.
    pub fn deconstruct(self) -> (SyncRecordSchema, Vec<SyncValue>) {
        (self.schema, self.values)
    }

    /// Creates a new record by taking `count` elements starting from `offset`.
    pub fn slice(&self, offset: usize, count: usize) -> SyncRecord {
        let new_schema = self.schema.slice(offset, count);
        let new_values = self.iter().skip(offset).take(count).cloned().collect_vec();
        SyncRecord::new(new_values, new_schema)
    }
}

impl Clone for SyncRecord {
    fn clone(&self) -> Self {
        SyncRecord {
            values: self.values.clone(),
            schema: self.schema.clone(),
        }
    }
}

impl Index<usize> for SyncRecord {
    type Output = SyncValue;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl IndexMut<usize> for SyncRecord {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.values[index]
    }
}

impl_record_display!(SyncRecord);

#[macro_export]
macro_rules! sync_record {
    ($schema:expr, $($values:expr),*) => {{
            let values = vec![$($crate::SyncValue::from($values),)*];
            $crate::SyncRecord::new(values, $schema)
        }};
    ($($key:expr => $value:expr,)+) => { sync_record!($($key => $value),+) };
    ($($key:expr => $value:expr),*) => {
        {
            let mut keys = vec![];
            let mut values = vec![];
            $(
                keys.push($key.to_string().into());
                values.push($crate::SyncValue::from($value));
            )*
            $crate::SyncRecord::new(values, $crate::SyncRecordSchema::new(keys).unwrap())
        }
    };
}

impl From<Record> for SyncRecord {
    /// Creates a [`SyncRecord`] from a [`Record`].
    ///
    /// This call will clone the values in the record and create a new instance of the schema. If
    /// multiple records will be created with the same schema, this is sub-optimal. Prefer creating
    /// the schema separately and invoking [`SyncRecord::new()`] instead.
    fn from(record: Record) -> Self {
        SyncRecord::new(
            record.values.iter().map(|v| SyncValue::from(v.clone())).collect_vec(),
            SyncRecordSchema::from(record.schema),
        )
    }
}

impl From<&Record> for SyncRecord {
    /// Creates a [`SyncRecord`] from a [`Record`].
    ///
    /// This call will clone the values in the record and create a new instance of the schema. If
    /// multiple records will be created with the same schema, this is sub-optimal. Prefer creating
    /// the schema separately and invoking [`SyncRecord::new()`] instead.
    fn from(record: &Record) -> Self {
        SyncRecord::new(
            record.values.iter().map(|v| SyncValue::from(v.clone())).collect_vec(),
            SyncRecordSchema::from(&record.schema),
        )
    }
}

impl From<SyncRecord> for Record {
    /// Creates a [`Record`] from a [`SyncRecord`].
    ///
    /// This call will clone the values in the record and create a new instance of the schema. If
    /// multiple records will be created with the same schema, this is sub-optimal. Prefer creating
    /// the schema separately and invoking [`Record::new()`] instead.
    fn from(record: SyncRecord) -> Self {
        Record::new(
            record.values.into_iter().map(Value::from).collect_vec(),
            RecordSchema::from(record.schema),
        )
    }
}

impl From<&SyncRecord> for Record {
    /// Creates a [`Record`] from a [`SyncRecord`].
    ///
    /// This call will clone the values in the record and create a new instance of the schema. If
    /// multiple records will be created with the same schema, this is sub-optimal. Prefer creating
    /// the schema separately and invoking [`Record::new()`] instead.
    fn from(record: &SyncRecord) -> Self {
        Record::new(
            record.values.iter().map(Value::from).collect_vec(),
            RecordSchema::from(&record.schema),
        )
    }
}

impl PartialEq<SyncRecord> for Record {
    fn eq(&self, other: &SyncRecord) -> bool {
        self.schema == other.schema && self.values.as_slice() == other.values.as_slice()
    }
}

impl PartialEq<Record> for SyncRecord {
    fn eq(&self, other: &Record) -> bool {
        other == self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;

    mod record_schema {
        use super::*;

        #[test]
        fn empty_returns_empty_schema() {
            let schema = RecordSchema::empty();
            assert!(schema.is_empty());
        }

        #[test]
        fn new_returns_correct_schema() {
            let columns = vec!["A", "B"];
            let schema = RecordSchema::new(columns.iter().map(|c| c.to_string().into()).collect_vec()).unwrap();
            assert_eq!(schema.len(), columns.len());
            schema.iter().map(|s| s.as_ref()).should().equal_iterator(columns.into_iter());
        }

        #[test]
        fn new_with_repeated_column_returns_error() {
            let columns = vec!["A", "A"];
            let result = RecordSchema::new(columns.iter().map(|c| c.to_string().into()).collect_vec());
            assert_eq!(result.err().unwrap(), FieldNameConflict("A".to_string()));
        }

        #[test]
        fn from_column_names_sets_correct_columns() {
            let columns = vec!["A", "B"];
            let schema = RecordSchema::try_from(&columns).unwrap();
            assert_eq!(&schema[0], "A");
            assert_eq!(&schema[1], "B");
        }

        #[test]
        fn from_sync_schema_creates_correct_schema() {
            let original_schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let schema = RecordSchema::from(original_schema);
            assert_eq!(&schema[0], "A");
            assert_eq!(&schema[1], "B");
        }

        #[test]
        fn indexer_returns_correct_column() {
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            assert_eq!(&schema[1], "B");
        }

        #[test]
        fn index_of_for_matching_column_returns_correct_index() {
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            assert_eq!(schema.index_of("B").unwrap(), 1);
        }

        #[test]
        fn index_of_for_missing_column_returns_none() {
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            assert!(schema.index_of("C").is_none());
        }

        #[test]
        fn delete_field_returns_schema_without_specified_field() {
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let new_schema = schema.delete_field("A");
            assert!(new_schema.index_of("A").is_none());
        }

        #[test]
        fn delete_field_when_field_is_not_in_schema_returns_same_schema() {
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let new_schema = schema.delete_field("C");
            assert_eq!(schema, new_schema)
        }

        #[test]
        fn union_with_no_conflicts_returns_schema_with_new_column_appended_to_right() {
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let schema_to_append = RecordSchema::try_from(vec!["C", "D"]).unwrap();
            let union_schema = schema.union(&schema_to_append).unwrap();
            union_schema
                .iter()
                .should()
                .equal_iterator(schema.iter().chain(schema_to_append.iter()));
        }

        #[test]
        fn insert_with_no_conflicts_returns_schema_with_new_columns_inserted_at_beginning() {
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let schema_to_insert = RecordSchema::try_from(vec!["C", "D"]).unwrap();
            let result_schema = schema.insert(&schema_to_insert, 0).unwrap();
            result_schema
                .iter()
                .should()
                .equal_iterator(RecordSchema::try_from(vec!["C", "D", "A", "B"]).unwrap().iter());
        }

        #[test]
        fn insert_with_no_conflicts_returns_schema_with_new_columns_inserted_at_end() {
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let schema_to_insert = RecordSchema::try_from(vec!["C", "D"]).unwrap();
            let result_schema = schema.insert(&schema_to_insert, 2).unwrap();
            result_schema
                .iter()
                .should()
                .equal_iterator(RecordSchema::try_from(vec!["A", "B", "C", "D"]).unwrap().iter());
        }

        #[test]
        fn insert_with_no_conflicts_returns_schema_with_new_columns_inserted_at_middle() {
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let schema_to_insert = RecordSchema::try_from(vec!["C", "D"]).unwrap();
            let result_schema = schema.insert(&schema_to_insert, 1).unwrap();
            result_schema
                .iter()
                .should()
                .equal_iterator(RecordSchema::try_from(vec!["A", "C", "D", "B"]).unwrap().iter());
        }

        #[test]
        fn insert_with_conflicts_should_return_field_conflict_error() {
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let schema_to_insert = RecordSchema::try_from(vec!["C", "A"]).unwrap();
            let result_schema = schema.insert(&schema_to_insert, 1);
            assert_eq!(result_schema.err().unwrap(), FieldNameConflict("A".to_string()));
        }

        #[test]
        fn eq_same_schemas_returns_true() {
            assert_eq!(
                RecordSchema::try_from(vec!["A", "B"]).unwrap(),
                RecordSchema::try_from(vec!["A", "B"]).unwrap()
            );
        }

        #[test]
        fn eq_different_schemas_returns_false() {
            assert_ne!(
                RecordSchema::try_from(vec!["A", "B"]).unwrap(),
                RecordSchema::try_from(vec!["A", "C"]).unwrap()
            );
        }

        #[test]
        fn eq_same_schema_and_sync_schema_returns_true() {
            assert_eq!(
                RecordSchema::try_from(vec!["A", "B"]).unwrap(),
                SyncRecordSchema::try_from(vec!["A", "B"]).unwrap()
            );
        }
    }

    mod sync_record_schema {
        use super::*;

        #[test]
        fn empty_returns_empty_schema() {
            let schema = SyncRecordSchema::empty();
            assert!(schema.is_empty());
        }

        #[test]
        fn new_returns_correct_schema() {
            let columns = vec!["A", "B"];
            let schema = SyncRecordSchema::new(columns.iter().map(|c| c.to_string().into()).collect_vec()).unwrap();
            assert_eq!(schema.len(), columns.len());
            schema.iter().map(|s| s.as_ref()).should().equal_iterator(columns.into_iter());
        }

        #[test]
        fn new_with_repeated_column_returns_error() {
            let columns = vec!["A", "A"];
            let result = SyncRecordSchema::new(columns.iter().map(|c| c.to_string().into()).collect_vec());
            assert_eq!(result.err().unwrap(), FieldNameConflict("A".to_string()));
        }

        #[test]
        fn from_column_names_sets_correct_columns() {
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            assert_eq!(&schema[0], "A");
            assert_eq!(&schema[1], "B");
        }

        #[test]
        fn from_schema_creates_correct_schema() {
            let original_schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let schema = SyncRecordSchema::from(original_schema);
            assert_eq!(&schema[0], "A");
            assert_eq!(&schema[1], "B");
        }

        #[test]
        fn indexer_returns_correct_column() {
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            assert_eq!(&schema[1], "B");
        }

        #[test]
        fn index_of_for_matching_column_returns_correct_index() {
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            assert_eq!(schema.index_of("B").unwrap(), 1);
        }

        #[test]
        fn index_of_for_missing_column_returns_none() {
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            assert!(schema.index_of("C").is_none());
        }

        #[test]
        fn delete_field_returns_schema_without_specified_field() {
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let new_schema = schema.delete_field("A");
            assert!(new_schema.index_of("A").is_none());
        }

        #[test]
        fn delete_field_when_field_is_not_in_schema_returns_same_schema() {
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let new_schema = schema.delete_field("C");
            assert_eq!(schema, new_schema)
        }

        #[test]
        fn union_with_no_conflicts_returns_schema_with_new_column_appended_to_right() {
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let schema_to_append = SyncRecordSchema::try_from(vec!["C", "D"]).unwrap();
            let union_schema = schema.union(&schema_to_append).unwrap();
            union_schema
                .iter()
                .should()
                .equal_iterator(schema.iter().chain(schema_to_append.iter()));
        }

        #[test]
        fn insert_with_no_conflicts_returns_schema_with_new_columns_inserted_at_beginning() {
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let schema_to_insert = SyncRecordSchema::try_from(vec!["C", "D"]).unwrap();
            let result_schema = schema.insert(&schema_to_insert, 0).unwrap();
            result_schema
                .iter()
                .should()
                .equal_iterator(SyncRecordSchema::try_from(vec!["C", "D", "A", "B"]).unwrap().iter());
        }

        #[test]
        fn insert_with_no_conflicts_returns_schema_with_new_columns_inserted_at_end() {
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let schema_to_insert = SyncRecordSchema::try_from(vec!["C", "D"]).unwrap();
            let result_schema = schema.insert(&schema_to_insert, 2).unwrap();
            result_schema
                .iter()
                .should()
                .equal_iterator(SyncRecordSchema::try_from(vec!["A", "B", "C", "D"]).unwrap().iter());
        }

        #[test]
        fn insert_with_no_conflicts_returns_schema_with_new_columns_inserted_at_middle() {
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let schema_to_insert = SyncRecordSchema::try_from(vec!["C", "D"]).unwrap();
            let result_schema = schema.insert(&schema_to_insert, 1).unwrap();
            result_schema
                .iter()
                .should()
                .equal_iterator(SyncRecordSchema::try_from(vec!["A", "C", "D", "B"]).unwrap().iter());
        }

        #[test]
        fn insert_with_conflicts_should_return_field_conflict_error() {
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let schema_to_insert = SyncRecordSchema::try_from(vec!["C", "A"]).unwrap();
            let result_schema = schema.insert(&schema_to_insert, 1);
            assert_eq!(result_schema.err().unwrap(), FieldNameConflict("A".to_string()));
        }

        #[test]
        fn eq_same_schemas_returns_true() {
            assert_eq!(
                SyncRecordSchema::try_from(vec!["A", "B"]).unwrap(),
                SyncRecordSchema::try_from(vec!["A", "B"]).unwrap()
            );
        }

        #[test]
        fn eq_different_schemas_returns_false() {
            assert_ne!(
                SyncRecordSchema::try_from(vec!["A", "B"]).unwrap(),
                SyncRecordSchema::try_from(vec!["A", "C"]).unwrap()
            );
        }

        #[test]
        fn eq_same_sync_schema_and_schema_returns_true() {
            assert_eq!(
                SyncRecordSchema::try_from(vec!["A", "B"]).unwrap(),
                RecordSchema::try_from(vec!["A", "B"]).unwrap()
            );
        }
    }

    mod record {
        use super::*;

        #[test]
        fn empty_returns_empty_record() {
            let record = Record::empty();
            assert_eq!(record.schema, RecordSchema::empty());
            record.iter().should().equal_iterator(std::iter::empty::<&Value>());
        }

        #[test]
        fn new_creates_record_with_correct_contents() {
            let values = vec![Value::Int64(1), Value::Float64(3.0)];
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let record = Record::new(values.clone(), schema.clone());
            assert_eq!(record.schema, schema);
            assert_eq!(record[0], values[0]);
            assert_eq!(record[1], values[1]);
        }

        #[test]
        fn iter_returns_correct_iterator() {
            let values = vec![Value::Int64(1), Value::Float64(3.0)];
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let record = Record::new(values.clone(), schema);
            record.iter().should().equal_iterator(values.iter());
        }

        #[test]
        fn get_value_for_valid_field_returns_value() {
            let expected_value = Value::Int64(2);
            let record = Record::new(vec![expected_value.clone()], RecordSchema::try_from(vec!["A"]).unwrap());
            let value = record.get_value("A").expect("Expected get_value to return value.");
            assert_eq!(value, &expected_value);
        }

        #[test]
        fn from_sync_record_returns_correct_record() {
            let values = vec![SyncValue::Int64(1), SyncValue::Float64(3.0)];
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let sync_record = SyncRecord::new(values.clone(), schema);
            let record = Record::from(sync_record);
            let expected_values = values.into_iter().map(Value::from).collect_vec();
            record.iter().should().equal_iterator(expected_values.iter());
        }

        #[test]
        fn deconstruct_returns_correct_values_and_schema() {
            let values = vec![Value::Int64(1)];
            let schema = RecordSchema::try_from(vec!["A"]).unwrap();
            let record = Record::new(values.clone(), schema.clone());
            let (returned_schema, returned_values) = record.deconstruct();
            assert_eq!(returned_schema, schema);
            assert_eq!(returned_values.to_vec(), values);
        }

        #[test]
        fn eq_for_records_with_same_data_and_schema_returns_true() {
            let values = vec![Value::Int64(1), Value::Float64(2.0)];
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let first = Record::new(values.clone(), schema.clone());
            let second = Record::new(values, schema);
            assert_eq!(first, second);
        }

        #[test]
        fn eq_for_records_with_different_data_but_same_schema_returns_false() {
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let first = Record::new(vec![Value::Int64(1), Value::Float64(2.0)], schema.clone());
            let second = Record::new(vec![Value::Int64(2), Value::Float64(2.0)], schema);
            assert_ne!(first, second);
        }

        #[test]
        fn eq_for_records_with_same_data_but_different_schema_returns_false() {
            let values = vec![Value::Int64(1), Value::Float64(2.0)];
            let first = Record::new(values.clone(), RecordSchema::try_from(vec!["A", "B"]).unwrap());
            let second = Record::new(values, RecordSchema::try_from(vec!["A", "C"]).unwrap());
            assert_ne!(first, second);
        }

        #[test]
        fn eq_for_record_and_sync_record_with_same_data_and_schema_returns_true() {
            let values = vec![Value::Int64(1), Value::Float64(2.0)];
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let record = Record::new(values.clone(), schema.clone());
            let sync_record = SyncRecord::new(
                values.into_iter().map(SyncValue::from).collect_vec(),
                SyncRecordSchema::from(schema),
            );
            assert_eq!(record, sync_record);
        }
    }

    mod sync_record {
        use super::*;

        #[test]
        fn empty_returns_empty_record() {
            let record = SyncRecord::empty();
            assert_eq!(record.schema, SyncRecordSchema::empty());
            assert!(record.values.is_empty());
        }

        #[test]
        fn new_creates_record_with_correct_contents() {
            let values = vec![SyncValue::Int64(1), SyncValue::Float64(3.0)];
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let record = SyncRecord::new(values.clone(), schema.clone());
            assert_eq!(record.schema, schema);
            assert_eq!(record[0], values[0]);
            assert_eq!(record[1], values[1]);
        }

        #[test]
        fn iter_returns_correct_iterator() {
            let values = vec![SyncValue::Int64(1), SyncValue::Float64(3.0)];
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let record = SyncRecord::new(values.clone(), schema);
            record.iter().should().equal_iterator(values.iter());
        }

        #[test]
        fn get_value_for_valid_field_returns_value() {
            let expected_value = SyncValue::Int64(2);
            let record = SyncRecord::new(vec![expected_value.clone()], SyncRecordSchema::try_from(vec!["A"]).unwrap());
            let value = record.get_value("A").expect("Expected get_value to return value.");
            assert_eq!(value, &expected_value);
        }

        #[test]
        fn from_record_returns_correct_record() {
            let values = vec![Value::Int64(1), Value::Float64(3.0)];
            let schema = RecordSchema::try_from(vec!["A", "B"]).unwrap();
            let record = Record::new(values.clone(), schema);
            let sync_record = SyncRecord::from(record);
            let expected_values = values.into_iter().map(SyncValue::from).collect_vec();
            sync_record.iter().should().equal_iterator(expected_values.iter());
        }

        #[test]
        fn deconstruct_returns_correct_values_and_schema() {
            let values = vec![SyncValue::Int64(1)];
            let schema = SyncRecordSchema::try_from(vec!["A"]).unwrap();
            let record = SyncRecord::new(values.clone(), schema.clone());
            let (returned_schema, returned_values) = record.deconstruct();
            assert_eq!(returned_schema, schema);
            assert_eq!(returned_values, values);
        }

        #[test]
        fn eq_for_records_with_same_data_and_schema_returns_true() {
            let values = vec![SyncValue::Int64(1), SyncValue::Float64(2.0)];
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let first = SyncRecord::new(values.clone(), schema.clone());
            let second = SyncRecord::new(values, schema);
            assert_eq!(first, second);
        }

        #[test]
        fn eq_for_records_with_different_data_but_same_schema_returns_false() {
            let schema = SyncRecordSchema::try_from(vec!["A", "B"]).unwrap();
            let first = SyncRecord::new(vec![SyncValue::Int64(1), SyncValue::Float64(2.0)], schema.clone());
            let second = SyncRecord::new(vec![SyncValue::Int64(2), SyncValue::Float64(2.0)], schema);
            assert_ne!(first, second);
        }

        #[test]
        fn eq_for_records_with_same_data_but_different_schema_returns_false() {
            let values = vec![SyncValue::Int64(1), SyncValue::Float64(2.0)];
            let first = SyncRecord::new(values.clone(), SyncRecordSchema::try_from(vec!["A", "B"]).unwrap());
            let second = SyncRecord::new(values, SyncRecordSchema::try_from(vec!["A", "C"]).unwrap());
            assert_ne!(first, second);
        }
    }
}
