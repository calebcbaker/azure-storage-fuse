//! Efficient selection of fields in [`Records`](crate::Record).

use super::{ExpectedFieldMissing, Record, RecordSchema, SyncRecord, SyncRecordSchema, SyncValue, Value};
use itertools::Itertools;
use regex::Regex;
use std::{collections::HashSet, fmt::Debug, sync::Arc};

/// A builder for [`FieldSelectors`](FieldSelector).
///
/// [`FieldSelector`] is a single-threaded object. However, functions that start multi-threaded
/// processing often need a way to select which fields to act on. This trait enables that scenario
/// by allowing all threads to reference the same builder which can then create thread-specific
/// selectors.
pub trait FieldSelectorBuilder: Send + Sync + Debug {
    fn build(&self) -> Box<dyn FieldSelector>;
}

/// A FieldSelector enables optimized retrieval of values from a record by column name.
///
/// This trait exists to optimize field lookup in records. Looking up a column in a schema
/// requires a String lookup in a hash table. A field selector will cache the result for a specific
/// schema, requiring a lookup only when the schema of input records changes.
pub trait FieldSelector: Debug {
    /// Gets the values for this selector from the input record. For columns that are not present
    /// in the record, `None` is returned.
    fn get_values<'a>(&mut self, record: &'a Record) -> Vec<Option<&'a Value>> {
        self.get_indices(record.schema())
            .iter()
            .map(|o| o.map(|i| &record[i]))
            .collect_vec()
    }

    /// Returns a FieldRemover that can be used to delete fields from records.
    fn get_field_remover(&self) -> FieldRemover {
        FieldRemover {
            columns: self.to_builder().build(),
            last_processed_schema: RecordSchema::empty(),
            target_schema: RecordSchema::empty(),
            mapper: Vec::new(),
        }
    }

    /// Get the indices within the input schema for the columns targeted by this selector. For columns
    /// that are not present in the schema, `None` is returned.
    fn get_indices(&mut self, schema: &RecordSchema) -> &[Option<usize>];

    /// Creates a [`FieldSelectorBuilder`] that can create selectors equivalent to this one.
    fn to_builder(&self) -> Arc<dyn FieldSelectorBuilder>;
}

pub struct FieldRemover {
    columns: Box<dyn FieldSelector>,
    last_processed_schema: RecordSchema,
    target_schema: RecordSchema,
    mapper: Vec<usize>,
}

impl FieldRemover {
    /// Remove the fields captured by this FieldRemover from the input [`Record`].
    pub fn remove_fields(&mut self, record: Record) -> Record {
        let (schema, mut values) = record.deconstruct();
        if schema != self.last_processed_schema {
            self.last_processed_schema = schema.clone();
            self.target_schema = schema.clone();
            let mut columns_to_remove = HashSet::new();
            let indices = self.columns.get_indices(&schema);
            for index in indices.iter().filter_map(|o| *o) {
                // we need a better way to map record schema
                columns_to_remove.insert(&schema[index]);
            }

            self.mapper.clear();

            for (idx, name) in schema.iter().enumerate() {
                if columns_to_remove.contains(name.as_ref()) {
                    self.target_schema = self.target_schema.delete_field(name.as_ref());
                } else {
                    self.mapper.push(idx);
                }
            }
        }

        for (to, from) in self.mapper.iter().enumerate() {
            values.swap(to, *from);
        }
        values.resize(self.mapper.len());

        Record::new(values, self.target_schema.clone())
    }
}

#[derive(Debug)]
struct SingleFieldSelectorBuilder {
    field: Arc<String>,
}

impl FieldSelectorBuilder for SingleFieldSelectorBuilder {
    fn build(&self) -> Box<dyn FieldSelector> {
        Box::new(SingleFieldSelector {
            field: self.field.clone(),
            last_schema: None,
            index: Ok(0),
            index_vec: vec![None],
        })
    }
}

/// A selector that can retrieve a specific field from a [`Record`].
///
/// This struct exists to optimize field lookup in records. Looking up a column in a schema
/// requires a String lookup in a hash table. A field selector will cache the result for a specific
/// schema, requiring a lookup only when the schema of input records changes.
#[derive(Debug)]
pub struct SingleFieldSelector {
    field: Arc<String>,
    last_schema: Option<RecordSchema>,
    // We store the information twice just to make both paths optimal.
    index: Result<usize, ExpectedFieldMissing>,
    index_vec: Vec<Option<usize>>,
}

impl SingleFieldSelector {
    /// Creates a selector for the specified field.
    pub fn new<S: Into<String>>(field: S) -> SingleFieldSelector {
        SingleFieldSelector {
            field: Arc::new(field.into()),
            last_schema: None,
            index: Ok(0),
            index_vec: vec![None],
        }
    }

    fn apply_schema(&mut self, schema: &RecordSchema) {
        if self.last_schema.is_none() || self.last_schema.as_ref().unwrap() != schema {
            self.last_schema = Some(schema.clone());
            self.index = schema.index_of(self.field.as_str()).map_or(
                Err(ExpectedFieldMissing {
                    field: self.field.as_ref().clone(),
                    schema: schema.clone().into(),
                }),
                Ok,
            );
            self.index_vec[0] = self.index.as_ref().map_or(None, |i| Some(*i));
        }
    }

    /// Retrieves the field assigned to this selector.
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Retrieves the associated field from the target record. If the field does not exist in the
    /// record, en [`ExpectedFieldMissing`] error is returned.
    pub fn get_value<'a>(&mut self, record: &'a Record) -> Result<&'a Value, ExpectedFieldMissing> {
        self.get_index(&record.schema()).map(|i| &record[i])
    }

    /// Retrieves the index at which the associated field would be found in a values array based on
    /// the input schema.
    pub fn get_index(&mut self, schema: &RecordSchema) -> Result<usize, ExpectedFieldMissing> {
        self.apply_schema(schema);
        self.index.clone()
    }
}

impl FieldSelector for SingleFieldSelector {
    fn get_values<'a>(&mut self, record: &'a Record) -> Vec<Option<&'a Value>> {
        self.get_index(&record.schema())
            .map_or_else(|_| vec![None], |i| vec![Some(&record[i])])
    }

    fn get_indices(&mut self, schema: &RecordSchema) -> &[Option<usize>] {
        self.apply_schema(schema);
        self.index_vec.as_slice()
    }

    fn to_builder(&self) -> Arc<dyn FieldSelectorBuilder> {
        Arc::new(SingleFieldSelectorBuilder { field: self.field.clone() })
    }
}

impl Clone for SingleFieldSelector {
    fn clone(&self) -> Self {
        SingleFieldSelector {
            field: self.field.clone(),
            last_schema: None,
            index: Ok(0),
            index_vec: vec![None],
        }
    }
}

#[derive(Debug)]
struct MultiFieldSelectorBuilder {
    fields: Arc<Vec<Arc<str>>>,
}

impl FieldSelectorBuilder for MultiFieldSelectorBuilder {
    fn build(&self) -> Box<dyn FieldSelector> {
        Box::new(MultiFieldSelector {
            fields: self.fields.clone(),
            indices: vec![None; self.fields.len()],
            last_schema: RecordSchema::empty(),
        })
    }
}

/// Enables efficient retrieval of multiple values from a record by column name.
///
/// This struct exists to optimize field lookup in records. Looking up a column in a schema
/// requires a String lookup in a hash table. A field selector will cache the result for a specific
/// schema, requiring a lookup only when the schema of input records changes.
#[derive(Debug)]
pub struct MultiFieldSelector {
    fields: Arc<Vec<Arc<str>>>,
    indices: Vec<Option<usize>>,
    last_schema: RecordSchema,
}

impl MultiFieldSelector {
    /// Creates a selector for the specified fields.
    pub fn new(fields: Vec<Arc<str>>) -> MultiFieldSelector {
        let field_count = fields.len();
        MultiFieldSelector {
            fields: Arc::new(fields),
            indices: vec![None; field_count],
            last_schema: RecordSchema::empty(),
        }
    }

    fn apply_schema(&mut self, schema: &RecordSchema) {
        if &self.last_schema != schema {
            self.last_schema = schema.clone();
            for i in 0..self.indices.len() {
                self.indices[i] = schema.index_of(&self.fields[i]);
            }
        }
    }

    /// Retrieves the fields targeted by this selector.
    pub fn fields(&self) -> &[Arc<str>] {
        self.fields.as_slice()
    }
}

impl FieldSelector for MultiFieldSelector {
    /// Get the indices within the input schema for the columns targeted by this selector. For columns
    /// that are not present in the schema, `None` is returned.
    ///
    /// The indices are returned in the same order in which the target columns were specified
    /// when creating the selector.
    fn get_indices(&mut self, schema: &RecordSchema) -> &[Option<usize>] {
        self.apply_schema(schema);
        self.indices.as_slice()
    }

    fn to_builder(&self) -> Arc<dyn FieldSelectorBuilder> {
        Arc::new(MultiFieldSelectorBuilder {
            fields: self.fields.clone(),
        })
    }
}

impl Clone for MultiFieldSelector {
    fn clone(&self) -> Self {
        MultiFieldSelector {
            fields: self.fields.clone(),
            indices: vec![None; self.fields.len()],
            last_schema: RecordSchema::empty(),
        }
    }
}

#[derive(Debug)]
struct RegexFieldSelectorBuilder {
    regex: Regex,
    invert: bool,
}

impl FieldSelectorBuilder for RegexFieldSelectorBuilder {
    fn build(&self) -> Box<dyn FieldSelector> {
        Box::new(RegexFieldSelector::new(self.regex.clone(), self.invert))
    }
}

/// A field selector that selects columns based on a regular expression.
///
/// The regular expression will be evaluated against all columns in the schema every time
/// a new input is schema is provided to the selector.
#[derive(Debug)]
pub struct RegexFieldSelector {
    regex: Regex,
    invert: bool,
    indices: Vec<Option<usize>>,
    last_schema: RecordSchema,
}

impl RegexFieldSelector {
    /// Creates a new [`RegexFieldSelector`] based on the specified [`regex::Regex`](regular expression).
    pub fn new(regex: Regex, invert: bool) -> RegexFieldSelector {
        RegexFieldSelector {
            regex,
            invert,
            indices: vec![],
            last_schema: RecordSchema::empty(),
        }
    }

    fn apply_schema(&mut self, schema: &RecordSchema) {
        if &self.last_schema != schema {
            self.last_schema = schema.clone();
            self.indices = self
                .last_schema
                .iter()
                .enumerate()
                .map(|(idx, column)| {
                    let is_match = self.regex.is_match(column);
                    let is_match = if self.invert { !is_match } else { is_match };
                    if is_match {
                        Some(idx)
                    } else {
                        None
                    }
                })
                .filter(|o| o.is_some())
                .collect_vec();
        }
    }
}

impl FieldSelector for RegexFieldSelector {
    fn get_indices(&mut self, schema: &RecordSchema) -> &[Option<usize>] {
        self.apply_schema(schema);
        self.indices.as_slice()
    }

    fn to_builder(&self) -> Arc<dyn FieldSelectorBuilder> {
        Arc::new(RegexFieldSelectorBuilder {
            regex: self.regex.clone(),
            invert: self.invert,
        })
    }
}

/// A selector that can retrieve a specific field from a [`SyncRecord`].
///
/// This struct exists to optimize field lookup in records. Looking up a column in a schema
/// requires a String lookup in a hash table. A field selector will cache the result for a specific
/// schema, requiring a lookup only when the schema of input records changes.
pub struct SyncSingleFieldSelector {
    field: String,
    last_schema: Option<SyncRecordSchema>,
    index: Option<Result<usize, ExpectedFieldMissing>>,
}

impl SyncSingleFieldSelector {
    /// Creates a selector for the specified field.
    pub fn new<S: Into<String>>(field: S) -> SyncSingleFieldSelector {
        SyncSingleFieldSelector {
            field: field.into(),
            last_schema: None,
            index: None,
        }
    }

    /// Retrieves the field assigned to this selector.
    #[allow(dead_code)]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Retrieves the associated field from the target record. If the field does not exist in the
    /// record, en [`ExpectedFieldMissing`] error is returned.
    pub fn get_value<'a>(&mut self, record: &'a SyncRecord) -> Result<&'a SyncValue, ExpectedFieldMissing> {
        if self.last_schema.is_none() || self.last_schema.as_ref().unwrap() != record.schema() {
            self.last_schema = Some(record.schema().clone());
            self.index = Some(record.schema().index_of(self.field.as_str()).ok_or_else(|| ExpectedFieldMissing {
                field: self.field.clone(),
                schema: record.schema().clone(),
            }));
        }

        self.index.as_ref().unwrap().clone().map(|i| &record[i])
    }

    /// Retrieves the index at which the associated field would be found in a values array based on
    /// the input schema.
    #[allow(dead_code)]
    pub fn get_index(&mut self, schema: &SyncRecordSchema) -> Result<usize, ExpectedFieldMissing> {
        if self.last_schema.is_none() || self.last_schema.as_ref().unwrap() != schema {
            self.last_schema = Some(schema.clone());
            self.index = Some(schema.index_of(self.field.as_str()).ok_or_else(|| ExpectedFieldMissing {
                field: self.field.clone(),
                schema: schema.clone(),
            }));
        }

        self.index.as_ref().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use std::convert::TryFrom;

    #[test]
    fn select_for_valid_column_returns_value() {
        let record = Record::new(vec![Value::Int64(2)], RecordSchema::try_from(vec!["A"]).unwrap());
        let mut selector = SingleFieldSelector::new("A".to_owned());
        selector.get_value(&record).should().be_ok().with_value(&Value::Int64(2));
    }

    #[test]
    fn select_for_missing_column_returns_error() {
        let record = Record::new(vec![Value::Int64(2)], RecordSchema::try_from(vec!["A"]).unwrap());
        let mut selector = SingleFieldSelector::new("B".to_owned());
        selector.get_value(&record).should().be_err().with_value(ExpectedFieldMissing {
            field: "B".to_owned(),
            schema: record.schema().clone().into(),
        });
    }

    #[test]
    fn select_sync_for_valid_column_returns_value() {
        let record = SyncRecord::new(vec![SyncValue::Int64(2)], SyncRecordSchema::try_from(vec!["A"]).unwrap());
        let mut selector = SyncSingleFieldSelector::new("A".to_owned());
        selector.get_value(&record).should().be_ok().with_value(&SyncValue::Int64(2));
    }

    #[test]
    fn select_sync_for_missing_column_returns_error() {
        let record = SyncRecord::new(vec![SyncValue::Int64(2)], SyncRecordSchema::try_from(vec!["A"]).unwrap());
        let mut selector = SyncSingleFieldSelector::new("B".to_owned());
        selector.get_value(&record).should().be_err().with_value(ExpectedFieldMissing {
            field: "B".to_owned(),
            schema: record.schema().clone(),
        });
    }

    #[test]
    fn select_multiple_for_valid_columns_returns_values() {
        let record = Record::new(
            vec![Value::from(1), Value::from(2), Value::from(3)],
            RecordSchema::try_from(vec!["A", "B", "C"]).unwrap(),
        );
        let mut selector = MultiFieldSelector::new(vec!["A".to_string().into(), "C".to_string().into()]);
        let values = selector.get_values(&record);
        assert_eq!(values, vec![Some(&Value::from(1)), Some(&Value::from(3))])
    }

    #[test]
    fn select_multiple_when_some_columns_are_missing_returns_none_for_missing() {
        let record = Record::new(vec![Value::from(1)], RecordSchema::try_from(vec!["A"]).unwrap());
        let mut selector = MultiFieldSelector::new(vec!["A".to_string().into(), "C".to_string().into()]);
        let values = selector.get_values(&record);
        assert_eq!(values, vec![Some(&Value::from(1)), None])
    }

    mod regex_field_selector {
        use super::*;

        #[test]
        fn selects_columns_that_match_pattern() {
            let mut selector = RegexFieldSelector::new(Regex::new("a.+").unwrap(), false);
            let schema = RecordSchema::try_from(vec!["a", "aa"]).unwrap();
            assert_eq!(selector.get_indices(&schema), &[Some(1)]);
        }

        #[test]
        fn with_invert_true_returns_columns_not_matching_pattern() {
            let mut selector = RegexFieldSelector::new(Regex::new("a.+").unwrap(), true);
            let schema = RecordSchema::try_from(vec!["a", "aa"]).unwrap();
            assert_eq!(selector.get_indices(&schema), &[Some(0)]);
        }
    }

    mod field_remover {
        use super::*;
        use crate::record;

        #[test]
        fn remove_multiple_columns() {
            let mut remover = MultiFieldSelector::new(vec!["A".to_owned().into(), "C".to_owned().into()]).get_field_remover();
            remover
                .remove_fields(record!("A" => "a", "B" => 1, "C" => 50))
                .should()
                .equal(&record!("B" => 1));
        }

        #[test]
        fn remove_all_columns() {
            let mut remover = MultiFieldSelector::new(vec!["D".to_owned().into()]).get_field_remover();
            remover
                .remove_fields(record!("A" => "a", "B" => 1, "C" => 50))
                .should()
                .equal(&record!("A" => "a", "B" => 1, "C" => 50));
        }

        #[test]
        fn remove_no_columns() {
            let mut remover = MultiFieldSelector::new(vec!["A".to_owned().into(), "C".to_owned().into()]).get_field_remover();
            remover
                .remove_fields(record!("A" => "a", "B" => 1, "C" => 50))
                .should()
                .equal(&record!("B" => 1));
        }
    }
}
