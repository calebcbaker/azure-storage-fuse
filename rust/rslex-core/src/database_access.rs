use crate::{
    file_io::ArgumentError,
    records::parse::{ParseRecord, ParsedRecord},
    Dataset, ExternalError, FieldNameConflict, RecordSchema, SyncRecord,
};
use derivative::Derivative;
use itertools::Itertools;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;

#[derive(Clone, Debug, Error, Derivative)]
#[derivative(PartialEq)]
pub enum DatabaseError {
    #[error("database handler not found for {0}")]
    NoHandler(String),
    #[error("invalid argument")]
    ArgumentError(#[from] ArgumentError),
    #[error("connection failure when access stream")]
    ConnectionFailure {
        #[derivative(PartialEq = "ignore")]
        source: Option<ExternalError>,
    },
    #[error("tbd")]
    FieldNameConflict(#[from] FieldNameConflict),
    #[error("unexpected error")]
    Unknown(
        String,
        #[derivative(PartialEq = "ignore")]
        #[source]
        Option<ExternalError>,
    ),
    #[error("Permission denied.")]
    PermissionDenied(String),
}

pub type DatabaseResult<T> = Result<T, DatabaseError>;

/// Strongly typed Record to be used as arguments to `StreamHandler` interface.
pub trait DatabaseArguments<'r>: ParseRecord<'r> {
    fn validate(&self, _database_accessor: &DatabaseAccessor) -> Result<(), ArgumentError> {
        Ok(())
    }
}

impl DatabaseArguments<'_> for () {}

pub trait DatabaseHandler: Send + Sync {
    fn handler_type(&self) -> &str;

    type ReadTableArguments: for<'r> DatabaseArguments<'r>;

    fn read_table(
        &self,
        query: &str,
        arguments: ParsedRecord<Self::ReadTableArguments>,
        accessor: &DatabaseAccessor,
    ) -> DatabaseResult<Dataset>;

    type CreateOrAppendTableArguments: for<'r> DatabaseArguments<'r>;
    fn create_or_append_table(
        &self,
        dataset: &Dataset,
        schema: &RecordSchema,
        arguments: ParsedRecord<Self::CreateOrAppendTableArguments>,
        accessor: &DatabaseAccessor,
    ) -> DatabaseResult<()>;
}

trait DynDatabaseHandler: Send + Sync {
    fn read_table(&self, query: &str, argument: &SyncRecord, accessor: &DatabaseAccessor) -> DatabaseResult<Dataset>;

    fn create_or_append_table(
        &self,
        dataset: &Dataset,
        schema: &RecordSchema,
        argument: &SyncRecord,
        accessor: &DatabaseAccessor,
    ) -> DatabaseResult<()>;

    fn validate_arguments_for_read_table(&self, argument: &SyncRecord, accessor: &DatabaseAccessor) -> Result<(), ArgumentError>;
}

impl<T: DatabaseHandler> DynDatabaseHandler for T {
    fn read_table(&self, query: &str, argument: &SyncRecord, accessor: &DatabaseAccessor) -> DatabaseResult<Dataset> {
        self.read_table(query, argument.parse()?, accessor)
    }

    fn create_or_append_table(
        &self,
        dataset: &Dataset,
        schema: &RecordSchema,
        argument: &SyncRecord,
        accessor: &DatabaseAccessor,
    ) -> DatabaseResult<()> {
        self.create_or_append_table(dataset, schema, argument.parse()?, accessor)
    }

    fn validate_arguments_for_read_table(&self, arguments: &SyncRecord, accessor: &DatabaseAccessor) -> Result<(), ArgumentError> {
        T::ReadTableArguments::parse(arguments)?.validate(accessor)
    }
}

/// Provides access to data from multiple databases through different [`DatabaseHandler`](DatabaseHandler).
#[derive(Default)]
pub struct DatabaseAccessor {
    handlers: HashMap<String, Arc<dyn DynDatabaseHandler>>,
}

impl DatabaseAccessor {
    pub fn add_handler(mut self, handler: impl DatabaseHandler + 'static) -> Self {
        self.handlers.insert(handler.handler_type().to_owned(), Arc::new(handler));

        self
    }

    pub fn read_table(&self, handler: impl AsRef<str>, query: impl AsRef<str>, argument: &SyncRecord) -> DatabaseResult<Dataset> {
        match self.handlers.get(handler.as_ref()) {
            Some(handler) => handler.read_table(query.as_ref(), argument, self),
            None => Err(DatabaseError::NoHandler(handler.as_ref().to_string())),
        }
    }

    pub fn validate_arguments_for_read_table(&self, handler: &str, arguments: &SyncRecord) -> Result<(), ArgumentError> {
        match self.handlers.get(handler) {
            Some(handler) => handler.validate_arguments_for_read_table(arguments, self),
            None => Err(ArgumentError::InvalidArgument {
                argument: "handler".to_owned(),
                expected: self.handlers.keys().join("|"),
                actual: handler.to_owned(),
            }),
        }
    }

    pub fn create_or_append_table(
        &self,
        handler: impl AsRef<str>,
        dataset: &Dataset,
        schema: &RecordSchema,
        argument: &SyncRecord,
    ) -> DatabaseResult<()> {
        match self.handlers.get(handler.as_ref()) {
            Some(handler) => handler.create_or_append_table(dataset, schema, argument, self),
            None => Err(DatabaseError::NoHandler(handler.as_ref().to_string())),
        }
    }
}
