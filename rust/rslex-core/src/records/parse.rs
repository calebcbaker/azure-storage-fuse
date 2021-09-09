use crate::{file_io::ArgumentError, SyncRecord};
use std::ops::Deref;

/// Anything implementing this trait can be instantiated from the parsed contents of an appropriately formatted SyncRecord.
pub trait ParseRecord<'r>: Sized {
    fn parse(record: &'r SyncRecord) -> std::result::Result<Self, ArgumentError>;
}

/// Blanket implementation for any ParseRecord to be parsed out of a SyncRecord.
impl SyncRecord {
    pub fn parse<'r, T: ParseRecord<'r>>(&'r self) -> std::result::Result<T, ArgumentError> {
        T::parse(self)
    }
}

/// A wrapper class keeping the parsed object, as well as the SyncRecord pointer.
/// It implements Deref so can be used as a parsed object reference.
/// get_record() function will return the original SyncRecord pointer.
pub struct ParsedRecord<'r, T: ParseRecord<'r>> {
    record: &'r SyncRecord,
    parsed: T,
}

impl<'r, T: ParseRecord<'r>> ParsedRecord<'r, T> {
    pub fn get_record(&self) -> &'r SyncRecord {
        self.record
    }
}

impl<'r, T: ParseRecord<'r>> Deref for ParsedRecord<'r, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.parsed
    }
}

impl<'r, T: ParseRecord<'r>> ParseRecord<'r> for ParsedRecord<'r, T> {
    fn parse(record: &'r SyncRecord) -> std::result::Result<Self, ArgumentError> {
        let parsed = T::parse(record)?;

        Ok(ParsedRecord { parsed, record })
    }
}
