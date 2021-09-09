use crate::{file_io::StreamProperties, SessionProperties};
use chrono::{DateTime, Utc};
use std::sync::Arc;

pub trait SessionPropertiesExt: Sized {
    fn is_seekable(&self) -> Option<bool>;
    fn size(&self) -> Option<u64>;
    fn created_time(&self) -> Option<DateTime<Utc>>;
    fn modified_time(&self) -> Option<DateTime<Utc>>;

    fn set_is_seekable(&mut self, is_seekable: bool);
    fn set_size(&mut self, size: u64);
    fn set_created_time(&mut self, created_time: DateTime<Utc>);
    fn set_modified_time(&mut self, modified_time: DateTime<Utc>);

    fn stream_properties(&self) -> Option<StreamProperties> {
        let size = self.size()?;

        Some(StreamProperties {
            size,
            created_time: self.created_time(),
            modified_time: self.modified_time(),
        })
    }

    // utils to make fluent syntax
    fn with_size(mut self, size: u64) -> Self {
        self.set_size(size);
        self
    }

    fn with_is_seekable(mut self, is_seekable: bool) -> Self {
        self.set_is_seekable(is_seekable);
        self
    }

    fn with_modified_time(mut self, modified_time: DateTime<Utc>) -> Self {
        self.set_modified_time(modified_time);
        self
    }

    fn with_created_time(mut self, created_time: DateTime<Utc>) -> Self {
        self.set_created_time(created_time);
        self
    }
}

const IS_SEEKABLE_KEY: &str = "isSeekable";
const SIZE_KEY: &str = "size";
const CREATED_TIME_KEY: &str = "createdTime";
const MODIFIED_TIME_KEY: &str = "modifiedTime";

impl SessionPropertiesExt for SessionProperties {
    fn is_seekable(&self) -> Option<bool> {
        self.get(IS_SEEKABLE_KEY)
            .map(|a| a.downcast_ref::<bool>().expect("is_seekable entry should be bool").clone())
    }

    fn size(&self) -> Option<u64> {
        self.get(SIZE_KEY).map(|a| {
            a.downcast_ref::<u64>()
                .cloned()
                .or_else(|| a.downcast_ref::<i64>().cloned().map(|v| v as u64))
                .expect("size entry should be u64 or i64")
        })
    }

    fn created_time(&self) -> Option<DateTime<Utc>> {
        self.get(CREATED_TIME_KEY).map(|a| {
            a.downcast_ref::<DateTime<Utc>>()
                .expect("created_time entry should be DateTime")
                .clone()
        })
    }

    fn modified_time(&self) -> Option<DateTime<Utc>> {
        self.get(MODIFIED_TIME_KEY).map(|a| {
            a.downcast_ref::<DateTime<Utc>>()
                .expect("modified_time entry should be DateTime")
                .clone()
        })
    }

    fn set_is_seekable(&mut self, is_seekable: bool) {
        self.insert(IS_SEEKABLE_KEY.to_owned(), Arc::new(is_seekable));
    }

    fn set_size(&mut self, size: u64) {
        self.insert(SIZE_KEY.to_owned(), Arc::new(size));
    }

    fn set_created_time(&mut self, created_time: DateTime<Utc>) {
        self.insert(CREATED_TIME_KEY.to_owned(), Arc::new(created_time));
    }

    fn set_modified_time(&mut self, modified_time: DateTime<Utc>) {
        self.insert(MODIFIED_TIME_KEY.to_owned(), Arc::new(modified_time));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn downcast_size_u64() {
        let size: u64 = 10000;
        let expected_size: u64 = 10000;
        let mut session_properties: SessionProperties = HashMap::new();
        session_properties.insert(SIZE_KEY.to_owned(), Arc::new(size));

        assert_eq!(session_properties.size().unwrap(), expected_size);
    }

    #[test]
    fn downcast_size_i64() {
        let size: i64 = 10000;
        let expected_size: u64 = size as u64;
        let mut session_properties: SessionProperties = HashMap::new();
        session_properties.insert(SIZE_KEY.to_owned(), Arc::new(size));

        assert_eq!(session_properties.size().unwrap(), expected_size);
    }
}
