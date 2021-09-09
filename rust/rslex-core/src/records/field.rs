use crate::{file_io::ArgumentError, StreamInfo, SyncRecord, SyncValue, SyncValueKind};
use std::{convert::TryFrom, sync::Arc};

pub trait RecordFieldType: TryFrom<SyncValue> {
    fn get_value_kind() -> SyncValueKind;
}

impl RecordFieldType for String {
    fn get_value_kind() -> SyncValueKind {
        SyncValueKind::String
    }
}

impl RecordFieldType for Arc<StreamInfo> {
    fn get_value_kind() -> SyncValueKind {
        SyncValueKind::StreamInfo
    }
}

impl RecordFieldType for SyncRecord {
    fn get_value_kind() -> SyncValueKind {
        SyncValueKind::Record
    }
}

impl RecordFieldType for f64 {
    fn get_value_kind() -> SyncValueKind {
        SyncValueKind::Float64
    }
}

impl RecordFieldType for bool {
    fn get_value_kind() -> SyncValueKind {
        SyncValueKind::Boolean
    }
}

impl RecordFieldType for i64 {
    fn get_value_kind() -> SyncValueKind {
        SyncValueKind::Int64
    }
}

fn prefix(error: ArgumentError, path: impl AsRef<str>) -> ArgumentError {
    match error {
        ArgumentError::MissingArgument { argument } => ArgumentError::MissingArgument {
            argument: format!("{}.{}", path.as_ref(), argument),
        },
        ArgumentError::InvalidArgument {
            argument,
            expected,
            actual,
        } => ArgumentError::InvalidArgument {
            argument: format!("{}.{}", path.as_ref(), argument),
            expected,
            actual,
        },
    }
}

pub trait FieldExtensions {
    fn get_optional<T: RecordFieldType>(&self, key: &str) -> Result<Option<T>, ArgumentError>;

    fn get_list<T: RecordFieldType>(&self, key: &str) -> Result<Vec<T>, ArgumentError>;

    fn get_list_required<T: RecordFieldType>(&self, key: &str) -> Result<Vec<T>, ArgumentError>;

    fn get_required<T: RecordFieldType>(&self, key: &str) -> Result<T, ArgumentError> {
        self.get_optional(key)
            .transpose()
            .unwrap_or(Err(ArgumentError::MissingArgument { argument: key.to_owned() }))
    }

    fn get_list_non_empty<T: RecordFieldType>(&self, key: &str) -> Result<Vec<T>, ArgumentError> {
        let list = self.get_list(key)?;

        if list.is_empty() {
            Err(ArgumentError::MissingArgument { argument: key.to_owned() })
        } else {
            Ok(list)
        }
    }

    fn with_required<T, F: FnOnce(&SyncRecord) -> Result<T, ArgumentError>>(&self, key: &str, fun: F) -> Result<T, ArgumentError> {
        let record = self.get_required::<SyncRecord>(key)?;

        fun(&record).map_err(|e| prefix(e, key))
    }

    fn with_optional<T, F: FnOnce(&SyncRecord) -> Result<T, ArgumentError>>(&self, key: &str, fun: F) -> Result<Option<T>, ArgumentError> {
        match self.get_optional::<SyncRecord>(key)? {
            None => Ok(None),
            Some(record) => Ok(Some(fun(&record).map_err(|e| prefix(e, key))?)),
        }
    }

    fn with_list_required<T, F: Fn(&SyncRecord) -> Result<T, ArgumentError>>(&self, key: &str, fun: F) -> Result<Vec<T>, ArgumentError> {
        let record = self.get_list_required::<SyncRecord>(key)?;

        record
            .iter()
            .enumerate()
            .map(|(id, r)| fun(r).map_err(|e| prefix(e, format!("{}[{}]", key, id))))
            .collect()
    }

    fn with_list_non_empty<T, F: Fn(&SyncRecord) -> Result<T, ArgumentError>>(&self, key: &str, fun: F) -> Result<Vec<T>, ArgumentError> {
        let record = self.get_list_non_empty::<SyncRecord>(key)?;

        record
            .iter()
            .enumerate()
            .map(|(id, r)| fun(r).map_err(|e| prefix(e, format!("{}[{}]", key, id))))
            .collect()
    }
}

impl FieldExtensions for SyncRecord {
    fn get_optional<T: RecordFieldType>(&self, key: &str) -> Result<Option<T>, ArgumentError> {
        match self.get_value(key) {
            Ok(SyncValue::Null) => Ok(None),
            Ok(v) => T::try_from(v.clone())
                .map_err(|_| ArgumentError::InvalidArgument {
                    argument: key.to_owned(),
                    expected: format!("{:?}", T::get_value_kind()),
                    actual: format!("{:?}", v),
                })
                .map(|v| Some(v)),
            Err(_) => Ok(None),
        }
    }

    fn get_list<T: RecordFieldType>(&self, key: &str) -> Result<Vec<T>, ArgumentError> {
        do_get_list(self, key, false)
    }

    fn get_list_required<T: RecordFieldType>(&self, key: &str) -> Result<Vec<T>, ArgumentError> {
        do_get_list(self, key, true)
    }
}

fn do_get_list<T: RecordFieldType>(record: &SyncRecord, key: &str, required: bool) -> Result<Vec<T>, ArgumentError> {
    match record.get_value(key) {
        Ok(SyncValue::List(l)) => l
            .iter()
            .enumerate()
            .map(|(id, v)| {
                T::try_from(v.clone()).map_err(|_| ArgumentError::InvalidArgument {
                    argument: format!("{}[{}]", key, id),
                    expected: format!("{:?}", T::get_value_kind()),
                    actual: format!("{:?}", v),
                })
            })
            .collect(),
        Ok(SyncValue::Null) | Err(_) => {
            if required {
                Err(ArgumentError::MissingArgument { argument: key.to_owned() })
            } else {
                Ok(vec![])
            }
        },
        Ok(v) => Err(ArgumentError::InvalidArgument {
            argument: key.to_owned(),
            expected: "List".to_owned(),
            actual: format!("{:?}", v),
        }),
    }
}
