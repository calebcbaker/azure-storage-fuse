use rslex_core::{
    file_io::{StreamError, StreamResult},
    MapErrToUnknown,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum FileType {
    FILE,
    DIRECTORY,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct FileStatus {
    pub length: u64,
    #[serde(rename = "pathSuffix")]
    pub path_suffix: String,
    #[serde(rename = "type")]
    pub file_type: FileType,
    #[serde(rename = "modificationTime")]
    pub modification_time: i64,
}

impl FromStr for FileStatus {
    type Err = StreamError;

    fn from_str(s: &str) -> StreamResult<Self> {
        let value: Value = serde_json::from_str(s).map_err_to_unknown()?;

        let deserialized: FileStatus = serde_json::from_value(value["FileStatus"].to_owned()).map_err_to_unknown()?;

        tracing::trace!("file status deserialized with length {}", deserialized.length);

        Ok(deserialized)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct FileList {
    #[serde(rename = "FileStatus")]
    pub files: Vec<FileStatus>,
}

impl FromStr for FileList {
    type Err = StreamError;

    /// deserialize from the response of LISTSTATUS
    fn from_str(s: &str) -> StreamResult<Self> {
        let value: Value = serde_json::from_str(s).map_err_to_unknown()?;

        let deserialized: FileList = serde_json::from_value(value["FileStatuses"].to_owned()).map_err_to_unknown()?;

        tracing::trace!("file list deserialized, with {} items", deserialized.files.len());

        Ok(deserialized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use serde_json::json;

    impl ToString for FileStatus {
        fn to_string(&self) -> String {
            json!({ "FileStatus": serde_json::to_value(self).unwrap() }).to_string()
        }
    }

    impl ToString for FileList {
        fn to_string(&self) -> String {
            json!({ "FileStatuses": serde_json::to_value(self).unwrap() }).to_string()
        }
    }

    #[test]
    fn file_list_deserialize_from_json() {
        r#"{
        "FileStatuses": {
            "FileStatus": [
                {
                    "length": 6282131,
                    "pathSuffix": "PP0716.MUIDFLT33.txt",
                    "type": "FILE",
                    "blockSize": 268435456,
                    "accessTime": 1579841718935,
                    "modificationTime": 1579841728048,
                    "replication": 1,
                    "permission": "770",
                    "owner": "4a978291-0e92-43f9-b321-7ab504eadfce",
                    "group": "de2318af-d4d8-432f-b08e-66ee06a63783"
                },
                {
                    "length": 0,
                    "pathSuffix": "input",
                    "type": "DIRECTORY",
                    "blockSize": 0,
                    "accessTime": 1545460785918,
                    "modificationTime": 1545460798364,
                    "replication": 0,
                    "permission": "770",
                    "owner": "de2318af-d4d8-432f-b08e-66ee06a63783",
                    "group": "de2318af-d4d8-432f-b08e-66ee06a63783"
                }
            ]
        }}"#
        .parse::<FileList>()
        .should()
        .be_ok()
        .with_value(FileList {
            files: vec![
                FileStatus {
                    length: 6282131,
                    path_suffix: String::from("PP0716.MUIDFLT33.txt"),
                    file_type: FileType::FILE,
                    modification_time: 1579841728048,
                },
                FileStatus {
                    length: 0,
                    path_suffix: String::from("input"),
                    file_type: FileType::DIRECTORY,
                    modification_time: 1545460798364,
                },
            ],
        });
    }

    #[test]
    fn file_list_serialize_to_json() {
        let expected = json!({
        "FileStatuses": {
            "FileStatus": [
                {
                    "length": 6282131,
                    "pathSuffix": "PP0716.MUIDFLT33.txt",
                    "type": "FILE",
                    "modificationTime": 1579841728048i64,
                },
                {
                    "length": 0,
                    "pathSuffix": "input",
                    "type": "DIRECTORY",
                    "modificationTime": 1545460798364i64,
                }
            ]
        }})
        .to_string();

        FileList {
            files: vec![
                FileStatus {
                    length: 6282131,
                    path_suffix: String::from("PP0716.MUIDFLT33.txt"),
                    file_type: FileType::FILE,
                    modification_time: 1579841728048,
                },
                FileStatus {
                    length: 0,
                    path_suffix: String::from("input"),
                    file_type: FileType::DIRECTORY,
                    modification_time: 1545460798364,
                },
            ],
        }
        .should()
        .equal_string(expected);
    }

    #[test]
    fn file_status_deserialize_from_json() {
        r#"{
            "FileStatus": {
                "length": 6282131,
                "pathSuffix": "",
                "type": "FILE",
                "blockSize": 268435456,
                "accessTime": 1579841718935,
                "modificationTime": 1579841728048,
                "replication": 1,
                "permission": "770",
                "owner": "4a978291-0e92-43f9-b321-7ab504eadfce",
                "group": "de2318af-d4d8-432f-b08e-66ee06a63783"
            }
        }"#
        .parse::<FileStatus>()
        .should()
        .be_ok()
        .with_value(FileStatus {
            length: 6282131,
            path_suffix: "".to_owned(),
            file_type: FileType::FILE,
            modification_time: 1579841728048,
        });
    }

    #[test]
    fn file_status_serialize_to_json() {
        let expected = json!({
        "FileStatus":
        {
            "length": 6282131,
            "pathSuffix": "PP0716.MUIDFLT33.txt",
            "type": "FILE",
            "modificationTime": 1579841728048i64,
        }})
        .to_string();

        FileStatus {
            length: 6282131,
            path_suffix: String::from("PP0716.MUIDFLT33.txt"),
            file_type: FileType::FILE,
            modification_time: 1579841728048,
        }
        .should()
        .equal_string(expected);
    }
}
