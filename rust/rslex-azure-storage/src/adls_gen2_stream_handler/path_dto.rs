use chrono::{DateTime, Utc};
use rslex_core::{
    file_io::{StreamError, StreamResult},
    MapErrToUnknown,
};
use rslex_http_stream::Response;
use serde::{Deserialize, Serialize};
use std::{
    convert::TryFrom,
    num::ParseIntError,
    str::{FromStr, ParseBoolError},
};

#[derive(Deserialize, Serialize)]
struct PathDto {
    #[serde(rename = "contentLength")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_length: Option<String>,

    pub name: String,

    #[serde(rename = "isDirectory")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_directory: Option<String>,

    #[serde(rename = "lastModified")]
    pub last_modified: String,
}

impl TryFrom<PathDto> for Path {
    type Error = String;

    fn try_from(dto: PathDto) -> std::result::Result<Self, Self::Error> {
        let content_length: u64 = dto
            .content_length
            .unwrap_or("0".to_owned())
            .parse()
            .map_err(|e: ParseIntError| e.to_string())?;
        let name = dto.name.clone();
        let is_directory = dto
            .is_directory
            .unwrap_or("false".to_owned())
            .parse()
            .map_err(|e: ParseBoolError| e.to_string())?;
        let last_modified: DateTime<Utc> = DateTime::parse_from_rfc2822(&dto.last_modified).map_err(|e| e.to_string())?.into();

        Ok(Path {
            content_length,
            name,
            is_directory,
            last_modified,
        })
    }
}

impl Into<PathDto> for Path {
    fn into(self) -> PathDto {
        PathDto {
            content_length: match self.content_length {
                0 => None,
                o => Some(o.to_string()),
            },
            name: self.name,
            is_directory: match self.is_directory {
                true => Some(true.to_string()),
                false => None,
            },
            last_modified: self.last_modified.to_rfc2822(),
        }
    }
}

/// see https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list#path
#[derive(Deserialize, Clone, Serialize, Debug, PartialEq)]
#[serde(try_from = "PathDto")]
#[serde(into = "PathDto")]
pub struct Path {
    pub content_length: u64,

    pub name: String,

    pub is_directory: bool,

    pub last_modified: DateTime<Utc>,
}

impl Path {
    pub(crate) fn try_from_response(name: String, response: &Response) -> Result<Self, StreamError> {
        let headers = response.headers();
        let content_length = headers
            .get("Content-Length")
            .ok_or(StreamError::Unknown(
                "[adls_gen2_stream_handler::path_dto] Content-Length missing from HTTP header".to_owned(),
                None,
            ))?
            .to_str()
            .map_err_to_unknown()?
            .parse::<u64>()
            .map_err_to_unknown()?;

        let last_modified = headers
            .get("Last-Modified")
            .map::<StreamResult<_>, _>(|s| {
                Ok(DateTime::parse_from_rfc2822(s.to_str().map_err_to_unknown()?)
                    .map_err_to_unknown()?
                    .into())
            })
            .transpose()?
            .unwrap_or(chrono::MIN_DATE.and_hms(0, 0, 0));

        Ok(Path {
            content_length,
            name,
            is_directory: headers
                .get("x-ms-resource-type")
                .map_or(Ok::<bool, StreamError>(content_length == 0), |val| {
                    Ok(val.to_str().map_err_to_unknown()?.to_lowercase() == "directory")
                })?,
            last_modified,
        })
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct PathList {
    pub paths: Vec<Path>,
}

impl FromStr for PathList {
    type Err = StreamError;

    fn from_str(s: &str) -> StreamResult<Self> {
        let deserialized: PathList = serde_json::from_str(s).map_err_to_unknown()?;

        tracing::debug!(
            "[PathList::from_str()] path list deserialized, with {} items",
            deserialized.paths.len()
        );

        Ok(deserialized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use fluent_assertions::*;
    use serde_json::json;

    impl ToString for PathList {
        fn to_string(&self) -> String {
            serde_json::to_string(self).unwrap()
        }
    }

    #[test]
    fn path_list_deserialize_from_json() {
        r#"{
        "paths": [
            {
                "etag": "0x8D7000640BEE314",
                "isDirectory": "true",
                "lastModified": "Wed, 03 Jul 2019 22:31:55 GMT",
                "name": "input"
            },
            {
                "contentLength": "111983476",
                "etag": "0x8D6CE83EFFFB338",
                "lastModified": "Wed, 01 May 2019 22:25:38 GMT",
                "name": "nyctaxi.csv"
            }]}"#
            .parse::<PathList>()
            .should()
            .be_ok()
            .with_value(PathList {
                paths: vec![
                    Path {
                        content_length: 0,
                        is_directory: true,
                        last_modified: Utc.ymd(2019, 7, 3).and_hms(22, 31, 55),
                        name: "input".to_owned(),
                    },
                    Path {
                        content_length: 111983476,
                        is_directory: false,
                        last_modified: Utc.ymd(2019, 5, 1).and_hms(22, 25, 38),
                        name: "nyctaxi.csv".to_owned(),
                    },
                ],
            });
    }

    #[test]
    fn path_list_serialize_to_json() {
        let expected = json!({
        "paths": [
            {
                "name": "input",
                "isDirectory": "true",
                "lastModified": "Wed, 03 Jul 2019 22:31:55 +0000",
            },
            {
                "contentLength": "111983476",
                "name": "nyctaxi.csv",
                "lastModified": "Wed, 01 May 2019 22:25:38 +0000",
            }]})
        .to_string();

        PathList {
            paths: vec![
                Path {
                    content_length: 0,
                    is_directory: true,
                    last_modified: Utc.ymd(2019, 7, 3).and_hms(22, 31, 55),
                    name: "input".to_owned(),
                },
                Path {
                    content_length: 111983476,
                    is_directory: false,
                    last_modified: Utc.ymd(2019, 5, 1).and_hms(22, 25, 38),
                    name: "nyctaxi.csv".to_owned(),
                },
            ],
        }
        .should()
        .equal_string(expected);
    }
}
