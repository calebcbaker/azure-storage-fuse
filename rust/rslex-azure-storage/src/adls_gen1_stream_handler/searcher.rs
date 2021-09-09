use super::{
    file_dto::{FileList, FileType},
    request_builder::RequestBuilder,
    HANDLER_TYPE,
};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use rslex_core::{file_io::StreamResult, StreamInfo, SyncRecord};
use rslex_http_stream::{AsyncResponseExt, AsyncSearch, HttpClient, SearchContext, SessionPropertiesExt};
use std::{collections::HashMap, sync::Arc};
use tracing_sensitive::AsSensitive;

pub(super) struct Searcher {
    request_builder: RequestBuilder,
    http_client: Arc<dyn HttpClient>,
    arguments: SyncRecord,
}

impl Searcher {
    pub fn new(request_builder: RequestBuilder, http_client: Arc<dyn HttpClient>, arguments: SyncRecord) -> Self {
        Searcher {
            request_builder,
            http_client,
            arguments,
        }
    }
}

#[async_trait]
impl AsyncSearch for Searcher {
    async fn search(&self, search_context: SearchContext) -> StreamResult<(Vec<StreamInfo>, Vec<SearchContext>)> {
        tracing::debug!("[Searcher::search()] searching {}", search_context.as_sensitive_ref());

        let request = self.request_builder.find_streams_async(&search_context).await;

        let res = self.http_client.clone().request_async(request.into()).await?.success().await?;

        let file_list: FileList = res.into_string().await?.parse()?;

        let mut stream_infos = vec![];
        let mut search_contexts = vec![];

        for file in file_list.files.iter() {
            let prefix = search_context.prefix(true);
            let full_path = if prefix.is_empty() {
                file.path_suffix.clone()
            } else if file.path_suffix.is_empty() {
                prefix.to_string()
            } else {
                format!("{}/{}", prefix.trim_end_matches('/'), file.path_suffix.trim_start_matches('/'))
            };

            match file.file_type {
                FileType::FILE => {
                    if search_context.filter_file(&full_path) {
                        let session_properties = HashMap::default()
                            .with_size(file.length)
                            .with_modified_time(Utc.timestamp_millis(file.modification_time))
                            .with_is_seekable(true);
                        stream_infos.push(
                            StreamInfo::new(HANDLER_TYPE, self.request_builder.path_to_uri(&full_path), self.arguments.clone())
                                .with_session_properties(session_properties),
                        );
                    }
                },
                FileType::DIRECTORY => {
                    if let Some(sc) = search_context.map_dir(&full_path) {
                        search_contexts.push(sc);
                    }
                },
            }
        }

        tracing::info!(
            "[Searcher::search()] found {} files, {} more searches",
            stream_infos.len(),
            search_contexts.len()
        );

        Ok((stream_infos, search_contexts))
    }

    fn root(&self) -> StreamResult<SearchContext> {
        self.request_builder.path().parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adls_gen1_stream_handler::{
        file_dto::{FileList, FileStatus, FileType},
        HANDLER_TYPE,
    };
    use chrono::{TimeZone, Utc};
    use fluent_assertions::*;
    use itertools::Itertools;
    use rslex_core::file_io::{SearchResults, StreamError, StreamProperties};
    use rslex_http_stream::{ApplyCredential, FakeHttpClient, SessionPropertiesExt};

    trait IntoAdlsGen1SearchResults {
        fn into_search_results(self, uri: &str, credential: Arc<dyn ApplyCredential>) -> StreamResult<Box<dyn SearchResults>>;
    }

    impl IntoAdlsGen1SearchResults for FakeHttpClient {
        fn into_search_results(self, uri: &str, credential: Arc<dyn ApplyCredential>) -> StreamResult<Box<dyn SearchResults>> {
            Searcher::new(RequestBuilder::new(uri, credential)?, Arc::new(self), SyncRecord::empty()).into_search_results()
        }
    }

    #[test]
    fn with_single_file_return_iterator() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/somefile.csv?op=LISTSTATUS");
            })
            .body(
                FileList {
                    files: vec![FileStatus {
                        length: 100,
                        path_suffix: "".to_owned(),
                        file_type: FileType::FILE,
                        modification_time: 300,
                    }],
                }
                .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&[StreamInfo::new(
                HANDLER_TYPE,
                "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv",
                SyncRecord::empty(),
            )]);
    }

    #[test]
    fn with_directory_without_end_slash_return_iterator() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder";

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder?op=LISTSTATUS");
            })
            .body(
                FileList {
                    files: vec![
                        FileStatus {
                            length: 100,
                            path_suffix: "somefile1.csv".to_owned(),
                            file_type: FileType::FILE,
                            modification_time: 300,
                        },
                        FileStatus {
                            length: 100,
                            path_suffix: "subfolder".to_owned(),
                            file_type: FileType::DIRECTORY,
                            modification_time: 300,
                        },
                    ],
                }
                .to_string(),
            )
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/subfolder/?op=LISTSTATUS");
            })
            .body(
                FileList {
                    files: vec![FileStatus {
                        length: 100,
                        path_suffix: "somefile2.csv".to_owned(),
                        file_type: FileType::FILE,
                        modification_time: 300,
                    }],
                }
                .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&[
                StreamInfo::new(
                    HANDLER_TYPE,
                    "adl://someaccount.azuredatalakestore.net/somefolder/somefile1.csv",
                    SyncRecord::empty(),
                ),
                StreamInfo::new(
                    HANDLER_TYPE,
                    "adl://someaccount.azuredatalakestore.net/somefolder/subfolder/somefile2.csv",
                    SyncRecord::empty(),
                ),
            ]);
    }

    #[test]
    fn searcher_created_streaminfo_containing_session_properties() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/somefolder/somefile.csv?op=LISTSTATUS");
            })
            .body(
                FileList {
                    files: vec![FileStatus {
                        length: 100,
                        path_suffix: "".to_owned(),
                        file_type: FileType::FILE,
                        modification_time: Utc.ymd(2000, 1, 1).and_hms(1, 0, 0).timestamp_millis(),
                    }],
                }
                .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .be_single()
            .which_value()
            .session_properties()
            .should()
            .pass(|si| {
                si.stream_properties().should().be_some().with_value(StreamProperties {
                    size: 100,
                    created_time: None,
                    modified_time: Some(Utc.ymd(2000, 1, 1).and_hms(1, 0, 0)),
                });
                si.is_seekable().should().be_some().with_value(true);
            });
    }

    #[test]
    fn with_authentication_failure_return_permission_denied() {
        let uri = "adl://someaccount.azuredatalakestore.net/somefolder/somefile.csv";

        FakeHttpClient::default()
            .status(401)
            .body(String::default())
            .into_search_results(uri, Arc::new(()))
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn with_patterns_will_trigger_multiple_calls() {
        let uri = "adl://someaccount.azuredatalakestore.net/a*b/somefile.csv";

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/?op=LISTSTATUS");
            })
            .body(
                FileList {
                    files: vec![
                        FileStatus {
                            length: 100,
                            path_suffix: "acb".to_owned(),
                            file_type: FileType::FILE,
                            modification_time: 300,
                        },
                        FileStatus {
                            length: 0,
                            path_suffix: "adb".to_owned(),
                            file_type: FileType::DIRECTORY,
                            modification_time: 300,
                        },
                        FileStatus {
                            length: 0,
                            path_suffix: "bbb".to_owned(),
                            file_type: FileType::DIRECTORY,
                            modification_time: 300,
                        },
                    ],
                }
                .to_string(),
            )
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/adb/somefile.csv?op=LISTSTATUS");
            })
            .body(
                FileList {
                    files: vec![FileStatus {
                        length: 100,
                        path_suffix: "".to_owned(),
                        file_type: FileType::FILE,
                        modification_time: 300,
                    }],
                }
                .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&[StreamInfo::new(
                HANDLER_TYPE,
                "adl://someaccount.azuredatalakestore.net/adb/somefile.csv",
                SyncRecord::empty(),
            )]);
    }

    #[test]
    fn new_search_result_stops_at_first_result_iter_run_remaining_searches() {
        let uri = "adl://someaccount.azuredatalakestore.net/**.csv";

        let search_results = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/?op=LISTSTATUS");
            })
            .assert_num_requests(1) // this will be called by new(), and result will be cached
            .body(
                FileList {
                    files: vec![
                        FileStatus {
                            length: 100,
                            path_suffix: "1.csv".to_owned(),
                            file_type: FileType::FILE,
                            modification_time: 300,
                        },
                        FileStatus {
                            length: 0,
                            path_suffix: "a".to_owned(),
                            file_type: FileType::DIRECTORY,
                            modification_time: 300,
                        },
                    ],
                }
                .to_string(),
            )
            .with_request(|_, r| r.uri().to_string() == "https://someaccount.azuredatalakestore.net/webhdfs/v1/a/?op=LISTSTATUS")
            .assert_num_requests(2) // this will be called with iter(). Each iter() will trigger one call
            .body(
                FileList {
                    files: vec![FileStatus {
                        length: 100,
                        path_suffix: "2.csv".to_owned(),
                        file_type: FileType::FILE,
                        modification_time: 300,
                    }],
                }
                .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .expect("build SearchResults should succeed");

        // first run
        search_results
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&[
                StreamInfo::new(HANDLER_TYPE, "adl://someaccount.azuredatalakestore.net/1.csv", SyncRecord::empty()),
                StreamInfo::new(
                    HANDLER_TYPE,
                    "adl://someaccount.azuredatalakestore.net/a/2.csv",
                    SyncRecord::empty(),
                ),
            ]);

        // second run
        search_results
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&[
                StreamInfo::new(HANDLER_TYPE, "adl://someaccount.azuredatalakestore.net/1.csv", SyncRecord::empty()),
                StreamInfo::new(
                    HANDLER_TYPE,
                    "adl://someaccount.azuredatalakestore.net/a/2.csv",
                    SyncRecord::empty(),
                ),
            ]);
    }

    #[test]
    fn new_search_result_with_connection_failure_returns_error() {
        let uri = "adl://someaccount.azuredatalakestore.net/**.csv";

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri()
                    .should()
                    .equal_string("https://someaccount.azuredatalakestore.net/webhdfs/v1/?op=LISTSTATUS");
            })
            .body(
                FileList {
                    files: vec![
                        FileStatus {
                            length: 100,
                            path_suffix: "1.csv".to_owned(),
                            file_type: FileType::FILE,
                            modification_time: 300,
                        },
                        FileStatus {
                            length: 0,
                            path_suffix: "b".to_owned(),
                            file_type: FileType::DIRECTORY,
                            modification_time: 300,
                        },
                        FileStatus {
                            length: 0,
                            path_suffix: "a".to_owned(),
                            file_type: FileType::DIRECTORY,
                            modification_time: 300,
                        },
                    ],
                }
                .to_string(),
            )
            .with_request(|_, r| r.uri() == "https://someaccount.azuredatalakestore.net/webhdfs/v1/a/?op=LISTSTATUS")
            .status(401) // connection failure on second call
            .with_request(|_, r| r.uri() == "https://someaccount.azuredatalakestore.net/webhdfs/v1/b/?op=LISTSTATUS")
            .body(
                FileList {
                    files: vec![FileStatus {
                        length: 100,
                        path_suffix: "2.csv".to_owned(),
                        file_type: FileType::FILE,
                        modification_time: 300,
                    }],
                }
                .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .iter()
            .should()
            .contain(Err(StreamError::PermissionDenied));
    }

    #[test]
    fn new_search_result_with_three_searches() {
        let uri = "adl://someaccount.azuredatalakestore.net/**.csv";

        FakeHttpClient::default()
            .with_request(|_, r| r.uri() == "https://someaccount.azuredatalakestore.net/webhdfs/v1/?op=LISTSTATUS")
            .body(
                FileList {
                    files: vec![
                        FileStatus {
                            length: 100,
                            path_suffix: "1.csv".to_owned(),
                            file_type: FileType::FILE,
                            modification_time: 300,
                        },
                        FileStatus {
                            length: 0,
                            path_suffix: "a".to_owned(),
                            file_type: FileType::DIRECTORY,
                            modification_time: 300,
                        },
                    ],
                }
                .to_string(),
            )
            .with_request(|_, r| r.uri() == "https://someaccount.azuredatalakestore.net/webhdfs/v1/a/?op=LISTSTATUS")
            .body(
                FileList {
                    files: vec![
                        FileStatus {
                            length: 100,
                            path_suffix: "2.csv".to_owned(),
                            file_type: FileType::FILE,
                            modification_time: 300,
                        },
                        FileStatus {
                            length: 0,
                            path_suffix: "b".to_owned(),
                            file_type: FileType::DIRECTORY,
                            modification_time: 300,
                        },
                    ],
                }
                .to_string(),
            )
            .with_request(|_, r| r.uri() == "https://someaccount.azuredatalakestore.net/webhdfs/v1/a/b/?op=LISTSTATUS")
            .body(
                FileList {
                    files: vec![FileStatus {
                        length: 100,
                        path_suffix: "3.csv".to_owned(),
                        file_type: FileType::FILE,
                        modification_time: 300,
                    }],
                }
                .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .should()
            .be_ok()
            .which_value()
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            // with multi-threading, the result order is underterministic
            // to convert the result to String and sorting to force an order
            .iter()
            .map(|r| r.resource_id().to_string())
            .sorted()
            .should()
            .equal_iterator(&[
                "adl://someaccount.azuredatalakestore.net/1.csv".to_string(),
                "adl://someaccount.azuredatalakestore.net/a/2.csv".to_string(),
                "adl://someaccount.azuredatalakestore.net/a/b/3.csv".to_string(),
            ]);
    }
}
