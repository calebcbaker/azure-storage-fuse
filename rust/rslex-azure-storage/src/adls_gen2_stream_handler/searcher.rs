use super::{path_dto::PathList, request_builder::RequestBuilder, HANDLER_TYPE};
use async_trait::async_trait;
use rslex_core::{file_io::StreamResult, MapErrToUnknown, StreamInfo, SyncRecord};
use rslex_http_stream::{AsyncResponseExt, AsyncSearch, HttpClient, SearchContext, SessionPropertiesExt};
use std::{collections::HashMap, sync::Arc};
use tracing_sensitive::AsSensitive;

// use one pass recursive search, or parallel search
const ONE_PASS_RECURSIVE: bool = false;

pub(super) struct Searcher {
    request_builder: RequestBuilder,
    arguments: SyncRecord,
    http_client: Arc<dyn HttpClient>,
}

#[async_trait]
impl AsyncSearch for Searcher {
    async fn search(&self, search_context: SearchContext) -> StreamResult<(Vec<StreamInfo>, Vec<SearchContext>)> {
        tracing::debug!("[Searcher::search()] searching {}", search_context.as_sensitive_ref());
        let sc = search_context.to_string();

        let request = self.request_builder.find_streams_async(&search_context).await?;

        let mut stream_infos = vec![];
        let mut search_contexts = vec![];

        let res = self.http_client.clone().request_async(request.into()).await?.success().await?;

        if let Some(continuation) = res.headers().get("x-ms-continuation") {
            // if there is continuation token, create a new search context here
            search_contexts.push(
                search_context
                    .clone()
                    .with_continuation(continuation.to_str().map_err_to_unknown()?),
            );
        }

        let path_list: PathList = res.into_string().await?.parse()?;

        let mut num_paths = 0;
        let mut num_dirs = 0;
        for path in path_list.paths.iter() {
            num_paths += 1;

            if path.is_directory {
                if let Some(sc) = search_context.map_dir(&path.name) {
                    search_contexts.push(sc);
                    num_dirs += 1;
                }
            } else if search_context.filter_file(&path.name) {
                let session_properties = HashMap::default()
                    .with_size(path.content_length)
                    .with_modified_time(path.last_modified);
                stream_infos.push(
                    StreamInfo::new(HANDLER_TYPE, self.request_builder.path_to_uri(&path.name), self.arguments.clone())
                        .with_session_properties(session_properties),
                );
            }
        }

        tracing::info!(
            "[Searcher::search()] scan {}: {} items({} files and {} dirs), matching {} files, {} more searches",
            sc.as_sensitive_ref(),
            num_paths,
            num_paths - num_dirs,
            num_dirs,
            stream_infos.len(),
            search_contexts.len()
        );

        Ok((stream_infos, search_contexts))
    }

    fn root(&self) -> StreamResult<SearchContext> {
        if ONE_PASS_RECURSIVE {
            self.request_builder
                .path()
                .parse::<SearchContext>()
                .map(|s| s.into_one_pass_search())
        } else {
            self.request_builder.path().parse()
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adls_gen2_stream_handler::{
        path_dto::{Path, PathList},
        HANDLER_TYPE,
    };
    use chrono::{TimeZone, Utc};
    use fluent_assertions::*;
    use itertools::Itertools;
    use rslex_core::file_io::{SearchResults, StreamError, StreamProperties};
    use rslex_http_stream::{ApplyCredential, FakeHttpClient, SessionPropertiesExt};
    use std::sync::Arc;

    trait IntoAdlsGen2SearchResults {
        fn into_search_results(self, uri: &str, credential: Arc<dyn ApplyCredential>) -> StreamResult<Box<dyn SearchResults>>;
    }

    impl IntoAdlsGen2SearchResults for FakeHttpClient {
        fn into_search_results(self, uri: &str, credential: Arc<dyn ApplyCredential>) -> StreamResult<Box<dyn SearchResults>> {
            Searcher::new(RequestBuilder::new(uri, credential)?, Arc::new(self), SyncRecord::empty()).into_search_results()
        }
    }

    #[test]
    fn with_single_file_return_iterator() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.uri().should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false&directory=somefile.csv");
            })
            .body(
                PathList {
                    paths: vec![Path {
                        content_length: 100,
                        name: "somefile.csv".to_owned(),
                        is_directory: false,
                        last_modified: Utc.ymd(2020, 1, 1).and_hms(0, 0, 0),
                    }],
                }
                    .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .expect("build SearchResults should succeed").iter().should().all_be_ok().which_inner_values().should().equal_iterator(&[StreamInfo::new(
            HANDLER_TYPE,
            "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv",
            SyncRecord::empty(),
        )]);
    }

    #[test]
    fn returned_stream_info_contains_session_properties() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

        FakeHttpClient::default()
            .assert_request(|_, r| {
                r.uri().should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false&directory=somefile.csv");
            })
            .body(
                PathList {
                    paths: vec![Path {
                        content_length: 100,
                        name: "somefile.csv".to_owned(),
                        is_directory: false,
                        last_modified: Utc.ymd(2020, 1, 1).and_hms(0, 0, 0),
                    }],
                }
                    .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .expect("build SearchResults should succeed").iter().should().all_be_ok().which_inner_values().should()
            .be_single()
            .which_value().session_properties().stream_properties()
            .should().be_some().with_value(StreamProperties {
            size: 100,
            created_time: None,
            modified_time: Some(Utc.ymd(2020, 1, 1).and_hms(0, 0, 0)),
        });
    }

    #[test]
    fn with_authentication_failure_return_permission_denied() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/somefile.csv";

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
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/a*b/somefile.csv";

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri().should().equal_string("https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false");
            })
            .body(
                PathList {
                    paths: vec![
                        Path {
                            content_length: 100,
                            name: "acb".to_owned(),
                            is_directory: false,
                            last_modified: Utc::now(),
                        },
                        Path {
                            content_length: 0,
                            name: "adb".to_owned(),
                            is_directory: true,
                            last_modified: Utc::now(),
                        },
                        Path {
                            content_length: 0,
                            name: "bbb".to_owned(),
                            is_directory: true,
                            last_modified: Utc::now(),
                        },
                    ],
                }
                    .to_string(),
            )
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.uri().should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false&directory=adb%2Fsomefile.csv");
            })
            .body(
                PathList {
                    paths: vec![
                        Path {
                            content_length: 100,
                            name: "adb/somefile.csv".to_owned(),
                            is_directory: false,
                            last_modified: Utc::now(),
                        },
                    ],
                }
                    .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .expect("build SearchResults should succeed").iter().should().all_be_ok().which_inner_values().should().equal_iterator(&[StreamInfo::new(
            HANDLER_TYPE,
            "https://someaccount.dfs.core.windows.net/somefilesystem/adb/somefile.csv",
            SyncRecord::empty(),
        )]);
    }

    #[test]
    fn with_continuation_token() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/*.csv";

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri().should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false");
            })
            .header("x-ms-continuation", "aaa")
            .body(
                PathList {
                    paths: vec![
                        Path {
                            content_length: 100,
                            name: "1.csv".to_owned(),
                            is_directory: false,
                            last_modified: Utc::now(),
                        },
                    ],
                }
                    .to_string(),
            )
            .with_request(|id, _| id == 1)
            .assert_request(|_, r| {
                r.uri().should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false&continuation=aaa");
            })
            .body(
                PathList {
                    paths: vec![
                        Path {
                            content_length: 100,
                            name: "2.csv".to_owned(),
                            is_directory: false,
                            last_modified: Utc::now(),
                        },
                    ],
                }
                    .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .expect("build SearchResults should succeed").iter().should().all_be_ok().which_inner_values().should().equal_iterator(&[
            StreamInfo::new(
                HANDLER_TYPE,
                "https://someaccount.dfs.core.windows.net/somefilesystem/1.csv",
                SyncRecord::empty(),
            ),
            StreamInfo::new(
                HANDLER_TYPE,
                "https://someaccount.dfs.core.windows.net/somefilesystem/2.csv",
                SyncRecord::empty(),
            ),
        ]);
    }

    #[test]
    fn new_search_result_stops_at_first_result_iter_run_remaining_searches() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/**.csv";

        let search_results = FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri().should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false");
            })
            .body(
                PathList {
                    paths: vec![
                        Path {
                            content_length: 100,
                            name: "1.csv".to_owned(),
                            is_directory: false,
                            last_modified: Utc::now(),
                        },
                        Path {
                            content_length: 0,
                            name: "a".to_owned(),
                            is_directory: true,
                            last_modified: Utc::now(),
                        },
                    ],
                }
                    .to_string(),
            )
            .with_request(|_, r| r.uri().to_string() == "https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false&directory=a%2F")
            .assert_num_requests(2) // this will be called with iter(). Each iter() will trigger one call
            .body(
                PathList {
                    paths: vec![
                        Path {
                            content_length: 100,
                            name: "a/2.csv".to_owned(),
                            is_directory: false,
                            last_modified: Utc::now(),
                        },
                    ],
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
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.dfs.core.windows.net/somefilesystem/1.csv",
                    SyncRecord::empty(),
                ),
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.dfs.core.windows.net/somefilesystem/a/2.csv",
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
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.dfs.core.windows.net/somefilesystem/1.csv",
                    SyncRecord::empty(),
                ),
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.dfs.core.windows.net/somefilesystem/a/2.csv",
                    SyncRecord::empty(),
                ),
            ]);
    }

    #[test]
    fn new_search_result_with_connection_failure_returns_error() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/**/*.csv";

        FakeHttpClient::default()
            .with_request(|id, _| id == 0)
            .assert_request(|_, r| {
                r.uri().should()
                    .equal_string("https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false");
            })
            .body(
                PathList {
                    paths: vec![
                        Path {
                            content_length: 100,
                            name: "1.csv".to_owned(),
                            is_directory: false,
                            last_modified: Utc::now(),
                        },
                        Path {
                            content_length: 0,
                            name: "a".to_owned(),
                            is_directory: true,
                            last_modified: Utc::now(),
                        },
                        Path {
                            content_length: 0,
                            name: "b".to_owned(),
                            is_directory: true,
                            last_modified: Utc::now(),
                        },
                    ],
                }
                    .to_string(),
            )
            .with_request(|_, r| r.uri() == "https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false&directory=a%2F")
            .status(401) // connection failure on second call
            .with_request(|_, r| r.uri() == "https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false&directory=b%2F")
            .body(
                PathList {
                    paths: vec![
                        Path {
                            content_length: 100,
                            name: "b/1.csv".to_owned(),
                            is_directory: false,
                            last_modified: Utc::now(),
                        },
                    ],
                }
                    .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .expect("build SearchResults should succeed").iter().should().contain(Err(StreamError::PermissionDenied));
    }

    #[test]
    fn new_search_result_with_three_searches() {
        let uri = "https://someaccount.dfs.core.windows.net/somefilesystem/**.csv";

        FakeHttpClient::default()
            .with_request(|_, r| r.uri() == "https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false")
            .body(
                PathList {
                    paths: vec![
                        Path {
                            content_length: 100,
                            name: "1.csv".to_owned(),
                            is_directory: false,
                            last_modified: Utc::now(),
                        },
                        Path {
                            content_length: 0,
                            name: "a".to_owned(),
                            is_directory: true,
                            last_modified: Utc::now(),
                        },
                    ],
                }
                    .to_string(),
            )
            .with_request(|_, r| r.uri() == "https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false&directory=a%2F")
            .body(
                PathList {
                    paths: vec![
                        Path {
                            content_length: 100,
                            name: "a/2.csv".to_owned(),
                            is_directory: false,
                            last_modified: Utc::now(),
                        },
                        Path {
                            content_length: 0,
                            name: "a/b".to_owned(),
                            is_directory: true,
                            last_modified: Utc::now(),
                        },
                    ],
                }
                    .to_string(),
            )
            .with_request(|_, r| r.uri() == "https://someaccount.dfs.core.windows.net/somefilesystem?resource=filesystem&maxResults=5000&recursive=false&directory=a%2Fb%2F")
            .body(
                PathList {
                    paths: vec![
                        Path {
                            content_length: 100,
                            name: "a/b/3.csv".to_owned(),
                            is_directory: false,
                            last_modified: Utc::now(),
                        },
                    ],
                }
                    .to_string(),
            )
            .into_search_results(uri, Arc::new(()))
            .expect("build SearchResults should succeed").iter().should().all_be_ok().which_inner_values()
            // with multi-threading, the result order is underterministic
            // to convert the result to String and sorting to force an order
            .iter()
            .map(|r| r.resource_id().to_string())
            .sorted()
            .should()
            .equal_iterator(&[
                "https://someaccount.dfs.core.windows.net/somefilesystem/1.csv".to_string(),
                "https://someaccount.dfs.core.windows.net/somefilesystem/a/2.csv".to_string(),
                "https://someaccount.dfs.core.windows.net/somefilesystem/a/b/3.csv".to_string(),
            ]);
    }
}
