use super::{
    blob_dto::{BlobEntry, BlobList},
    request_builder::RequestBuilder,
    HANDLER_TYPE,
};
use crate::blob_stream_handler::request_builder::HEADER_HDI_FOLDER_METADATA;
use async_trait::async_trait;
use http::StatusCode;
use rslex_core::{
    file_io::{StreamError, StreamResult},
    MapErrToUnknown, StreamInfo, SyncRecord,
};
use rslex_http_stream::{AsyncResponseExt, AsyncSearch, HeadRequest, HttpClient, SearchContext, SpawnBlocking};
use std::{collections::HashMap, sync::Arc};
use tracing_sensitive::AsSensitive;

pub(super) struct Searcher {
    request_builder: RequestBuilder,
    arguments: SyncRecord,
    http_client: Arc<dyn HttpClient>,
}

// use one pass recursive search, or parallel search
// with blob, seems the traffic is throttled, running parallel search is much slower than
// serialized search. So default to ONE_PASS.
const ONE_PASS_RECURSIVE: bool = true;

#[async_trait]
impl AsyncSearch for Searcher {
    #[tracing::instrument(skip(self, search_context))]
    async fn search(&self, search_context: SearchContext) -> StreamResult<(Vec<StreamInfo>, Vec<SearchContext>)> {
        tracing::debug!("[Searcher::search()] searching {}", search_context.as_sensitive_ref());
        let mut search_context = search_context;
        // In some scenario, the search pattern is just a single stream. In this case, the LIST operation is not required at all.
        // We could get the information we want by calling only HEAD. There is not much different efficiency-wise. But it's useful
        // as in such case, the customer don't need permission for LIST (with blob, LIST and READ are two separate RBAC action.
        // This is only meaningful is the whole search pattern is a single file. So we just match if the search context match the whole path.
        if Some(self.request_builder.path()) == search_context.single_stream() {
            let request = self.request_builder.head();
            let client = self.http_client.clone();
            let response = (move || client.request(request)).spawn_blocking().await.map_err_to_unknown()??;

            if response.status() != StatusCode::NOT_FOUND
                && !response
                    .headers()
                    .get(HEADER_HDI_FOLDER_METADATA)
                    .map_or(false, |m| m.to_str().unwrap_or("false").to_lowercase() == "true")
            {
                let mut session_properties = HashMap::default();
                RequestBuilder::parse_response(response, &mut session_properties)?;

                let si = StreamInfo::new(
                    HANDLER_TYPE,
                    self.request_builder.path_to_uri(self.request_builder.path()),
                    self.arguments.clone(),
                )
                .with_session_properties(session_properties);

                return Ok((vec![si], vec![]));
            } else {
                // If path was not found as a file it still can be a folder.
                // Trying to enumerate its content to check.
                search_context = search_context.into_folder(ONE_PASS_RECURSIVE);
            }
        }
        let sc = search_context.to_string();

        let request = self.request_builder.find_streams_async(&search_context).await?;

        let mut stream_infos = vec![];
        let mut search_contexts = vec![];

        let res = self
            .http_client
            .clone()
            .request_async(request)
            .await?
            .success()
            .await
            .map_err(|e| {
                if e.status_code == 404 {
                    // when authentication fail, service will return 404 - NotFound. But if it's actually NotFound, it will return an empty list.
                    // so we map 404 to permission failure
                    tracing::warn!(
                        "[Searcher::search()] blob service return 404 - not found, but this could mostly be authentication failure"
                    );
                    StreamError::PermissionDenied
                } else {
                    e.into()
                }
            })?;

        let blob_list: BlobList = res.into_string().await?.parse()?;

        if let Some(continuation) = blob_list.next_marker {
            // if there is continuation token, create a new search context here
            search_contexts.push(search_context.clone().with_continuation(continuation));
        }

        let mut files_found = 0u16;
        let mut dirs_found = 0u16;

        for blob_entry in blob_list.blobs {
            match blob_entry {
                BlobEntry::Blob(blob) => {
                    files_found += 1;
                    if !blob.is_hdi_folder() && search_context.filter_file(&blob.name) {
                        stream_infos.push(blob.to_stream_info(&self.request_builder, self.arguments.clone()));
                    }
                },
                BlobEntry::BlobPrefix(dir_name) => {
                    dirs_found += 1;
                    if let Some(dir) = search_context.map_dir(dir_name.trim_end_matches("/")) {
                        search_contexts.push(dir);
                    }
                },
            }
        }

        tracing::info!(
            "[Searcher::search()] scan {} {} files and {} dirs, matching {} files, {} more searches",
            sc.as_sensitive_ref(),
            files_found,
            dirs_found,
            stream_infos.len(),
            search_contexts.len()
        );

        Ok((stream_infos, search_contexts))
    }

    fn root(&self) -> StreamResult<SearchContext> {
        if !ONE_PASS_RECURSIVE {
            self.request_builder.path().parse()
        } else {
            self.request_builder
                .path()
                .parse::<SearchContext>()
                .map(|s| s.into_one_pass_search())
        }
    }

    // blob has throttling, so reduce the number of parallel searches
    fn max_parallel_searches(&self) -> usize {
        100
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
    use crate::blob_stream_handler::{
        blob_dto::{Blob, BlobEntry, BlobList},
        HANDLER_TYPE,
    };
    use chrono::{TimeZone, Utc};
    use fluent_assertions::*;
    use http::Method;
    use itertools::Itertools;
    use maplit::hashmap;
    use rslex_core::file_io::{SearchResults, StreamProperties};
    use rslex_http_stream::{FakeHttpClient, HttpError, SessionPropertiesExt};
    use std::{collections::HashMap, sync::Arc};

    trait IntoBlobSearchResults {
        fn into_search_results(self, uri: &str) -> StreamResult<Box<dyn SearchResults>>;
    }

    impl IntoBlobSearchResults for FakeHttpClient {
        fn into_search_results(self, uri: &str) -> StreamResult<Box<dyn SearchResults>> {
            Searcher::new(RequestBuilder::new(uri, Arc::new(()))?, Arc::new(self), SyncRecord::empty()).into_search_results()
        }
    }

    #[test]
    /// when all the result could be retrieved in one call, it will be retrieved and cached when creating search result
    fn iters_returns_blobs_with_one_batch() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/*.csv";

        FakeHttpClient::default()
            .body(
                BlobList {
                    blobs: vec![
                        BlobEntry::Blob(Blob {
                            name: "somefile.csv".to_string(),
                            properties: HashMap::default(),
                            metadata: hashmap! {},
                        }),
                        BlobEntry::Blob(Blob {
                            name: "somefile.csv.1".to_string(),
                            properties: HashMap::default(),
                            metadata: hashmap! {},
                        }),
                    ],
                    next_marker: None,
                }
                .to_string(),
            )
            .assert_num_requests(1) // should be called only once
            .into_search_results(uri)
            .expect("search result should be created")
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(vec![StreamInfo::new(
                HANDLER_TYPE,
                "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv",
                SyncRecord::empty(),
            )]);
    }

    #[test]
    fn iters_ignores_hbi_folders() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/*.csv";

        FakeHttpClient::default()
            .body(
                BlobList {
                    blobs: vec![
                        BlobEntry::Blob(Blob {
                            name: "somefile1.csv".to_string(),
                            properties: HashMap::default(),
                            metadata: hashmap! {
                                "hdi_isfolder".to_owned() => "true".to_owned()
                            },
                        }),
                        BlobEntry::Blob(Blob {
                            name: "somefile2.csv".to_string(),
                            properties: HashMap::default(),
                            metadata: hashmap! {},
                        }),
                    ],
                    next_marker: None,
                }
                .to_string(),
            )
            .assert_num_requests(1) // should be called only once
            .into_search_results(uri)
            .expect("search result should be created")
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(vec![StreamInfo::new(
                HANDLER_TYPE,
                "https://someaccount.blob.core.windows.net/somecontainer/somefile2.csv",
                SyncRecord::empty(),
            )]);
    }

    #[test]
    fn iters_ignores_hbi_folder_placeholder_files_when_pointed_directly() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/test.csv";

        FakeHttpClient::default()
            .with_request(|_, q| {
                q.method() == Method::HEAD && q.uri().eq("https://someaccount.blob.core.windows.net/somecontainer/test.csv")
            })
            .header(HEADER_HDI_FOLDER_METADATA, "true")
            .status(200)
            .assert_num_requests(1)
            .with_request(|_, q| q.method() == Method::GET)
            .body(
                BlobList {
                    blobs: vec![
                        BlobEntry::Blob(Blob {
                            name: "test.csv/somefile1.csv".to_string(),
                            properties: HashMap::default(),
                            metadata: hashmap! {},
                        }),
                        BlobEntry::Blob(Blob {
                            name: "test.csv/somefile2.csv".to_string(),
                            properties: HashMap::default(),
                            metadata: hashmap! {},
                        }),
                    ],
                    next_marker: None,
                }
                .to_string(),
            )
            .assert_num_requests(1) // should be called only once
            .into_search_results(uri)
            .expect("search result should be created")
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(vec![
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.blob.core.windows.net/somecontainer/test.csv/somefile1.csv",
                    SyncRecord::empty(),
                ),
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.blob.core.windows.net/somecontainer/test.csv/somefile2.csv",
                    SyncRecord::empty(),
                ),
            ]);
    }

    #[test]
    /// when the uri is a single file, searcher will call metadata to return the ResourceInfo directly
    fn with_single_stream_call_metadata_instead_of_list() {
        let uri: &str = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .assert_request(|_, q| {
                q.method().should().be(&Method::HEAD);
                q.uri()
                    .should()
                    .equal_string("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv");
            })
            .header("Content-Length", "12")
            .into_search_results(uri)
            .expect("search result should be created")
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .be_single()
            .which_value()
            .should()
            .pass(|si| {
                si.should().be(&StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv",
                    SyncRecord::empty(),
                ));

                si.session_properties().size().should().be_some().with_value(12);
            });
    }

    #[test]
    /// when the uri is a single file, searcher will call head to return the ResourceInfo directly
    fn with_single_stream_not_exist_returns_not_found() {
        let uri: &str = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            // HEAD request to get file properties
            .with_request(|_, q| {
                q.method() == Method::HEAD && q.uri().eq("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv")
            })
            .status(404)
            // If file was not found we will check if path is a folder path
            .with_request(|_, q| {
                let url_str = q.uri().to_string();
                q.method() == Method::GET
                    && url_str.contains("https://someaccount.blob.core.windows.net/somecontainer")
                    && url_str.contains("&prefix=somefile.csv")
            })
            .status(200)
            .body(
                BlobList {
                    blobs: vec![],
                    next_marker: None,
                }
                .to_string(),
            )
            .into_search_results(uri)
            .should()
            .be_err()
            .with_value(StreamError::NotFound);
    }

    #[test]
    fn returned_stream_info_contains_session_properties() {
        let uri = "https://someaccount.blob.core.windows.net/somecontainer/*.csv";

        FakeHttpClient::default()
            .body(
                BlobList {
                    blobs: vec![BlobEntry::Blob(Blob {
                        name: "somefile.csv".to_string(),
                        properties: hashmap! {
                            "Creation-Time".to_owned() => "Sat, 13 Oct 2018 00:18:24 GMT".to_owned(),
                            "Last-Modified".to_owned() => "Sat, 13 Oct 2018 00:18:24 GMT".to_owned(),
                            "Content-Length".to_owned() => "12213".to_owned(),
                        },
                        metadata: hashmap! {},
                    })],
                    next_marker: None,
                }
                .to_string(),
            )
            .assert_num_requests(1) // should be called only once
            .into_search_results(uri)
            .expect("search result should be created")
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .be_single()
            .which_value()
            .session_properties()
            .stream_properties()
            .should()
            .be_some()
            .with_value(StreamProperties {
                size: 12213,
                created_time: Some(Utc.ymd(2018, 10, 13).and_hms(0, 18, 24)),
                modified_time: Some(Utc.ymd(2018, 10, 13).and_hms(0, 18, 24)),
            });
    }

    #[test]
    /// if the search results take two batches, the first batch will be retrieved and cached when creating search result,
    /// while the second batch will be called when the search result is retrieved
    /// when there are multiple iterators, the first batch will be called only once
    /// while the second batch will be called multiple times, each with one iteration
    fn iters_returns_blobs_with_two_batchs() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/**.csv";

        let results = FakeHttpClient::default()
            // the first call
            .with_request(|id, _| id == 0)
            .body(
                BlobList {
                    blobs: vec![BlobEntry::Blob(Blob {
                        name: "1.csv".to_string(),
                        properties: HashMap::default(),
                        metadata: hashmap! {},
                    })],
                    next_marker: Some("1".to_string()),
                }
                .to_string(),
            )
            .assert_num_requests(1)
            // the second & third call
            .with_request(|id, _| id > 0)
            .body(
                BlobList {
                    blobs: vec![BlobEntry::Blob(Blob {
                        name: "2.csv".to_string(),
                        properties: HashMap::default(),
                        metadata: hashmap! {},
                    })],
                    next_marker: None,
                }
                .to_string(),
            )
            .assert_num_requests(2)
            .into_search_results(file_path)
            .expect("search result should be created");

        let expected = vec![
            StreamInfo::new(
                HANDLER_TYPE,
                "https://someaccount.blob.core.windows.net/somecontainer/1.csv",
                SyncRecord::empty(),
            ),
            StreamInfo::new(
                HANDLER_TYPE,
                "https://someaccount.blob.core.windows.net/somecontainer/2.csv",
                SyncRecord::empty(),
            ),
        ];

        // the first iteration
        results
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&expected);

        // the second iteration
        results
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&expected);
    }

    #[test]
    /// if the first batch is fully filtered out but there is continuation tokens
    /// will continue call the next batch. NotFound will only be returned after
    /// searching the whole tree. This also means stream_handler.find_streams()
    /// could take long even if the iteration is not triggered.
    fn iters_returns_blobs_with_two_batchs_and_first_batch_filtered_out() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/**.csv";

        FakeHttpClient::default()
            // the first call
            .with_request(|id, _| id == 0)
            .body(
                BlobList {
                    blobs: vec![BlobEntry::Blob(Blob {
                        name: "1.notcsv".to_string(),
                        properties: HashMap::default(),
                        metadata: hashmap! {},
                    })],
                    next_marker: Some("1".to_string()),
                }
                .to_string(),
            )
            // the second call
            .with_request(|id, _| id == 1)
            .body(
                BlobList {
                    blobs: vec![BlobEntry::Blob(Blob {
                        name: "2.csv".to_string(),
                        properties: HashMap::default(),
                        metadata: hashmap! {},
                    })],
                    next_marker: None,
                }
                .to_string(),
            )
            .into_search_results(file_path)
            .expect("search result should be created")
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&[StreamInfo::new(
                HANDLER_TYPE,
                "https://someaccount.blob.core.windows.net/somecontainer/2.csv",
                SyncRecord::empty(),
            )]);
    }

    #[test]
    /// connection in second batch will return stream_err
    fn iters_when_connection_failure_in_continuation_return_err() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/**.csv";

        FakeHttpClient::default()
            // the first call
            .with_request(|id, _| id == 0)
            .body(
                BlobList {
                    blobs: vec![BlobEntry::Blob(Blob {
                        name: "1.csv".to_string(),
                        properties: HashMap::default(),
                        metadata: hashmap! {},
                    })],
                    next_marker: Some("1".to_string()),
                }
                .to_string(),
            )
            // the second call
            .with_request(|id, _| id == 1)
            .error(HttpError::new(true))
            .into_search_results(file_path)
            .expect("search result should be created")
            .iter()
            .should()
            .equal_iterator(&[
                Ok(StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.blob.core.windows.net/somecontainer/1.csv",
                    SyncRecord::empty(),
                )),
                Err(StreamError::ConnectionFailure { source: None }),
            ]);
    }

    #[test]
    /// with three batches, the first batch is called when creating search_result
    /// while the next two batches happens when calling the iteration
    fn iters_returns_blobs_with_three_batchs() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/**.csv";

        FakeHttpClient::default()
            // the first call
            .with_request(|id, _| id == 0)
            .body(
                BlobList {
                    blobs: vec![BlobEntry::Blob(Blob {
                        name: "1.csv".to_string(),
                        properties: HashMap::default(),
                        metadata: hashmap! {},
                    })],
                    next_marker: Some("1".to_string()),
                }
                .to_string(),
            )
            // the second call
            .with_request(|id, _| id == 1)
            .body(
                BlobList {
                    blobs: vec![BlobEntry::Blob(Blob {
                        name: "2.csv".to_string(),
                        properties: HashMap::default(),
                        metadata: hashmap! {},
                    })],
                    next_marker: Some("2".to_string()),
                }
                .to_string(),
            )
            // the third call
            .with_request(|id, _| id == 2)
            .body(
                BlobList {
                    blobs: vec![BlobEntry::Blob(Blob {
                        name: "3.csv".to_string(),
                        properties: HashMap::default(),
                        metadata: hashmap! {},
                    })],
                    next_marker: None,
                }
                .to_string(),
            )
            .into_search_results(file_path)
            .expect("search result should be created")
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&[
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.blob.core.windows.net/somecontainer/1.csv",
                    SyncRecord::empty(),
                ),
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.blob.core.windows.net/somecontainer/2.csv",
                    SyncRecord::empty(),
                ),
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.blob.core.windows.net/somecontainer/3.csv",
                    SyncRecord::empty(),
                ),
            ]);
    }

    #[test]
    fn iters_returns_blobs_with_sub_dirs() {
        if !ONE_PASS_RECURSIVE {
            let file_path = "https://someaccount.blob.core.windows.net/somecontainer/**.csv";
            FakeHttpClient::default()
                // the first call
                .with_request(|_, q| q.uri().to_string().contains("prefix=&"))
                .body(
                    BlobList {
                        blobs: vec![
                            BlobEntry::Blob(Blob {
                                name: "1.csv".to_string(),
                                properties: HashMap::default(),
                                metadata: hashmap! {},
                            }),
                            BlobEntry::BlobPrefix("a/".to_string()),
                            BlobEntry::BlobPrefix("b/".to_string()),
                        ],
                        next_marker: None,
                    }
                    .to_string(),
                )
                .with_request(|_, q| q.uri().to_string().contains("prefix=a"))
                .body(
                    BlobList {
                        blobs: vec![BlobEntry::Blob(Blob {
                            name: "a/2.csv".to_string(),
                            properties: HashMap::default(),
                            metadata: hashmap! {},
                        })],
                        next_marker: None,
                    }
                    .to_string(),
                )
                .with_request(|_, q| q.uri().to_string().contains("prefix=b"))
                .body(
                    BlobList {
                        blobs: vec![BlobEntry::Blob(Blob {
                            name: "b/3.csv".to_string(),
                            properties: HashMap::default(),
                            metadata: hashmap! {},
                        })],
                        next_marker: None,
                    }
                    .to_string(),
                )
                .into_search_results(file_path)
                .expect("search result should be created")
                .iter()
                .should()
                .all_be_ok()
                .which_inner_values()
                .iter()
                .map(|r| r.resource_id().to_owned())
                .sorted()
                .should()
                .equal_iterator(&[
                    "https://someaccount.blob.core.windows.net/somecontainer/1.csv".to_owned(),
                    "https://someaccount.blob.core.windows.net/somecontainer/a/2.csv".to_owned(),
                    "https://someaccount.blob.core.windows.net/somecontainer/b/3.csv".to_owned(),
                ]);
        }
    }

    #[test]
    /// connection failure when creating search results (i.e. stream_handler.find_streams())
    fn new_search_results_http_request_failure_returns_connection_failure() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .error(HttpError::new(true))
            .into_search_results(file_path)
            .should()
            .be_err()
            .with_value(StreamError::ConnectionFailure { source: None });
    }

    #[test]
    /// return NotFound when server returning empty result
    fn new_search_results_with_no_matches_returns_not_found() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/*.csv";

        FakeHttpClient::default()
            .body(
                BlobList {
                    blobs: Vec::default(),
                    next_marker: None,
                }
                .to_string(),
            )
            .into_search_results(file_path)
            .should()
            .be_err()
            .with_value(StreamError::NotFound);
    }

    #[test]
    /// return NotFound when server returning non-empty result, but local filter kills them
    fn new_search_results_not_exactly_match_returns_not_found() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/*.csv";

        FakeHttpClient::default()
            .body(
                BlobList {
                    blobs: vec![BlobEntry::Blob(Blob {
                        name: "somefile.csv.1".to_string(),
                        properties: HashMap::default(),
                        metadata: hashmap! {},
                    })],
                    next_marker: None,
                }
                .to_string(),
            )
            .into_search_results(file_path)
            .should()
            .be_err()
            .with_value(StreamError::NotFound);
    }

    /// with authentication failure, blob service won't tell you permission denied,
    /// instead it will return 404(not found). However if the container is right,
    /// file not found will return an empty blob list. So more likely 404 here means
    /// permission error (another possibility is the container name spells wrong).
    #[test]
    fn new_search_results_with_404_response_return_permission_error() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/*.csv";

        FakeHttpClient::default()
            .status(404)
            .into_search_results(file_path)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn new_search_results_with_403_response_return_permission_error() {
        let file_path = "https://someaccount.blob.core.windows.net/somecontainer/somefile.csv";

        FakeHttpClient::default()
            .status(403)
            .into_search_results(file_path)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }
}
