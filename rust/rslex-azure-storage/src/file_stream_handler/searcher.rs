use super::{file_dto::DirectoriesAndFiles, request_builder::RequestBuilder, HANDLER_TYPE};
use async_trait::async_trait;
use rslex_core::{
    file_io::{StreamError, StreamResult},
    StreamInfo, SyncRecord,
};
use rslex_http_stream::{AsyncResponseExt, AsyncSearch, HttpClient, SearchContext};
use std::sync::Arc;
use tracing_sensitive::AsSensitive;

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

        let res = self
            .http_client
            .clone()
            .request_async(request.into())
            .await?
            .success()
            .await
            .map_err(|e| {
                if e.status_code == 404 {
                    // when authentication fail, service will return 404 - NotFound. But if it's actually NotFound, it will return an empty list.
                    // so we map 404 to permission failure
                    tracing::warn!(
                        "[Searcher::search()] file service return 404 - not found, but this could mostly be authentication failure"
                    );
                    StreamError::PermissionDenied
                } else {
                    e.into()
                }
            })?;

        let directories_and_files: DirectoriesAndFiles = res.into_string().await?.parse()?;

        let directory_path = if directories_and_files.directory_path.is_empty() {
            "".to_owned()
        } else {
            format!("{}/", &directories_and_files.directory_path)
        };

        if let Some(ref continuation) = directories_and_files.next_marker {
            // if there is continuation token, create a new search context here
            search_contexts.push(search_context.clone().with_continuation(continuation));
        }

        stream_infos.extend(directories_and_files.files.iter().filter_map(|file| {
            let filename = format!("{}{}", directory_path, file.name);
            if search_context.filter_file(&filename) {
                // the files dto actually contains property information about content-length
                // but it doesn't contain created time or modified time
                // we could put content-length in session_properties here, but that will skip
                // HEAD operation to retrieve the full stream_properties (though gain some perf win).
                // Given Azure File has very small usage, we will skip the optimization here.
                // By not putting any session properties, it will force a HEAD operation to retrieve
                // stream_properties.
                Some(StreamInfo::new(
                    HANDLER_TYPE,
                    self.request_builder.path_to_uri(&filename),
                    self.arguments.clone(),
                ))
            } else {
                None
            }
        }));

        search_contexts.extend(
            directories_and_files
                .directories
                .iter()
                .filter_map(|dir| search_context.map_dir(format!("{}{}", directory_path, dir.name.trim_end_matches("/")))),
        );

        tracing::info!(
            "[Searcher::search()] scan {} {} files and {} dirs, matching {} files, {} more searches",
            sc.as_sensitive_ref(),
            directories_and_files.files.len(),
            directories_and_files.directories.len(),
            stream_infos.len(),
            search_contexts.len()
        );

        Ok((stream_infos, search_contexts))
    }

    fn root(&self) -> StreamResult<SearchContext> {
        self.request_builder.path().parse()
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
    use crate::file_stream_handler::file_dto::{Directory, File};
    use fluent_assertions::*;
    use itertools::Itertools;
    use rslex_core::file_io::SearchResults;
    use rslex_http_stream::{FakeHttpClient, HttpError};
    use std::{collections::HashMap, sync::Arc};

    trait IntoFileSearchResults {
        fn into_search_results(self, uri: &str) -> StreamResult<Box<dyn SearchResults>>;
    }

    impl IntoFileSearchResults for FakeHttpClient {
        fn into_search_results(self, uri: &str) -> StreamResult<Box<dyn SearchResults>> {
            let searcher = Searcher::new(RequestBuilder::new(uri, Arc::new(()))?, Arc::new(self), SyncRecord::empty());

            searcher.into_search_results()
        }
    }

    #[test]
    /// when all the result could be retrieved in one call, it will be retrieved and cached when creating search result
    fn iters_returns_files_with_one_batch() {
        let uri = "https://someaccount.file.core.windows.net/someshare/somefile.csv";

        FakeHttpClient::default()
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: vec![
                        File {
                            name: "somefile.csv".to_string(),
                            properties: HashMap::default(),
                        },
                        File {
                            name: "somefile.csv.1".to_string(),
                            properties: HashMap::default(),
                        },
                    ],
                    directories: vec![],
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
                "https://someaccount.file.core.windows.net/someshare/somefile.csv",
                SyncRecord::empty(),
            )]);
    }

    #[test]
    /// if the search results take two batches, the first batch will be retrieved and cached when creating search result,
    /// while the second batch will be called when the search result is retrieved
    /// when there are multiple iterators, the first batch will be called only once
    /// while the second batch will be called multiple times, each with one iteration
    fn iters_returns_files_with_two_batchs() {
        let file_path = "https://someaccount.file.core.windows.net/someshare/**.csv";

        let results = FakeHttpClient::default()
            // the first call
            .with_request(|id, _| id == 0)
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: vec![File {
                        name: "1.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![],
                    next_marker: Some("1".to_string()),
                }
                .to_string(),
            )
            .assert_num_requests(1)
            // the second & third call
            .with_request(|id, _| id > 0)
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: vec![File {
                        name: "2.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![],
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
                "https://someaccount.file.core.windows.net/someshare/1.csv",
                SyncRecord::empty(),
            ),
            StreamInfo::new(
                HANDLER_TYPE,
                "https://someaccount.file.core.windows.net/someshare/2.csv",
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
    fn iters_returns_files_with_two_batchs_and_first_batch_filtered_out() {
        let file_path = "https://someaccount.file.core.windows.net/someshare/**.csv";

        FakeHttpClient::default()
            // the first call
            .with_request(|id, _| id == 0)
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: vec![File {
                        name: "1.notcsv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![],
                    next_marker: Some("1".to_string()),
                }
                .to_string(),
            )
            // the second call
            .with_request(|id, _| id == 1)
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: vec![File {
                        name: "2.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![],
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
                "https://someaccount.file.core.windows.net/someshare/2.csv",
                SyncRecord::empty(),
            )]);
    }

    #[test]
    /// connection in second batch will return stream_err
    fn iters_when_connection_failure_in_continuation_return_err() {
        let file_path = "https://someaccount.file.core.windows.net/someshare/**.csv";

        FakeHttpClient::default()
            // the first call
            .with_request(|id, _| id == 0)
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: vec![File {
                        name: "1.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![],
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
                    "https://someaccount.file.core.windows.net/someshare/1.csv",
                    SyncRecord::empty(),
                )),
                Err(StreamError::ConnectionFailure { source: None }),
            ]);
    }

    #[test]
    /// with three batches, the first batch is called when creating search_result
    /// while the next two batches happens when calling the iteration
    fn iters_returns_files_with_three_batchs() {
        let file_path = "https://someaccount.file.core.windows.net/someshare/**.csv";

        FakeHttpClient::default()
            // the first call
            .with_request(|id, _| id == 0)
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: vec![File {
                        name: "1.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![],
                    next_marker: Some("1".to_string()),
                }
                .to_string(),
            )
            // the second call
            .with_request(|id, _| id == 1)
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: vec![File {
                        name: "2.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![],
                    next_marker: Some("2".to_string()),
                }
                .to_string(),
            )
            // the third call
            .with_request(|id, _| id == 2)
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: vec![File {
                        name: "3.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![],
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
                    "https://someaccount.file.core.windows.net/someshare/1.csv",
                    SyncRecord::empty(),
                ),
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.file.core.windows.net/someshare/2.csv",
                    SyncRecord::empty(),
                ),
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.file.core.windows.net/someshare/3.csv",
                    SyncRecord::empty(),
                ),
            ]);
    }

    #[test]
    fn iters_returns_files_with_three_level_dirs() {
        let file_path = "https://someaccount.file.core.windows.net/someshare/**.csv";

        FakeHttpClient::default()
            // the first call
            .with_request(|id, _| id == 0)
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: vec![File {
                        name: "1.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![Directory { name: "a".to_string() }],
                    next_marker: None,
                }
                .to_string(),
            )
            // the second call
            .with_request(|id, _| id == 1)
            .body(
                DirectoriesAndFiles {
                    directory_path: "a".to_owned(),
                    files: vec![File {
                        name: "2.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![Directory { name: "b".to_string() }],
                    next_marker: None,
                }
                .to_string(),
            )
            // the third call
            .with_request(|id, _| id == 2)
            .body(
                DirectoriesAndFiles {
                    directory_path: "a/b".to_owned(),
                    files: vec![File {
                        name: "3.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![],
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
                    "https://someaccount.file.core.windows.net/someshare/1.csv",
                    SyncRecord::empty(),
                ),
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.file.core.windows.net/someshare/a/2.csv",
                    SyncRecord::empty(),
                ),
                StreamInfo::new(
                    HANDLER_TYPE,
                    "https://someaccount.file.core.windows.net/someshare/a/b/3.csv",
                    SyncRecord::empty(),
                ),
            ]);
    }

    #[test]
    fn iters_returns_files_with_sub_dirs() {
        let file_path = "https://someaccount.file.core.windows.net/someshare/**.csv";

        FakeHttpClient::default()
            // the first call
            .with_request(|_, q| q.uri().to_string().contains("someshare/?"))
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: vec![File {
                        name: "1.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![Directory { name: "a".to_string() }, Directory { name: "b".to_string() }],
                    next_marker: None,
                }
                .to_string(),
            )
            .with_request(|_, q| q.uri().to_string().contains("someshare/a"))
            .body(
                DirectoriesAndFiles {
                    directory_path: "a".to_owned(),
                    files: vec![File {
                        name: "2.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![],
                    next_marker: None,
                }
                .to_string(),
            )
            .with_request(|_, q| q.uri().to_string().contains("someshare/b"))
            .body(
                DirectoriesAndFiles {
                    directory_path: "b".to_owned(),
                    files: vec![File {
                        name: "3.csv".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![],
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
                "https://someaccount.file.core.windows.net/someshare/1.csv".to_owned(),
                "https://someaccount.file.core.windows.net/someshare/a/2.csv".to_owned(),
                "https://someaccount.file.core.windows.net/someshare/b/3.csv".to_owned(),
            ]);
    }

    #[test]
    /// connection failure when creating search results (i.e. stream_handler.find_streams())
    fn new_search_results_http_request_failure_returns_connection_failure() {
        let file_path = "https://someaccount.file.core.windows.net/someshare/somefile.csv";

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
        let file_path = "https://someaccount.file.core.windows.net/someshare/somefile.csv";

        FakeHttpClient::default()
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: Vec::default(),
                    directories: vec![],
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
        let file_path = "https://someaccount.file.core.windows.net/someshare/somefile.csv";

        FakeHttpClient::default()
            .body(
                DirectoriesAndFiles {
                    directory_path: "".to_owned(),
                    files: vec![File {
                        name: "somefile.csv.1".to_string(),
                        properties: HashMap::default(),
                    }],
                    directories: vec![],
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
        let file_path = "https://someaccount.file.core.windows.net/someshare/somefile.csv";

        FakeHttpClient::default()
            .status(404)
            .into_search_results(file_path)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn new_search_results_with_403_response_return_permission_error() {
        let file_path = "https://someaccount.file.core.windows.net/someshare/somefile.csv";

        FakeHttpClient::default()
            .status(403)
            .into_search_results(file_path)
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }
}
