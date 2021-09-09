use crate::{SearchContext, SearchResults as BaseSearchResults, Spawn, StreamError, StreamInfo, StreamResult, Wait};
use async_trait::async_trait;
use std::{
    fmt::{Debug, Formatter},
    future::Future,
    pin::Pin,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc,
    },
};
use tokio::sync::Semaphore;
use tracing::Instrument;
use tracing_sensitive::AsSensitive;

/// This trait provides a way to implement your search strategy in [`find_streams`](rslex-core::file_io::StreamHandler),
/// without re-implement all the glob pattern parsing, multiple-thread parallelization, etc.
///
/// ___How to use the trait___
/// - Create a type that implement AsyncSearch;
/// - In your [`StreamHandler`](rslex_core::file_io::StreamHandler) ```find_streams```, create a AyncSearch instance passing the ```search_pattern```
///   (you are supposed to parse the search pattern and return a [`SearchContext`] with the ```root``` function).
/// - Call ```into_search_results```, which will return ['SearchResults'](rslex-core::file_io::SearchResults), you
///   can return directly from your ```find_streams```.
///
/// Behind the scene, the tool will do these for you:
/// - Use ['SearchContext'] to parse from a glob pattern for you;
/// - Managing a task pool to execute Search tasks in parallel;
/// - The search results was pushed to a result queue;
/// - Implement the SearchResults interface, which will pull from the results queue async
///   -- that means your main logic won't be blocked by the search while the searching is still ongoing.
/// - You can control whether you want **One-Pass** search or **In-Parallel** search (see ['SearchContext'])
///   by the ```root()``` function. By default, [`SearchContext::from_str`] will return **In-Parallel** search.
///   Call into_one_pass_search() to convert the SearchContext to **One-Pass** search.
#[async_trait]
pub trait AsyncSearch: Send + Sync + 'static {
    /// Execute one search step represented by ```search_context```. The return value will be:
    /// - Vec<StreamInfo>: all matching files with this search;
    /// - Vec<SearchContext>: new search tasks, either subfolders to search, or with continuation token.
    async fn search(&self, search_context: SearchContext) -> StreamResult<(Vec<StreamInfo>, Vec<SearchContext>)>;

    /// The root SearchContext. Usually you should just call parse
    /// ```
    /// use rslex_core::file_io::StreamResult;
    /// use rslex_http_stream::SearchContext;
    ///
    /// fn root(uri: &str) -> StreamResult<SearchContext> {
    ///     uri.parse()
    /// }
    /// ```
    /// If you want to use One-Pass search, call
    /// ```
    /// use rslex_core::file_io::StreamResult;
    /// use rslex_http_stream::SearchContext;
    ///
    /// fn root(uri: &str) -> StreamResult<SearchContext> {
    ///     Ok(uri.parse::<SearchContext>()?.into_one_pass_search())
    /// }
    /// ```
    fn root(&self) -> StreamResult<SearchContext>;

    /// Specify the max parallel searches. It will throttle the traffic
    /// for folders with big number of sub-folders (therefore trigger large
    /// number of searches. For some storage, if you trigger too many parallel
    /// searches, you will hit the throttling error from server side.
    /// Default is 1000.
    fn max_parallel_searches(&self) -> usize {
        1000
    }

    /// Return a boxed [`SearchResults`](rslex_core::file_io::SearchResults).
    fn into_search_results(self) -> StreamResult<Box<dyn BaseSearchResults>>
    where Self: Sized {
        Ok(box SearchResults::new(Arc::new(self))?)
    }
}

struct SearchResultsIterator {
    receiver: Option<Receiver<StreamResult<Vec<StreamInfo>>>>,
    current_batch: Option<Box<dyn Iterator<Item = StreamInfo>>>,
}

impl SearchResultsIterator {
    fn box_search<T: AsyncSearch>(
        searcher: Arc<T>,
        search_context: SearchContext,
        sender: Sender<StreamResult<Vec<StreamInfo>>>,
        throttle: Arc<Semaphore>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(async move {
            let _permit = throttle.acquire().await;
            match searcher.search(search_context).await {
                Ok((stream_infos, search_contexts)) => {
                    // send result back and create new tasks. However, we will only create new tasks
                    // if the send succeed. A send failure means the receiver has been dropped,
                    // which is a signal search should stopped because errors occurred in other searches
                    let active = if stream_infos.is_empty() {
                        // if stream_info is empty, skip sending
                        // assuming the recriver is still active,
                        // so continue spaw new tasks
                        true
                    } else {
                        sender.send(Ok(stream_infos)).is_ok()
                    };
                    if active {
                        // create new sub tasks
                        for sc in search_contexts {
                            let search_context = format!("{}", sc);
                            let future = SearchResultsIterator::box_search(searcher.clone(), sc, sender.clone(), throttle.clone());
                            future
                                .instrument(tracing::info_span!(parent: &tracing::Span::current(), "search", search_context = %search_context.as_sensitive()))
                                .spawn();
                        }
                    }
                },
                Err(e) => {
                    if let Err(se) = sender.send(Err(e)) {
                        // this is ok. A send failure means search has stopped by another error
                        // it won't hurt we miss this error message
                        tracing::warn!(
                            "[SearchResultsIterator::box_search()] failed to send back error message {:?}",
                            se.as_sensitive_ref()
                        );
                    }
                },
            }
        })
    }

    fn new<T: AsyncSearch>(searcher: Arc<T>, search_contexts: Vec<SearchContext>) -> SearchResultsIterator {
        let (sender, receiver) = channel();

        let throttle = Arc::new(Semaphore::new(searcher.max_parallel_searches()));

        for sc in search_contexts {
            let search_context = format!("{}", sc);
            SearchResultsIterator::box_search(searcher.clone(), sc, sender.clone(), throttle.clone())
                .instrument(tracing::info_span!(parent: &tracing::Span::current(), "search", sc = %search_context.as_sensitive()))
                .spawn();
        }

        SearchResultsIterator {
            receiver: Some(receiver),
            current_batch: None,
        }
    }
}

impl Iterator for SearchResultsIterator {
    type Item = StreamResult<StreamInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        // first check whether the current batch is still valid
        if let Some(mut batch) = self.current_batch.take() {
            // current batch is not null, try to get next
            if let Some(stream_info) = batch.next() {
                // current batch still valid, return the result and set current_batch
                self.current_batch = Some(batch);

                // return the result
                return Some(Ok(stream_info));
            }
        }

        // otherwise, need to retrive the next batch
        if let Some(receiver) = &self.receiver {
            match receiver.recv() {
                Ok(Ok(vec)) => {
                    // successful call with data
                    self.current_batch = Some(box vec.into_iter());
                    // call next to retrieve the next result
                    self.next()
                },
                Ok(Err(e)) => {
                    // connection errors, return the error
                    // dropping the receiver to stop the search
                    // next call will stop the iterator
                    drop(self.receiver.take());

                    Some(Err(e))
                },
                Err(_) => {
                    // end of channel
                    // end the iterator
                    None
                },
            }
        } else {
            // receiver has been dropped, search stoped by previous error
            // stop the iterator
            None
        }
    }
}

/// SearchResults for adls gen1 find stream.
/// It will do search in two stages:
/// - first stage, called with SearchResults::new() will search until we find one match, or finish the searches without match.
///     The result will be used to decide Success or NotFound; This stage will be done in a single
///     thread blocking way.
///     NOTE: search results form this stage will be cached and returned directly with iter() later.
///           Ideally this stage is light. But in some situation, like glob pattern **, we might need to
///           search the whole tree if without luck. In this case the new() will be slow.
/// - second stage, called with SearchResults.iter(), which will continue the search in async manner.
///     NOTE: this stage will run in multi-thread with tokio runtime. It's supposed to be heavy lifting
///           search but in more efficient way.
struct SearchResults<T: AsyncSearch> {
    cached_results: Vec<StreamInfo>,
    cached_contexts: Vec<SearchContext>,
    searcher: Arc<T>,
}

impl<T: AsyncSearch> Debug for SearchResults<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let results: Vec<_> = self.iter().collect();
        write!(f, "{:?}", results)?;
        Ok(())
    }
}

impl<T: AsyncSearch> SearchResults<T> {
    fn find_first_stream(searcher: Arc<T>) -> StreamResult<(Vec<StreamInfo>, Vec<SearchContext>)> {
        async move {
            let mut search_contexts = vec![searcher.root()?];
            let mut search_results = vec![];

            while let Some(sc) = search_contexts.pop() {
                tracing::debug!(
                    "[SearchResults::find_first_stream()] searching with context {}",
                    sc.as_sensitive_ref()
                );

                let s = searcher.clone();
                let (r, c) = s.search(sc).await?;

                search_results.extend(r);
                search_contexts.extend(c);

                // stop as soon as we find something to tell it's NOTFOUND
                if !search_results.is_empty() {
                    break;
                }
            }

            if search_results.is_empty() {
                Err(StreamError::NotFound)
            } else {
                Ok((search_results, search_contexts))
            }
        }
        .wait()?
    }

    pub fn new(searcher: Arc<T>) -> StreamResult<SearchResults<T>> {
        let (cached_results, cached_contexts) = SearchResults::find_first_stream(searcher.clone())?;

        Ok(SearchResults {
            cached_results,
            cached_contexts,
            searcher,
        })
    }
}

impl<T: AsyncSearch> BaseSearchResults for SearchResults<T> {
    fn iter(&self) -> Box<dyn Iterator<Item = StreamResult<StreamInfo>>> {
        let cached_results = self.cached_results.clone().into_iter().map(|r| Ok::<StreamInfo, StreamError>(r));

        box cached_results.chain(SearchResultsIterator::new(self.searcher.clone(), self.cached_contexts.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use itertools::Itertools;

    macro_rules! stream_info {
        ($ri:expr) => {
            StreamInfo::new("SOME_HANDLER", $ri, crate::SyncRecord::empty())
        };
    }

    #[test]
    fn with_single_file_return_iterator() {
        struct Dummy;
        #[async_trait]
        impl AsyncSearch for Dummy {
            async fn search(&self, search_context: SearchContext) -> StreamResult<(Vec<StreamInfo>, Vec<SearchContext>)> {
                search_context.should().equal_string("somefolder/somefile.csv()");

                let stream_infos = vec![stream_info!("prefix://somefolder/somefile.csv")];
                let search_contexts = vec![];

                Ok((stream_infos, search_contexts))
            }

            fn root(&self) -> StreamResult<SearchContext> {
                "somefolder/somefile.csv".parse()
            }
        }

        Dummy
            .into_search_results()
            .should()
            .be_ok()
            .which_value()
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&[stream_info!("prefix://somefolder/somefile.csv")]);
    }

    #[test]
    fn with_authentication_failure_return_permission_denied() {
        struct Dummy;
        #[async_trait]
        impl AsyncSearch for Dummy {
            async fn search(&self, _: SearchContext) -> StreamResult<(Vec<StreamInfo>, Vec<SearchContext>)> {
                Err(StreamError::PermissionDenied)
            }

            fn root(&self) -> StreamResult<SearchContext> {
                "somefolder/somefile.csv".parse()
            }
        }

        Dummy
            .into_search_results()
            .should()
            .be_err()
            .with_value(StreamError::PermissionDenied);
    }

    #[test]
    fn with_patterns_will_trigger_multiple_calls() {
        struct Dummy;
        #[async_trait]
        impl AsyncSearch for Dummy {
            async fn search(&self, search_context: SearchContext) -> StreamResult<(Vec<StreamInfo>, Vec<SearchContext>)> {
                match search_context.to_string().as_str() {
                    "(a*b/somefile.csv)" => Ok((vec![], vec!["abb/somefile.csv".parse().unwrap()])),
                    "abb/somefile.csv()" => Ok((vec![stream_info!("prefix://abb/somefile.csv")], vec![])),
                    _ => unimplemented!(),
                }
            }

            fn root(&self) -> StreamResult<SearchContext> {
                "a*b/somefile.csv".parse()
            }
        }

        Dummy
            .into_search_results()
            .should()
            .be_ok()
            .which_value()
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&[stream_info!("prefix://abb/somefile.csv")]);
    }

    #[test]
    fn new_search_result_stops_at_first_result_iter_run_remaining_searches() {
        struct Dummy;
        #[async_trait]
        impl AsyncSearch for Dummy {
            async fn search(&self, search_context: SearchContext) -> StreamResult<(Vec<StreamInfo>, Vec<SearchContext>)> {
                match search_context.to_string().as_str() {
                    "(**/*.csv)" => Ok((vec![stream_info!("prefix://1.csv")], vec!["a/**/*.csv".parse().unwrap()])),
                    "a/(**/*.csv)" => Ok((vec![stream_info!("prefix://a/2.csv")], vec![])),
                    _ => unimplemented!(),
                }
            }

            fn root(&self) -> StreamResult<SearchContext> {
                "**/*.csv".parse()
            }
        }

        let search_results = Dummy.into_search_results().unwrap();

        // the first run
        search_results
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&[stream_info!("prefix://1.csv"), stream_info!("prefix://a/2.csv")]);

        // the second run
        search_results
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .should()
            .equal_iterator(&[stream_info!("prefix://1.csv"), stream_info!("prefix://a/2.csv")]);
    }

    #[test]
    fn new_search_result_with_connection_failure_returns_error() {
        struct Dummy;
        #[async_trait]
        impl AsyncSearch for Dummy {
            async fn search(&self, search_context: SearchContext) -> StreamResult<(Vec<StreamInfo>, Vec<SearchContext>)> {
                match search_context.to_string().as_str() {
                    "(**/*.csv)" => Ok((
                        vec![stream_info!("prefix://1.csv")],
                        vec!["a/**/*.csv".parse().unwrap(), "b/**/*.csv".parse().unwrap()],
                    )),
                    "a/(**/*.csv)" => Err(StreamError::PermissionDenied),
                    "b/(**/*.csv)" => Ok((vec![stream_info!("prefix://a/2.csv")], vec![])),
                    _ => unimplemented!(),
                }
            }

            fn root(&self) -> StreamResult<SearchContext> {
                "**/*.csv".parse()
            }
        }

        Dummy
            .into_search_results()
            .should()
            .be_ok()
            .which_value()
            .iter()
            .should()
            .contain(Err(StreamError::PermissionDenied));
    }

    #[test]
    fn new_search_result_with_three_searches() {
        struct Dummy;
        #[async_trait]
        impl AsyncSearch for Dummy {
            async fn search(&self, search_context: SearchContext) -> StreamResult<(Vec<StreamInfo>, Vec<SearchContext>)> {
                match search_context.to_string().as_str() {
                    "(**/*.csv)" => Ok((vec![stream_info!("prefix://1.csv")], vec!["a/**/*.csv".parse().unwrap()])),
                    "a/(**/*.csv)" => Ok((vec![stream_info!("prefix://a/2.csv")], vec!["a/b/**/*.csv".parse().unwrap()])),
                    "a/b/(**/*.csv)" => Ok((vec![stream_info!("prefix://a/b/3.csv")], vec![])),
                    _ => unimplemented!(),
                }
            }

            fn root(&self) -> StreamResult<SearchContext> {
                "**/*.csv".parse()
            }
        }

        Dummy
            .into_search_results()
            .should()
            .be_ok()
            .which_value()
            .iter()
            .should()
            .all_be_ok()
            .which_inner_values()
            .iter()
            .map(|r| r.resource_id().to_string())
            .sorted()
            .should()
            .equal_iterator(&[
                "prefix://1.csv".to_string(),
                "prefix://a/2.csv".to_string(),
                "prefix://a/b/3.csv".to_string(),
            ]);
    }
}
