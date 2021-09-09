use crate::{PathExt, StreamError, StreamResult};
use itertools::Itertools;
use regex::Regex;
use std::{
    borrow::Cow,
    fmt::{Display, Error, Formatter},
    iter,
    str::FromStr,
};

/// split the glob pattern to multiple segments
#[derive(Clone, PartialEq, Debug)]
enum Segment {
    /// fixed match, not wild card. could we multiple level
    /// e.g. "a/b/c"
    Literal(String),

    /// one level with wild card, e.g., "a*b.c"
    SingleLevelPattern(String),

    /// multi level with wild card, e.g., "a*b/c/*.csv"
    /// create this Segment when the searcher support recursive search
    /// with narrow but deep directory tree, this might be a better option,
    /// as you get all the file list in one batch. But with wide tree, this
    /// will load more data, and the loading cannot be parallized. So usually not a good idea.
    /// by default, SearchContext will parse into SingleLevelPattern.
    /// it could be converted to MultiLevelPattern explicitly.
    OnePassRecursive(String),

    /// matching multiple levels, i.e., with **, like "a**bc"
    MultiLevelPattern(String),
}

impl Display for Segment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            Segment::Literal(str) | Segment::SingleLevelPattern(str) | Segment::OnePassRecursive(str) | Segment::MultiLevelPattern(str) => {
                write!(f, "{}", str)
            },
        }
    }
}

/// Retrieving a large directory tree from a remote storage system could be one of the most
/// time-consuming and error-prone step. A good strategy to pull information in small batches,
/// either serially or in parallel is critical for the success and experience. For small folders
/// with only tens of files, it doesn't really matter how you pull the information. For big
/// directory with millions of files, like "/a/**.csv" there are two different approaches:
/// - **One-Pass**: Trigger one search which will traverse the whole directory including sub folders, but
///   returns the result in multiple batches. The remote storage system usually return a continuation-token
///   with each batch, for you to trigger next batch. For some storage system pulling the whole
///   directory tree (instead of making multiple calls once for one sub-folder) has a performance gain.
///   But the downside is you cannot parallel pull the
///   batches as they must be serially. Also, sometimes you don't need the whole directory, like "/a/**/b/c.txt".
///   Sending the whole directory over is a waste.
/// - **In-Parallel**: Do a tree traverse from client side, retrieve one folder (without subfolder) at one time. If a single
///   folder is still huge, they can still be split with continuation token. But peer folders could be retrieved
///   in parallel. And as you can early-stop a folder you end up retrieve less information. E.g., "/a/*b/**/c.txt".
///   You can first pull the root folder "/a/", and only expand subfolders matching "*b".
///
/// The SearchContext here is a way to represent one step in the search process. It will be used in [`AsyncSearch`](crate::AsyncSearch)
/// as input, representing one request sending to remote storage. The ['AsyncSearch'] implementation should take
/// a SearchContext, translate into a HTTP Request, send to remote storage, retrieve the results, returning in
/// (1) next search steps like sub folder, in Vec<SearchContext>; and (2) matching files in current folder.
///
/// To create a SearchContext, parse from the glob pattern. See also [`StreamHandler::find_streams`](rslex-core::file_io::StreamHandler).
/// - **`?`** -- match any single character except `/`
/// - **`*`** -- match any number of any characters except `/`
/// - **`**`** -- match any number of any characters including `/`, so this will search across folder levels
/// ## Example
/// ```
/// use rslex_http_stream::SearchContext;
///
/// let search_context: SearchContext = "/a/b*c/**.txt".parse().unwrap();
///
/// println!("{}", search_context);
/// ```
#[derive(Clone, Debug)]
pub struct SearchContext {
    prefix: String,
    segments: Vec<Vec<Segment>>,
    continuation: Option<String>,

    pat_file: Vec<Regex>,
    pat_dir: Vec<(Regex, Vec<Segment>)>,
}

impl PartialEq for SearchContext {
    fn eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix && self.segments == other.segments && self.continuation == other.continuation
    }
}

impl Display for SearchContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), Error> {
        write!(
            f,
            "{}({}){}",
            self.prefix,
            self.segments.iter().map(|g| g.iter().map(|s| s.to_string()).join("/")).join("|"),
            if let Some(ref s) = self.continuation {
                format!("@{}", s)
            } else {
                "".to_string()
            }
        )
    }
}

/// Parse the glob pattern to SearchContext. See also [`find_streams`](rslex-core::file_io::StreamHandler).
/// - **`?`** -- match any single character except `/`
/// - **`*`** -- match any number of any characters except `/`
/// - **`**`** -- match any number of any characters including `/`, so this will search across folder levels
/// ## Example
/// ```
/// use rslex_http_stream::SearchContext;
///
/// let search_context: SearchContext = "/a/b*c/**.txt".parse().unwrap();
///
/// println!("{}", search_context);
/// ```
impl FromStr for SearchContext {
    type Err = StreamError;

    fn from_str(path: &str) -> StreamResult<Self> {
        let mut prefix = vec![];
        let mut segments = vec![];

        // we treat somefolder/ as somefolder/**, i.e., the whole tree
        let path = if path.ends_with("/") || path == "" {
            // for folder, append ** to match multiple levels
            format!("{}**", path)
        } else {
            path.to_owned()
        };

        for s in path.split("/") {
            if s.contains("**") {
                segments.push(Segment::MultiLevelPattern(s.to_string()));
            } else if s.contains("*") {
                // segment with single star
                segments.push(Segment::SingleLevelPattern(s.to_string()));
            } else {
                // fixed segment
                match segments.pop() {
                    None => {
                        // this is the first segment, merge to prefix
                        prefix.push(s.to_string());
                    },
                    Some(Segment::Literal(ref last)) => {
                        // previous segment is fixed, merge to it
                        segments.push(Segment::Literal(format!("{}/{}", last, s)));
                    },
                    Some(seg) => {
                        segments.push(seg);
                        segments.push(Segment::Literal(s.to_string()));
                    },
                }
            }
        }

        if !segments.is_empty() {
            // add a separator between prefix and segments if any
            prefix.push("".to_owned());
        }

        Ok(SearchContext::new(prefix.as_slice().join("/"), vec![segments]))
    }
}

impl SearchContext {
    fn new<S: Into<String>>(prefix: S, segments: Vec<Vec<Segment>>) -> Self {
        let prefix = prefix.into();

        // construct map_file
        let mut pat_file: Vec<Regex> = vec![];
        let mut pat_dir: Vec<(Regex, Vec<Segment>)> = vec![];
        for g in segments.iter() {
            match (g.get(0), g.get(1)) {
                // no search pattern. In this case, prefix is already a path of file or dir
                (None, _) => {
                    // when prefix is already file path, matches file with exact path
                    pat_file.push(prefix.to_regex());

                    // when prefix is dir path, matches subdir and files under the dir recursively
                    pat_file.push((prefix.clone() + "/**").to_regex());
                    pat_dir.push((
                        (prefix.clone() + "/*").to_regex(),
                        vec![Segment::MultiLevelPattern("**".to_owned())],
                    ));
                },

                // fixed, will match whole
                (Some(Segment::Literal(fixed)), None) => {
                    pat_file.push((prefix.clone() + fixed).to_regex());

                    if let Some(pos) = fixed.find("/") {
                        pat_dir.push((
                            (prefix.clone() + &fixed[..pos]).to_regex(),
                            vec![Segment::Literal(fixed[pos + 1..].to_owned())],
                        ));
                    }
                },

                (Some(Segment::Literal(fixed)), Some(_)) => {
                    if let Some(pos) = fixed.find("/") {
                        pat_dir.push((
                            (prefix.clone() + &fixed[..pos]).to_regex(),
                            iter::once(Segment::Literal(fixed[pos + 1..].to_owned()))
                                .chain(g[1..].to_vec())
                                .collect_vec(),
                        ));
                    } else {
                        pat_dir.push(((prefix.clone() + fixed).to_regex(), g[1..].to_vec()));
                    }
                },

                // a*c/.., match a*c, keep the remaining matching patterns
                (Some(Segment::SingleLevelPattern(pat)), Some(_)) => {
                    pat_dir.push(((prefix.clone() + pat).to_regex(), g[1..].to_vec()));
                },

                // a*c, should match any a*c
                (Some(Segment::SingleLevelPattern(pat)), None) => {
                    pat_file.push((prefix.clone() + pat).to_regex());
                },

                // a**b**c/ will match (1)a*/**b**c/; (2) a*b*/**c/; (3) a*b*c/, etc.
                // a**b**c will match (1)a*/**b**c; (2) a*b*/**c;  etc.
                (Some(Segment::MultiLevelPattern(pat)), seg) => {
                    for (pos, _) in pat.match_indices("**") {
                        pat_dir.push((
                            (prefix.clone() + &pat[..pos + 1]).to_regex(), // match a*, note we steal one * from the pattern to make it a*
                            iter::once(Segment::MultiLevelPattern(pat[pos..].to_owned())) // **b**c
                                .chain(g[1..].to_vec())
                                .collect_vec(), // keep the same matching pattern, including **
                        ));
                    }

                    if seg.is_some() {
                        // match whole pattern
                        pat_dir.push(((prefix.clone() + pat).to_regex(), g[1..].to_vec()));
                    } else {
                        pat_file.push((prefix.clone() + pat).to_regex());
                    }
                },

                // with one pass recursive, this pattern might contain /, in which case it won't match any file anyway
                (Some(Segment::OnePassRecursive(pat)), _) => {
                    pat_file.push((prefix.clone() + pat).to_regex());
                },
            };
        }

        SearchContext {
            prefix,
            segments,
            continuation: None,
            pat_file,
            pat_dir,
        }
    }

    /// The SearchContext carries a result filter. E.g., ```/a/*.txt``` will be represented
    /// as ```root="/a/"``` (search folder) and ```pattern="*.txt"```.
    /// In [`AsyncSearch`](crate::AsyncSearch), call this function on each returned file to decide
    /// whether return or not.
    pub fn filter_file<S: AsRef<str>>(&self, path: S) -> bool {
        let path = path.as_ref();
        self.pat_file.iter().any(|reg| reg.is_match(path))
    }

    /// The SearchContext carries a filter to decide which file is a match.
    /// In [`AsyncSearch`](crate::AsyncSearch), call this function on each returned sub-folder to decide
    /// continue searching or not.
    pub fn map_dir<S: AsRef<str>>(&self, path: S) -> Option<SearchContext> {
        let dir = path.as_ref();
        let segments = self
            .pat_dir
            .iter()
            .filter_map(|pat| if pat.0.is_match(dir) { Some(pat.1.clone()) } else { None })
            .collect_vec();

        if segments.is_empty() {
            return None;
        }

        if segments.len() == 1 {
            if let Some(Segment::Literal(fixed)) = segments[0].get(0) {
                return Some(SearchContext::new(format!("{}/{}", dir, fixed), vec![segments[0][1..].to_vec()]));
            }
        }

        return Some(SearchContext::new(format!("{}/", dir), segments));
    }

    /// Convert to **One-Pass** search, which will request the [`AsyncSearch`](crate::AsyncSearch) to retrieve the full directory tree,
    /// and apply the filter pattern afterwards. Not all remote storage support one-pass search.
    pub fn into_one_pass_search(self) -> SearchContext {
        SearchContext::new(
            self.prefix,
            self.segments
                .into_iter()
                .map(|g| match (g.get(0), g.get(1)) {
                    (_, Some(_)) | (Some(Segment::MultiLevelPattern(_)), None) => {
                        vec![Segment::OnePassRecursive(g.iter().map(|s| s.to_string()).join("/"))]
                    },
                    (_, None) => g,
                })
                .collect_vec(),
        )
    }

    /// Whether the SearchContext is One-Pass search.
    pub fn is_one_pass(&self) -> bool {
        self.segments.iter().any(|g| {
            if let Some(Segment::OnePassRecursive(_)) = g.get(0) {
                true
            } else {
                false
            }
        })
    }

    /// whether the search pattern is a single file
    pub fn single_stream(&self) -> Option<&str> {
        debug_assert!(!self.segments.is_empty(), "segments should never be empty");
        if self.segments.len() == 1 && self.segments[0].is_empty() {
            Some(self.prefix.as_str())
        } else {
            None
        }
    }

    /// The root folder to search from. Set align_to_dir=true to get the exact folder. Some
    /// storage system could do a prefix-match even not at folder boundary, like return all
    /// files starting with ```"/a/b"``` could return ```"/a/bcd.txt"```. In this case, set align_to_dir=false,
    /// to retrieve the maximum prefix (in the above case ```"/a/b"``` instead of ```"/a/"```.
    pub fn prefix(&self, align_to_dir: bool) -> Cow<str> {
        if align_to_dir || self.segments.len() != 1 {
            Cow::Borrowed(&self.prefix)
        } else {
            match self.segments[0].get(0) {
                Some(Segment::SingleLevelPattern(p)) | Some(Segment::OnePassRecursive(p)) | Some(Segment::MultiLevelPattern(p)) => {
                    Cow::Owned(format!("{}{}", self.prefix, p.prefix()))
                },
                _ => Cow::Borrowed(&self.prefix),
            }
        }
    }

    /// Retrieve the continuation token. Usually returned by previous search.
    pub fn continuation(&self) -> Option<&str> {
        self.continuation.as_ref().map(|s| s.as_str())
    }

    /// Create a new SearchContext by setting the continuation token. Used by [`AsyncSearch`](crate::AsyncSearch) implementation,
    /// when retrieving result from remote containing a continuation token. In this case, [`AsyncSearch`](crate::AsyncSearch)
    /// should return a new SearchContext by attaching the continuation token, to indicate next search.
    pub fn with_continuation<S: Into<String>>(mut self, token: S) -> Self {
        self.continuation = Some(token.into());
        self
    }

    /// Converts single path search context into multi-level path with a single folder expansion
    /// After checking that path is not a single blob search context should be converted to folder
    pub fn into_folder(self, one_pass: bool) -> SearchContext {
        if self.single_stream().is_some() && self.segments.len() == 1 {
            let segments = self.segments.get(0);
            return match segments {
                Some(segments) => {
                    let mut segments = segments.clone();
                    segments.push(Segment::MultiLevelPattern("**".to_string()));
                    let prefix = if self.prefix.ends_with('/') {
                        self.prefix
                    } else {
                        format!("{}/", self.prefix)
                    };
                    let dir_search_context = SearchContext::new(prefix, vec![segments]);
                    if one_pass {
                        dir_search_context.into_one_pass_search()
                    } else {
                        dir_search_context
                    }
                },
                None => self,
            };
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;

    #[test]
    fn parse_search_context_with_single_file() {
        let search_context: SearchContext = "a/b".parse().unwrap();

        search_context.prefix.should().equal_string("a/b");
        search_context.segments.should().be_single().which_value().should().be_empty();
    }

    #[test]
    fn parse_search_context_with_root() {
        let search_context: SearchContext = "".parse().unwrap();

        search_context.prefix.should().equal_string("");
        search_context
            .segments
            .should()
            .be_single()
            .which_value()
            .should()
            .equal_iterator(&[Segment::MultiLevelPattern("**".to_string())]);
    }

    #[test]
    fn parse_search_context_with_single_dir() {
        let search_context: SearchContext = "a/b/".parse().unwrap();

        search_context.prefix.should().equal_string("a/b/");
        search_context
            .segments
            .should()
            .be_single()
            .which_value()
            .should()
            .equal_iterator(&[Segment::MultiLevelPattern("**".to_string())]);
    }

    #[test]
    fn parse_search_context_with_single_star() {
        let search_context: SearchContext = "a/b*c".parse().unwrap();

        search_context.prefix.should().equal_string("a/");
        search_context
            .segments
            .should()
            .be_single()
            .which_value()
            .should()
            .equal_iterator(&[Segment::SingleLevelPattern("b*c".to_owned())]);
    }

    #[test]
    fn parse_search_context_with_single_star_and_fixed() {
        let search_context: SearchContext = "a/b*c/d".parse().unwrap();

        search_context.prefix.should().equal_string("a/");
        search_context
            .segments
            .should()
            .be_single()
            .which_value()
            .should()
            .equal_iterator(&[Segment::SingleLevelPattern("b*c".to_owned()), Segment::Literal("d".to_owned())]);
    }

    #[test]
    fn parse_search_context_with_dot_will_be_escaped() {
        let search_context: SearchContext = "a/b*.d".parse().unwrap();

        search_context.prefix.should().equal_string("a/");
        search_context
            .segments
            .should()
            .be_single()
            .which_value()
            .should()
            .equal_iterator(&[Segment::SingleLevelPattern("b*.d".to_owned())]);
    }

    #[test]
    fn parse_search_context_with_double_star() {
        let search_context: SearchContext = "a/b**c".parse().unwrap();

        search_context.prefix.should().equal_string("a/");
        search_context
            .segments
            .should()
            .be_single()
            .which_value()
            .should()
            .equal_iterator(&[Segment::MultiLevelPattern("b**c".to_owned())]);
    }

    #[test]
    fn parse_search_context_with_multiple_fixed_will_be_merged() {
        let search_context: SearchContext = "a/b*/c/d".parse().unwrap();

        search_context.prefix.should().equal_string("a/");
        search_context
            .segments
            .should()
            .be_single()
            .which_value()
            .should()
            .equal_iterator(&[Segment::SingleLevelPattern("b*".to_owned()), Segment::Literal("c/d".to_owned())]);
    }

    #[test]
    fn filter_file_with_single_file() {
        let search_context = SearchContext::new("somefolder/somefile.csv", vec![vec![]]);

        search_context.filter_file("somefolder/somefile.csv").should().be_true();
        search_context.filter_file("somefolder/somefile.csvanything").should().be_false();
    }

    #[test]
    fn map_dir_with_single_file() {
        let search_context = SearchContext::new("somefolder/somefile.csv", vec![vec![]]);

        search_context.map_dir("somefolder/somefile.csvanydir").should().be_none();
    }

    #[test]
    fn filter_file_with_single_dir_path() {
        let search_context = SearchContext::new("somefolder", vec![vec![]]);

        search_context.filter_file("somefolder/acb").should().be_true();
        search_context.filter_file("somefolder/acb/def").should().be_true();
        search_context.filter_file("otherfolder/acb").should().be_false();
    }

    #[test]
    fn map_dir_with_single_dir_path() {
        let search_context = SearchContext::new("somefolder", vec![vec![]]);

        search_context
            .map_dir("somefolder/subfoder")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/subfoder/",
                vec![vec![Segment::MultiLevelPattern("**".to_owned())]],
            ));
        search_context.map_dir("otherfolder/subfolder").should().be_none();
    }

    #[test]
    fn filter_file_with_single_level_pattern() {
        let search_context = SearchContext::new("somefolder/", vec![vec![Segment::SingleLevelPattern("a*b".to_owned())]]);

        search_context.filter_file("somefolder/acb").should().be_true();
        search_context.filter_file("somefolder/notmatch").should().be_false();
    }

    #[test]
    fn filter_file_with_two_patterns_will_match_any() {
        let search_context = SearchContext::new(
            "somefolder/",
            vec![
                vec![Segment::SingleLevelPattern("a*b".to_owned())],
                vec![Segment::Literal("defg".to_owned())],
            ],
        );

        search_context.filter_file("somefolder/acb").should().be_true();
        search_context.filter_file("somefolder/defg").should().be_true();
        search_context.filter_file("somefolder/notmatch").should().be_false();
    }

    #[test]
    fn map_dir_with_single_level_pattern() {
        let search_context = SearchContext::new("somefolder/", vec![vec![Segment::SingleLevelPattern("a*b".to_owned())]]);

        search_context.map_dir("somefolder/anydir").should().be_none();
    }

    #[test]
    fn map_dir_with_literal() {
        let search_context = SearchContext::new("somefolder/", vec![vec![Segment::Literal("a/b".to_owned())]]);

        search_context
            .map_dir("somefolder/a")
            .should()
            .be_some()
            .with_value(SearchContext::new("somefolder/a/b", vec![vec![]]));
    }

    #[test]
    fn map_dir_with_multiple_pattern_will_match_any() {
        let search_context = SearchContext::new(
            "somefolder/",
            vec![
                vec![Segment::Literal("a/b".to_owned())],
                vec![Segment::Literal("a/c".to_owned())],
                vec![Segment::Literal("b/d".to_owned())],
            ],
        );

        search_context
            .map_dir("somefolder/a")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/a/",
                vec![vec![Segment::Literal("b".to_owned())], vec![Segment::Literal("c".to_owned())]],
            ));
        search_context
            .map_dir("somefolder/b")
            .should()
            .be_some()
            .with_value(SearchContext::new("somefolder/b/d", vec![vec![]]));
        search_context.map_dir("somefolder/c").should().be_none();
    }

    #[test]
    fn filter_file_with_two_single_level_patterns() {
        let search_context = SearchContext::new(
            "somefolder/",
            vec![vec![
                Segment::SingleLevelPattern("a*b".to_owned()),
                Segment::SingleLevelPattern("*c".to_owned()),
            ]],
        );

        search_context.filter_file("somefolder/acb").should().be_false();
        search_context.filter_file("somefolder/anyfile").should().be_false();
    }

    #[test]
    fn filter_file_with_literal_and_some() {
        let search_context = SearchContext::new(
            "somefolder/",
            vec![vec![
                Segment::Literal("ab".to_owned()),
                Segment::SingleLevelPattern("*c".to_owned()),
            ]],
        );

        search_context.filter_file("somefolder/acb").should().be_false();
        search_context.filter_file("somefolder/anyfile").should().be_false();
    }

    #[test]
    fn map_dir_with_two_single_level_patterns() {
        let search_context = SearchContext::new(
            "somefolder/",
            vec![vec![
                Segment::SingleLevelPattern("a*b".to_owned()),
                Segment::SingleLevelPattern("*c".to_owned()),
            ]],
        );

        search_context
            .map_dir("somefolder/afb")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/afb/",
                vec![vec![Segment::SingleLevelPattern("*c".to_owned())]],
            ));
        search_context.map_dir("somefolder/others").should().be_none();
    }

    #[test]
    fn map_dir_with_multi_level_literal_and_some() {
        let search_context = SearchContext::new(
            "somefolder/",
            vec![
                vec![Segment::Literal("a/b".to_owned()), Segment::SingleLevelPattern("*c".to_owned())],
                vec![Segment::Literal("a".to_owned()), Segment::SingleLevelPattern("*c".to_owned())],
            ],
        );

        search_context
            .map_dir("somefolder/a")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/a/",
                vec![
                    vec![Segment::Literal("b".to_owned()), Segment::SingleLevelPattern("*c".to_owned())],
                    vec![Segment::SingleLevelPattern("*c".to_owned())],
                ],
            ));
        search_context.map_dir("somefolder/others").should().be_none();
    }

    #[test]
    fn filter_file_with_single_level_pattern_and_literal() {
        let search_context = SearchContext::new(
            "somefolder/",
            vec![vec![
                Segment::SingleLevelPattern("a*b".to_owned()),
                Segment::Literal("c/d/e.csv".to_owned()),
            ]],
        );

        search_context.filter_file("somefolder/anyfile").should().be_false();
    }

    #[test]
    fn map_dir_with_single_level_pattern_and_literal() {
        let search_context = SearchContext::new(
            "somefolder/",
            vec![vec![
                Segment::SingleLevelPattern("a*b".to_owned()),
                Segment::Literal("c/d/e.csv".to_owned()),
            ]],
        );

        search_context
            .map_dir("somefolder/afb")
            .should()
            .be_some()
            .with_value(SearchContext::new("somefolder/afb/c/d/e.csv", vec![vec![]]));
        search_context.map_dir("somefolder/others").should().be_none();
    }

    #[test]
    fn filter_file_with_multi_level_pattern() {
        let search_context = SearchContext::new("somefolder/", vec![vec![Segment::MultiLevelPattern("**c.csv".to_owned())]]);

        search_context.filter_file("somefolder/c.csv").should().be_true();
        search_context.filter_file("somefolder/abc.csv").should().be_true();
        search_context.filter_file("somefolder/anyotherfile").should().be_false();
    }

    #[test]
    fn map_dir_with_multi_level_pattern_starting_with_double_star_and_some_other_segments() {
        let search_context = SearchContext::new(
            "somefolder/",
            vec![vec![
                Segment::MultiLevelPattern("**c".to_owned()),
                Segment::Literal("txt.csv".to_owned()),
            ]],
        );

        // with dir not matching pattern
        search_context
            .map_dir("somefolder/anydir")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/anydir/",
                vec![vec![
                    Segment::MultiLevelPattern("**c".to_owned()),
                    Segment::Literal("txt.csv".to_owned()),
                ]],
            ));
        // with dir matching pattern
        search_context
            .map_dir("somefolder/abc")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/abc/",
                vec![
                    vec![Segment::MultiLevelPattern("**c".to_owned()), Segment::Literal("txt.csv".to_owned())],
                    vec![Segment::Literal("txt.csv".to_owned())],
                ],
            ));
    }

    #[test]
    fn map_dir_with_multi_level_pattern_not_starting_with_double_star_and_some_other_segments() {
        let search_context = SearchContext::new(
            "somefolder/",
            vec![vec![
                Segment::MultiLevelPattern("a**b".to_owned()),
                Segment::SingleLevelPattern("*.csv".to_owned()),
            ]],
        );

        search_context.map_dir("somefolder/xxx").should().be_none();
        search_context
            .map_dir("somefolder/axx")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/axx/",
                vec![vec![
                    Segment::MultiLevelPattern("**b".to_owned()),
                    Segment::SingleLevelPattern("*.csv".to_owned()),
                ]],
            ));
        search_context
            .map_dir("somefolder/ab")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/ab/",
                vec![
                    vec![
                        Segment::MultiLevelPattern("**b".to_owned()),
                        Segment::SingleLevelPattern("*.csv".to_owned()),
                    ],
                    vec![Segment::SingleLevelPattern("*.csv".to_owned())],
                ],
            ));
    }

    #[test]
    fn map_dir_with_multi_level_pattern_with_multi_double_star_and_some_other_segments() {
        let search_context = SearchContext::new(
            "somefolder/",
            vec![vec![
                Segment::MultiLevelPattern("a**b**c".to_owned()),
                Segment::SingleLevelPattern("*.csv".to_owned()),
            ]],
        );

        search_context.map_dir("somefolder/xxx").should().be_none();
        search_context
            .map_dir("somefolder/axx")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/axx/",
                vec![vec![
                    Segment::MultiLevelPattern("**b**c".to_owned()),
                    Segment::SingleLevelPattern("*.csv".to_owned()),
                ]],
            ));
        search_context
            .map_dir("somefolder/ab")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/ab/",
                vec![
                    vec![
                        Segment::MultiLevelPattern("**b**c".to_owned()),
                        Segment::SingleLevelPattern("*.csv".to_owned()),
                    ],
                    vec![
                        Segment::MultiLevelPattern("**c".to_owned()),
                        Segment::SingleLevelPattern("*.csv".to_owned()),
                    ],
                ],
            ));
        search_context
            .map_dir("somefolder/abc")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/abc/",
                vec![
                    vec![
                        Segment::MultiLevelPattern("**b**c".to_owned()),
                        Segment::SingleLevelPattern("*.csv".to_owned()),
                    ],
                    vec![
                        Segment::MultiLevelPattern("**c".to_owned()),
                        Segment::SingleLevelPattern("*.csv".to_owned()),
                    ],
                    vec![Segment::SingleLevelPattern("*.csv".to_owned())],
                ],
            ));
    }

    #[test]
    fn map_dir_with_multi_level_pattern_with_multi_double_star() {
        let search_context = SearchContext::new("somefolder/", vec![vec![Segment::MultiLevelPattern("a**b**c".to_owned())]]);

        search_context.map_dir("somefolder/xxx").should().be_none();
        search_context
            .map_dir("somefolder/axx")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/axx/",
                vec![vec![Segment::MultiLevelPattern("**b**c".to_owned())]],
            ));
        search_context
            .map_dir("somefolder/ab")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/ab/",
                vec![
                    vec![Segment::MultiLevelPattern("**b**c".to_owned())],
                    vec![Segment::MultiLevelPattern("**c".to_owned())],
                ],
            ));
        // note here matching whole abc is not an option
        search_context
            .map_dir("somefolder/abc")
            .should()
            .be_some()
            .with_value(SearchContext::new(
                "somefolder/abc/",
                vec![
                    vec![Segment::MultiLevelPattern("**b**c".to_owned())],
                    vec![Segment::MultiLevelPattern("**c".to_owned())],
                ],
            ));
    }

    #[test]
    fn into_one_pass_combine_all_segments() {
        let search_context: SearchContext = "prefix/a*b/cd**/efg.hij".parse().unwrap();
        let search_context = search_context.into_one_pass_search();

        search_context.filter_file("prefix/ab/cd/suv/efg.hij").should().be_true();
        search_context.filter_file("prefix/someotherfile").should().be_false();
        search_context.map_dir("prefix/anything").should().be_none();

        search_context
            .should()
            .be(SearchContext::new(
                "prefix/",
                vec![vec![Segment::OnePassRecursive("a*b/cd**/efg.hij".to_owned())]],
            ))
            .and()
            .is_one_pass()
            .should()
            .be_true();
    }

    #[test]
    fn into_one_pass_with_single_level_pattern_return_itself() {
        let search_context: SearchContext = "prefix/a*b".parse().unwrap();
        let search_context = search_context.into_one_pass_search();

        search_context.is_one_pass().should().be_false();
        search_context.should().be(SearchContext::new(
            "prefix/",
            vec![vec![Segment::SingleLevelPattern("a*b".to_owned())]],
        ));
    }

    #[test]
    fn into_one_pass_without_segments_return_itself() {
        let search_context: SearchContext = "prefix/abc".parse().unwrap();
        let search_context = search_context.into_one_pass_search();

        search_context.is_one_pass().should().be_false();
        search_context.should().be(SearchContext::new("prefix/abc", vec![vec![]]));
    }

    #[test]
    fn into_one_pass_with_multi_level_pattern_convert_to_recursive() {
        let search_context: SearchContext = "prefix/a**b".parse().unwrap();
        let search_context = search_context.into_one_pass_search();

        search_context.is_one_pass().should().be_true();
        search_context.should().be(SearchContext::new(
            "prefix/",
            vec![vec![Segment::OnePassRecursive("a**b".to_owned())]],
        ));
    }

    #[test]
    fn into_one_pass_with_multiple_patterns_will_convert_respectively() {
        let search_context = SearchContext::new(
            "prefix/",
            vec![
                vec![
                    Segment::SingleLevelPattern("a*b".to_owned()),
                    Segment::Literal("c/d/e.txt".to_owned()),
                ],
                vec![Segment::SingleLevelPattern("h*j".to_owned())],
                vec![Segment::MultiLevelPattern("u**v".to_owned())],
            ],
        )
        .into_one_pass_search();

        search_context.is_one_pass().should().be_true();
        search_context.should().be(SearchContext::new(
            "prefix/",
            vec![
                vec![Segment::OnePassRecursive("a*b/c/d/e.txt".to_owned())],
                vec![Segment::SingleLevelPattern("h*j".to_owned())],
                vec![Segment::OnePassRecursive("u**v".to_owned())],
            ],
        ));
    }

    #[test]
    fn prefix_align_to_dir_return_prefix_only() {
        let search_context: SearchContext = "prefix/a**b".parse().unwrap();

        search_context.prefix(true).should().equal_string("prefix/");
    }

    #[test]
    fn prefix_not_align_to_dir_and_single_level_pattern() {
        let search_context: SearchContext = "prefix/a*b".parse().unwrap();

        search_context.prefix(false).should().equal_string("prefix/a");
    }

    #[test]
    fn prefix_not_align_to_dir_and_multi_level_pattern() {
        let search_context: SearchContext = "prefix/a**b".parse().unwrap();

        search_context.prefix(false).should().equal_string("prefix/a");
    }

    #[test]
    fn prefix_not_align_to_dir_and_one_pass_recursive() {
        let search_context = "prefix/a**b/c/d".parse::<SearchContext>().unwrap().into_one_pass_search();

        search_context.prefix(false).should().equal_string("prefix/a");
    }

    #[test]
    fn prefix_not_align_to_dir_and_multiple_patterns() {
        let search_context = SearchContext::new(
            "prefix/",
            vec![
                vec![
                    Segment::SingleLevelPattern("a*b".to_owned()),
                    Segment::Literal("c/d/e.txt".to_owned()),
                ],
                vec![Segment::SingleLevelPattern("h*j".to_owned())],
                vec![Segment::MultiLevelPattern("u**v".to_owned())],
            ],
        );

        search_context.prefix(false).should().equal_string("prefix/");
    }
}
