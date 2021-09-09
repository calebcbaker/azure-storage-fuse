use regex::{escape, Regex};

/// An extension trait for String types, deal with glob pattern.
pub trait PathExt {
    /// Convert a string to a ['Regex`].
    /// Glob pattern has different sync than regular expression.
    /// This function builds the conversion.
    ///
    /// ## Example
    /// ```
    /// use regex::Regex;
    /// use rslex_core::file_io::PathExt;
    ///
    /// assert!("/a/b*c**.?sv".to_regex().is_match("/a/bdddcd/ff.tsv"));
    /// ```
    fn to_regex(&self) -> Regex;

    /// Retrieve the file extension from path.
    fn extension(&self) -> Option<&str>;

    /// Retrieve the prefix part which does not contain any wildcard match in glob pattern.
    /// Useful in case you want to determine the prefix of a path that remote storage could understand.
    ///
    /// ## Example
    /// ```
    /// use rslex_core::file_io::PathExt;
    ///
    /// assert_eq!("/a/b*c".prefix(), "/a/b");
    /// ```
    fn prefix(&self) -> &str;
}

impl<T: AsRef<str>> PathExt for T {
    fn to_regex(&self) -> Regex {
        let pattern = format!(
            "^{}$",
            escape(self.as_ref())
                .replace("\\*\\*/", ".*")
                .replace("\\*\\*", ".*")
                .replace("\\*", "[^/]*")
                .replace("\\?", "[^/]")
        );
        Regex::new(&pattern).unwrap()
    }

    fn extension(&self) -> Option<&str> {
        let path = self.as_ref();
        path.rfind('.').map(|idx| path.get((idx + 1)..).unwrap())
    }

    fn prefix(&self) -> &str {
        let path = self.as_ref();
        match path.find(|c: char| c == '*' || c == '?') {
            Some(p) => &path[..p],
            _ => &path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;

    #[test]
    fn to_regex_with_literal() {
        let regex = "abc".to_regex();

        regex.should().equal_string("^abc$");
    }

    #[test]
    fn to_regex_with_single_star() {
        let regex = "a*b".to_regex();

        regex.should().equal_string("^a[^/]*b$");
    }

    #[test]
    fn to_regex_with_double_star() {
        let regex = "a**b".to_regex();

        regex.should().equal_string("^a.*b$");
    }

    #[test]
    fn to_regex_with_double_star_in_path() {
        let regex = "a/**/b/**/c*.csv".to_regex();

        regex.should().equal_string("^a/.*b/.*c[^/]*\\.csv$");
    }

    #[test]
    fn to_regex_with_question_mark() {
        let regex = "a?b".to_regex();

        regex.should().equal_string("^a[^/]b$");
    }

    #[test]
    fn to_regex_with_escape() {
        let regex = "a.b".to_regex();

        regex.should().equal_string("^a\\.b$");
    }

    #[test]
    fn prefix_with_literal() {
        "abc".prefix().should().equal_string("abc");
    }

    #[test]
    fn prefix_with_single_star() {
        "a*c".prefix().should().equal_string("a");
    }

    #[test]
    fn prefix_with_double_star() {
        "a**c".prefix().should().equal_string("a");
    }

    #[test]
    fn prefix_with_question_mark() {
        "a?c".prefix().should().equal_string("a");
    }
}
