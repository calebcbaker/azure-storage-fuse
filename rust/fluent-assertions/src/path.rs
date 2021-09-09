use crate::{
    utils::{AndConstraint, AssertionFailure, DebugMessage},
    Assertions,
};
use std::{borrow::Borrow, path::Path};

/// Assertions for types implementing [`Borrow`](std::borrow::Borrow)<[`Path`](std::path::Path)>.
///
/// ### Examples
/// ```rust, no_run
/// use fluent_assertions::*;
/// use std::path::Path;
///
/// // test subject path exists
/// Path::new("/tmp/file").should().exist();
///
/// // test path not exists
/// Path::new("/tmp/file").should().not_exist();
///
/// // test path points to a file
/// Path::new("/tmp/file").should().be_a_file();
///
/// // test path points to a directory
/// Path::new("/tmp/dir/").should().be_a_directory();
///
/// // test path has a valid filename
/// Path::new("/tmp/file").should().have_file_name(&"file");
/// ```
pub trait PathAssertions<T> {
    fn exist(self) -> AndConstraint<T>;
    fn not_exist(self) -> AndConstraint<T>;
    fn be_a_file(self) -> AndConstraint<T>;
    fn be_a_directory(self) -> AndConstraint<T>;
    fn have_file_name<E: AsRef<str>>(self, expected_file_name: E) -> AndConstraint<T>;
}

impl<T: Borrow<Path>> PathAssertions<T> for Assertions<T> {
    fn exist(self) -> AndConstraint<T> {
        if !self.subject().borrow().exists() {
            AssertionFailure::new(format!(
                "should().exist() expecting path of <{}> to exist but failed",
                self.subject().debug_message()
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn not_exist(self) -> AndConstraint<T> {
        if self.subject().borrow().exists() {
            AssertionFailure::new(format!(
                "should().exist() expecting path of <{}> not to exist but failed",
                self.subject().debug_message()
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn be_a_file(self) -> AndConstraint<T> {
        if !self.subject().borrow().is_file() {
            AssertionFailure::new(format!(
                "should().be_a_directory() expecting path of <{}> to be a file but failed",
                self.subject().debug_message()
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn be_a_directory(self) -> AndConstraint<T> {
        if !self.subject().borrow().is_dir() {
            AssertionFailure::new(format!(
                "should().be_a_directory() expecting path of <{}> to be a directory but failed",
                self.subject().debug_message()
            ))
            .fail();
        }

        AndConstraint::new(self.into_inner())
    }

    fn have_file_name<E: AsRef<str>>(self, expected_file_name: E) -> AndConstraint<T> {
        let subject_file_name = match self.subject().borrow().file_name() {
            Some(os_string) => match os_string.to_str() {
                Some(val) => val,
                None => {
                    AssertionFailure::new("should().have_file_name() expectation failed")
                        .expecting(format!("subject to have file name {}", expected_file_name.as_ref()))
                        .but("subject filename is an invalid UTF-8 file name")
                        .subject(os_string)
                        .fail();
                    unreachable!();
                },
            },
            None => {
                AssertionFailure::new("should().have_file_name() expectation failed")
                    .expecting(format!("subject to have file name {}", expected_file_name.as_ref()))
                    .but("subject filename is a non-resolvable path")
                    .subject(self.subject())
                    .fail();
                unreachable!();
            },
        };

        if !subject_file_name.eq(expected_file_name.as_ref()) {
            AssertionFailure::new("should().have_file_name() expectation failed")
                .expected(expected_file_name)
                .but_was(subject_file_name)
                .subject(self.subject())
                .fail();
        }

        AndConstraint::new(self.into_inner())
    }
}

#[cfg(test)]
mod tests {

    use crate::*;

    use std::path::{Path, PathBuf};

    static MANIFEST_PATH: &'static str = env!("CARGO_MANIFEST_DIR");

    #[test]
    pub fn should_not_panic_if_path_exists() {
        Path::new(MANIFEST_PATH).should().exist();
    }

    #[test]
    pub fn should_panic_if_path_does_not_exist() {
        let failing_path = MANIFEST_PATH.to_string() + "/does-not-exist";

        should_fail_with_message!(
            Path::new(&failing_path).should().exist(),
            "expecting path of <\"*/does-not-exist\"> to exist but failed"
        );
    }

    #[test]
    pub fn should_not_panic_if_path_represents_a_directory() {
        Path::new(MANIFEST_PATH).should().be_a_directory();
    }

    #[test]
    pub fn should_not_panic_if_path_does_not_exist_when_expected() {
        let failing_path = MANIFEST_PATH.to_string() + "/does-not-exist";
        Path::new(&failing_path).should().not_exist();
    }

    #[test]
    pub fn should_panic_if_path_exists_when_not_expected() {
        should_fail_with_message!(
            Path::new(MANIFEST_PATH).should().not_exist(),
            "expecting path of * not to exist but failed"
        );
    }

    #[test]
    pub fn should_panic_if_path_does_not_represent_a_directory() {
        let path = MANIFEST_PATH.to_string() + "/Cargo.toml";

        should_fail_with_message!(
            Path::new(&path).should().be_a_directory(),
            "expecting path of * to be a directory but failed"
        );
    }

    #[test]
    pub fn should_not_panic_if_path_represents_a_file() {
        let path = MANIFEST_PATH.to_string() + "/Cargo.toml";
        Path::new(&path).should().be_a_file();
    }

    #[test]
    pub fn should_panic_if_path_does_not_represent_a_file() {
        should_fail_with_message!(
            Path::new(&MANIFEST_PATH).should().be_a_file(),
            "expecting path of * to be a file but failed"
        );
    }

    #[test]
    pub fn has_file_name_should_allow_multiple_borrow_forms_for_path() {
        let path = MANIFEST_PATH.to_string() + "/Cargo.toml";
        Path::new(&path)
            .should()
            .be_a_file()
            .and()
            .should()
            .have_file_name("Cargo.toml")
            .and()
            .should()
            .have_file_name(&mut "Cargo.toml")
            .and()
            .should()
            .have_file_name(&"Cargo.toml");
    }

    #[test]
    pub fn should_panic_if_path_does_not_have_correct_file_name() {
        let path = MANIFEST_PATH.to_string() + "/Cargo.toml";

        should_fail_with_message!(
            Path::new(&path).should().have_file_name(&"pom.xml"),
            "expected: \"pom.xml\"*but was: \"Cargo.toml\""
        );
    }

    #[test]
    pub fn should_panic_if_path_does_not_have_a_file_name() {
        let path = MANIFEST_PATH.to_string() + "/..";

        should_fail_with_message!(
            Path::new(&path).should().have_file_name(&"pom.xml"),
            "subject filename is a non-resolvable path"
        );
    }

    #[test]
    pub fn should_not_panic_if_pathbuf_exists() {
        PathBuf::from(MANIFEST_PATH).should().exist();
    }
}
