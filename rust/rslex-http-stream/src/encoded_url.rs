use std::fmt::{Display, Formatter};
use urlencoding::encode;

/// This type provides safety for url-encoded string to prevent accidental double encoding.
/// So far the only way to extract encoded string from it is to place it into "format!" macro.
/// It should never be possible to create a new EncodedUrl from another EncodedUrl and double encode string.
/// If you need to pass encoded string around consider using this type instead of using strings directly.
/// It will give clear visibility if the value is encoded or not.
pub struct EncodedUrl(String);

impl<'a> From<&'a str> for EncodedUrl {
    fn from(path: &'a str) -> Self {
        EncodedUrl(encode(path).replace("%2F", "/"))
    }
}

impl From<&String> for EncodedUrl {
    fn from(path: &String) -> Self {
        EncodedUrl(encode(path).replace("%2F", "/"))
    }
}

impl Display for EncodedUrl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::new_request;
    use fluent_assertions::*;

    #[test]
    fn from_should_url_encode_strings() {
        // creation from &str
        let encoded_url_str = format!("{}", EncodedUrl::from("path/with=%some^chars*to&url?encode123"));
        encoded_url_str
            .as_str()
            .should()
            .be("path/with%3D%25some%5Echars%2Ato%26url%3Fencode123");

        // creation from String
        let encoded_url_str = format!("{}", EncodedUrl::from(&"path/with=%some^chars*to&url?encode123".to_string()));
        encoded_url_str
            .as_str()
            .should()
            .be("path/with%3D%25some%5Echars%2Ato%26url%3Fencode123");
    }

    #[test]
    fn test_encoding_returns_valid_uri() {
        let uri_str = "testfiles/文件夹/数据.csv";

        new_request()
            .uri(format!("https://root/{}", EncodedUrl::from(uri_str)))
            .body(Vec::<u8>::default())
            .should()
            .be_ok();
    }

    #[test]
    fn test_encoding_keeps_slash() {
        let uri_str = "testfiles/文件夹/数据.csv";
        let encoded_url_str = format!("{}", EncodedUrl::from(uri_str));

        encoded_url_str.split('/').should().have_length(3);
    }
}
