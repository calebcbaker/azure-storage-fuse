use async_trait::async_trait;
use chrono::Utc;
use hmac::{Hmac, Mac, NewMac};
use http::HeaderValue;
use itertools::Itertools;
use rslex_core::file_io::{StreamError, StreamResult};
use rslex_http_stream::{ApplyCredential, AsyncResponse, AuthenticatedRequest, HttpError, Request, RetryCondition};
use serde_json::json;
use sha2::Sha256;
use std::fmt::{Debug, Error, Formatter};

type HmacSha256 = Hmac<Sha256>;

const HEADER_X_MS_VERSION: &str = "x-ms-version";
const HEADER_X_MS_DATE: &str = "x-ms-date";
const HEADER_RANGE: &str = "Range";
const HEADER_AUTHORIZATION: &str = "Authorization";

#[derive(Debug)]
struct Signature<'s> {
    http_verb: &'s str,
    content_encoding: &'s str,
    content_language: &'s str,
    content_length: &'s str,
    content_md5: &'s str,
    content_type: &'s str,
    date: &'s str,
    if_modified_since: &'s str,
    if_match: &'s str,
    if_none_match: &'s str,
    if_unmodified_since: &'s str,
    range: &'s str,
    canonicalized_headers: &'s str,
    canonicalized_resource: &'s str,
}

impl<'s> ToString for Signature<'s> {
    fn to_string(&self) -> String {
        format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
            self.http_verb,
            self.content_encoding,
            self.content_language,
            self.content_length,
            self.content_md5,
            self.content_type,
            self.date,
            self.if_modified_since,
            self.if_match,
            self.if_none_match,
            self.if_unmodified_since,
            self.range,
            self.canonicalized_headers,
            self.canonicalized_resource
        )
    }
}

impl<'s> Signature<'s> {
    fn sign(self, key: &[u8]) -> String {
        let mut mac = HmacSha256::new_varkey(key).expect("[account_key::Signature::sign] unable to get HmacSha256");

        mac.update(self.to_string().as_bytes());

        let result = mac.finalize();
        base64::encode(result.into_bytes())
    }
}

pub(super) struct AccountKey {
    key: Vec<u8>,
}

impl AccountKey {
    pub fn new(key: impl Into<Vec<u8>>) -> AccountKey {
        AccountKey { key: key.into() }
    }
}

#[async_trait]
impl RetryCondition for AccountKey {
    type Request = AuthenticatedRequest;
    type Response = Result<AsyncResponse, HttpError>;

    async fn should_retry(&self, _: &Self::Request, response_result: Self::Response, _: u32) -> (bool, Self::Response) {
        (false, response_result)
    }
}

impl ApplyCredential for AccountKey {
    fn apply(&self, request: Request) -> StreamResult<Request> {
        let (mut parts, body) = request.into_parts();

        // add x-ms-date header if not exist
        if !parts.headers.contains_key(HEADER_X_MS_DATE) {
            let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
            parts.headers.insert(HEADER_X_MS_DATE, date.parse().unwrap());
        };

        if parts.headers.contains_key(HEADER_X_MS_VERSION) {
            // verify the version is higher than 2015-02-21
            // earlier version is not compatible with the signature string
            let version = parts.headers[HEADER_X_MS_VERSION].to_str().unwrap();
            if version < "2005-02-21" {
                tracing::error!(
                    "[AccountKey::apply()] x-ms-version({}) earlier than 2005-02-21 used different signing string format and is not supported",
                    version
                );
                panic!(
                    "x-ms-version({}) earlier than 2005-02-21 used different signing string format and is not supported",
                    version
                );
            }
        } else {
            // use the latest version: https://docs.microsoft.com/en-us/rest/api/storageservices/versioning-for-the-azure-storage-services#version-2019-07-07
            parts.headers.insert(HEADER_X_MS_VERSION, "2019-07-07".parse().unwrap());
        }

        let range = parts.headers.get(HEADER_RANGE).map_or("", |h| h.to_str().unwrap());

        // with azure blob, uri will be account.blob.core.windows.net
        // extract the account from uri
        let account = parts.uri.authority().unwrap().as_str().split(".").next().unwrap();

        let canonicalized_headers = parts
            .headers
            .iter()
            .filter(|h| h.0.as_str().starts_with("x-ms-"))
            .map(|h| format!("{}:{}", h.0, h.1.to_str().unwrap()))
            .sorted()
            .join("\n");

        let url_decoded_query = match parts.uri.query() {
            Some(q) => {
                Some(urlencoding::decode(q).map_err(|e| StreamError::Unknown(format!("failed to url decode query string {:?}", e), None))?)
            },
            None => None,
        };

        let canonicalized_resource = format!(
            "/{}{}{}",
            account,
            parts.uri.path(),
            url_decoded_query
                .as_ref()
                .map_or::<Vec<&str>, _>(vec![], |q| q.split("&").collect())
                .iter()
                .sorted()
                .map(|q| format!("\n{}", q.replacen("=", ":", 1)))
                .join("")
        );

        let headers = &parts.headers;

        let dhv = HeaderValue::from_static("");

        let content_length = match parts.headers.get("content-length").unwrap_or(&dhv).to_str().unwrap() {
            "0" => "",
            other => other,
        };

        let signature = Signature {
            http_verb: parts.method.as_str(),
            content_encoding: headers.get("content-encoding").unwrap_or(&dhv).to_str().unwrap(),
            content_language: headers.get("content-language").unwrap_or(&dhv).to_str().unwrap(),
            content_length,
            content_md5: headers.get("content-md5").unwrap_or(&dhv).to_str().unwrap(),
            content_type: headers.get("content-type").unwrap_or(&dhv).to_str().unwrap(),
            date: headers.get("date").unwrap_or(&dhv).to_str().unwrap(),
            if_modified_since: headers.get("if-modified-since").unwrap_or(&dhv).to_str().unwrap(),
            if_match: headers.get("if-match").unwrap_or(&dhv).to_str().unwrap(),
            if_none_match: headers.get("if-none-match").unwrap_or(&dhv).to_str().unwrap(),
            if_unmodified_since: headers.get("if-unmodified-since").unwrap_or(&dhv).to_str().unwrap(),
            range,
            canonicalized_headers: &canonicalized_headers,
            canonicalized_resource: &canonicalized_resource,
        };

        let signature = signature.sign(&self.key);

        parts.headers.insert(
            HEADER_AUTHORIZATION,
            format!("SharedKey {}:{}", account, signature).parse().unwrap(),
        );

        Ok(Request::from_parts(parts, body))
    }
}

impl Debug for AccountKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), Error> {
        write!(
            f,
            "{}",
            json!({"AccountKey": {
            "key": std::str::from_utf8(&self.key).unwrap()}})
            .to_string()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use http::{Method, StatusCode};
    use rslex_http_stream::{new_request, RequestWithCredential};
    use std::sync::Arc;
    use tokio_test::block_on;

    const HEADER_CONTENT_LENGTH: &str = "Content-Length";
    const HEADER_X_MS_BLOB_TYPE: &str = "x-ms-blob-type";

    #[test]
    fn apply_account_key_with_list_blobs() {
        let key = b"this&is&the&account&key";
        let credential = AccountKey { key: key.to_vec() };
        let date = "Fri, 26 Jun 2015 23:39:12 GMT";

        let request = new_request()
            .uri("https://someaccount.blob.core.windows.net/somecontainer/?comp=list&a=b&c=d&e===")
            .header(HEADER_X_MS_DATE, date)
            .header(HEADER_X_MS_VERSION, "2019-09-19")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(Arc::new(credential))
            .into_request()
            .unwrap();

        request.headers()[HEADER_AUTHORIZATION].to_ref().should().equal(&&format!(
            "SharedKey someaccount:{}",
            Signature {
                http_verb: "GET",
                content_encoding: "",
                content_language: "",
                content_length: "",
                content_md5: "",
                content_type: "",
                date: "",
                if_modified_since: "",
                if_match: "",
                if_none_match: "",
                if_unmodified_since: "",
                range: "",
                canonicalized_headers: "x-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\nx-ms-version:2019-09-19",
                canonicalized_resource: "/someaccount/somecontainer/\na:b\nc:d\ncomp:list\ne:==",
            }
            .sign(key)
        ));
    }

    #[test]
    fn apply_account_key_with_get_blob_properties() {
        let key = b"this&is&the&account&key";
        let credential = AccountKey { key: key.to_vec() };

        let date = "Fri, 26 Jun 2015 23:39:12 GMT";
        let request = new_request()
            .method(Method::HEAD)
            .uri("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv")
            .header(HEADER_X_MS_DATE, date)
            .header(HEADER_X_MS_VERSION, "2019-09-19")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(Arc::new(credential))
            .into_request()
            .unwrap();

        request.headers()[HEADER_AUTHORIZATION].to_ref().should().equal(&&format!(
            "SharedKey someaccount:{}",
            Signature {
                http_verb: "HEAD",
                content_encoding: "",
                content_language: "",
                content_length: "",
                content_md5: "",
                content_type: "",
                date: "",
                if_modified_since: "",
                if_match: "",
                if_none_match: "",
                if_unmodified_since: "",
                range: "",
                canonicalized_headers: "x-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\nx-ms-version:2019-09-19",
                canonicalized_resource: "/someaccount/somecontainer/somefile.csv",
            }
            .sign(key)
        ));
    }

    #[test]
    fn apply_account_key_with_read_blob_with_range() {
        let key = b"this&is&the&account&key";
        let credential = AccountKey { key: key.to_vec() };

        let date = "Fri, 26 Jun 2015 23:39:12 GMT";
        let request = new_request()
            .method(Method::GET)
            .uri("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv")
            .header(HEADER_X_MS_DATE, date)
            .header(HEADER_X_MS_VERSION, "2019-09-19")
            .header(HEADER_RANGE, "bytes=100-250")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(Arc::new(credential))
            .into_request()
            .unwrap();

        request.headers()[HEADER_AUTHORIZATION].to_ref().should().equal(&&format!(
            "SharedKey someaccount:{}",
            Signature {
                http_verb: "GET",
                content_encoding: "",
                content_language: "",
                content_length: "",
                content_md5: "",
                content_type: "",
                date: "",
                if_modified_since: "",
                if_match: "",
                if_none_match: "",
                if_unmodified_since: "",
                range: "bytes=100-250",
                canonicalized_headers: "x-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\nx-ms-version:2019-09-19",
                canonicalized_resource: "/someaccount/somecontainer/somefile.csv",
            }
            .sign(key)
        ));
    }

    #[test]
    fn apply_account_key_with_list_blobs_handles_url_encoding() {
        let key = b"this&is&the&account&key";
        let credential = AccountKey { key: key.to_vec() };

        let date = "Fri, 26 Jun 2015 23:39:12 GMT";
        let request = new_request()
            .uri("https://someaccount.blob.core.windows.net/somecontainer/?comp=list&delimiter=%2F&c=d")
            .header(HEADER_X_MS_DATE, date)
            .header(HEADER_X_MS_VERSION, "2019-09-19")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(Arc::new(credential))
            .into_request()
            .unwrap();

        request.headers()[HEADER_AUTHORIZATION].to_ref().should().equal(&&format!(
            "SharedKey someaccount:{}",
            Signature {
                http_verb: "GET",
                content_encoding: "",
                content_language: "",
                content_length: "",
                content_md5: "",
                content_type: "",
                date: "",
                if_modified_since: "",
                if_match: "",
                if_none_match: "",
                if_unmodified_since: "",
                range: "",
                canonicalized_headers: "x-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\nx-ms-version:2019-09-19",
                canonicalized_resource: "/someaccount/somecontainer/\nc:d\ncomp:list\ndelimiter:/",
            }
            .sign(key)
        ));
    }

    #[test]
    fn apply_account_key_with_put_blob_handles_zero_content_length() {
        let key = b"this&is&the&account&key";
        let credential = AccountKey { key: key.to_vec() };
        let body = Vec::<u8>::default();

        let date = "Fri, 26 Jun 2015 23:39:12 GMT";
        let request = new_request()
            .method(Method::PUT)
            .uri("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv")
            .header(HEADER_X_MS_DATE, date)
            .header(HEADER_X_MS_VERSION, "2019-09-19")
            .header(HEADER_X_MS_BLOB_TYPE, "BlockBlob")
            .header(HEADER_CONTENT_LENGTH, body.len())
            .body(body)
            .unwrap()
            .with_credential(Arc::new(credential))
            .into_request()
            .unwrap();

        request.headers()[HEADER_AUTHORIZATION].to_ref().should().equal(&&format!(
            "SharedKey someaccount:{}",
            Signature {
                http_verb: "PUT",
                content_encoding: "",
                content_language: "",
                content_length: "",
                content_md5: "",
                content_type: "",
                date: "",
                if_modified_since: "",
                if_match: "",
                if_none_match: "",
                if_unmodified_since: "",
                range: "",
                canonicalized_headers: "x-ms-blob-type:BlockBlob\nx-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\nx-ms-version:2019-09-19",
                canonicalized_resource: "/someaccount/somecontainer/somefile.csv",
            }
            .sign(key)
        ));
    }

    #[test]
    fn apply_account_key_with_put_block_handles_content_length() {
        let key = b"this&is&the&account&key";
        let credential = AccountKey { key: key.to_vec() };
        let body = vec![1u8, 2, 3];

        let date = "Fri, 26 Jun 2015 23:39:12 GMT";
        let request = new_request()
            .method(Method::PUT)
            .uri("https://someaccount.blob.core.windows.net/somecontainer/somefile.csv?comp=block&blockid=MA==")
            .header(HEADER_X_MS_DATE, date)
            .header(HEADER_X_MS_VERSION, "2019-09-19")
            .header(HEADER_CONTENT_LENGTH, body.len())
            .body(body)
            .unwrap()
            .with_credential(Arc::new(credential))
            .into_request()
            .unwrap();

        request.headers()[HEADER_AUTHORIZATION].to_ref().should().equal(&&format!(
            "SharedKey someaccount:{}",
            Signature {
                http_verb: "PUT",
                content_encoding: "",
                content_language: "",
                content_length: "3",
                content_md5: "",
                content_type: "",
                date: "",
                if_modified_since: "",
                if_match: "",
                if_none_match: "",
                if_unmodified_since: "",
                range: "",
                canonicalized_headers: "x-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\nx-ms-version:2019-09-19",
                canonicalized_resource: "/someaccount/somecontainer/somefile.csv\nblockid:MA==\ncomp:block",
            }
            .sign(key)
        ));
    }

    #[test]
    fn does_not_retry_on_error() {
        let key = b"this&is&the&account&key";
        let credential = Arc::new(AccountKey { key: key.to_vec() });

        let request = new_request()
            .uri("https://someaccount.blob.core.windows.net/somecontainer/?comp=list&a=b&c=d&e===")
            .body(Vec::<u8>::default())
            .unwrap()
            .with_credential(credential.clone());

        let mut response = AsyncResponse::default();
        *response.status_mut() = StatusCode::UNAUTHORIZED;

        let (should_retry, _) = block_on(credential.should_retry(&request, Ok(response), 0));
        should_retry.should().be_false();
    }
}
