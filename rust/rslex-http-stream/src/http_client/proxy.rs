use crate::http_client::HttpClientCreationError;
use headers::Authorization;
use http::Uri;
use hyper::client::HttpConnector;
use hyper_proxy::{Intercept, Proxy, ProxyConnector};
use hyper_rustls::HttpsConnector;
use rslex_core::ExternalError;
use std::{io::Error, sync::Arc};
use thiserror::Error;
use url::Url;

#[derive(Error, Debug, Clone)]
pub enum ProxySettingsError {
    #[error("Proxy environment variable {0} is not a valid UTF-8 encoded string.")]
    ValueEncoding(String),
    #[error("Proxy environment variable {env_variable} is not a valid url.")]
    ValueFormat { env_variable: String, error: ExternalError },
    #[error("Proxy url should be an absolute http url with defined schema and host")]
    RelativeUrl,
    #[error("Unknown error \"{0:?}\" when trying to create proxy")]
    Unknown(ExternalError),
}

impl From<ProxySettingsError> for HttpClientCreationError {
    fn from(e: ProxySettingsError) -> Self {
        HttpClientCreationError::ProxySettings(e)
    }
}

impl From<std::io::Error> for ProxySettingsError {
    fn from(error: Error) -> Self {
        ProxySettingsError::Unknown(Arc::new(error))
    }
}

pub enum ProxyInterceptSettings {
    /// Use proxy only for http traffic
    Http(Url),
    /// Use proxy only for https traffic
    Https(Url),
    /// Use proxy for both http and https
    Both(Url, Url),
}

const HTTP_PROXY_ENV_VARIABLE: &'static str = "http_proxy";
const HTTPS_PROXY_ENV_VARIABLE: &'static str = "https_proxy";

fn get_proxy_env_setting(env_variable_name: &str) -> Option<Result<Url, ProxySettingsError>> {
    match std::env::var(env_variable_name) {
        Ok(value) => {
            let value = value.trim();
            if value.is_empty() {
                None
            } else {
                match Url::parse(value.trim()) {
                    Ok(url) => Some(Ok(url)),
                    Err(e) => {
                        tracing::error!(
                            "[rslex-http-stream::http_client::proxy] Unable to parse {} env variable value as a proxy url.",
                            env_variable_name
                        );
                        Some(Err(ProxySettingsError::ValueFormat {
                            env_variable: env_variable_name.to_string(),
                            error: Arc::new(e),
                        }))
                    },
                }
            }
        },
        Err(std::env::VarError::NotPresent) => Option::None,
        Err(std::env::VarError::NotUnicode(_)) => {
            tracing::error!(
                "[rslex-http-stream::http_client::proxy] Unable to encode proxy setting env variable {} as unicode.",
                env_variable_name
            );
            Some(Err(ProxySettingsError::ValueEncoding(env_variable_name.to_string())))
        },
    }
}

impl ProxyInterceptSettings {
    pub(crate) fn create_from_env_variables() -> Result<Option<ProxyInterceptSettings>, ProxySettingsError> {
        let http_proxy = get_proxy_env_setting(HTTP_PROXY_ENV_VARIABLE).transpose()?;
        let https_proxy = get_proxy_env_setting(HTTPS_PROXY_ENV_VARIABLE).transpose()?;

        Ok(match (http_proxy, https_proxy) {
            (None, None) => None,
            (Some(http_proxy), None) => Some(ProxyInterceptSettings::Http(http_proxy)),
            (None, Some(https_proxy)) => Some(ProxyInterceptSettings::Https(https_proxy)),
            (Some(http_proxy), Some(https_proxy)) => Some(ProxyInterceptSettings::Both(http_proxy, https_proxy)),
        })
    }

    pub fn try_get_proxy_connector(&self) -> Result<ProxyConnector<HttpsConnector<HttpConnector>>, ProxySettingsError> {
        let mut proxy_connector = ProxyConnector::new(HttpsConnector::with_native_roots())?;
        match self {
            ProxyInterceptSettings::Http(http_proxy) => {
                proxy_connector.add_proxy(get_proxy(http_proxy, HTTP_PROXY_ENV_VARIABLE, Intercept::Http)?);
            },
            ProxyInterceptSettings::Https(https_proxy) => {
                proxy_connector.add_proxy(get_proxy(https_proxy, HTTPS_PROXY_ENV_VARIABLE, Intercept::Https)?);
            },
            ProxyInterceptSettings::Both(http_proxy, https_proxy) => {
                proxy_connector.add_proxy(get_proxy(http_proxy, HTTP_PROXY_ENV_VARIABLE, Intercept::Http)?);
                proxy_connector.add_proxy(get_proxy(https_proxy, HTTPS_PROXY_ENV_VARIABLE, Intercept::Https)?);
            },
        };

        Ok(proxy_connector)
    }
}

fn get_proxy(proxy_url: &Url, config_env_variable_name: &str, intercept: Intercept) -> Result<Proxy, ProxySettingsError> {
    // This is a weird looking thing but this is the easiest way to drop username nad password from url if any
    let authority = match (proxy_url.host(), proxy_url.port()) {
        (Some(host), Some(port)) => Ok(format!("{}:{}", host, port)),
        (Some(host), None) => Ok(host.to_string()),
        _ => Err(ProxySettingsError::RelativeUrl),
    }?;

    let proxy_uri_without_auth = Uri::builder()
        .scheme(proxy_url.scheme())
        .authority(authority.as_str())
        .path_and_query(if let Some(query) = proxy_url.query() {
            format!("{}?{}", proxy_url.path(), query)
        } else {
            proxy_url.path().to_string()
        })
        .build()
        .map_err(|e| ProxySettingsError::ValueFormat {
            env_variable: config_env_variable_name.to_string(),
            error: Arc::new(e),
        })?;

    let mut proxy = Proxy::new(intercept, proxy_uri_without_auth);
    if let Some(password) = proxy_url.password() {
        proxy.set_authorization(Authorization::basic(proxy_url.username(), password));
    }

    Ok(proxy)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use serial_test::serial;
    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use std::{convert::TryFrom, ffi::OsStr, panic::UnwindSafe};
    use url::ParseError;

    fn set_env_variable<S: AsRef<OsStr>>(variable_name: &str, value: Option<S>) {
        if let Some(value) = value {
            std::env::set_var(variable_name, value);
        } else {
            std::env::remove_var(variable_name);
        }
    }

    fn test_with_env_proxy_env_variables<S: AsRef<OsStr>, R, F: FnOnce() -> R + UnwindSafe>(
        http_proxy: Option<S>,
        https_proxy: Option<S>,
        test_action: F,
    ) -> R {
        let original_http_proxy = std::env::var(HTTP_PROXY_ENV_VARIABLE).ok();
        let original_https_proxy = std::env::var(HTTPS_PROXY_ENV_VARIABLE).ok();

        set_env_variable(HTTP_PROXY_ENV_VARIABLE, http_proxy);
        set_env_variable(HTTPS_PROXY_ENV_VARIABLE, https_proxy);

        let test_result = std::panic::catch_unwind(|| test_action());

        set_env_variable(HTTP_PROXY_ENV_VARIABLE, original_http_proxy);
        set_env_variable(HTTPS_PROXY_ENV_VARIABLE, original_https_proxy);

        test_result.should().be_ok().which_value()
    }

    #[test]
    fn proxy_settings_error_from_io_error() {
        let io_error = std::io::Error::from(std::io::ErrorKind::NotFound);
        let proxy_settings_error: ProxySettingsError = io_error.into();

        format!("{:?}", proxy_settings_error).should().be(format!(
            "{:?}",
            ProxySettingsError::Unknown(Arc::new(std::io::Error::from(std::io::ErrorKind::NotFound,)))
        ));
    }

    #[test]
    fn http_client_creation_error_from_proxy_settings_error() {
        let proxy_settings_error = ProxySettingsError::RelativeUrl;

        let http_client_creation_error: HttpClientCreationError = proxy_settings_error.into();
        format!("{:?}", http_client_creation_error).should().be(format!(
            "{:?}",
            HttpClientCreationError::ProxySettings(ProxySettingsError::RelativeUrl)
        ));
    }

    #[test]
    #[serial]
    fn create_from_env_variables_no_env_variables_set() {
        let proxy_settings = test_with_env_proxy_env_variables(Option::<&str>::None, Option::<&str>::None, || {
            ProxyInterceptSettings::create_from_env_variables().should().be_ok().which_value()
        });

        proxy_settings.should().be_none();
    }

    #[test]
    #[serial]
    fn create_from_env_variables_env_variables_set_to_empty_strings() {
        let proxy_settings = test_with_env_proxy_env_variables(Some(""), Some("   "), || {
            ProxyInterceptSettings::create_from_env_variables().should().be_ok().which_value()
        });

        proxy_settings.should().be_none();
    }

    #[test]
    #[serial]
    fn create_from_env_variables_http_proxy_set() {
        let http_proxy = "http://user:password@myproxy.com:1234/subproxy";
        let proxy_settings = test_with_env_proxy_env_variables(Some(http_proxy), None, || {
            ProxyInterceptSettings::create_from_env_variables().should().be_ok().which_value()
        });

        proxy_settings
            .should()
            .be_some()
            .which_value()
            .should()
            .satisfy(|value| match value {
                ProxyInterceptSettings::Http(http) => http.eq(&Url::parse(http_proxy).unwrap()),
                _ => false,
            });
    }

    #[test]
    #[serial]
    fn create_from_env_variables_https_proxy_set() {
        let https_proxy = "https://user:password@myproxy.com:1234/subproxy";
        let proxy_settings = test_with_env_proxy_env_variables(None, Some(https_proxy), || {
            ProxyInterceptSettings::create_from_env_variables().should().be_ok().which_value()
        });

        proxy_settings
            .should()
            .be_some()
            .which_value()
            .should()
            .satisfy(|value| match value {
                ProxyInterceptSettings::Https(https) => https.eq(&Url::parse(https_proxy).unwrap()),
                _ => false,
            });
    }

    #[test]
    #[serial]
    fn create_from_env_variables_http_and_https_proxy_set() {
        let http_proxy = "http://user:password@myproxy.com:1234/subproxy";
        let https_proxy = "https://user:password@myproxy.com:1234/subproxy";

        let proxy_settings = test_with_env_proxy_env_variables(Some(http_proxy), Some(https_proxy), || {
            ProxyInterceptSettings::create_from_env_variables().should().be_ok().which_value()
        });

        proxy_settings
            .should()
            .be_some()
            .which_value()
            .should()
            .satisfy(|value| match value {
                ProxyInterceptSettings::Both(http, https) => {
                    http.eq(&Url::parse(http_proxy).unwrap()) && https.eq(&Url::parse(https_proxy).unwrap())
                },
                _ => false,
            });
    }

    #[test]
    #[serial]
    #[cfg(unix)]
    fn create_from_env_variables_env_variable_is_not_utf_encoded() {
        let http_proxy = OsString::from_vec(vec![0xC1]); // 0xC1 - non-valid UTF-8 sequence

        let proxy_settings =
            test_with_env_proxy_env_variables(Some(http_proxy), None, || ProxyInterceptSettings::create_from_env_variables());

        let error = proxy_settings.should().be_err().which_value();

        format!("{:?}", error).should().be(format!(
            "{:?}",
            ProxySettingsError::ValueEncoding(HTTP_PROXY_ENV_VARIABLE.to_string())
        ));
    }

    #[test]
    #[serial]
    fn create_from_env_variables_env_variable_is_not_a_valid_url() {
        let https_proxy = "this_is_not_a_valid_proxy_url";

        let proxy_settings =
            test_with_env_proxy_env_variables(None, Some(https_proxy), || ProxyInterceptSettings::create_from_env_variables());

        let error = proxy_settings.should().be_err().which_value();

        format!("{:?}", error).should().be(format!(
            "{:?}",
            ProxySettingsError::ValueFormat {
                env_variable: HTTPS_PROXY_ENV_VARIABLE.to_string(),
                error: Arc::new(ParseError::RelativeUrlWithoutBase)
            }
        ));
    }

    #[test]
    #[serial]
    fn get_proxy_connector_for_http_proxy() {
        let http_proxy = "http://user:password@myproxy.com:1234/subproxy";

        let proxy_settings = test_with_env_proxy_env_variables(Some(http_proxy), None, || {
            ProxyInterceptSettings::create_from_env_variables().should().be_ok().which_value()
        })
        .should()
        .be_some()
        .which_value();

        let proxy_connector = proxy_settings.try_get_proxy_connector().should().be_ok().which_value();
        let proxies = proxy_connector.proxies();
        proxies.len().should().be(1);
        proxies
            .get(0)
            .should()
            .be_some()
            .which_value()
            .uri()
            .should()
            .be(&Uri::try_from("http://myproxy.com:1234/subproxy").unwrap());
    }

    #[test]
    #[serial]
    fn get_proxy_connector_for_https_proxy() {
        let https_proxy = "https://user:password@myproxy.com:1234/subproxy";

        let proxy_settings = test_with_env_proxy_env_variables(None, Some(https_proxy), || {
            ProxyInterceptSettings::create_from_env_variables().should().be_ok().which_value()
        })
        .should()
        .be_some()
        .which_value();

        let proxy_connector = proxy_settings.try_get_proxy_connector().should().be_ok().which_value();
        let proxies = proxy_connector.proxies();
        proxies.len().should().be(1);
        proxies
            .get(0)
            .should()
            .be_some()
            .which_value()
            .uri()
            .should()
            .be(&Uri::try_from("https://myproxy.com:1234/subproxy").unwrap());
    }

    #[test]
    #[serial]
    fn get_proxy_connector_for_http_and_https_proxy() {
        let http_proxy = "http://user:password@myhttpproxy.com:1234/subproxy";
        let https_proxy = "https://user:password@myhttpsproxy.com:1234/subproxy";

        let proxy_settings = test_with_env_proxy_env_variables(Some(http_proxy), Some(https_proxy), || {
            ProxyInterceptSettings::create_from_env_variables().should().be_ok().which_value()
        })
        .should()
        .be_some()
        .which_value();

        let proxy_connector = proxy_settings.try_get_proxy_connector().should().be_ok().which_value();
        let proxies = proxy_connector.proxies();
        proxies.len().should().be(2);
        proxies
            .get(0)
            .should()
            .be_some()
            .which_value()
            .uri()
            .should()
            .be(&Uri::try_from("http://myhttpproxy.com:1234/subproxy").unwrap());
        proxies
            .get(1)
            .should()
            .be_some()
            .which_value()
            .uri()
            .should()
            .be(&Uri::try_from("https://myhttpsproxy.com:1234/subproxy").unwrap());
    }
}
