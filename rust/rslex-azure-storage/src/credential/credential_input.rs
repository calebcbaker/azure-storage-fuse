use super::{account_key::AccountKey, sas::Sas, service_principal::ServicePrincipal};
use lazy_static::lazy_static;
use rslex_core::{
    file_io::{ArgumentError, StreamArguments},
    records::parse::ParseRecord,
    SyncRecord, SyncValue,
};
use rslex_http_stream::{ApplyCredential, HttpClient};
use serde_json::json;
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    fmt::Debug,
    sync::Arc,
};

/// A data structure for credential info
/// will add a Customer variant later
/// to support extension
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialInput {
    None,
    Sas {
        token: String,
    },
    AccountKey {
        key: Vec<u8>,
    },
    ServicePrincipal {
        resource_url: String,
        authority_url: String,
        tenant_id: String,
        client_id: String,
        client_secret: String,
    },
}

impl Default for CredentialInput {
    fn default() -> Self {
        CredentialInput::None
    }
}

lazy_static! {
    static ref CREDENTIAL_FORMAT: String = json!({
        "type": "servicePrincipal",
        "resourceUrl": "your resource url",
        "authorityUrl": "your authority url",
        "tenantId": "your tenant id",
        "clientId": "your client id",
        "clientSecret": "your client secret"})
    .to_string();
}

impl CredentialInput {
    pub(crate) fn to_credential(&self, http_client: &Arc<dyn HttpClient>) -> Option<Arc<dyn ApplyCredential>> {
        match self {
            CredentialInput::None => None,
            CredentialInput::Sas { token } => Some(Arc::new(Sas::new(token))),
            CredentialInput::AccountKey { key } => Some(Arc::new(AccountKey::new(key.clone()))),
            CredentialInput::ServicePrincipal {
                resource_url,
                authority_url,
                tenant_id,
                client_id,
                client_secret,
            } => Some(Arc::new(ServicePrincipal::new(
                http_client.clone(),
                resource_url,
                authority_url,
                tenant_id,
                client_id,
                client_secret,
            ))),
        }
    }
}

impl TryFrom<&SyncRecord> for CredentialInput {
    type Error = ArgumentError;
    fn try_from(arguments: &SyncRecord) -> Result<Self, ArgumentError> {
        if let Ok(value) = arguments.get_value("sas") {
            Ok(CredentialInput::Sas { token: value.to_string() })
        } else if let Ok(value) = arguments.get_value("accountKey") {
            Ok(CredentialInput::AccountKey {
                key: base64::decode(&value.to_string()).map_err(|_| ArgumentError::InvalidArgument {
                    argument: "paths[].arguments.accountKey".to_owned(),
                    expected: "base64 encoded account key".to_owned(),
                    actual: value.to_string(),
                })?,
            })
        } else if let Ok(value) = arguments.get_value("credential") {
            if let SyncValue::String(credential_str) = value {
                if credential_str.is_empty() {
                    return Ok(CredentialInput::None);
                }
            }
            let json: HashMap<String, String> = serde_json::from_str(&value.to_string()).map_err(|_| ArgumentError::InvalidArgument {
                argument: "paths[].arguments.credential".to_owned(),
                expected: CREDENTIAL_FORMAT.clone(),
                actual: value.to_string(),
            })?;
            let mut has_empty_non_sp_credentials = false;
            if let Some(sas) = json.get("sasToken") {
                if !sas.is_empty() {
                    return Ok(CredentialInput::Sas { token: sas.to_string() });
                } else {
                    has_empty_non_sp_credentials = true;
                }
            }
            if let Some(account_key) = json.get("accountKey") {
                if !account_key.is_empty() {
                    return Ok(CredentialInput::AccountKey {
                        key: base64::decode(&account_key.to_string()).map_err(|_| ArgumentError::InvalidArgument {
                            argument: "paths[].arguments.accountKey".to_owned(),
                            expected: "base64 encoded account key".to_owned(),
                            actual: account_key.to_string(),
                        })?,
                    });
                } else {
                    has_empty_non_sp_credentials = true;
                }
            }

            if has_empty_non_sp_credentials {
                return Ok(CredentialInput::None);
            }

            match json.get("type") {
                Some(str) => {
                    if str != "servicePrincipal" {
                        return Err(ArgumentError::InvalidArgument {
                            argument: "paths[].arguments.credential.type".to_owned(),
                            expected: "servicePrincipal".to_owned(),
                            actual: str.clone(),
                        });
                    }
                },
                None => {
                    return Err(ArgumentError::MissingArgument {
                        argument: "paths[].arguments.credential.type".to_owned(),
                    })
                },
            }

            let resource_url = json
                .get("resourceUrl")
                .ok_or(ArgumentError::MissingArgument {
                    argument: "paths[].arguments.credential.resourceUrl".to_owned(),
                })?
                .clone();
            let authority_url = json
                .get("authorityUrl")
                .ok_or(ArgumentError::MissingArgument {
                    argument: "paths[].arguments.credential.authorityUrl".to_owned(),
                })?
                .clone();
            let tenant_id = json
                .get("tenantId")
                .ok_or(ArgumentError::MissingArgument {
                    argument: "paths[].arguments.credential.tenantId".to_owned(),
                })?
                .clone();
            let client_id = json
                .get("clientId")
                .ok_or(ArgumentError::MissingArgument {
                    argument: "paths[].arguments.credential.clientId".to_owned(),
                })?
                .clone();
            let client_secret = json
                .get("clientSecret")
                .ok_or(ArgumentError::MissingArgument {
                    argument: "paths[].arguments.credential.clientSecret".to_owned(),
                })?
                .clone();

            Ok(CredentialInput::ServicePrincipal {
                resource_url,
                authority_url,
                tenant_id,
                client_id,
                client_secret,
            })
        } else {
            Ok(CredentialInput::None)
        }
    }
}

impl<'r> ParseRecord<'r> for CredentialInput {
    fn parse(arguments: &SyncRecord) -> Result<Self, ArgumentError> {
        arguments.try_into()
    }
}

impl StreamArguments<'_> for CredentialInput {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::access_token::{
        fake_access_token_resolver::FakeAccessTokenResolver, Scope, ScopedAccessToken, ScopedAccessTokenResolver,
    };
    use fluent_assertions::{utils::AndConstraint, *};
    use rslex_core::sync_record;
    use rslex_http_stream::FakeHttpClient;
    use std::borrow::Borrow;

    fn fake_http_client() -> Arc<dyn HttpClient> {
        Arc::new(FakeHttpClient::default())
    }

    trait CredentialAssertions<T> {
        fn equal_credential<B, C>(self, expected: B) -> AndConstraint<T>
        where
            B: Borrow<C>,
            C: ApplyCredential;
    }

    impl<T: Borrow<dyn ApplyCredential>> CredentialAssertions<T> for Assertions<T> {
        fn equal_credential<B, C>(self, expected: B) -> AndConstraint<T>
        where
            B: Borrow<C>,
            C: ApplyCredential,
        {
            format!("{:?}", self.subject().borrow())
                .should()
                .equal_string(format!("{:?}", expected.borrow()));

            AndConstraint::new(self.into_inner())
        }
    }

    #[test]
    fn new_sas_from_sync_record() {
        let sas_token = "this&is&the&sas&token";
        let sync_record = sync_record! { "sas" => sas_token };

        CredentialInput::parse(&sync_record)
            .should()
            .be_ok()
            .with_value(CredentialInput::Sas {
                token: sas_token.to_owned(),
            });
    }

    #[test]
    fn new_account_key_from_sync_record() {
        let key = b"this&is&the&account&key";
        let sync_record = sync_record! { "accountKey" => SyncValue::String(base64::encode(key)) };

        CredentialInput::parse(&sync_record)
            .should()
            .be_ok()
            .with_value(CredentialInput::AccountKey { key: key.to_vec() });
    }

    #[test]
    fn new_account_key_with_invalide_key_returns_error() {
        let key = "this is not a valid base64 encoded account key";
        let sync_record = sync_record! { "accountKey" => key };
        CredentialInput::parse(&sync_record)
            .should()
            .be_err()
            .with_value(ArgumentError::InvalidArgument {
                argument: "paths[].arguments.accountKey".to_owned(),
                expected: "base64 encoded account key".to_owned(),
                actual: key.to_owned(),
            });
    }

    #[test]
    fn new_with_service_principal_from_sync_record() {
        let resource_url = "http://resource.url.net/";
        let authority_url = "http://authrity.url.net/";
        let tenant_id = "this is tenant id";
        let client_id = "this is client id";
        let client_secret = "this is the client secret";
        let cred = json!({
            "type": "servicePrincipal",
            "resourceUrl": resource_url,
            "authorityUrl": authority_url,
            "tenantId": tenant_id,
            "clientId": client_id,
            "clientSecret": client_secret})
        .to_string();
        let sync_record = sync_record! { "credential" => cred };

        CredentialInput::parse(&sync_record)
            .should()
            .be_ok()
            .with_value(CredentialInput::ServicePrincipal {
                resource_url: resource_url.to_owned(),
                authority_url: authority_url.to_owned(),
                tenant_id: tenant_id.to_owned(),
                client_id: client_id.to_owned(),
                client_secret: client_secret.to_owned(),
            });
    }

    #[test]
    fn new_with_sas_from_credential_json() {
        let sas_token = "this&is&the&sas&token";
        let cred = json!({ "sasToken": sas_token, "accountKey": "" }).to_string();
        let sync_record = sync_record! { "credential" => cred };

        CredentialInput::parse(&sync_record)
            .should()
            .be_ok()
            .with_value(CredentialInput::Sas {
                token: sas_token.to_owned(),
            });
    }

    #[test]
    fn new_with_account_key_from_credential_json() {
        let key = b"this&is&the&account&key";
        let cred = json!({ "accountKey": base64::encode(key), "sasToken": "" }).to_string();
        let sync_record = sync_record! { "credential" => cred };

        CredentialInput::parse(&sync_record)
            .should()
            .be_ok()
            .with_value(CredentialInput::AccountKey { key: key.to_vec() });
    }

    #[test]
    fn new_with_empty_non_sp_credentials() {
        let cred = json!({ "sasToken": "", "accountKey": "" }).to_string();
        let sync_record = sync_record! { "credential" => cred };

        CredentialInput::parse(&sync_record)
            .should()
            .be_ok()
            .with_value(CredentialInput::None);
    }

    #[test]
    fn new_with_service_principal_with_malformed_json_returns_error() {
        let cred = "not a valid json";

        let sync_record = sync_record! { "credential" => cred };
        CredentialInput::parse(&sync_record)
            .should()
            .be_err()
            .with_value(ArgumentError::InvalidArgument {
                argument: "paths[].arguments.credential".to_owned(),
                expected: CREDENTIAL_FORMAT.clone(),
                actual: cred.to_owned(),
            });
    }

    #[test]
    fn new_with_service_principal_with_empty_credential() {
        let cred = "";

        let sync_record = sync_record! { "credential" => cred };
        CredentialInput::parse(&sync_record)
            .should()
            .be_ok()
            .with_value(CredentialInput::None);
    }

    #[test]
    fn new_with_credential_when_type_not_principal_returns_error() {
        let resource_url = "http://resource.url.net/";
        let authority_url = "http://authrity.url.net/";
        let tenant_id = "this is tenant id";
        let client_id = "this is client id";
        let client_secret = "this is the client secret";
        let cred = json!({
            "type": "other",
            "resourceUrl": resource_url,
            "authorityUrl": authority_url,
            "tenantId": tenant_id,
            "clientId": client_id,
            "clientSecret": client_secret})
        .to_string();

        let sync_record = sync_record! { "credential" => cred };
        CredentialInput::parse(&sync_record)
            .should()
            .be_err()
            .with_value(ArgumentError::InvalidArgument {
                argument: "paths[].arguments.credential.type".to_owned(),
                expected: "servicePrincipal".to_owned(),
                actual: "other".to_owned(),
            });
    }

    #[test]
    fn new_with_credential_without_type_returns_error() {
        let resource_url = "http://resource.url.net/";
        let authority_url = "http://authrity.url.net/";
        let tenant_id = "this is tenant id";
        let client_id = "this is client id";
        let client_secret = "this is the client secret";
        let cred = json!({
            "resourceUrl": resource_url,
            "authorityUrl": authority_url,
            "tenantId": tenant_id,
            "clientId": client_id,
            "clientSecret": client_secret})
        .to_string();

        let sync_record = sync_record! { "credential" => cred };
        CredentialInput::parse(&sync_record)
            .should()
            .be_err()
            .with_value(ArgumentError::MissingArgument {
                argument: "paths[].arguments.credential.type".to_owned(),
            });
    }

    #[test]
    fn new_with_credential_without_resource_url_returns_error() {
        let authority_url = "http://authrity.url.net/";
        let tenant_id = "this is tenant id";
        let client_id = "this is client id";
        let client_secret = "this is the client secret";
        let cred = json!({
            "type": "servicePrincipal",
            "authorityUrl": authority_url,
            "tenantId": tenant_id,
            "clientId": client_id,
            "clientSecret": client_secret})
        .to_string();

        let sync_record = sync_record! { "credential" => cred };
        CredentialInput::parse(&sync_record)
            .should()
            .be_err()
            .with_value(ArgumentError::MissingArgument {
                argument: "paths[].arguments.credential.resourceUrl".to_owned(),
            });
    }

    #[test]
    fn new_with_credential_without_authroty_url_returns_error() {
        let resource_url = "http://resource.url.net/";
        let tenant_id = "this is tenant id";
        let client_id = "this is client id";
        let client_secret = "this is the client secret";
        let cred = json!({
            "type": "servicePrincipal",
            "resourceUrl": resource_url,
            "tenantId": tenant_id,
            "clientId": client_id,
            "clientSecret": client_secret})
        .to_string();

        let sync_record = sync_record! { "credential" => cred };
        CredentialInput::parse(&sync_record)
            .should()
            .be_err()
            .with_value(ArgumentError::MissingArgument {
                argument: "paths[].arguments.credential.authorityUrl".to_owned(),
            });
    }

    #[test]
    fn new_with_credential_without_tenant_id_returns_error() {
        let resource_url = "http://resource.url.net/";
        let authority_url = "http://authrity.url.net/";
        let client_id = "this is client id";
        let client_secret = "this is the client secret";
        let cred = json!({
            "type": "servicePrincipal",
            "resourceUrl": resource_url,
            "authorityUrl": authority_url,
            "clientId": client_id,
            "clientSecret": client_secret})
        .to_string();

        let sync_record = sync_record! { "credential" => cred };
        CredentialInput::parse(&sync_record)
            .should()
            .be_err()
            .with_value(ArgumentError::MissingArgument {
                argument: "paths[].arguments.credential.tenantId".to_owned(),
            });
    }

    #[test]
    fn new_with_credential_without_client_id_returns_error() {
        let resource_url = "http://resource.url.net/";
        let authority_url = "http://authrity.url.net/";
        let tenant_id = "this is tenant id";
        let client_secret = "this is the client secret";
        let cred = json!({
            "type": "servicePrincipal",
            "resourceUrl": resource_url,
            "authorityUrl": authority_url,
            "tenantId": tenant_id,
            "clientSecret": client_secret})
        .to_string();

        let sync_record = sync_record! { "credential" => cred };
        CredentialInput::parse(&sync_record)
            .should()
            .be_err()
            .with_value(ArgumentError::MissingArgument {
                argument: "paths[].arguments.credential.clientId".to_owned(),
            });
    }

    #[test]
    fn new_with_credential_without_client_secret_returns_error() {
        let resource_url = "http://resource.url.net/";
        let authority_url = "http://authrity.url.net/";
        let tenant_id = "this is tenant id";
        let client_id = "this is client id";
        let cred = json!({
            "type": "servicePrincipal",
            "resourceUrl": resource_url,
            "authorityUrl": authority_url,
            "tenantId": tenant_id,
            "clientId": client_id,
        })
        .to_string();

        let sync_record = sync_record! { "credential" => cred };
        CredentialInput::parse(&sync_record)
            .should()
            .be_err()
            .with_value(ArgumentError::MissingArgument {
                argument: "paths[].arguments.credential.clientSecret".to_owned(),
            });
    }

    #[test]
    fn new_from_sync_record_returns_none() {
        let sync_record = SyncRecord::empty();

        CredentialInput::parse(&sync_record)
            .should()
            .be_ok()
            .with_value(CredentialInput::None);
    }

    #[test]
    fn new_credential_from_none_returns_none() {
        let credential_input = CredentialInput::None;

        credential_input.to_credential(&fake_http_client()).should().be_none();
    }

    #[test]
    fn new_sas_from_credential_input() {
        let sas_token = "this&is&the&sas&token";
        let credential_input = CredentialInput::Sas {
            token: sas_token.to_owned(),
        };

        credential_input
            .to_credential(&fake_http_client())
            .should()
            .be_some()
            .which_value()
            .should()
            .equal_credential(Sas::new(sas_token));
    }

    #[test]
    fn new_account_key_from_credential_input() {
        let key = b"this&is&the&account&key";
        let credential_input = CredentialInput::AccountKey { key: key.to_vec() };

        credential_input
            .to_credential(&fake_http_client())
            .should()
            .be_some()
            .which_value()
            .should()
            .equal_credential(AccountKey::new(key.to_vec()));
    }

    #[test]
    fn new_with_service_principal_from_credential_input() {
        let resource_url = "http://resource.url.net/";
        let authority_url = "http://authrity.url.net/";
        let tenant_id = "this is tenant id";
        let client_id = "this is client id";
        let client_secret = "this is the client secret";
        let credential_input = CredentialInput::ServicePrincipal {
            resource_url: resource_url.to_owned(),
            authority_url: authority_url.to_owned(),
            tenant_id: tenant_id.to_owned(),
            client_id: client_id.to_owned(),
            client_secret: client_secret.to_owned(),
        };
        let http_client = fake_http_client();

        credential_input
            .to_credential(&http_client)
            .should()
            .be_some()
            .which_value()
            .should()
            .equal_credential(ServicePrincipal::new(
                http_client,
                resource_url,
                authority_url,
                tenant_id,
                client_id,
                client_secret,
            ));
    }

    #[test]
    fn new_scoped_access_token_from_credential_input() {
        let credential_input = CredentialInput::None;
        let access_token_resolver: Arc<dyn ScopedAccessTokenResolver> = Arc::new(FakeAccessTokenResolver::new());
        let credential = credential_input
            .to_credential(&fake_http_client())
            .unwrap_or(Arc::new(ScopedAccessToken::new(access_token_resolver.clone(), Scope::Storage)));

        credential
            .should()
            .equal_credential(ScopedAccessToken::new(access_token_resolver, Scope::Storage));
    }
}
