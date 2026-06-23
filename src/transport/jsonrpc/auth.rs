#![forbid(unsafe_code)]

use jsonrpsee::core::server::Extensions;

use crate::core::publish_proxy::{PublishPrincipal, PublishProxyStore, hash_bearer_token};

use super::RpcError;

#[cfg(test)]
pub(crate) const PUBLISH_PROXY_AUTH_MODE: &str = "scoped_bearer_token";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublishProxyAuthorization {
    Authorized(PublishPrincipal),
    Missing,
    Invalid,
}

pub(crate) fn authorize_publish_proxy_request(
    authorization_header: Option<&str>,
    store: &PublishProxyStore,
) -> PublishProxyAuthorization {
    let Some(authorization_header) = authorization_header else {
        return PublishProxyAuthorization::Missing;
    };

    let mut parts = authorization_header.split_whitespace();
    let scheme = parts.next().unwrap_or_default();
    let token = parts.next().unwrap_or_default();

    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() || parts.next().is_some() {
        return PublishProxyAuthorization::Invalid;
    }

    match store.principal_for_token_hash(hash_bearer_token(token).as_str()) {
        Ok(Some(principal)) => PublishProxyAuthorization::Authorized(principal),
        Ok(None) | Err(_) => PublishProxyAuthorization::Invalid,
    }
}

pub(crate) fn require_publish_principal(
    extensions: &Extensions,
) -> Result<PublishPrincipal, RpcError> {
    match extensions
        .get::<PublishProxyAuthorization>()
        .cloned()
        .unwrap_or(PublishProxyAuthorization::Missing)
    {
        PublishProxyAuthorization::Authorized(principal) => Ok(principal),
        PublishProxyAuthorization::Missing => Err(RpcError::Unauthorized(
            "publish proxy bearer token required".to_string(),
        )),
        PublishProxyAuthorization::Invalid => Err(RpcError::Unauthorized(
            "invalid publish proxy bearer token".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use jsonrpsee::core::server::Extensions;
    use radroots_publish_proxy_protocol::PublishRelayPolicy;

    use super::{
        PUBLISH_PROXY_AUTH_MODE, PublishProxyAuthorization, authorize_publish_proxy_request,
        require_publish_principal,
    };
    use crate::core::publish_proxy::{
        PublishJobVisibility, PublishPrincipalInit, PublishProxyStore, generate_bearer_token,
        hash_bearer_token,
    };

    fn store_with_token() -> (PublishProxyStore, String) {
        let store = PublishProxyStore::memory().expect("store");
        let token = generate_bearer_token();
        store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: hash_bearer_token(token.as_str()),
                allowed_pubkeys: vec!["a".repeat(64)],
                allowed_kinds: vec![30_402],
                allowed_relay_policies: vec![PublishRelayPolicy::DaemonDefaultOnly],
                allow_request_relays: false,
                job_visibility: PublishJobVisibility::Own,
                expires_at_unix: None,
            })
            .expect("principal");
        (store, token)
    }

    #[test]
    fn publish_proxy_auth_accepts_matching_bearer_token() {
        let (store, token) = store_with_token();
        let header = format!("Bearer {token}");
        let auth = authorize_publish_proxy_request(Some(header.as_str()), &store);
        assert!(matches!(auth, PublishProxyAuthorization::Authorized(_)));
        assert_eq!(PUBLISH_PROXY_AUTH_MODE, "scoped_bearer_token");
    }

    #[test]
    fn publish_proxy_auth_rejects_missing_and_invalid_headers() {
        let (store, _token) = store_with_token();
        assert_eq!(
            authorize_publish_proxy_request(None, &store),
            PublishProxyAuthorization::Missing
        );
        assert_eq!(
            authorize_publish_proxy_request(Some("Basic secret"), &store),
            PublishProxyAuthorization::Invalid
        );
        assert_eq!(
            authorize_publish_proxy_request(Some("Bearer wrong"), &store),
            PublishProxyAuthorization::Invalid
        );
    }

    #[test]
    fn require_publish_principal_reads_authorized_extensions() {
        let (store, token) = store_with_token();
        let header = format!("Bearer {token}");
        let auth = authorize_publish_proxy_request(Some(header.as_str()), &store);
        let mut extensions = Extensions::new();
        extensions.insert(auth);
        require_publish_principal(&extensions).expect("authorized");
    }

    #[test]
    fn require_publish_principal_rejects_missing_extensions() {
        let err =
            require_publish_principal(&Extensions::new()).expect_err("missing auth should fail");
        assert!(err.to_string().contains("required"));
    }
}
