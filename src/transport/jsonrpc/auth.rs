#![forbid(unsafe_code)]

use jsonrpsee::core::server::Extensions;

use crate::core::transport_publish::{PublishPrincipal, TransportPublishStore, hash_bearer_token};

use super::RpcError;

#[cfg(test)]
pub(crate) const TRANSPORT_PUBLISH_AUTH_MODE: &str = "scoped_bearer_token";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TransportPublishAuthorization {
    Authorized(PublishPrincipal),
    Missing,
    Invalid,
}

pub(crate) fn authorize_transport_publish_request(
    authorization_header: Option<&str>,
    store: &TransportPublishStore,
) -> TransportPublishAuthorization {
    let Some(authorization_header) = authorization_header else {
        return TransportPublishAuthorization::Missing;
    };

    let mut parts = authorization_header.split_whitespace();
    let scheme = parts.next().unwrap_or_default();
    let token = parts.next().unwrap_or_default();

    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() || parts.next().is_some() {
        return TransportPublishAuthorization::Invalid;
    }

    match store.principal_for_token_hash(hash_bearer_token(token).as_str()) {
        Ok(Some(principal)) => TransportPublishAuthorization::Authorized(principal),
        Ok(None) | Err(_) => TransportPublishAuthorization::Invalid,
    }
}

pub(crate) fn require_publish_principal(
    extensions: &Extensions,
) -> Result<PublishPrincipal, RpcError> {
    match extensions
        .get::<TransportPublishAuthorization>()
        .cloned()
        .unwrap_or(TransportPublishAuthorization::Missing)
    {
        TransportPublishAuthorization::Authorized(principal) => Ok(principal),
        TransportPublishAuthorization::Missing => Err(RpcError::Unauthorized(
            "transport publish bearer token required".to_string(),
        )),
        TransportPublishAuthorization::Invalid => Err(RpcError::Unauthorized(
            "invalid transport publish bearer token".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use jsonrpsee::core::server::Extensions;
    use radroots_transport_publish_protocol::{
        NostrPublishTargetSourcePolicy, TransportPublishTargetPolicyName,
    };

    use super::{
        TRANSPORT_PUBLISH_AUTH_MODE, TransportPublishAuthorization,
        authorize_transport_publish_request, require_publish_principal,
    };
    use crate::core::transport_publish::{
        PublishJobVisibility, PublishPrincipalInit, TransportPublishStore, generate_bearer_token,
        hash_bearer_token,
    };

    fn store_with_token() -> (TransportPublishStore, String) {
        let store = TransportPublishStore::memory().expect("store");
        let token = generate_bearer_token();
        store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: hash_bearer_token(token.as_str()),
                allowed_pubkeys: vec!["a".repeat(64)],
                allowed_kinds: vec![30_402],
                allowed_target_policies: vec![TransportPublishTargetPolicyName::Nostr],
                allowed_explicit_transport_kinds: Vec::new(),
                allowed_nostr_source_policies: vec![
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                ],
                allow_request_targets: false,
                job_visibility: PublishJobVisibility::Own,
                expires_at_unix: None,
            })
            .expect("principal");
        (store, token)
    }

    #[test]
    fn transport_publish_auth_accepts_matching_bearer_token() {
        let (store, token) = store_with_token();
        let header = format!("Bearer {token}");
        let auth = authorize_transport_publish_request(Some(header.as_str()), &store);
        assert!(matches!(auth, TransportPublishAuthorization::Authorized(_)));
        assert_eq!(TRANSPORT_PUBLISH_AUTH_MODE, "scoped_bearer_token");
    }

    #[test]
    fn transport_publish_auth_rejects_missing_and_invalid_headers() {
        let (store, _token) = store_with_token();
        assert_eq!(
            authorize_transport_publish_request(None, &store),
            TransportPublishAuthorization::Missing
        );
        assert_eq!(
            authorize_transport_publish_request(Some("Basic secret"), &store),
            TransportPublishAuthorization::Invalid
        );
        assert_eq!(
            authorize_transport_publish_request(Some("Bearer wrong"), &store),
            TransportPublishAuthorization::Invalid
        );
    }

    #[test]
    fn require_publish_principal_reads_authorized_extensions() {
        let (store, token) = store_with_token();
        let header = format!("Bearer {token}");
        let auth = authorize_transport_publish_request(Some(header.as_str()), &store);
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
