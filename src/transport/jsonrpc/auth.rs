#![forbid(unsafe_code)]

use jsonrpsee::core::server::Extensions;

use super::RpcError;

pub(crate) const BRIDGE_AUTH_MODE: &str = "bearer_token";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BridgeAuthorization {
    Disabled,
    Authorized,
    Missing,
    Invalid,
}

pub(crate) fn authorize_bridge_request(
    authorization_header: Option<&str>,
    expected_token: Option<&str>,
) -> BridgeAuthorization {
    let Some(expected_token) = expected_token else {
        return BridgeAuthorization::Disabled;
    };
    let Some(authorization_header) = authorization_header else {
        return BridgeAuthorization::Missing;
    };

    let mut parts = authorization_header.split_whitespace();
    let scheme = parts.next().unwrap_or_default();
    let token = parts.next().unwrap_or_default();

    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() || parts.next().is_some() {
        return BridgeAuthorization::Invalid;
    }

    if token == expected_token {
        BridgeAuthorization::Authorized
    } else {
        BridgeAuthorization::Invalid
    }
}

pub(crate) fn require_bridge_auth(extensions: &Extensions) -> Result<(), RpcError> {
    match extensions
        .get::<BridgeAuthorization>()
        .copied()
        .unwrap_or(BridgeAuthorization::Missing)
    {
        BridgeAuthorization::Authorized => Ok(()),
        BridgeAuthorization::Disabled | BridgeAuthorization::Missing => Err(
            RpcError::Unauthorized("bridge bearer token required".to_string()),
        ),
        BridgeAuthorization::Invalid => Err(RpcError::Unauthorized(
            "invalid bridge bearer token".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use jsonrpsee::core::server::Extensions;

    use super::{
        BRIDGE_AUTH_MODE, BridgeAuthorization, authorize_bridge_request, require_bridge_auth,
    };

    #[test]
    fn authorize_bridge_request_returns_disabled_without_configured_token() {
        let auth = authorize_bridge_request(None, None);
        assert_eq!(auth, BridgeAuthorization::Disabled);
    }

    #[test]
    fn authorize_bridge_request_accepts_matching_bearer_token() {
        let auth = authorize_bridge_request(Some("Bearer secret"), Some("secret"));
        assert_eq!(auth, BridgeAuthorization::Authorized);
        assert_eq!(BRIDGE_AUTH_MODE, "bearer_token");
    }

    #[test]
    fn authorize_bridge_request_rejects_invalid_headers() {
        assert_eq!(
            authorize_bridge_request(Some("Basic secret"), Some("secret")),
            BridgeAuthorization::Invalid
        );
        assert_eq!(
            authorize_bridge_request(Some("Bearer wrong"), Some("secret")),
            BridgeAuthorization::Invalid
        );
        assert_eq!(
            authorize_bridge_request(None, Some("secret")),
            BridgeAuthorization::Missing
        );
    }

    #[test]
    fn require_bridge_auth_accepts_authorized_extensions() {
        let mut extensions = Extensions::new();
        extensions.insert(BridgeAuthorization::Authorized);
        require_bridge_auth(&extensions).expect("authorized");
    }

    #[test]
    fn require_bridge_auth_rejects_missing_extensions() {
        let err = require_bridge_auth(&Extensions::new()).expect_err("missing auth should fail");
        assert!(err.to_string().contains("required"));
    }
}
