use anyhow::Result;
use radroots_nostr_signer::prelude::RadrootsNostrSignerBackend;
use serde::Serialize;

use crate::core::bridge::store::BridgeJobRecord;
use crate::transport::jsonrpc::{RpcContext, RpcError};

#[derive(Clone, Debug, Serialize)]
pub(super) struct BridgePublishResponse {
    pub deduplicated: bool,
    pub job: BridgeJobRecord,
}

pub(super) fn ensure_bridge_enabled(ctx: &RpcContext) -> Result<(), RpcError> {
    if !ctx.state.bridge_config.enabled {
        return Err(RpcError::Other("bridge ingress is disabled".to_string()));
    }
    Ok(())
}

pub(super) fn bridge_signer_pubkey_hex(ctx: &RpcContext) -> Result<String, RpcError> {
    Ok(ctx
        .state
        .bridge_signer
        .signer_identity()
        .map_err(|error| RpcError::Other(format!("bridge signer unavailable: {error}")))?
        .ok_or_else(|| RpcError::Other("bridge signer identity is missing".to_string()))?
        .public_key_hex)
}

pub(super) fn normalize_idempotency_key(value: Option<String>) -> Result<Option<String>, RpcError> {
    let value = value.map(|value| value.trim().to_string());
    match value {
        Some(value) if value.is_empty() => Err(RpcError::InvalidParams(
            "idempotency_key cannot be empty".to_string(),
        )),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_idempotency_key;

    #[test]
    fn normalize_idempotency_key_rejects_empty_values() {
        let err = normalize_idempotency_key(Some("   ".to_string())).expect_err("empty key");
        assert!(err.to_string().contains("idempotency_key"));
    }
}
