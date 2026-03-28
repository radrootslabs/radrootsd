use anyhow::Result;
use nostr::Event;
use radroots_nostr::prelude::RadrootsNostrEventBuilder;
use radroots_nostr_signer::prelude::RadrootsNostrSignerBackend;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::core::bridge::store::{BridgeJobRecord, BridgeJobReservation, BridgeJobStoreError};
use crate::transport::jsonrpc::nip46::{client as nip46_client, session as nip46_session};
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

#[derive(Clone)]
pub(super) enum BridgeSignerSelection {
    EmbeddedServiceIdentity {
        signer_pubkey_hex: String,
    },
    Nip46Session {
        session_id: String,
        session: crate::core::nip46::session::Nip46Session,
    },
}

impl BridgeSignerSelection {
    pub(super) fn signer_pubkey_hex(&self) -> String {
        match self {
            Self::EmbeddedServiceIdentity { signer_pubkey_hex } => signer_pubkey_hex.clone(),
            Self::Nip46Session { session, .. } => session.remote_signer_pubkey.to_hex(),
        }
    }

    pub(super) fn signer_mode(&self) -> String {
        match self {
            Self::EmbeddedServiceIdentity { .. } => "embedded_service_identity".to_string(),
            Self::Nip46Session { session_id, .. } => format!("nip46_session:{session_id}"),
        }
    }
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

pub(super) async fn resolve_bridge_signer(
    ctx: &RpcContext,
    signer_session_id: Option<&str>,
    event_kind: u32,
) -> Result<BridgeSignerSelection, RpcError> {
    match signer_session_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(session_id) => {
            let session = nip46_session::get_session(ctx, session_id).await?;
            nip46_session::require_sign_event_permission(&session, event_kind)?;
            Ok(BridgeSignerSelection::Nip46Session {
                session_id: session_id.to_string(),
                session,
            })
        }
        None => Ok(BridgeSignerSelection::EmbeddedServiceIdentity {
            signer_pubkey_hex: bridge_signer_pubkey_hex(ctx)?,
        }),
    }
}

pub(super) async fn sign_bridge_event_builder(
    ctx: &RpcContext,
    signer: &BridgeSignerSelection,
    builder: RadrootsNostrEventBuilder,
    label: &str,
) -> Result<Event, RpcError> {
    match signer {
        BridgeSignerSelection::EmbeddedServiceIdentity { .. } => ctx
            .state
            .bridge_signer
            .sign_event_builder(builder)
            .map(|signed| signed.event)
            .map_err(|error| RpcError::Other(format!("failed to sign {label} event: {error}"))),
        BridgeSignerSelection::Nip46Session { session, .. } => {
            let unsigned = builder.build(session.remote_signer_pubkey);
            nip46_client::sign_event(session, unsigned, label).await
        }
    }
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

#[derive(Serialize)]
struct BridgeRequestFingerprint<'a, T> {
    command: &'a str,
    signer_mode: &'a str,
    signer_pubkey_hex: &'a str,
    payload: &'a T,
}

pub(super) fn fingerprint_bridge_request<T: Serialize>(
    command: &str,
    signer: &BridgeSignerSelection,
    payload: &T,
) -> Result<String, RpcError> {
    let payload = serde_json::to_vec(&BridgeRequestFingerprint {
        command,
        signer_mode: &signer.signer_mode(),
        signer_pubkey_hex: &signer.signer_pubkey_hex(),
        payload,
    })
    .map_err(|error| RpcError::Other(format!("failed to fingerprint bridge request: {error}")))?;
    let digest = Sha256::digest(payload);
    Ok(format!("{digest:x}"))
}

pub(super) fn reserve_bridge_job(
    ctx: &RpcContext,
    record: BridgeJobRecord,
    request_fingerprint: String,
    label: &str,
) -> Result<BridgeJobReservation, RpcError> {
    ctx.state
        .bridge_jobs
        .reserve(record, request_fingerprint)
        .map_err(|error| match error {
            BridgeJobStoreError::IdempotencyConflict { .. } => {
                RpcError::InvalidParams(error.to_string())
            }
            _ => RpcError::Other(format!("failed to persist {label} job: {error}")),
        })
}

#[cfg(test)]
mod tests {
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::{RadrootsNostrClient, RadrootsNostrKeys, RadrootsNostrMetadata};

    use crate::app::config::{BridgeConfig, Nip46Config};
    use crate::core::Radrootsd;
    use crate::core::nip46::session::Nip46Session;
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

    use super::{fingerprint_bridge_request, normalize_idempotency_key, resolve_bridge_signer};
    use std::time::Instant;

    #[test]
    fn normalize_idempotency_key_rejects_empty_values() {
        let err = normalize_idempotency_key(Some("   ".to_string())).expect_err("empty key");
        assert!(err.to_string().contains("idempotency_key"));
    }

    #[tokio::test]
    async fn resolve_bridge_signer_prefers_requested_nip46_session() {
        let identity = RadrootsIdentity::generate();
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let state = Radrootsd::new(
            identity.clone(),
            metadata,
            BridgeConfig::default(),
            Nip46Config::default(),
        )
        .expect("state");
        let session_keys = RadrootsNostrKeys::generate();
        state
            .nip46_sessions
            .insert(Nip46Session {
                id: "session-1".to_string(),
                client: RadrootsNostrClient::new(session_keys.clone()),
                client_keys: session_keys.clone(),
                client_pubkey: session_keys.public_key(),
                remote_signer_pubkey: session_keys.public_key(),
                user_pubkey: None,
                relays: vec!["wss://relay.example.com".to_string()],
                perms: vec!["sign_event".to_string()],
                name: None,
                url: None,
                image: None,
                expires_at: Some(Instant::now() + std::time::Duration::from_secs(60)),
                auth_required: false,
                authorized: true,
                auth_url: None,
                pending_request: None,
            })
            .await;
        let ctx = RpcContext::new(state, MethodRegistry::default());

        let signer = resolve_bridge_signer(&ctx, Some("session-1"), 30402)
            .await
            .expect("session signer");
        assert_eq!(
            signer.signer_pubkey_hex(),
            session_keys.public_key().to_hex()
        );
        assert_eq!(signer.signer_mode(), "nip46_session:session-1");
    }

    #[test]
    fn fingerprint_bridge_request_changes_when_request_changes() {
        let signer = super::BridgeSignerSelection::EmbeddedServiceIdentity {
            signer_pubkey_hex: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
        };
        let first = fingerprint_bridge_request(
            "bridge.order.request",
            &signer,
            &serde_json::json!({"order_id":"one"}),
        )
        .expect("first");
        let second = fingerprint_bridge_request(
            "bridge.order.request",
            &signer,
            &serde_json::json!({"order_id":"two"}),
        )
        .expect("second");
        assert_ne!(first, second);
    }
}
