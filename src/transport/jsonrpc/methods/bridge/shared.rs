use anyhow::Result;
use nostr::Event;
use radroots_nostr::prelude::RadrootsNostrEventBuilder;
use radroots_nostr_signer::prelude::RadrootsNostrSignerBackend;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::core::bridge::publish::BridgeRelayPublishResult;
use crate::core::bridge::store::{
    BridgeJobRecord, BridgeJobReservation, BridgeJobStatus, BridgeJobStoreError,
};
use crate::transport::jsonrpc::nip46::{client as nip46_client, session as nip46_session};
use crate::transport::jsonrpc::{RpcContext, RpcError};

#[derive(Clone, Debug, Serialize)]
pub(super) struct BridgePublishResponse {
    pub deduplicated: bool,
    pub job: BridgeJobView,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct BridgeJobView {
    pub job_id: String,
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub status: BridgeJobStatus,
    pub terminal: bool,
    pub recovered_after_restart: bool,
    pub requested_at_unix: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_unix: Option<u64>,
    pub signer_mode: String,
    pub event_kind: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_addr: Option<String>,
    pub delivery_policy: crate::app::config::BridgeDeliveryPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_quorum: Option<usize>,
    pub relay_count: usize,
    pub acknowledged_relay_count: usize,
    pub required_acknowledged_relay_count: usize,
    pub attempt_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempt_summaries: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relay_results: Vec<BridgeRelayPublishResult>,
    pub relay_outcome_summary: String,
}

impl From<BridgeJobRecord> for BridgeJobView {
    fn from(record: BridgeJobRecord) -> Self {
        Self {
            terminal: record.is_terminal(),
            recovered_after_restart: record.recovered_after_restart(),
            job_id: record.job_id,
            command: record.command,
            idempotency_key: record.idempotency_key,
            status: record.status,
            requested_at_unix: record.requested_at_unix,
            completed_at_unix: record.completed_at_unix,
            signer_mode: record.signer_mode,
            event_kind: record.event_kind,
            event_id: record.event_id,
            event_addr: record.event_addr,
            delivery_policy: record.delivery_policy,
            delivery_quorum: record.delivery_quorum,
            relay_count: record.relay_count,
            acknowledged_relay_count: record.acknowledged_relay_count,
            required_acknowledged_relay_count: record.required_acknowledged_relay_count,
            attempt_count: record.attempt_count,
            attempt_summaries: record.attempt_summaries,
            relay_results: record.relay_results,
            relay_outcome_summary: record.relay_outcome_summary,
        }
    }
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

    use crate::app::config::{BridgeConfig, BridgeDeliveryPolicy, Nip46Config};
    use crate::core::Radrootsd;
    use crate::core::bridge::store::{
        BRIDGE_PENDING_RECOVERY_SUMMARY, BridgeJobStatus, new_listing_publish_job,
    };
    use crate::core::nip46::session::Nip46Session;
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

    use super::{
        BridgeJobView, fingerprint_bridge_request, normalize_idempotency_key, resolve_bridge_signer,
    };
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

    #[test]
    fn fingerprint_bridge_request_is_stable_across_nip46_session_renewal() {
        let session_keys = RadrootsNostrKeys::generate();
        let session = Nip46Session {
            id: "session-1".to_string(),
            client: RadrootsNostrClient::new(session_keys.clone()),
            client_keys: RadrootsNostrKeys::generate(),
            client_pubkey: RadrootsNostrKeys::generate().public_key(),
            remote_signer_pubkey: session_keys.public_key(),
            user_pubkey: None,
            relays: vec!["wss://relay.example.com".to_string()],
            perms: vec!["sign_event".to_string()],
            name: None,
            url: None,
            image: None,
            expires_at: None,
            auth_required: false,
            authorized: true,
            auth_url: None,
            pending_request: None,
        };
        let renewed_session = Nip46Session {
            id: "session-2".to_string(),
            ..session.clone()
        };
        let first = fingerprint_bridge_request(
            "bridge.order.request",
            &super::BridgeSignerSelection::Nip46Session {
                session_id: "session-1".to_string(),
                session,
            },
            &serde_json::json!({"order_id":"same"}),
        )
        .expect("first");
        let second = fingerprint_bridge_request(
            "bridge.order.request",
            &super::BridgeSignerSelection::Nip46Session {
                session_id: "session-2".to_string(),
                session: renewed_session,
            },
            &serde_json::json!({"order_id":"same"}),
        )
        .expect("second");
        assert_eq!(first, second);
    }

    #[test]
    fn bridge_job_view_exposes_terminal_and_recovery_flags() {
        let mut job = new_listing_publish_job(
            "job-1".to_string(),
            Some("same".to_string()),
            "embedded_service_identity".to_string(),
            30402,
            Some("event-1".to_string()),
            "30402:author:listing".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );
        job.status = BridgeJobStatus::Failed;
        job.completed_at_unix = Some(1);
        job.relay_outcome_summary = BRIDGE_PENDING_RECOVERY_SUMMARY.to_string();
        let view = BridgeJobView::from(job);
        assert!(view.terminal);
        assert!(view.recovered_after_restart);
    }
}
