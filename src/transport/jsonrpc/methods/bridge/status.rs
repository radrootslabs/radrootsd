use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;

use crate::app::config::BridgeDeliveryPolicy;
use crate::core::nip46::session::Nip46SessionRole;
use crate::transport::jsonrpc::auth::{BRIDGE_AUTH_MODE, require_bridge_auth};
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

const BRIDGE_SIGNER_SELECTION_MODE: &str = "selectable_per_request";
const BRIDGE_DEFAULT_SIGNER_MODE: &str = "embedded_service_identity";
const BRIDGE_NIP46_SIGNER_MODE: &str = "nip46_session";

#[derive(Clone, Debug, Serialize)]
struct BridgeStatusResponse {
    enabled: bool,
    ready: bool,
    auth_mode: String,
    signer_mode: String,
    default_signer_mode: String,
    supported_signer_modes: Vec<String>,
    available_nip46_signer_sessions: usize,
    relay_count: usize,
    delivery_policy: BridgeDeliveryPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery_quorum: Option<usize>,
    publish_max_attempts: usize,
    publish_initial_backoff_millis: u64,
    publish_max_backoff_millis: u64,
    job_status_retention: usize,
    retained_jobs: usize,
    retained_idempotency_keys: usize,
    accepted_jobs: usize,
    published_jobs: usize,
    failed_jobs: usize,
    recovered_failed_jobs: usize,
    methods: Vec<String>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("bridge.status");
    m.register_async_method("bridge.status", |_params, ctx, extensions| async move {
        require_bridge_auth(&extensions)?;
        let relay_count = ctx.state.client.relays().await.len();
        let snapshot = ctx.state.bridge_jobs.snapshot();
        let available_nip46_signer_sessions = ctx
            .state
            .nip46_sessions
            .list()
            .await
            .into_iter()
            .filter(|session| session.role() == Nip46SessionRole::OutboundRemoteSigner)
            .count();
        Ok::<BridgeStatusResponse, RpcError>(BridgeStatusResponse {
            enabled: ctx.state.bridge_config.enabled,
            ready: ctx.state.bridge_config.enabled && relay_count > 0,
            auth_mode: BRIDGE_AUTH_MODE.to_string(),
            signer_mode: BRIDGE_SIGNER_SELECTION_MODE.to_string(),
            default_signer_mode: BRIDGE_DEFAULT_SIGNER_MODE.to_string(),
            supported_signer_modes: vec![
                BRIDGE_DEFAULT_SIGNER_MODE.to_string(),
                BRIDGE_NIP46_SIGNER_MODE.to_string(),
            ],
            available_nip46_signer_sessions,
            relay_count,
            delivery_policy: ctx.state.bridge_config.delivery_policy,
            delivery_quorum: ctx.state.bridge_config.delivery_quorum,
            publish_max_attempts: ctx.state.bridge_config.publish_max_attempts,
            publish_initial_backoff_millis: ctx.state.bridge_config.publish_initial_backoff_millis,
            publish_max_backoff_millis: ctx.state.bridge_config.publish_max_backoff_millis,
            job_status_retention: ctx.state.bridge_config.job_status_retention,
            retained_jobs: snapshot.retained_jobs,
            retained_idempotency_keys: snapshot.retained_idempotency_keys,
            accepted_jobs: snapshot.accepted_jobs,
            published_jobs: snapshot.published_jobs,
            failed_jobs: snapshot.failed_jobs,
            recovered_failed_jobs: snapshot.recovered_failed_jobs,
            methods: ctx.methods.list(),
        })
    })?;
    Ok(())
}
