use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;

use crate::app::config::BridgeDeliveryPolicy;
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Clone, Debug, Serialize)]
struct BridgeStatusResponse {
    enabled: bool,
    ready: bool,
    signer_mode: String,
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
    methods: Vec<String>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("bridge.status");
    m.register_async_method("bridge.status", |_params, ctx, _| async move {
        let relay_count = ctx.state.client.relays().await.len();
        let snapshot = ctx.state.bridge_jobs.snapshot();
        Ok::<BridgeStatusResponse, RpcError>(BridgeStatusResponse {
            enabled: ctx.state.bridge_config.enabled,
            ready: ctx.state.bridge_config.enabled && relay_count > 0,
            signer_mode: "embedded_service_identity".to_string(),
            relay_count,
            delivery_policy: ctx.state.bridge_config.delivery_policy,
            delivery_quorum: ctx.state.bridge_config.delivery_quorum,
            publish_max_attempts: ctx.state.bridge_config.publish_max_attempts,
            publish_initial_backoff_millis: ctx.state.bridge_config.publish_initial_backoff_millis,
            publish_max_backoff_millis: ctx.state.bridge_config.publish_max_backoff_millis,
            job_status_retention: ctx.state.bridge_config.job_status_retention,
            retained_jobs: snapshot.retained_jobs,
            retained_idempotency_keys: snapshot.retained_idempotency_keys,
            methods: ctx.methods.list(),
        })
    })?;
    Ok(())
}
