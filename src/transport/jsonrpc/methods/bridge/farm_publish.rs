use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_events::{farm::RadrootsFarm, kinds::KIND_FARM};
use radroots_events_codec::farm::encode::to_wire_parts_with_kind;
use radroots_nostr::prelude::radroots_nostr_build_event;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::core::bridge::publish::{
    BridgePublishSettings, connect_and_publish_event, failed_prepublish_execution,
};
use crate::core::bridge::store::new_publish_job;
use crate::core::nip46::session::Nip46SessionAuthority;
use crate::transport::jsonrpc::auth::require_bridge_auth;
use crate::transport::jsonrpc::methods::bridge::shared::{
    BridgePublishResponse, ensure_bridge_enabled, fingerprint_bridge_request,
    normalize_idempotency_key, reserve_bridge_job, resolve_actor_bridge_signer,
    sign_bridge_event_builder,
};
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct BridgeFarmPublishParams {
    farm: RadrootsFarm,
    #[serde(default)]
    kind: Option<u32>,
    #[serde(default)]
    signer_session_id: Option<String>,
    #[serde(default)]
    signer_authority: Option<Nip46SessionAuthority>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalBridgeFarmPublishRequest {
    kind: u32,
    farm: RadrootsFarm,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("bridge.farm.publish");
    m.register_async_method(
        "bridge.farm.publish",
        |params, ctx, extensions| async move {
            require_bridge_auth(&extensions)?;
            let params: BridgeFarmPublishParams = params
                .parse()
                .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
            let response = publish_farm(ctx.as_ref().clone(), params).await?;
            Ok::<BridgePublishResponse, RpcError>(response)
        },
    )?;
    Ok(())
}

async fn publish_farm(
    ctx: RpcContext,
    params: BridgeFarmPublishParams,
) -> Result<BridgePublishResponse, RpcError> {
    ensure_bridge_enabled(&ctx)?;
    let idempotency_key = normalize_idempotency_key(params.idempotency_key)?;
    let kind = params.kind.unwrap_or(KIND_FARM);
    if kind != KIND_FARM {
        return Err(RpcError::InvalidParams(format!(
            "farm publish only supports kind {KIND_FARM}, got {kind}"
        )));
    }
    let signer = resolve_actor_bridge_signer(
        &ctx,
        params.signer_session_id.as_deref(),
        params.signer_authority.as_ref(),
        kind,
        "bridge.farm.publish",
    )
    .await?;
    let signer_pubkey = signer.signer_pubkey_hex();
    let canonical = CanonicalBridgeFarmPublishRequest {
        kind,
        farm: params.farm,
    };
    let request_fingerprint =
        fingerprint_bridge_request("bridge.farm.publish", &signer, &canonical)?;
    let parts = to_wire_parts_with_kind(&canonical.farm, canonical.kind)
        .map_err(|error| RpcError::InvalidParams(format!("invalid farm contract: {error}")))?;
    let event_addr = format!("{}:{}:{}", parts.kind, signer_pubkey, canonical.farm.d_tag);
    let builder = radroots_nostr_build_event(parts.kind, parts.content, parts.tags)
        .map_err(|error| RpcError::Other(format!("failed to build farm event: {error}")))?;

    let reserved = reserve_bridge_job(
        &ctx,
        new_publish_job(
            "bridge.farm.publish",
            Uuid::new_v4().to_string(),
            idempotency_key,
            signer.signer_mode(),
            parts.kind,
            None,
            Some(event_addr.clone()),
            ctx.state.bridge_config.delivery_policy,
            ctx.state.bridge_config.delivery_quorum,
        ),
        request_fingerprint,
        "bridge farm",
    )?;
    let job = match reserved {
        crate::core::bridge::store::BridgeJobReservation::Accepted(job) => job,
        crate::core::bridge::store::BridgeJobReservation::Duplicate(existing) => {
            return Ok(BridgePublishResponse {
                deduplicated: true,
                job: existing.into(),
            });
        }
    };

    let publish_settings = BridgePublishSettings::from_config(&ctx.state.bridge_config);
    let event = match sign_bridge_event_builder(&ctx, &signer, builder, "bridge.farm.publish").await
    {
        Ok(event) => event,
        Err(error) => {
            let _ = ctx.state.bridge_jobs.complete(
                &job.job_id,
                None,
                failed_prepublish_execution(&publish_settings, error.to_string()),
            );
            return Err(error);
        }
    };

    let execution = connect_and_publish_event(&ctx.state.client, &publish_settings, &event).await;
    let job = ctx
        .state
        .bridge_jobs
        .complete(&job.job_id, Some(event.id.to_hex()), execution)
        .map_err(|error| RpcError::Other(format!("failed to persist bridge farm job: {error}")))?
        .ok_or_else(|| RpcError::Other("bridge job disappeared during completion".to_string()))?;
    debug_assert_eq!(job.event_addr.as_deref(), Some(event_addr.as_str()));

    Ok(BridgePublishResponse {
        deduplicated: false,
        job: job.into(),
    })
}
