use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_events::{
    kinds::KIND_PROFILE,
    profile::{RadrootsProfile, RadrootsProfileType},
};
use radroots_events_codec::profile::encode::to_wire_parts_with_profile_type;
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
struct BridgeProfilePublishParams {
    profile: RadrootsProfile,
    #[serde(default)]
    profile_type: Option<RadrootsProfileType>,
    #[serde(default)]
    signer_session_id: Option<String>,
    #[serde(default)]
    signer_authority: Option<Nip46SessionAuthority>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalBridgeProfilePublishRequest {
    profile: RadrootsProfile,
    profile_type: Option<RadrootsProfileType>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("bridge.profile.publish");
    m.register_async_method(
        "bridge.profile.publish",
        |params, ctx, extensions| async move {
            require_bridge_auth(&extensions)?;
            let params: BridgeProfilePublishParams = params
                .parse()
                .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
            let response = publish_profile(ctx.as_ref().clone(), params).await?;
            Ok::<BridgePublishResponse, RpcError>(response)
        },
    )?;
    Ok(())
}

async fn publish_profile(
    ctx: RpcContext,
    params: BridgeProfilePublishParams,
) -> Result<BridgePublishResponse, RpcError> {
    ensure_bridge_enabled(&ctx)?;
    let idempotency_key = normalize_idempotency_key(params.idempotency_key)?;
    let signer = resolve_actor_bridge_signer(
        &ctx,
        params.signer_session_id.as_deref(),
        params.signer_authority.as_ref(),
        KIND_PROFILE,
        "bridge.profile.publish",
    )
    .await?;
    let canonical = CanonicalBridgeProfilePublishRequest {
        profile: params.profile,
        profile_type: params.profile_type,
    };
    let request_fingerprint =
        fingerprint_bridge_request("bridge.profile.publish", &signer, &canonical)?;
    let parts = to_wire_parts_with_profile_type(&canonical.profile, canonical.profile_type)
        .map_err(|error| RpcError::InvalidParams(format!("invalid profile contract: {error}")))?;
    let builder = radroots_nostr_build_event(parts.kind, parts.content, parts.tags)
        .map_err(|error| RpcError::Other(format!("failed to build profile event: {error}")))?;

    let reserved = reserve_bridge_job(
        &ctx,
        new_publish_job(
            "bridge.profile.publish",
            Uuid::new_v4().to_string(),
            idempotency_key,
            signer.signer_mode(),
            parts.kind,
            None,
            None,
            ctx.state.bridge_config.delivery_policy,
            ctx.state.bridge_config.delivery_quorum,
        ),
        request_fingerprint,
        "bridge profile",
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
    let event =
        match sign_bridge_event_builder(&ctx, &signer, builder, "bridge.profile.publish").await {
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
        .map_err(|error| RpcError::Other(format!("failed to persist bridge profile job: {error}")))?
        .ok_or_else(|| RpcError::Other("bridge job disappeared during completion".to_string()))?;

    Ok(BridgePublishResponse {
        deduplicated: false,
        job: job.into(),
    })
}
