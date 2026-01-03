use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::nostr::{publish_response, PublishResponse};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::KIND_RESOURCE_HARVEST_CAP;
use radroots_events::resource_cap::RadrootsResourceHarvestCap;
use radroots_events_codec::resource_cap::encode::resource_harvest_cap_build_tags;
use radroots_nostr::prelude::{radroots_nostr_build_event, radroots_nostr_send_event};

#[derive(Debug, Deserialize)]
struct PublishResourceCapParams {
    resource_cap: RadrootsResourceHarvestCap,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.resource_cap.publish");
    m.register_async_method("events.resource_cap.publish", |params, ctx, _| async move {
        let relays = ctx.state.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishResourceCapParams { resource_cap, tags } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let content = serde_json::to_string(&resource_cap).map_err(|e| {
            RpcError::InvalidParams(format!("invalid resource_cap json: {e}"))
        })?;
        let mut tag_slices = resource_harvest_cap_build_tags(&resource_cap)
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        if let Some(extra_tags) = tags {
            tag_slices.extend(extra_tags);
        }

        let builder = radroots_nostr_build_event(KIND_RESOURCE_HARVEST_CAP, content, tag_slices)
            .map_err(|e| RpcError::Other(format!("failed to build resource_cap event: {e}")))?;

        let output = radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish resource_cap: {e}")))?;

        Ok::<PublishResponse, RpcError>(publish_response(output))
    })?;

    Ok(())
}
