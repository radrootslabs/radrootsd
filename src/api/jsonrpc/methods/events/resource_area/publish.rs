use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::nostr::{publish_response, PublishResponse};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::KIND_RESOURCE_AREA;
use radroots_events::resource_area::RadrootsResourceArea;
use radroots_events_codec::resource_area::encode::resource_area_build_tags;
use radroots_nostr::prelude::{radroots_nostr_build_event, radroots_nostr_send_event};

#[derive(Debug, Deserialize)]
struct PublishResourceAreaParams {
    resource_area: RadrootsResourceArea,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.resource_area.publish");
    m.register_async_method("events.resource_area.publish", |params, ctx, _| async move {
        let relays = ctx.state.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishResourceAreaParams { resource_area, tags } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let content = serde_json::to_string(&resource_area).map_err(|e| {
            RpcError::InvalidParams(format!("invalid resource_area json: {e}"))
        })?;
        let mut tag_slices = resource_area_build_tags(&resource_area)
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        if let Some(extra_tags) = tags {
            tag_slices.extend(extra_tags);
        }

        let builder = radroots_nostr_build_event(KIND_RESOURCE_AREA, content, tag_slices)
            .map_err(|e| RpcError::Other(format!("failed to build resource_area event: {e}")))?;

        let output = radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish resource_area: {e}")))?;

        Ok::<PublishResponse, RpcError>(publish_response(output))
    })?;

    Ok(())
}
