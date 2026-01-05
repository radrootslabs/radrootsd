use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::nostr::{publish_response, PublishResponse};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::farm::RadrootsFarm;
use radroots_events::kinds::KIND_FARM;
use radroots_events_codec::farm::encode::farm_build_tags;
use radroots_nostr::prelude::{radroots_nostr_build_event, radroots_nostr_send_event};

#[derive(Debug, Deserialize)]
struct PublishFarmParams {
    farm: RadrootsFarm,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.farm.publish");
    m.register_async_method("events.farm.publish", |params, ctx, _| async move {
        let relays = ctx.state.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishFarmParams { farm, tags } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let content = serde_json::to_string(&farm)
            .map_err(|e| RpcError::InvalidParams(format!("invalid farm json: {e}")))?;
        let mut tag_slices =
            farm_build_tags(&farm).map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        if let Some(extra_tags) = tags {
            tag_slices.extend(extra_tags);
        }

        let builder = radroots_nostr_build_event(KIND_FARM, content, tag_slices)
            .map_err(|e| RpcError::Other(format!("failed to build farm event: {e}")))?;

        let output = radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish farm: {e}")))?;

        Ok::<PublishResponse, RpcError>(publish_response(output))
    })?;

    Ok(())
}
