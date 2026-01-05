use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::nostr::{publish_response, PublishResponse};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::KIND_PLOT;
use radroots_events::plot::RadrootsPlot;
use radroots_events_codec::plot::encode::plot_build_tags;
use radroots_nostr::prelude::{radroots_nostr_build_event, radroots_nostr_send_event};

#[derive(Debug, Deserialize)]
struct PublishPlotParams {
    plot: RadrootsPlot,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.plot.publish");
    m.register_async_method("events.plot.publish", |params, ctx, _| async move {
        let relays = ctx.state.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishPlotParams { plot, tags } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let content = serde_json::to_string(&plot)
            .map_err(|e| RpcError::InvalidParams(format!("invalid plot json: {e}")))?;
        let mut tag_slices =
            plot_build_tags(&plot).map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        if let Some(extra_tags) = tags {
            tag_slices.extend(extra_tags);
        }

        let builder = radroots_nostr_build_event(KIND_PLOT, content, tag_slices)
            .map_err(|e| RpcError::Other(format!("failed to build plot event: {e}")))?;

        let output = radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish plot: {e}")))?;

        Ok::<PublishResponse, RpcError>(publish_response(output))
    })?;

    Ok(())
}
