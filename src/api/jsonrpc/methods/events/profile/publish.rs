use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::nostr::{publish_response, PublishResponse};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};

use radroots_events::profile::RadrootsProfile;
use radroots_events_codec::profile::encode::to_metadata;
use radroots_nostr::prelude::{
    radroots_nostr_build_metadata_event,
    radroots_nostr_send_event,
};

#[derive(Debug, Deserialize)]
struct PublishProfileParams {
    profile: RadrootsProfile,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.profile.publish");
    m.register_async_method("events.profile.publish", |params, ctx, _| async move {
        let relays = ctx.state.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishProfileParams { profile } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let metadata = to_metadata(&profile).map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let builder = radroots_nostr_build_metadata_event(&metadata);

        let output = radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish metadata: {e}")))?;

        Ok::<PublishResponse, RpcError>(publish_response(output))
    })?;

    Ok(())
}
