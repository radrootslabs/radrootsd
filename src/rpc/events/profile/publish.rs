use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::{radrootsd::Radrootsd, rpc::RpcError};

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

pub fn register(m: &mut RpcModule<Radrootsd>) -> Result<()> {
    m.register_async_method("events.profile.publish", |params, ctx, _| async move {
        let relays = ctx.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishProfileParams { profile } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let metadata = to_metadata(&profile).map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let builder = radroots_nostr_build_metadata_event(&metadata);

        let output = radroots_nostr_send_event(&ctx.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish metadata: {e}")))?;

        let id_hex = output.id().to_string();
        let sent: Vec<String> = output.success.into_iter().map(|u| u.to_string()).collect();
        let failed: Vec<(String, String)> = output
            .failed
            .into_iter()
            .map(|(u, e)| (u.to_string(), e.to_string()))
            .collect();

        Ok::<JsonValue, RpcError>(json!({
            "id": id_hex,
            "sent": sent,
            "failed": failed
        }))
    })?;

    Ok(())
}
