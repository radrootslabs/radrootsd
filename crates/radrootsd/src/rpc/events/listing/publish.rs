use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::{radrootsd::Radrootsd, rpc::RpcError};
use radroots_events::listing::models::RadrootsListing;
use radroots_nostr::prelude::{build_nostr_event, nostr_send_event};

#[derive(Debug, Deserialize)]
struct PublishListingParams {
    listing: RadrootsListing,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
}

pub fn register(m: &mut RpcModule<Radrootsd>) -> Result<()> {
    m.register_async_method("events.listing.publish", |params, ctx, _| async move {
        if ctx.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishListingParams { listing, tags } =
            params.parse().map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let content = serde_json::to_string(&listing)
            .map_err(|e| RpcError::InvalidParams(format!("invalid listing json: {e}")))?;
        let builder = build_nostr_event(30402, content, tags.unwrap_or_default())
            .map_err(|e| RpcError::Other(format!("failed to build listing event: {e}")))?;

        let out = nostr_send_event(&ctx.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish listing: {e}")))?;

        Ok::<JsonValue, RpcError>(json!({
            "id": out.id().to_string(),
            "sent": out.success.into_iter().map(|u| u.to_string()).collect::<Vec<_>>(),
            "failed": out.failed.into_iter().map(|(u,e)| (u.to_string(), e.to_string())).collect::<Vec<_>>(),
        }))
    })?;
    Ok(())
}
