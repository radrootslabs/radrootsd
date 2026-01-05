use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::methods::events::helpers::send_event_with_options;
use crate::api::jsonrpc::nostr::{publish_response, PublishResponse};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::KIND_LISTING;
use radroots_events::listing::RadrootsListing;
use radroots_nostr::prelude::radroots_nostr_build_event;
use radroots_trade::listing::codec::listing_tags_build;

#[derive(Debug, Deserialize)]
struct PublishListingParams {
    listing: RadrootsListing,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
    #[serde(default)]
    author_secret_key: Option<String>,
    #[serde(default)]
    created_at: Option<u64>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.listing.publish");
    m.register_async_method("events.listing.publish", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishListingParams {
            listing,
            tags,
            author_secret_key,
            created_at,
        } =
            params.parse().map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let content = serde_json::to_string(&listing)
            .map_err(|e| RpcError::InvalidParams(format!("invalid listing json: {e}")))?;
        let mut tag_slices = listing_tags_build(&listing)
            .map_err(|e| RpcError::InvalidParams(format!("invalid listing tags: {e}")))?;
        if let Some(extra_tags) = tags {
            tag_slices.extend(extra_tags);
        }
        let builder = radroots_nostr_build_event(KIND_LISTING, content, tag_slices)
            .map_err(|e| RpcError::Other(format!("failed to build listing event: {e}")))?;

        let out = send_event_with_options(&ctx, builder, author_secret_key, created_at).await?;

        Ok::<PublishResponse, RpcError>(publish_response(out))
    })?;
    Ok(())
}
