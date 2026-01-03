#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_nostr::prelude::{
    radroots_nostr_parse_pubkeys,
    RadrootsNostrFilter,
    RadrootsNostrKind,
    RadrootsNostrTimestamp,
};

use super::helpers::{listing_view, LISTING_KIND};
use super::types::ListingEventView;

#[derive(Debug, Default, Deserialize)]
struct TradeListingListParams {
    #[serde(default)]
    authors: Option<Vec<String>>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    since: Option<u64>,
    #[serde(default)]
    until: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct TradeListingListResponse {
    listings: Vec<ListingEventView>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("trade.listing.list");
    m.register_async_method("trade.listing.list", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let TradeListingListParams {
            authors,
            limit,
            since,
            until,
        } = params.parse().unwrap_or_default();

        let limit = limit.unwrap_or(50).min(1000) as usize;

        let mut filter = RadrootsNostrFilter::new()
            .kind(RadrootsNostrKind::Custom(LISTING_KIND))
            .limit(limit);
        if let Some(authors) = authors {
            let pks = radroots_nostr_parse_pubkeys(&authors)
                .map_err(|e| RpcError::InvalidParams(format!("invalid author: {e}")))?;
            filter = filter.authors(pks);
        } else {
            filter = filter.author(ctx.state.pubkey);
        }
        if let Some(since) = since {
            filter = filter.since(RadrootsNostrTimestamp::from_secs(since));
        }
        if let Some(until) = until {
            filter = filter.until(RadrootsNostrTimestamp::from_secs(until));
        }

        let events = ctx
            .state
            .client
            .fetch_events(filter, Duration::from_secs(10))
            .await
            .map_err(|e| RpcError::Other(format!("fetch failed: {e}")))?;

        let mut listings = events.into_iter().map(|ev| listing_view(&ev)).collect::<Vec<_>>();
        listings.sort_by(|a, b| b.event.created_at.cmp(&a.event.created_at));

        Ok::<TradeListingListResponse, RpcError>(TradeListingListResponse { listings })
    })?;
    Ok(())
}
