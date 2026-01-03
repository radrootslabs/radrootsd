use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;
use std::time::Duration;

use crate::api::jsonrpc::nostr::{event_tags, event_view_with_tags, NostrEventView};
use crate::api::jsonrpc::params::{
    apply_time_bounds,
    limit_or,
    parse_pubkeys_opt,
    timeout_or,
    EventListParams,
};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::KIND_LISTING;
use radroots_events::listing::RadrootsListing;
use radroots_nostr::prelude::{
    RadrootsNostrFilter,
    RadrootsNostrKind,
};
use radroots_trade::listing::codec::listing_from_event_parts;

#[derive(Clone, Debug, Serialize)]
struct ListingEventFlat {
    #[serde(flatten)]
    event: NostrEventView,
    listing: Option<RadrootsListing>,
}

#[derive(Clone, Debug, Serialize)]
struct ListingListResponse {
    listings: Vec<ListingEventFlat>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.listing.list");
    m.register_async_method("events.listing.list", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let EventListParams {
            authors,
            limit,
            since,
            until,
            timeout_secs,
        } = params
            .parse::<Option<EventListParams>>()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?
            .unwrap_or_default();

        let limit = limit_or(limit);

        let mut filter = RadrootsNostrFilter::new()
            .limit(limit)
            .kind(RadrootsNostrKind::Custom(KIND_LISTING as u16));

        if let Some(authors) = parse_pubkeys_opt("author", authors)? {
            filter = filter.authors(authors);
        } else {
            filter = filter.author(ctx.state.pubkey);
        }
        filter = apply_time_bounds(filter, since, until);

        let events = ctx
            .state
            .client
            .fetch_events(filter, Duration::from_secs(timeout_or(timeout_secs)))
            .await
            .map_err(|e| RpcError::Other(format!("fetch failed: {e}")))?;

        let mut items = events
            .into_iter()
            .map(|ev| {
                let tags = event_tags(&ev);
                let listing = listing_from_event_parts(&tags, &ev.content).ok();
                ListingEventFlat {
                    event: event_view_with_tags(&ev, tags),
                    listing,
                }
            })
            .collect::<Vec<_>>();
        items.sort_by(|a, b| b.event.created_at.cmp(&a.event.created_at));

        Ok::<ListingListResponse, RpcError>(ListingListResponse { listings: items })
    })?;
    Ok(())
}
