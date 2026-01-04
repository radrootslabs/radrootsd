use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;
use std::time::Duration;

use crate::api::jsonrpc::nostr::{event_tags, event_view_with_tags};
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
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};
use radroots_trade::listing::codec::listing_from_event_parts;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ListingRow {
    id: String,
    author: String,
    created_at: u64,
    kind: u32,
    tags: Vec<Vec<String>>,
    content: String,
    sig: String,
    listing: Option<RadrootsListing>,
}

#[derive(Clone, Debug, Serialize)]
struct ListingListResponse {
    listings: Vec<ListingRow>,
}

pub(crate) fn build_listing_rows<I>(events: I) -> Vec<ListingRow>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut items = events
        .into_iter()
        .map(|ev| {
            let tags = event_tags(&ev);
            let listing = parse_listing_event(&ev, &tags);
            let event = event_view_with_tags(&ev, tags);
            ListingRow {
                id: event.id,
                author: event.author,
                created_at: event.created_at,
                kind: event.kind,
                tags: event.tags,
                content: event.content,
                sig: event.sig,
                listing,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    items
}

fn parse_listing_event(event: &RadrootsNostrEvent, tags: &[Vec<String>]) -> Option<RadrootsListing> {
    listing_from_event_parts(tags, &event.content).ok()
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

        let items = build_listing_rows(events);

        Ok::<ListingListResponse, RpcError>(ListingListResponse { listings: items })
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_listing_rows;
    use radroots_core::{
        RadrootsCoreCurrency,
        RadrootsCoreDecimal,
        RadrootsCoreMoney,
        RadrootsCoreQuantity,
        RadrootsCoreQuantityPrice,
        RadrootsCoreUnit,
    };
    use radroots_events::kinds::KIND_LISTING;
    use radroots_events::listing::{
        RadrootsListing,
        RadrootsListingBin,
        RadrootsListingFarmRef,
        RadrootsListingProduct,
    };
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use radroots_trade::listing::codec::listing_tags_build;
    use serde_json::json;

    fn listing_event(
        id: &str,
        pubkey: &str,
        created_at: u64,
        tags: Vec<Vec<String>>,
        content: &str,
    ) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 5);
        let event_json = json!({
            "id": id,
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": KIND_LISTING,
            "tags": tags,
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    fn sample_listing(farm_pubkey: &str) -> RadrootsListing {
        let quantity = RadrootsCoreQuantity::new(RadrootsCoreDecimal::from(1_u64), RadrootsCoreUnit::Each);
        let price = RadrootsCoreQuantityPrice::new(
            RadrootsCoreMoney::new(RadrootsCoreDecimal::from(10_u64), RadrootsCoreCurrency::USD),
            quantity.clone(),
        );
        let bin = RadrootsListingBin {
            bin_id: "bin-1".to_string(),
            quantity,
            price_per_canonical_unit: price,
            display_amount: None,
            display_unit: None,
            display_label: None,
            display_price: None,
            display_price_unit: None,
        };
        RadrootsListing {
            d_tag: "listing-1".to_string(),
            farm: RadrootsListingFarmRef {
                pubkey: farm_pubkey.to_string(),
                d_tag: "farm-1".to_string(),
            },
            product: RadrootsListingProduct {
                key: "coffee".to_string(),
                title: "Coffee".to_string(),
                category: "beverage".to_string(),
                summary: None,
                process: None,
                lot: None,
                location: None,
                profile: None,
                year: None,
            },
            primary_bin_id: "bin-1".to_string(),
            bins: vec![bin],
            resource_area: None,
            plot: None,
            discounts: None,
            inventory_available: None,
            availability: None,
            delivery_method: None,
            location: None,
            images: None,
        }
    }

    #[test]
    fn listing_list_sorts_by_created_at_desc() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let older = listing_event(&old_id, pubkey, 100, Vec::new(), "");
        let newer = listing_event(&new_id, pubkey, 200, Vec::new(), "");

        let listings = build_listing_rows(vec![older, newer]);

        assert_eq!(listings.len(), 2);
        assert_eq!(listings[0].id, new_id);
        assert_eq!(listings[0].created_at, 200);
        assert_eq!(listings[1].id, old_id);
        assert_eq!(listings[1].created_at, 100);
    }

    #[test]
    fn listing_list_builds_from_tags_when_content_empty() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let listing = sample_listing(pubkey);
        let tags = listing_tags_build(&listing).expect("tags");
        let id = format!("{:064x}", 3);
        let event = listing_event(&id, pubkey, 300, tags.clone(), "");

        let listings = build_listing_rows(vec![event]);

        assert_eq!(listings.len(), 1);
        assert_eq!(listings[0].tags, tags);
        let parsed = listings[0].listing.as_ref().expect("listing");
        assert_eq!(parsed.d_tag, "listing-1");
        assert_eq!(parsed.farm.pubkey, pubkey);
        assert_eq!(parsed.primary_bin_id, "bin-1");
    }
}
