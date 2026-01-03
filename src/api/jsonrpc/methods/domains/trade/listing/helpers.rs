#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::time::Duration;

use radroots_nostr::prelude::{
    radroots_nostr_parse_pubkey,
    RadrootsNostrClient,
    RadrootsNostrCoordinate,
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrKind,
    RadrootsNostrPublicKey,
    RadrootsNostrTimestamp,
};
use radroots_trade::listing::{
    codec::listing_from_event_parts,
    dvm::{TradeListingAddress, TradeListingEnvelope},
};

use super::types::{DvmEventView, ListingEventView, TradeListingOrderSummary};
use crate::api::jsonrpc::nostr::{event_tags, event_view, event_view_with_tags};
use crate::api::jsonrpc::params::MAX_LIMIT;
use crate::api::jsonrpc::RpcError;

pub(crate) const LISTING_KIND: u16 = 30402;

pub(crate) fn listing_view(event: &RadrootsNostrEvent) -> ListingEventView {
    let tags = event_tags(event);
    let listing = listing_from_event_parts(&tags, &event.content).ok();
    ListingEventView {
        event: event_view_with_tags(event, tags),
        listing,
    }
}

pub(crate) fn parse_listing_addr(listing_addr: &str) -> Result<TradeListingAddress, RpcError> {
    let addr = TradeListingAddress::parse(listing_addr)
        .map_err(|_| RpcError::InvalidParams("invalid listing_addr".to_string()))?;
    if addr.kind != LISTING_KIND {
        return Err(RpcError::InvalidParams("unsupported listing kind".to_string()));
    }
    Ok(addr)
}

pub(crate) fn listing_filter(addr: &TradeListingAddress) -> Result<RadrootsNostrFilter, RpcError> {
    let author = radroots_nostr_parse_pubkey(&addr.seller_pubkey)
        .map_err(|e| RpcError::InvalidParams(format!("invalid listing author: {e}")))?;
    Ok(RadrootsNostrFilter::new()
        .kind(RadrootsNostrKind::Custom(addr.kind))
        .author(author)
        .identifier(addr.listing_id.clone()))
}

pub(crate) async fn fetch_latest_listing_event(
    client: &RadrootsNostrClient,
    listing_addr: &TradeListingAddress,
    timeout_secs: u64,
) -> Result<Option<RadrootsNostrEvent>, RpcError> {
    let mut filter = listing_filter(listing_addr)?;
    filter = filter.limit(25);
    let events = client
        .fetch_events(filter, Duration::from_secs(timeout_secs))
        .await
        .map_err(|e| RpcError::Other(format!("fetch failed: {e}")))?;
    let mut latest: Option<RadrootsNostrEvent> = None;
    for event in events {
        match &latest {
            Some(cur) if event.created_at <= cur.created_at => {}
            _ => latest = Some(event),
        }
    }
    Ok(latest)
}

pub(crate) fn dvm_filter(
    listing_addr: &TradeListingAddress,
    kinds: &[u16],
) -> Result<RadrootsNostrFilter, RpcError> {
    let author = radroots_nostr_parse_pubkey(&listing_addr.seller_pubkey)
        .map_err(|e| RpcError::InvalidParams(format!("invalid listing author: {e}")))?;
    let coordinate = RadrootsNostrCoordinate::new(
        RadrootsNostrKind::Custom(listing_addr.kind),
        author,
    )
        .identifier(listing_addr.listing_id.clone());
    let kinds = kinds
        .iter()
        .map(|kind| RadrootsNostrKind::Custom(*kind))
        .collect::<Vec<_>>();
    Ok(RadrootsNostrFilter::new()
        .kinds(kinds)
        .coordinate(&coordinate))
}

pub(crate) fn dvm_event_view(event: &RadrootsNostrEvent) -> DvmEventView {
    let envelope = serde_json::from_str::<TradeListingEnvelope<serde_json::Value>>(&event.content)
        .ok();
    let envelope_error = envelope
        .as_ref()
        .and_then(|env| env.validate().err())
        .map(|err| err.to_string())
        .or_else(|| {
            if envelope.is_some() {
                None
            } else {
                Some("invalid envelope json".to_string())
            }
        });
    DvmEventView {
        event: event_view(event),
        envelope,
        envelope_error,
    }
}

pub(crate) async fn fetch_dvm_events(
    client: &RadrootsNostrClient,
    listing_addr: &TradeListingAddress,
    kinds: &[u16],
    order_id: Option<&str>,
    authors: Option<&[RadrootsNostrPublicKey]>,
    recipients: Option<&[RadrootsNostrPublicKey]>,
    since: Option<u64>,
    until: Option<u64>,
    limit: Option<u64>,
    timeout_secs: u64,
) -> Result<Vec<DvmEventView>, RpcError> {
    let mut filter = dvm_filter(listing_addr, kinds)?;

    if let Some(order_id) = order_id {
        filter = filter.identifier(order_id);
    }
    if let Some(authors) = authors {
        filter = filter.authors(authors.to_vec());
    }
    if let Some(recipients) = recipients {
        filter = filter.pubkeys(recipients.to_vec());
    }
    if let Some(since) = since {
        filter = filter.since(RadrootsNostrTimestamp::from_secs(since));
    }
    if let Some(until) = until {
        filter = filter.until(RadrootsNostrTimestamp::from_secs(until));
    }
    if let Some(limit) = limit {
        filter = filter.limit(limit.min(MAX_LIMIT) as usize);
    }

    let events = client
        .fetch_events(filter, Duration::from_secs(timeout_secs))
        .await
        .map_err(|e| RpcError::Other(format!("fetch failed: {e}")))?;

    let mut out = events
        .into_iter()
        .map(|event| dvm_event_view(&event))
        .collect::<Vec<_>>();
    out.sort_by(|a, b| a.event.created_at.cmp(&b.event.created_at));
    Ok(out)
}

pub(crate) fn order_id_from_event(event: &DvmEventView) -> Option<String> {
    if let Some(envelope) = &event.envelope {
        if let Some(order_id) = &envelope.order_id {
            return Some(order_id.clone());
        }
    }
    event
        .event
        .tags
        .iter()
        .find_map(|tag| match tag.get(0).map(String::as_str) {
            Some("d") => tag.get(1).cloned(),
            _ => None,
        })
}

pub(crate) fn order_summaries(
    events: &[DvmEventView],
    listing_addr: &str,
) -> Vec<TradeListingOrderSummary> {
    let mut summary_map: HashMap<String, TradeListingOrderSummary> = HashMap::new();

    for event in events {
        let order_id = match order_id_from_event(event) {
            Some(id) => id,
            None => continue,
        };
        let entry = summary_map.entry(order_id.clone()).or_insert_with(|| {
            TradeListingOrderSummary {
                order_id,
                listing_addr: listing_addr.to_string(),
                event_count: 0,
                first_seen_at: event.event.created_at,
                last_seen_at: event.event.created_at,
                last_event_id: event.event.id.clone(),
                last_event_kind: event.event.kind,
            }
        });
        entry.event_count += 1;
        if event.event.created_at < entry.first_seen_at {
            entry.first_seen_at = event.event.created_at;
        }
        if event.event.created_at >= entry.last_seen_at {
            entry.last_seen_at = event.event.created_at;
            entry.last_event_id = event.event.id.clone();
            entry.last_event_kind = event.event.kind;
        }
    }

    let mut summaries: Vec<TradeListingOrderSummary> = summary_map.into_values().collect();
    summaries.sort_by(|a, b| b.last_seen_at.cmp(&a.last_seen_at));
    summaries
}
