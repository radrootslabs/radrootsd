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

#[cfg(test)]
mod tests {
    use super::{dvm_event_view, order_id_from_event, order_summaries, LISTING_KIND};
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use radroots_trade::listing::dvm::{TradeListingEnvelope, TradeListingMessageType};
    use serde_json::json;

    fn dvm_event(
        id: &str,
        pubkey: &str,
        created_at: u64,
        kind: u16,
        tags: Vec<Vec<String>>,
        content: &str,
    ) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 6);
        let event_json = json!({
            "id": id,
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": kind,
            "tags": tags,
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    #[test]
    fn dvm_event_view_parses_envelope_and_prefers_envelope_order_id() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let listing_addr = format!("{LISTING_KIND}:{pubkey}:listing-1");
        let envelope = TradeListingEnvelope::new(
            TradeListingMessageType::OrderRequest,
            listing_addr,
            Some("env-order".to_string()),
            json!({}),
        );
        let content = serde_json::to_string(&envelope).expect("envelope");
        let id = format!("{:064x}", 1);
        let tags = vec![vec!["d".to_string(), "tag-order".to_string()]];
        let event = dvm_event(&id, pubkey, 100, 5321, tags, &content);

        let view = dvm_event_view(&event);

        assert!(view.envelope.is_some());
        assert!(view.envelope_error.is_none());
        assert_eq!(order_id_from_event(&view).as_deref(), Some("env-order"));
    }

    #[test]
    fn dvm_event_view_invalid_json_sets_error() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let id = format!("{:064x}", 2);
        let event = dvm_event(&id, pubkey, 120, 5321, Vec::new(), "not-json");

        let view = dvm_event_view(&event);

        assert!(view.envelope.is_none());
        assert_eq!(view.envelope_error.as_deref(), Some("invalid envelope json"));
    }

    #[test]
    fn order_summaries_counts_and_sorts() {
        let pubkey = "3bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let listing_addr = format!("{LISTING_KIND}:{pubkey}:listing-1");
        let order_a = vec![vec!["d".to_string(), "order-a".to_string()]];
        let order_b = vec![vec!["d".to_string(), "order-b".to_string()]];

        let id_a1 = format!("{:064x}", 3);
        let id_a2 = format!("{:064x}", 4);
        let id_b1 = format!("{:064x}", 5);

        let ev_a1 = dvm_event(&id_a1, pubkey, 10, 5321, order_a.clone(), "");
        let ev_a2 = dvm_event(&id_a2, pubkey, 20, 6321, order_a.clone(), "");
        let ev_b1 = dvm_event(&id_b1, pubkey, 15, 5321, order_b, "");

        let views = vec![
            dvm_event_view(&ev_a1),
            dvm_event_view(&ev_a2),
            dvm_event_view(&ev_b1),
        ];

        let summaries = order_summaries(&views, &listing_addr);

        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].order_id, "order-a");
        assert_eq!(summaries[0].event_count, 2);
        assert_eq!(summaries[0].first_seen_at, 10);
        assert_eq!(summaries[0].last_seen_at, 20);
        assert_eq!(summaries[0].last_event_id, id_a2);
        assert_eq!(summaries[0].last_event_kind, 6321);
        assert_eq!(summaries[1].order_id, "order-b");
        assert_eq!(summaries[1].event_count, 1);
        assert_eq!(summaries[1].last_seen_at, 15);
    }
}
