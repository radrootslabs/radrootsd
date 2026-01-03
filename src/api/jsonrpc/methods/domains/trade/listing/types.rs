#![forbid(unsafe_code)]

use radroots_events::listing::RadrootsListing;
use radroots_trade::listing::dvm::TradeListingEnvelope;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct NostrEventView {
    pub id: String,
    pub author: String,
    pub created_at: u64,
    pub kind: u32,
    pub tags: Vec<Vec<String>>,
    pub content: String,
    pub sig: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ListingEventView {
    pub event: NostrEventView,
    pub listing: Option<RadrootsListing>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DvmEventView {
    pub event: NostrEventView,
    pub envelope: Option<TradeListingEnvelope<serde_json::Value>>,
    pub envelope_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TradeListingOrderSummary {
    pub order_id: String,
    pub listing_addr: String,
    pub event_count: usize,
    pub first_seen_at: u64,
    pub last_seen_at: u64,
    pub last_event_id: String,
    pub last_event_kind: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct TradeListingSeriesView {
    pub listing: Option<ListingEventView>,
    pub dvm_events: Vec<DvmEventView>,
    pub orders: Vec<TradeListingOrderSummary>,
}
