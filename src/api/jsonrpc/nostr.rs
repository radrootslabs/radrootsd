#![forbid(unsafe_code)]

use serde::Serialize;

use radroots_nostr::prelude::RadrootsNostrEvent;

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

pub(crate) fn event_tags(event: &RadrootsNostrEvent) -> Vec<Vec<String>> {
    event.tags.iter().map(|t| t.as_slice().to_vec()).collect()
}

pub(crate) fn event_view(event: &RadrootsNostrEvent) -> NostrEventView {
    event_view_with_tags(event, event_tags(event))
}

pub(crate) fn event_view_with_tags(
    event: &RadrootsNostrEvent,
    tags: Vec<Vec<String>>,
) -> NostrEventView {
    NostrEventView {
        id: event.id.to_string(),
        author: event.pubkey.to_string(),
        created_at: event.created_at.as_secs(),
        kind: event.kind.as_u16() as u32,
        tags,
        content: event.content.clone(),
        sig: event.sig.to_string(),
    }
}
