use nostr::{key::PublicKey, nips::nip19::FromBech32};
use radroots_events::relay_document::models::RadrootsRelayDocument;

use crate::utils::ws_to_http;

pub fn parse_pubkey(s: &str) -> Option<PublicKey> {
    PublicKey::from_bech32(s)
        .or_else(|_| PublicKey::from_hex(s))
        .ok()
}

pub async fn fetch_nip11(ws_url: &str) -> Option<RadrootsRelayDocument> {
    let http_url = ws_to_http(ws_url)?;
    let client = reqwest::Client::new();
    client
        .get(&http_url)
        .header("Accept", "application/nostr+json")
        .send()
        .await
        .ok()?
        .json::<RadrootsRelayDocument>()
        .await
        .ok()
}
