use nostr::{key::PublicKey, nips::nip19::FromBech32};
use radroots_events::relay_document::models::RadrootsRelayDocument;

use crate::{rpc::RpcError, utils::ws_to_http};

#[derive(Debug, thiserror::Error)]
pub enum NostrError {
    #[error("invalid pubkey format: {0}")]
    InvalidPubkey(String),
}

impl From<NostrError> for RpcError {
    fn from(err: NostrError) -> Self {
        RpcError::InvalidParams(err.to_string())
    }
}

pub fn parse_pubkey(s: &str) -> Result<PublicKey, NostrError> {
    PublicKey::from_bech32(s)
        .or_else(|_| PublicKey::from_hex(s))
        .map_err(|_| NostrError::InvalidPubkey(s.to_string()))
}

pub fn parse_pubkeys(input: &[String]) -> Result<Vec<PublicKey>, RpcError> {
    input
        .iter()
        .map(|s| parse_pubkey(s).map_err(Into::into))
        .collect()
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
