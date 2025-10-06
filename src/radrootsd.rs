use nostr_sdk::Client;
use std::time::Instant;

#[derive(Clone)]
pub struct Radrootsd {
    pub(crate) started: Instant,
    pub client: Client,
    pub pubkey: nostr::PublicKey,
    pub metadata: nostr::Metadata,
    pub info: serde_json::Value,
}

impl Radrootsd {
    pub fn new(keys: nostr::Keys, metadata: nostr::Metadata) -> Self {
        let pubkey = keys.public_key();
        let client = Client::new(keys);
        let info = serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "build": option_env!("GIT_HASH").unwrap_or("unknown"),
        });

        Self {
            started: Instant::now(),
            client,
            pubkey,
            metadata,
            info,
        }
    }
}
