use std::time::Instant;

use radroots_nostr::prelude::{
    RadrootsNostrClient,
    RadrootsNostrKeys,
    RadrootsNostrMetadata,
    RadrootsNostrPublicKey,
};

#[derive(Clone)]
pub struct Radrootsd {
    pub(crate) started: Instant,
    pub client: RadrootsNostrClient,
    pub keys: RadrootsNostrKeys,
    pub pubkey: RadrootsNostrPublicKey,
    pub metadata: RadrootsNostrMetadata,
    pub info: serde_json::Value,
    pub(crate) nip46_sessions: crate::core::nip46::session::Nip46SessionStore,
}

impl Radrootsd {
    pub fn new(keys: RadrootsNostrKeys, metadata: RadrootsNostrMetadata) -> Self {
        let pubkey = keys.public_key();
        let client = RadrootsNostrClient::new(keys.clone());
        let info = serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "build": option_env!("GIT_HASH").unwrap_or("unknown"),
        });
        let nip46_sessions = crate::core::nip46::session::Nip46SessionStore::new();

        Self {
            started: Instant::now(),
            client,
            keys,
            pubkey,
            metadata,
            info,
            nip46_sessions,
        }
    }
}
