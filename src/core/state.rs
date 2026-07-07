use anyhow::Result;
use radroots_identity::RadrootsIdentity;
use radroots_nostr::prelude::{
    RadrootsNostrClient, RadrootsNostrKeys, RadrootsNostrMetadata, RadrootsNostrPublicKey,
};

use crate::app::config::{Nip46Config, TransportPublishConfig};
use crate::core::transport_publish::TransportPublish;

#[derive(Clone)]
pub struct Radrootsd {
    pub client: RadrootsNostrClient,
    pub keys: RadrootsNostrKeys,
    pub pubkey: RadrootsNostrPublicKey,
    pub metadata: RadrootsNostrMetadata,
    pub info: serde_json::Value,
    pub transport_publish: TransportPublish,
    pub(crate) nip46_sessions: crate::core::nip46::session::Nip46SessionStore,
    pub nip46_config: Nip46Config,
}

impl Radrootsd {
    pub fn new(
        identity: RadrootsIdentity,
        metadata: RadrootsNostrMetadata,
        transport_publish_config: TransportPublishConfig,
        nip46_config: Nip46Config,
    ) -> Result<Self> {
        let keys: RadrootsNostrKeys = identity.keys().clone();
        let pubkey = keys.public_key();
        let client = RadrootsNostrClient::new(keys.clone());
        let info = serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "build": option_env!("GIT_HASH").unwrap_or("unknown"),
        });
        #[cfg(test)]
        let transport_publish = TransportPublish::memory(transport_publish_config)?;
        #[cfg(not(test))]
        let transport_publish = TransportPublish::open(transport_publish_config)?;
        let nip46_sessions = crate::core::nip46::session::Nip46SessionStore::new();

        Ok(Self {
            client,
            keys,
            pubkey,
            metadata,
            info,
            transport_publish,
            nip46_sessions,
            nip46_config,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Radrootsd;
    use crate::app::config::{Nip46Config, TransportPublishConfig};
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::RadrootsNostrMetadata;

    #[test]
    fn new_sets_core_fields() {
        let identity = RadrootsIdentity::generate();
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let transport_publish_cfg = TransportPublishConfig::default();
        let cfg = Nip46Config::default();
        let state = Radrootsd::new(
            identity.clone(),
            metadata.clone(),
            transport_publish_cfg.clone(),
            cfg.clone(),
        )
        .expect("state");

        assert_eq!(state.pubkey, identity.public_key());
        assert_eq!(state.metadata, metadata);
        assert_eq!(
            state.transport_publish.config.enabled,
            transport_publish_cfg.enabled
        );
        assert_eq!(state.nip46_config.session_ttl_secs, cfg.session_ttl_secs);
        assert_eq!(state.nip46_config.perms, cfg.perms);
        assert_eq!(state.info["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(state.info["build"], "unknown");
    }
}
