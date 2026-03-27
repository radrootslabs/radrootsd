use radroots_nostr::prelude::{
    RadrootsNostrClient, RadrootsNostrKeys, RadrootsNostrMetadata, RadrootsNostrPublicKey,
};

use crate::app::config::{BridgeConfig, Nip46Config};

#[derive(Clone)]
pub struct Radrootsd {
    pub client: RadrootsNostrClient,
    pub keys: RadrootsNostrKeys,
    pub pubkey: RadrootsNostrPublicKey,
    pub metadata: RadrootsNostrMetadata,
    pub info: serde_json::Value,
    pub(crate) bridge_jobs: crate::core::bridge::store::BridgeJobStore,
    pub bridge_config: BridgeConfig,
    pub(crate) nip46_sessions: crate::core::nip46::session::Nip46SessionStore,
    pub nip46_config: Nip46Config,
}

impl Radrootsd {
    pub fn new(
        keys: RadrootsNostrKeys,
        metadata: RadrootsNostrMetadata,
        bridge_config: BridgeConfig,
        nip46_config: Nip46Config,
    ) -> Self {
        let pubkey = keys.public_key();
        let client = RadrootsNostrClient::new(keys.clone());
        let info = serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "build": option_env!("GIT_HASH").unwrap_or("unknown"),
        });
        let bridge_jobs =
            crate::core::bridge::store::BridgeJobStore::new(bridge_config.job_status_retention);
        let nip46_sessions = crate::core::nip46::session::Nip46SessionStore::new();

        Self {
            client,
            keys,
            pubkey,
            metadata,
            info,
            bridge_jobs,
            bridge_config,
            nip46_sessions,
            nip46_config,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Radrootsd;
    use crate::app::config::{BridgeConfig, Nip46Config};
    use radroots_nostr::prelude::{RadrootsNostrKeys, RadrootsNostrMetadata};

    #[test]
    fn new_sets_core_fields() {
        let keys = RadrootsNostrKeys::generate();
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let bridge_cfg = BridgeConfig::default();
        let cfg = Nip46Config::default();
        let state = Radrootsd::new(
            keys.clone(),
            metadata.clone(),
            bridge_cfg.clone(),
            cfg.clone(),
        );

        assert_eq!(state.pubkey, keys.public_key());
        assert_eq!(state.metadata, metadata);
        assert_eq!(state.bridge_config.enabled, bridge_cfg.enabled);
        assert_eq!(
            state.bridge_jobs.snapshot().capacity,
            bridge_cfg.job_status_retention
        );
        assert_eq!(state.nip46_config.session_ttl_secs, cfg.session_ttl_secs);
        assert_eq!(state.nip46_config.perms, cfg.perms);
        assert_eq!(state.info["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(state.info["build"], "unknown");
    }
}
