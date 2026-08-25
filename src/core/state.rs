use crate::app::identity_storage::DaemonIdentity;
use crate::host_nostr::{DaemonNostrClient, Keys, PublicKey};
use anyhow::Result;

use crate::app::config::{Nip46Config, TransportPublishConfig};
use crate::core::transport_publish::TransportPublish;

#[derive(Clone)]
pub struct Radrootsd {
    pub(crate) client: DaemonNostrClient,
    pub keys: Keys,
    pub pubkey: PublicKey,
    pub transport_publish: TransportPublish,
    pub(crate) nip46_sessions: crate::core::nip46::session::Nip46SessionStore,
    pub nip46_config: Nip46Config,
}

impl Radrootsd {
    pub(crate) fn new(
        identity: DaemonIdentity,
        transport_publish_config: TransportPublishConfig,
        nip46_config: Nip46Config,
    ) -> Result<Self> {
        let keys: Keys = identity.keys().clone();
        let pubkey = keys.public_key();
        let client = DaemonNostrClient::with_keys(keys.clone());
        #[cfg(test)]
        let transport_publish = TransportPublish::memory(transport_publish_config)?;
        #[cfg(not(test))]
        let transport_publish = TransportPublish::open(transport_publish_config)?;
        let nip46_sessions = crate::core::nip46::session::Nip46SessionStore::new();

        Ok(Self {
            client,
            keys,
            pubkey,
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
    use crate::app::identity_storage::DaemonIdentity;

    #[test]
    fn new_sets_core_fields() {
        let identity = DaemonIdentity::generate();
        let transport_publish_cfg = TransportPublishConfig::default();
        let cfg = Nip46Config::default();
        let state = Radrootsd::new(identity.clone(), transport_publish_cfg.clone(), cfg.clone())
            .expect("state");

        assert_eq!(state.pubkey, identity.public_key());
        assert_eq!(
            state.transport_publish.config.enabled,
            transport_publish_cfg.enabled
        );
        assert_eq!(state.nip46_config.session_ttl_secs, cfg.session_ttl_secs);
        assert_eq!(state.nip46_config.perms, cfg.perms);
    }
}
