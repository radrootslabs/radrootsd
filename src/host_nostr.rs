//! Daemon-owned Nostr client adapter.
//!
//! Radroots protocol crates deliberately do not own relay clients, process
//! lifecycle, or service keys. This private adapter keeps those host concerns
//! in `radrootsd` while using upstream protocol values at the explicit edge.

use core::time::Duration;

pub(crate) use nostr::{
    Event, Filter, Keys, Kind, Metadata, PublicKey, SecretKey, SubscriptionId, Timestamp,
};
pub(crate) use nostr_sdk::RelayPoolNotification;
pub(crate) use radroots_nostr::event::{
    ApplicationHandlerSpec, build_application_handler, build_profile,
};

#[derive(Clone)]
pub(crate) struct DaemonNostrClient {
    inner: nostr_sdk::Client,
}

impl DaemonNostrClient {
    pub(crate) fn signerless() -> Self {
        let inner = nostr_sdk::Client::default();
        inner.automatic_authentication(false);
        Self { inner }
    }

    pub(crate) fn with_keys(keys: Keys) -> Self {
        let inner = nostr_sdk::Client::new(keys);
        inner.automatic_authentication(false);
        Self { inner }
    }

    #[cfg(test)]
    pub(crate) fn from_identity(identity: &crate::app::identity_storage::DaemonIdentity) -> Self {
        Self::with_keys(identity.keys().clone())
    }

    pub(crate) fn into_inner(self) -> nostr_sdk::Client {
        self.inner
    }

    pub(crate) async fn connect(&self) {
        self.inner.connect().await;
    }

    pub(crate) async fn wait_for_connection(&self, timeout: Duration) {
        self.inner.wait_for_connection(timeout).await;
    }

    pub(crate) async fn add_relay(&self, url: &str) -> Result<bool, nostr_sdk::client::Error> {
        self.inner.add_relay(url).await
    }

    pub(crate) async fn add_read_relay(&self, url: &str) -> Result<bool, nostr_sdk::client::Error> {
        self.inner.add_read_relay(url).await
    }

    pub(crate) async fn relays(
        &self,
    ) -> std::collections::HashMap<nostr::RelayUrl, nostr_sdk::Relay> {
        self.inner.relays().await
    }

    pub(crate) async fn fetch_events(
        &self,
        filter: Filter,
        timeout: Duration,
    ) -> Result<Vec<Event>, nostr_sdk::client::Error> {
        self.inner
            .fetch_events(filter, timeout)
            .await
            .map(|events| events.to_vec())
    }

    pub(crate) async fn subscribe(
        &self,
        filter: Filter,
        options: Option<nostr_sdk::SubscribeAutoCloseOptions>,
    ) -> Result<nostr_sdk::prelude::Output<SubscriptionId>, nostr_sdk::client::Error> {
        self.inner.subscribe(filter, options).await
    }

    pub(crate) async fn unsubscribe(&self, subscription_id: &SubscriptionId) {
        self.inner.unsubscribe(subscription_id).await;
    }

    pub(crate) async fn send_event(
        &self,
        event: &Event,
    ) -> Result<nostr_sdk::prelude::Output<nostr::EventId>, nostr_sdk::client::Error> {
        self.inner.send_event(event).await
    }
}

pub(crate) fn parse_public_key(value: &str) -> Result<PublicKey, radroots_nostr::Error> {
    radroots_nostr::key::parse_public_key(value).and_then(radroots_nostr::key::public_key_to_nostr)
}

pub(crate) fn with_filter_tag(
    filter: Filter,
    tag: &str,
    values: Vec<String>,
) -> Result<Filter, radroots_nostr::Error> {
    radroots_nostr::filter::with_tag(filter, tag, values)
}
