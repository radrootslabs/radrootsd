use std::time::Duration;

use anyhow::{anyhow, Result};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::core::state::Radrootsd;
use radroots_nostr::prelude::{
    radroots_nostr_filter_tag,
    RadrootsNostrFilter,
    RadrootsNostrKind,
    RadrootsNostrRelayPoolNotification,
    RadrootsNostrTimestamp,
};

const DEFAULT_TIMEOUT_SECS: u64 = 10;

pub fn spawn_nip46_listener(radrootsd: Radrootsd) {
    tokio::spawn(async move {
        if let Err(error) = run_nip46_listener(radrootsd).await {
            warn!("NIP-46 listener stopped: {error}");
        }
    });
}

async fn run_nip46_listener(radrootsd: Radrootsd) -> Result<()> {
    radrootsd.client.connect().await;
    radrootsd
        .client
        .wait_for_connection(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
        .await;

    let filter = RadrootsNostrFilter::new()
        .kind(RadrootsNostrKind::NostrConnect)
        .since(RadrootsNostrTimestamp::now());
    let filter =
        radroots_nostr_filter_tag(filter, "p", vec![radrootsd.pubkey.to_hex()])?;
    let mut notifications = radrootsd.client.notifications();
    let subscription = radrootsd.client.subscribe(filter, None).await?;

    info!("NIP-46 listener subscribed: {}", subscription.val);

    loop {
        let notification = match notifications.recv().await {
            Ok(notification) => notification,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => {
                return Err(anyhow!("nip46 listener notification closed"));
            }
        };
        let RadrootsNostrRelayPoolNotification::Event { event, .. } = notification else {
            continue;
        };
        let event = (*event).clone();
        if event.kind != RadrootsNostrKind::NostrConnect {
            continue;
        }
        info!("NIP-46 request received: {}", event.id);
    }
}
