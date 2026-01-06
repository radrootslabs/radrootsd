use std::time::Duration;

use anyhow::{anyhow, Result};
use nostr::nips::nip44;
use nostr::nips::nip46::{NostrConnectMessage, NostrConnectRequest, NostrConnectResponse, ResponseResult};
use nostr::JsonUtil;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::core::state::Radrootsd;
use radroots_nostr::prelude::{
    radroots_nostr_filter_tag,
    RadrootsNostrEventBuilder,
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

        let decrypted = match nip44::decrypt(
            radrootsd.keys.secret_key(),
            &event.pubkey,
            &event.content,
        ) {
            Ok(value) => value,
            Err(err) => {
                warn!("NIP-46 decrypt failed: {err}");
                continue;
            }
        };
        let message = match NostrConnectMessage::from_json(&decrypted) {
            Ok(value) => value,
            Err(err) => {
                warn!("NIP-46 parse failed: {err}");
                continue;
            }
        };
        if !message.is_request() {
            continue;
        }

        let request_id = message.id().to_string();
        let request = match message.to_request() {
            Ok(value) => value,
            Err(err) => {
                warn!("NIP-46 request invalid: {err}");
                continue;
            }
        };
        let response = handle_request(&radrootsd, request);
        let response_message = NostrConnectMessage::response(request_id, response);
        let response_event = RadrootsNostrEventBuilder::nostr_connect(
            &radrootsd.keys,
            event.pubkey,
            response_message,
        )
        .map_err(|err| anyhow!("nip46 response build failed: {err}"))?;
        let _ = radrootsd.client.send_event_builder(response_event).await;
    }
}

fn handle_request(radrootsd: &Radrootsd, request: NostrConnectRequest) -> NostrConnectResponse {
    match request {
        NostrConnectRequest::Connect {
            remote_signer_public_key,
            ..
        } => {
            if remote_signer_public_key != radrootsd.pubkey {
                return NostrConnectResponse::with_error("remote signer pubkey mismatch");
            }
            NostrConnectResponse::with_result(ResponseResult::Ack)
        }
        NostrConnectRequest::GetPublicKey => {
            NostrConnectResponse::with_result(ResponseResult::GetPublicKey(radrootsd.pubkey))
        }
        NostrConnectRequest::Ping => NostrConnectResponse::with_result(ResponseResult::Pong),
        _ => NostrConnectResponse::with_error("unsupported request"),
    }
}
