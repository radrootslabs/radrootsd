#![forbid(unsafe_code)]

use std::time::Duration;

use crate::core::nip46::session::Nip46Session;
use crate::transport::jsonrpc::{RpcError, params::DEFAULT_TIMEOUT_SECS};
use nostr::JsonUtil;
use nostr::UnsignedEvent;
use nostr::nips::{
    nip44,
    nip46::{NostrConnectMessage, NostrConnectMethod, NostrConnectRequest, ResponseResult},
};
use radroots_nostr::prelude::{
    RadrootsNostrEventBuilder, RadrootsNostrFilter, RadrootsNostrKind,
    RadrootsNostrRelayPoolNotification, RadrootsNostrSubscriptionId, RadrootsNostrTimestamp,
    radroots_nostr_filter_tag,
};
use tokio::sync::broadcast;
use tokio::time::sleep;

pub async fn sign_event(
    session: &Nip46Session,
    unsigned: UnsignedEvent,
    label: &str,
) -> Result<nostr::Event, RpcError> {
    let req = NostrConnectRequest::SignEvent(unsigned);
    let response = request(session, req, label).await?;
    let response = response
        .to_response(NostrConnectMethod::SignEvent)
        .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;

    if let Some(error) = response.error {
        return Err(RpcError::Other(format!("nip46 {label} error: {error}")));
    }

    let event = match response.result {
        Some(ResponseResult::SignEvent(event)) => *event,
        Some(_) => {
            return Err(RpcError::Other(format!(
                "nip46 {label} unexpected response"
            )));
        }
        None => return Err(RpcError::Other(format!("nip46 {label} missing response"))),
    };

    event
        .verify()
        .map_err(|e| RpcError::Other(format!("nip46 {label} invalid event: {e}")))?;

    Ok(event)
}

pub async fn request(
    session: &Nip46Session,
    request: NostrConnectRequest,
    label: &str,
) -> Result<NostrConnectMessage, RpcError> {
    session.client.connect().await;
    session
        .client
        .wait_for_connection(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
        .await;

    let message = NostrConnectMessage::request(&request);
    let request_id = message.id().to_string();
    let filter = response_filter(session, RadrootsNostrTimestamp::now(), label)?;
    let notifications = session.client.notifications();
    let subscription = session
        .client
        .subscribe(filter, None)
        .await
        .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;
    let event = RadrootsNostrEventBuilder::nostr_connect(
        &session.client_keys,
        session.remote_signer_pubkey.clone(),
        message,
    )
    .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;

    if let Err(error) = session
        .client
        .send_event_builder(event)
        .await
        .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))
    {
        session.client.unsubscribe(&subscription.val).await;
        return Err(error);
    }

    wait_for_response(
        session,
        &request_id,
        label,
        notifications,
        &subscription.val,
    )
    .await
}

fn response_filter(
    session: &Nip46Session,
    since: RadrootsNostrTimestamp,
    label: &str,
) -> Result<RadrootsNostrFilter, RpcError> {
    let filter = RadrootsNostrFilter::new()
        .kind(RadrootsNostrKind::NostrConnect)
        .author(session.remote_signer_pubkey.clone())
        .since(since);
    radroots_nostr_filter_tag(filter, "p", vec![session.client_pubkey.to_hex()])
        .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))
}

async fn wait_for_response(
    session: &Nip46Session,
    request_id: &str,
    label: &str,
    mut notifications: broadcast::Receiver<RadrootsNostrRelayPoolNotification>,
    subscription_id: &RadrootsNostrSubscriptionId,
) -> Result<NostrConnectMessage, RpcError> {
    let timeout = sleep(Duration::from_secs(DEFAULT_TIMEOUT_SECS));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => {
                session.client.unsubscribe(subscription_id).await;
                return Err(RpcError::Other(format!("nip46 {label} response not found")));
            }
            msg = notifications.recv() => {
                let notification = match msg {
                    Ok(notification) => notification,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        session.client.unsubscribe(subscription_id).await;
                        return Err(RpcError::Other(format!("nip46 {label} notification closed")));
                    }
                };
                let RadrootsNostrRelayPoolNotification::Event { event, .. } = notification else {
                    continue;
                };
                let event = (*event).clone();
                if event.kind != RadrootsNostrKind::NostrConnect
                    || event.pubkey != session.remote_signer_pubkey
                {
                    continue;
                }
                let decrypted = nip44::decrypt(
                    session.client_keys.secret_key(),
                    &session.remote_signer_pubkey,
                    &event.content,
                )
                .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;
                let message = NostrConnectMessage::from_json(&decrypted)
                    .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;
                if message.is_response() && message.id() == request_id {
                    session.client.unsubscribe(subscription_id).await;
                    return Ok(message);
                }
            }
        }
    }
}
