#![forbid(unsafe_code)]

use std::time::Duration;

use crate::core::nip46::session::Nip46Session;
use crate::transport::jsonrpc::{RpcError, params::DEFAULT_TIMEOUT_SECS};
use crate::transport::nostr::protocol::sign_nip46_message;
use nostr::JsonUtil;
use nostr::UnsignedEvent;
use nostr::nips::{
    nip44,
    nip46::{NostrConnectMessage, NostrConnectMethod, NostrConnectRequest, ResponseResult},
};
use radroots_nostr::prelude::{
    RadrootsNostrFilter, RadrootsNostrKind, RadrootsNostrRelayPoolNotification,
    RadrootsNostrSubscriptionId, RadrootsNostrTimestamp, radroots_nostr_filter_tag,
};
use tokio::sync::broadcast;
use tokio::time::sleep;

pub async fn sign_event(
    session: &Nip46Session,
    mut unsigned: UnsignedEvent,
    label: &str,
) -> Result<nostr::Event, RpcError> {
    unsigned.verify_id().map_err(|_| {
        RpcError::InvalidParams(format!("nip46 {label} unsigned event ID mismatch"))
    })?;
    let expected_public_key = unsigned.pubkey;
    let expected_event_id = unsigned.id();
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

    validate_signed_event_response(expected_public_key, expected_event_id, event, label)
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
    let notifications = session.client.clone().into_inner().notifications();
    let subscription = session
        .client
        .subscribe(filter, None)
        .await
        .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;
    let event = sign_nip46_message(&session.client_keys, session.remote_signer_pubkey, message)
        .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;

    if let Err(error) = session
        .client
        .send_event(&event)
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

fn validate_signed_event_response(
    expected_public_key: nostr::PublicKey,
    expected_event_id: nostr::EventId,
    event: nostr::Event,
    label: &str,
) -> Result<nostr::Event, RpcError> {
    if event.pubkey != expected_public_key {
        return Err(RpcError::Other(format!(
            "nip46 {label} response author mismatch"
        )));
    }
    if event.id != expected_event_id {
        return Err(RpcError::Other(format!(
            "nip46 {label} response event ID mismatch"
        )));
    }
    event
        .verify()
        .map_err(|_| RpcError::Other(format!("nip46 {label} response event is invalid")))?;
    Ok(event)
}

fn response_filter(
    session: &Nip46Session,
    since: RadrootsNostrTimestamp,
    label: &str,
) -> Result<RadrootsNostrFilter, RpcError> {
    let filter = RadrootsNostrFilter::new()
        .kind(RadrootsNostrKind::NostrConnect)
        .author(session.remote_signer_pubkey)
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

#[cfg(test)]
mod tests {
    use nostr::{EventBuilder, EventId, Kind, Timestamp};
    use radroots_nostr::prelude::RadrootsNostrKeys;

    use super::validate_signed_event_response;

    fn signed_fixture() -> (nostr::UnsignedEvent, nostr::Event) {
        let keys = RadrootsNostrKeys::generate();
        let unsigned = EventBuilder::new(Kind::Custom(30_001), "checked")
            .custom_created_at(Timestamp::from_secs(1_784_347_200))
            .build(keys.public_key());
        let event = unsigned
            .clone()
            .sign_with_keys(&keys)
            .expect("signed fixture");
        (unsigned, event)
    }

    #[test]
    fn sign_event_response_accepts_only_the_exact_valid_event() {
        let (mut unsigned, event) = signed_fixture();
        let expected_public_key = unsigned.pubkey;
        let expected_event_id = unsigned.id();
        assert!(
            validate_signed_event_response(
                expected_public_key,
                expected_event_id,
                event.clone(),
                "test",
            )
            .is_ok()
        );

        let (_, wrong_author) = signed_fixture();
        let error = validate_signed_event_response(
            expected_public_key,
            expected_event_id,
            wrong_author,
            "test",
        )
        .expect_err("wrong author");
        assert!(error.to_string().contains("author mismatch"));

        let mut wrong_id = event.clone();
        wrong_id.id = EventId::all_zeros();
        let error = validate_signed_event_response(
            expected_public_key,
            expected_event_id,
            wrong_id,
            "test",
        )
        .expect_err("wrong event ID");
        assert!(error.to_string().contains("event ID mismatch"));

        let mut wrong_signature = event;
        wrong_signature.content.push('!');
        let error = validate_signed_event_response(
            expected_public_key,
            expected_event_id,
            wrong_signature,
            "test",
        )
        .expect_err("invalid event");
        assert!(error.to_string().contains("event is invalid"));
    }
}
