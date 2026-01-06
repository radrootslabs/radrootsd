#![forbid(unsafe_code)]

use std::time::Duration;

use crate::api::jsonrpc::{DEFAULT_TIMEOUT_SECS, RpcError};
use crate::nip46::session::Nip46Session;
use radroots_nostr::prelude::{
    radroots_nostr_filter_tag,
    RadrootsNostrEventBuilder,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};
use nostr::nips::{nip44, nip46::NostrConnectMessage, nip46::NostrConnectRequest};
use nostr::JsonUtil;

pub async fn request(
    session: &Nip46Session,
    request: NostrConnectRequest,
    label: &str,
) -> Result<NostrConnectMessage, RpcError> {
    let request_id = send_request(session, request, label).await?;
    wait_for_response(session, &request_id, label).await
}

async fn send_request(
    session: &Nip46Session,
    request: NostrConnectRequest,
    label: &str,
) -> Result<String, RpcError> {
    session.client.connect().await;

    let message = NostrConnectMessage::request(&request);
    let request_id = message.id().to_string();
    let event = RadrootsNostrEventBuilder::nostr_connect(
        &session.client_keys,
        session.remote_signer_pubkey.clone(),
        message,
    )
    .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;

    session
        .client
        .send_event_builder(event)
        .await
        .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;

    Ok(request_id)
}

async fn wait_for_response(
    session: &Nip46Session,
    request_id: &str,
    label: &str,
) -> Result<NostrConnectMessage, RpcError> {
    let filter = RadrootsNostrFilter::new()
        .kind(RadrootsNostrKind::NostrConnect)
        .author(session.remote_signer_pubkey.clone());
    let filter = radroots_nostr_filter_tag(filter, "p", vec![session.client_pubkey.to_hex()])
        .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;

    let events = session
        .client
        .fetch_events(filter, Duration::from_secs(DEFAULT_TIMEOUT_SECS))
        .await
        .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;

    for event in events {
        let decrypted = nip44::decrypt(
            session.client_keys.secret_key(),
            &session.remote_signer_pubkey,
            &event.content,
        )
        .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;
        let message = NostrConnectMessage::from_json(&decrypted)
            .map_err(|e| RpcError::Other(format!("nip46 {label} failed: {e}")))?;
        if message.is_response() && message.id() == request_id {
            return Ok(message);
        }
    }

    Err(RpcError::Other(format!(
        "nip46 {label} response not found"
    )))
}
