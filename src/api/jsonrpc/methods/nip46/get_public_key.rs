use std::time::Duration;

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::params::DEFAULT_TIMEOUT_SECS;
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use crate::nip46::session::Nip46Session;
use radroots_nostr::prelude::{
    radroots_nostr_filter_tag,
    RadrootsNostrEventBuilder,
    RadrootsNostrFilter,
    RadrootsNostrKind,
    RadrootsNostrPublicKey,
};
use nostr::nips::{
    nip44,
    nip46::{NostrConnectMessage, NostrConnectMethod, NostrConnectRequest, ResponseResult},
};
use nostr::JsonUtil;

#[derive(Debug, Deserialize)]
struct Nip46GetPublicKeyParams {
    session_id: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46GetPublicKeyResponse {
    pubkey: String,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.get_public_key");
    m.register_async_method("nip46.get_public_key", |params, ctx, _| async move {
        let Nip46GetPublicKeyParams { session_id } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let session = ctx
            .state
            .nip46_sessions
            .get(&session_id)
            .await
            .ok_or_else(|| RpcError::InvalidParams("unknown session".to_string()))?;
        let pubkey = request_public_key(&session).await?;
        Ok::<Nip46GetPublicKeyResponse, RpcError>(Nip46GetPublicKeyResponse {
            pubkey: pubkey.to_hex(),
        })
    })?;
    Ok(())
}

async fn request_public_key(
    session: &Nip46Session,
) -> Result<RadrootsNostrPublicKey, RpcError> {
    session.client.connect().await;

    let request = NostrConnectRequest::GetPublicKey;
    let message = NostrConnectMessage::request(&request);
    let request_id = message.id().to_string();
    let event = RadrootsNostrEventBuilder::nostr_connect(
        &session.client_keys,
        session.remote_signer_pubkey.clone(),
        message,
    )
    .map_err(|e| RpcError::Other(format!("nip46 get_public_key failed: {e}")))?;

    session
        .client
        .send_event_builder(event)
        .await
        .map_err(|e| RpcError::Other(format!("nip46 get_public_key failed: {e}")))?;

    let response = wait_for_response(session, &request_id).await?;
    let response = response
        .to_response(NostrConnectMethod::GetPublicKey)
        .map_err(|e| RpcError::Other(format!("nip46 get_public_key failed: {e}")))?;

    if let Some(error) = response.error {
        return Err(RpcError::Other(format!("nip46 get_public_key error: {error}")));
    }

    match response.result {
        Some(ResponseResult::GetPublicKey(pubkey)) => Ok(pubkey),
        Some(_) => Err(RpcError::Other(
            "nip46 get_public_key unexpected response".to_string(),
        )),
        None => Err(RpcError::Other(
            "nip46 get_public_key missing response".to_string(),
        )),
    }
}

async fn wait_for_response(
    session: &Nip46Session,
    request_id: &str,
) -> Result<NostrConnectMessage, RpcError> {
    let filter = RadrootsNostrFilter::new()
        .kind(RadrootsNostrKind::NostrConnect)
        .author(session.remote_signer_pubkey.clone());
    let filter = radroots_nostr_filter_tag(filter, "p", vec![session.client_pubkey.to_hex()])
        .map_err(|e| RpcError::Other(format!("nip46 get_public_key failed: {e}")))?;

    let events = session
        .client
        .fetch_events(filter, Duration::from_secs(DEFAULT_TIMEOUT_SECS))
        .await
        .map_err(|e| RpcError::Other(format!("nip46 get_public_key failed: {e}")))?;

    for event in events {
        let decrypted = nip44::decrypt(
            session.client_keys.secret_key(),
            &session.remote_signer_pubkey,
            &event.content,
        )
        .map_err(|e| RpcError::Other(format!("nip46 get_public_key failed: {e}")))?;
        let message = NostrConnectMessage::from_json(&decrypted)
            .map_err(|e| RpcError::Other(format!("nip46 get_public_key failed: {e}")))?;
        if message.is_response() && message.id() == request_id {
            return Ok(message);
        }
    }

    Err(RpcError::Other(
        "nip46 get_public_key response not found".to_string(),
    ))
}
