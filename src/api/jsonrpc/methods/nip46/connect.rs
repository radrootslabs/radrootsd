use std::time::Duration;

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::time::sleep;
use uuid::Uuid;

use crate::api::jsonrpc::params::DEFAULT_TIMEOUT_SECS;
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use crate::nip46::connection::{parse_connect_url, Nip46ConnectInfo, Nip46ConnectMode};
use crate::nip46::session::Nip46Session;
use radroots_nostr::prelude::{
    radroots_nostr_filter_tag,
    radroots_nostr_parse_pubkey,
    RadrootsNostrClient,
    RadrootsNostrEventBuilder,
    RadrootsNostrFilter,
    RadrootsNostrKind,
    RadrootsNostrKeys,
    RadrootsNostrPublicKey,
    RadrootsNostrRelayPoolNotification,
    RadrootsNostrTimestamp,
};
use nostr::nips::{nip44, nip46::NostrConnectMessage, nip46::NostrConnectRequest};
use nostr::JsonUtil;

#[derive(Debug, Deserialize)]
struct Nip46ConnectParams {
    url: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46ConnectResponse {
    session_id: String,
    mode: Nip46ConnectMode,
    remote_signer_pubkey: String,
    client_pubkey: String,
    relays: Vec<String>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.connect");
    m.register_async_method("nip46.connect", |params, ctx, _| async move {
        let Nip46ConnectParams { url } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let response = connect_nip46(ctx.as_ref().clone(), url).await?;
        Ok::<Nip46ConnectResponse, RpcError>(response)
    })?;
    Ok(())
}

async fn connect_nip46(ctx: RpcContext, url: String) -> Result<Nip46ConnectResponse, RpcError> {
    let info = parse_connect_url(&url)?;
    match info.mode {
        Nip46ConnectMode::Bunker => connect_bunker(ctx, info).await,
        Nip46ConnectMode::Nostrconnect => Err(RpcError::InvalidParams(
            "nostrconnect mode not supported yet".to_string(),
        )),
    }
}

async fn connect_bunker(
    ctx: RpcContext,
    info: Nip46ConnectInfo,
) -> Result<Nip46ConnectResponse, RpcError> {
    if info.relays.is_empty() {
        return Err(RpcError::InvalidParams("missing relay".to_string()));
    }

    let remote_signer_raw = info.remote_signer_pubkey.as_ref().ok_or_else(|| {
        RpcError::InvalidParams("missing remote signer pubkey".to_string())
    })?;
    let remote_signer_pubkey = radroots_nostr_parse_pubkey(remote_signer_raw)
        .map_err(|e| RpcError::InvalidParams(format!("invalid remote signer: {e}")))?;

    let client_keys = RadrootsNostrKeys::generate();
    let client_pubkey = client_keys.public_key();
    let client = RadrootsNostrClient::new(client_keys.clone());

    add_relays(&client, &info.relays).await?;
    client.connect().await;
    client
        .wait_for_connection(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
        .await;

    let request_id = send_connect_request(
        &client,
        &client_keys,
        &remote_signer_pubkey,
        info.secret.as_deref(),
    )
    .await?;

    let response = wait_for_connect_response(
        &client,
        &client_keys,
        &remote_signer_pubkey,
        &client_pubkey,
        &request_id,
    )
    .await?;

    validate_connect_response(&response, info.secret.as_deref())?;

    let session_id = Uuid::new_v4().to_string();
    let session = Nip46Session {
        id: session_id.clone(),
        client,
        client_keys,
        client_pubkey,
        remote_signer_pubkey,
        user_pubkey: None,
        relays: info.relays.clone(),
    };
    ctx.state.nip46_sessions.insert(session).await;

    Ok(Nip46ConnectResponse {
        session_id,
        mode: info.mode,
        remote_signer_pubkey: remote_signer_raw.to_string(),
        client_pubkey: client_pubkey.to_hex(),
        relays: info.relays,
    })
}

async fn add_relays(
    client: &RadrootsNostrClient,
    relays: &[String],
) -> Result<(), RpcError> {
    for relay in relays {
        client
            .add_relay(relay)
            .await
            .map_err(|e| RpcError::AddRelay(relay.to_string(), e.to_string()))?;
    }
    Ok(())
}

async fn send_connect_request(
    client: &RadrootsNostrClient,
    client_keys: &RadrootsNostrKeys,
    remote_signer_pubkey: &RadrootsNostrPublicKey,
    secret: Option<&str>,
) -> Result<String, RpcError> {
    let request = NostrConnectRequest::Connect {
        remote_signer_public_key: remote_signer_pubkey.clone(),
        secret: secret.map(str::to_string),
    };
    let message = NostrConnectMessage::request(&request);
    let request_id = message.id().to_string();
    let event = RadrootsNostrEventBuilder::nostr_connect(
        client_keys,
        remote_signer_pubkey.clone(),
        message,
    )
    .map_err(|e| RpcError::Other(format!("nip46 connect request failed: {e}")))?;
    client
        .send_event_builder(event)
        .await
        .map_err(|e| RpcError::Other(format!("nip46 connect request failed: {e}")))?;
    Ok(request_id)
}

async fn wait_for_connect_response(
    client: &RadrootsNostrClient,
    client_keys: &RadrootsNostrKeys,
    remote_signer_pubkey: &RadrootsNostrPublicKey,
    client_pubkey: &RadrootsNostrPublicKey,
    request_id: &str,
) -> Result<NostrConnectMessage, RpcError> {
    let filter = RadrootsNostrFilter::new()
        .kind(RadrootsNostrKind::NostrConnect)
        .author(remote_signer_pubkey.clone())
        .since(RadrootsNostrTimestamp::now());
    let filter = radroots_nostr_filter_tag(filter, "p", vec![client_pubkey.to_hex()])
        .map_err(|e| RpcError::Other(format!("nip46 connect filter failed: {e}")))?;
    let mut notifications = client.notifications();
    let subscription = client
        .subscribe(filter, None)
        .await
        .map_err(|e| RpcError::Other(format!("nip46 connect failed: {e}")))?;
    let timeout = sleep(Duration::from_secs(DEFAULT_TIMEOUT_SECS));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => {
                client.unsubscribe(&subscription.val).await;
                return Err(RpcError::Other("nip46 connect response not found".to_string()));
            }
            msg = notifications.recv() => {
                let notification = match msg {
                    Ok(notification) => notification,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(RpcError::Other("nip46 connect notification closed".to_string()));
                    }
                };
                let RadrootsNostrRelayPoolNotification::Event { event, .. } = notification else {
                    continue;
                };
                let event = (*event).clone();
                if event.kind != RadrootsNostrKind::NostrConnect || &event.pubkey != remote_signer_pubkey {
                    continue;
                }
                let decrypted = nip44::decrypt(
                    client_keys.secret_key(),
                    remote_signer_pubkey,
                    &event.content,
                )
                .map_err(|e| RpcError::Other(format!("nip46 connect decrypt failed: {e}")))?;
                let message = NostrConnectMessage::from_json(&decrypted)
                    .map_err(|e| RpcError::Other(format!("nip46 connect response parse failed: {e}")))?;
                if message.is_response() && message.id() == request_id {
                    client.unsubscribe(&subscription.val).await;
                    return Ok(message);
                }
            }
        }
    }
}

fn validate_connect_response(
    response: &NostrConnectMessage,
    secret: Option<&str>,
) -> Result<(), RpcError> {
    let (result, error) = match response {
        NostrConnectMessage::Response { result, error, .. } => (result, error),
        _ => {
            return Err(RpcError::Other(
                "nip46 connect response invalid".to_string(),
            ))
        }
    };

    if let Some(error) = error {
        return Err(RpcError::Other(format!("nip46 connect error: {error}")));
    }

    let result = result
        .as_deref()
        .ok_or_else(|| RpcError::Other("nip46 connect missing result".to_string()))?;

    if result == "ack" {
        return Ok(());
    }

    if secret.is_some_and(|expected| expected == result) {
        return Ok(());
    }

    Err(RpcError::Other(format!(
        "nip46 connect unexpected result: {result}"
    )))
}
