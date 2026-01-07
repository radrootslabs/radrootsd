use std::time::Duration;

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::time::sleep;
use uuid::Uuid;

use crate::core::nip46::session::{filter_perms, session_expires_at, Nip46Session};
use crate::transport::jsonrpc::nip46::connection::{
    parse_connect_url,
    Nip46ConnectInfo,
    Nip46ConnectMode,
};
use crate::transport::jsonrpc::params::DEFAULT_TIMEOUT_SECS;
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_nostr::prelude::{
    radroots_nostr_filter_tag,
    radroots_nostr_parse_pubkey,
    RadrootsNostrClient,
    RadrootsNostrEventBuilder,
    RadrootsNostrFilter,
    RadrootsNostrKind,
    RadrootsNostrKeys,
    RadrootsNostrPublicKey,
    RadrootsNostrSecretKey,
    RadrootsNostrRelayPoolNotification,
    RadrootsNostrTimestamp,
};
use nostr::nips::{nip44, nip46::NostrConnectMessage, nip46::NostrConnectRequest};
use nostr::JsonUtil;

#[derive(Debug, Deserialize)]
struct Nip46ConnectParams {
    url: String,
    client_secret_key: Option<String>,
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
        let Nip46ConnectParams {
            url,
            client_secret_key,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let response = connect_nip46(ctx.as_ref().clone(), url, client_secret_key).await?;
        Ok::<Nip46ConnectResponse, RpcError>(response)
    })?;
    Ok(())
}

async fn connect_nip46(
    ctx: RpcContext,
    url: String,
    client_secret_key: Option<String>,
) -> Result<Nip46ConnectResponse, RpcError> {
    let info = parse_connect_url(&url)?;
    match info.mode {
        Nip46ConnectMode::Bunker => connect_bunker(ctx, info).await,
        Nip46ConnectMode::Nostrconnect => {
            connect_nostrconnect(ctx, info, client_secret_key).await
        }
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
    claim_secret(&ctx, info.secret.as_deref()).await?;

    let perms = filter_perms(&info.perms, &ctx.state.nip46_config.perms);
    let expires_at = session_expires_at(ctx.state.nip46_config.session_ttl_secs);

    let session_id = Uuid::new_v4().to_string();
    let session = Nip46Session {
        id: session_id.clone(),
        client,
        client_keys,
        client_pubkey,
        remote_signer_pubkey,
        user_pubkey: None,
        relays: info.relays.clone(),
        perms,
        name: info.name.clone(),
        url: info.url.clone(),
        image: info.image.clone(),
        expires_at,
        auth_required: false,
        authorized: true,
        auth_url: None,
        pending_request: None,
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

async fn connect_nostrconnect(
    ctx: RpcContext,
    info: Nip46ConnectInfo,
    client_secret_key: Option<String>,
) -> Result<Nip46ConnectResponse, RpcError> {
    if info.relays.is_empty() {
        return Err(RpcError::InvalidParams("missing relay".to_string()));
    }
    let secret = info
        .secret
        .as_deref()
        .ok_or_else(|| RpcError::InvalidParams("missing secret".to_string()))?;
    let client_secret_key = client_secret_key
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RpcError::InvalidParams("missing client_secret_key".to_string()))?;
    let client_secret_key = RadrootsNostrSecretKey::parse(&client_secret_key)
        .map_err(|e| RpcError::InvalidParams(format!("invalid client_secret_key: {e}")))?;
    let client_keys = RadrootsNostrKeys::new(client_secret_key);
    let client_pubkey = client_keys.public_key();
    let client_pubkey_raw = info.client_pubkey.as_ref().ok_or_else(|| {
        RpcError::InvalidParams("missing client pubkey".to_string())
    })?;
    let expected_pubkey = radroots_nostr_parse_pubkey(client_pubkey_raw)
        .map_err(|e| RpcError::InvalidParams(format!("invalid client pubkey: {e}")))?;
    if expected_pubkey != client_pubkey {
        return Err(RpcError::InvalidParams(
            "client_secret_key does not match client pubkey".to_string(),
        ));
    }

    let client = RadrootsNostrClient::new(client_keys.clone());
    add_relays(&client, &info.relays).await?;
    client.connect().await;
    client
        .wait_for_connection(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
        .await;

    let (remote_signer_pubkey, response) = wait_for_nostrconnect_response(
        &client,
        &client_keys,
        &client_pubkey,
        secret,
    )
    .await?;
    validate_nostrconnect_response(&response, secret)?;
    claim_secret(&ctx, info.secret.as_deref()).await?;

    let perms = filter_perms(&info.perms, &ctx.state.nip46_config.perms);
    let expires_at = session_expires_at(ctx.state.nip46_config.session_ttl_secs);

    let session_id = Uuid::new_v4().to_string();
    let session = Nip46Session {
        id: session_id.clone(),
        client,
        client_keys,
        client_pubkey,
        remote_signer_pubkey,
        user_pubkey: None,
        relays: info.relays.clone(),
        perms,
        name: info.name.clone(),
        url: info.url.clone(),
        image: info.image.clone(),
        expires_at,
        auth_required: false,
        authorized: true,
        auth_url: None,
        pending_request: None,
    };
    ctx.state.nip46_sessions.insert(session).await;

    Ok(Nip46ConnectResponse {
        session_id,
        mode: info.mode,
        remote_signer_pubkey: remote_signer_pubkey.to_hex(),
        client_pubkey: client_pubkey.to_hex(),
        relays: info.relays,
    })
}

async fn add_relays(client: &RadrootsNostrClient, relays: &[String]) -> Result<(), RpcError> {
    for relay in relays.iter() {
        client
            .add_relay(relay)
            .await
            .map_err(|e| RpcError::Other(format!("nip46 relay add failed: {e}")))?;
    }
    Ok(())
}

async fn claim_secret(ctx: &RpcContext, secret: Option<&str>) -> Result<(), RpcError> {
    let Some(secret) = secret else {
        return Ok(());
    };
    let trimmed = secret.trim();
    if trimmed.is_empty() {
        return Err(RpcError::InvalidParams("secret is empty".to_string()));
    }
    if ctx.state.nip46_sessions.claim_secret(trimmed).await {
        Ok(())
    } else {
        Err(RpcError::InvalidParams("secret already used".to_string()))
    }
}

async fn send_connect_request(
    client: &RadrootsNostrClient,
    client_keys: &RadrootsNostrKeys,
    remote_signer_pubkey: &RadrootsNostrPublicKey,
    secret: Option<&str>,
) -> Result<String, RpcError> {
    let req = NostrConnectRequest::Connect {
        remote_signer_public_key: remote_signer_pubkey.clone(),
        secret: secret.map(|value| value.to_string()),
    };
    let message = NostrConnectMessage::request(&req);
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
                if event.kind != RadrootsNostrKind::NostrConnect
                    || event.pubkey != *remote_signer_pubkey
                {
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

fn validate_nostrconnect_response(
    response: &NostrConnectMessage,
    secret: &str,
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

    let Some(value) = result.as_deref() else {
        return Err(RpcError::Other(
            "nip46 connect missing result".to_string(),
        ));
    };

    if value == secret {
        return Ok(());
    }

    Err(RpcError::Other(format!(
        "nip46 connect unexpected result: {value}"
    )))
}

async fn wait_for_nostrconnect_response(
    client: &RadrootsNostrClient,
    client_keys: &RadrootsNostrKeys,
    client_pubkey: &RadrootsNostrPublicKey,
    secret: &str,
) -> Result<(RadrootsNostrPublicKey, NostrConnectMessage), RpcError> {
    let filter = RadrootsNostrFilter::new()
        .kind(RadrootsNostrKind::NostrConnect)
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
                if event.kind != RadrootsNostrKind::NostrConnect {
                    continue;
                }
                let decrypted = nip44::decrypt(
                    client_keys.secret_key(),
                    &event.pubkey,
                    &event.content,
                )
                .map_err(|e| RpcError::Other(format!("nip46 connect decrypt failed: {e}")))?;
                let message = NostrConnectMessage::from_json(&decrypted)
                    .map_err(|e| RpcError::Other(format!("nip46 connect response parse failed: {e}")))?;
                if !message.is_response() || message.id().is_empty() {
                    continue;
                }
                validate_nostrconnect_response(&message, secret)?;
                client.unsubscribe(&subscription.val).await;
                return Ok((event.pubkey, message));
            }
        }
    }
}
