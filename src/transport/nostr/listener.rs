use std::time::Duration;

use anyhow::{anyhow, Result};
use nostr::nips::nip04;
use nostr::nips::nip44;
use nostr::nips::nip46::{
    NostrConnectMessage,
    NostrConnectRequest,
    NostrConnectResponse,
    ResponseResult,
};
use nostr::JsonUtil;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::core::nip46::session::{
    session_expires_at,
    sign_event_allowed,
    Nip46Session,
    PendingNostrRequest,
};
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
        let response =
            handle_request(&radrootsd, &event.pubkey, &request_id, request).await;
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

pub(crate) async fn handle_request(
    radrootsd: &Radrootsd,
    client_pubkey: &radroots_nostr::prelude::RadrootsNostrPublicKey,
    request_id: &str,
    request: NostrConnectRequest,
) -> NostrConnectResponse {
    match request {
        NostrConnectRequest::Connect {
            remote_signer_public_key,
            secret,
        } => {
            if remote_signer_public_key != radrootsd.pubkey {
                return NostrConnectResponse::with_error("remote signer pubkey mismatch");
            }
            if let Some(secret) = secret.as_deref() {
                let trimmed = secret.trim();
                if trimmed.is_empty() {
                    return NostrConnectResponse::with_error("secret is empty");
                }
                if !radrootsd.nip46_sessions.claim_secret(trimmed).await {
                    return NostrConnectResponse::with_error("connect secret already used");
                }
            }
            let session_id = client_pubkey.to_hex();
            let expires_at =
                session_expires_at(radrootsd.nip46_config.session_ttl_secs);
            let session = Nip46Session {
                id: session_id,
                client: radrootsd.client.clone(),
                client_keys: radrootsd.keys.clone(),
                client_pubkey: client_pubkey.clone(),
                remote_signer_pubkey: radrootsd.pubkey,
                user_pubkey: Some(radrootsd.pubkey),
                relays: Vec::new(),
                perms: radrootsd.nip46_config.perms.clone(),
                name: None,
                url: None,
                image: None,
                expires_at,
                auth_required: false,
                authorized: true,
                auth_url: None,
                pending_request: None,
            };
            radrootsd.nip46_sessions.insert(session).await;
            NostrConnectResponse::with_result(ResponseResult::Ack)
        }
        NostrConnectRequest::GetPublicKey => {
            NostrConnectResponse::with_result(ResponseResult::GetPublicKey(radrootsd.pubkey))
        }
        NostrConnectRequest::SignEvent(unsigned) => {
            let session = match session_for_client(radrootsd, client_pubkey).await {
                Ok(session) => session,
                Err(response) => return response,
            };
            if !has_sign_event_permission(&session, u32::from(unsigned.kind.as_u16())) {
                return NostrConnectResponse::with_error("unauthorized sign_event");
            }
            if let Some(response) = auth_challenge(
                radrootsd,
                &session,
                request_id,
                client_pubkey,
                NostrConnectRequest::SignEvent(unsigned.clone()),
            )
            .await
            {
                return response;
            }
            if unsigned.pubkey != radrootsd.pubkey {
                return NostrConnectResponse::with_error("pubkey mismatch");
            }
            match unsigned.sign_with_keys(&radrootsd.keys) {
                Ok(event) => NostrConnectResponse::with_result(ResponseResult::SignEvent(Box::new(event))),
                Err(err) => NostrConnectResponse::with_error(format!("sign_event failed: {err}")),
            }
        }
        NostrConnectRequest::Nip04Encrypt { public_key, text } => {
            let session = match session_for_client(radrootsd, client_pubkey).await {
                Ok(session) => session,
                Err(response) => return response,
            };
            if !has_permission(&session, "nip04_encrypt") {
                return NostrConnectResponse::with_error("unauthorized nip04_encrypt");
            }
            if let Some(response) = auth_challenge(
                radrootsd,
                &session,
                request_id,
                client_pubkey,
                NostrConnectRequest::Nip04Encrypt {
                    public_key: public_key.clone(),
                    text: text.clone(),
                },
            )
            .await
            {
                return response;
            }
            match nip04::encrypt(radrootsd.keys.secret_key(), &public_key, text) {
                Ok(ciphertext) => {
                    NostrConnectResponse::with_result(ResponseResult::Nip04Encrypt { ciphertext })
                }
                Err(err) => NostrConnectResponse::with_error(format!("nip04_encrypt failed: {err}")),
            }
        }
        NostrConnectRequest::Nip04Decrypt { public_key, ciphertext } => {
            let session = match session_for_client(radrootsd, client_pubkey).await {
                Ok(session) => session,
                Err(response) => return response,
            };
            if !has_permission(&session, "nip04_decrypt") {
                return NostrConnectResponse::with_error("unauthorized nip04_decrypt");
            }
            if let Some(response) = auth_challenge(
                radrootsd,
                &session,
                request_id,
                client_pubkey,
                NostrConnectRequest::Nip04Decrypt {
                    public_key: public_key.clone(),
                    ciphertext: ciphertext.clone(),
                },
            )
            .await
            {
                return response;
            }
            match nip04::decrypt(radrootsd.keys.secret_key(), &public_key, ciphertext) {
                Ok(plaintext) => {
                    NostrConnectResponse::with_result(ResponseResult::Nip04Decrypt { plaintext })
                }
                Err(err) => NostrConnectResponse::with_error(format!("nip04_decrypt failed: {err}")),
            }
        }
        NostrConnectRequest::Nip44Encrypt { public_key, text } => {
            let session = match session_for_client(radrootsd, client_pubkey).await {
                Ok(session) => session,
                Err(response) => return response,
            };
            if !has_permission(&session, "nip44_encrypt") {
                return NostrConnectResponse::with_error("unauthorized nip44_encrypt");
            }
            if let Some(response) = auth_challenge(
                radrootsd,
                &session,
                request_id,
                client_pubkey,
                NostrConnectRequest::Nip44Encrypt {
                    public_key: public_key.clone(),
                    text: text.clone(),
                },
            )
            .await
            {
                return response;
            }
            match nip44::encrypt(radrootsd.keys.secret_key(), &public_key, text, nip44::Version::V2)
            {
                Ok(ciphertext) => {
                    NostrConnectResponse::with_result(ResponseResult::Nip44Encrypt { ciphertext })
                }
                Err(err) => NostrConnectResponse::with_error(format!("nip44_encrypt failed: {err}")),
            }
        }
        NostrConnectRequest::Nip44Decrypt { public_key, ciphertext } => {
            let session = match session_for_client(radrootsd, client_pubkey).await {
                Ok(session) => session,
                Err(response) => return response,
            };
            if !has_permission(&session, "nip44_decrypt") {
                return NostrConnectResponse::with_error("unauthorized nip44_decrypt");
            }
            if let Some(response) = auth_challenge(
                radrootsd,
                &session,
                request_id,
                client_pubkey,
                NostrConnectRequest::Nip44Decrypt {
                    public_key: public_key.clone(),
                    ciphertext: ciphertext.clone(),
                },
            )
            .await
            {
                return response;
            }
            match nip44::decrypt(radrootsd.keys.secret_key(), &public_key, ciphertext) {
                Ok(plaintext) => {
                    NostrConnectResponse::with_result(ResponseResult::Nip44Decrypt { plaintext })
                }
                Err(err) => NostrConnectResponse::with_error(format!("nip44_decrypt failed: {err}")),
            }
        }
        NostrConnectRequest::Ping => NostrConnectResponse::with_result(ResponseResult::Pong),
    }
}

async fn session_for_client(
    radrootsd: &Radrootsd,
    client_pubkey: &radroots_nostr::prelude::RadrootsNostrPublicKey,
) -> Result<Nip46Session, NostrConnectResponse> {
    let session_id = client_pubkey.to_hex();
    match radrootsd.nip46_sessions.get(&session_id).await {
        Some(session) => Ok(session),
        None => Err(NostrConnectResponse::with_error("unauthorized")),
    }
}

fn has_permission(session: &Nip46Session, perm: &str) -> bool {
    session.perms.iter().any(|entry| entry == perm)
}

fn has_sign_event_permission(session: &Nip46Session, kind: u32) -> bool {
    sign_event_allowed(&session.perms, kind)
}

async fn auth_challenge(
    radrootsd: &Radrootsd,
    session: &Nip46Session,
    request_id: &str,
    client_pubkey: &radroots_nostr::prelude::RadrootsNostrPublicKey,
    request: NostrConnectRequest,
) -> Option<NostrConnectResponse> {
    if !session.auth_required || session.authorized {
        return None;
    }
    let pending = PendingNostrRequest {
        request_id: request_id.to_string(),
        client_pubkey: client_pubkey.clone(),
        request,
    };
    let _ = radrootsd
        .nip46_sessions
        .set_pending_request(&session.id, pending)
        .await;
    let auth_url = session
        .auth_url
        .as_deref()
        .unwrap_or("auth required")
        .to_string();
    Some(NostrConnectResponse::new(
        Some(ResponseResult::AuthUrl),
        Some(auth_url),
    ))
}
