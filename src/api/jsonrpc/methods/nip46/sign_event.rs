use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::nostr::{event_view, NostrEventView};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use crate::nip46::client;
use crate::nip46::session::Nip46Session;
use radroots_nostr::prelude::{RadrootsNostrKind, RadrootsNostrTag, RadrootsNostrTimestamp};
use nostr::nips::nip46::{NostrConnectMethod, NostrConnectRequest, ResponseResult};
use nostr::UnsignedEvent;

#[derive(Debug, Deserialize)]
struct Nip46SignEventParams {
    session_id: String,
    event: Nip46UnsignedEvent,
}

#[derive(Clone, Debug, Deserialize)]
struct Nip46UnsignedEvent {
    kind: u16,
    content: String,
    tags: Vec<Vec<String>>,
    created_at: u64,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46SignEventResponse {
    event: NostrEventView,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.sign_event");
    m.register_async_method("nip46.sign_event", |params, ctx, _| async move {
        let Nip46SignEventParams { session_id, event } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let session = ctx
            .state
            .nip46_sessions
            .get(&session_id)
            .await
            .ok_or_else(|| RpcError::InvalidParams("unknown session".to_string()))?;
        let signed_event = sign_event(&session, event).await?;
        Ok::<Nip46SignEventResponse, RpcError>(Nip46SignEventResponse {
            event: event_view(&signed_event),
        })
    })?;
    Ok(())
}

async fn sign_event(
    session: &Nip46Session,
    input: Nip46UnsignedEvent,
) -> Result<nostr::Event, RpcError> {
    let user_pubkey = session.user_pubkey.clone().ok_or_else(|| {
        RpcError::InvalidParams("missing user pubkey; call nip46.get_public_key".to_string())
    })?;
    let tags = parse_tags(input.tags)?;
    let unsigned = UnsignedEvent::new(
        user_pubkey,
        RadrootsNostrTimestamp::from_secs(input.created_at),
        RadrootsNostrKind::from_u16(input.kind),
        tags,
        input.content,
    );

    let request = NostrConnectRequest::SignEvent(unsigned);
    let response = client::request(session, request, "sign_event").await?;
    let response = response
        .to_response(NostrConnectMethod::SignEvent)
        .map_err(|e| RpcError::Other(format!("nip46 sign_event failed: {e}")))?;

    if let Some(error) = response.error {
        return Err(RpcError::Other(format!("nip46 sign_event error: {error}")));
    }

    let event = match response.result {
        Some(ResponseResult::SignEvent(event)) => *event,
        Some(_) => {
            return Err(RpcError::Other(
                "nip46 sign_event unexpected response".to_string(),
            ))
        }
        None => {
            return Err(RpcError::Other(
                "nip46 sign_event missing response".to_string(),
            ))
        }
    };

    event
        .verify()
        .map_err(|e| RpcError::Other(format!("nip46 sign_event invalid event: {e}")))?;

    Ok(event)
}

fn parse_tags(tags: Vec<Vec<String>>) -> Result<Vec<RadrootsNostrTag>, RpcError> {
    tags.into_iter()
        .enumerate()
        .map(|(idx, tag)| {
            RadrootsNostrTag::parse(tag)
                .map_err(|e| RpcError::InvalidParams(format!("invalid tag {idx}: {e}")))
        })
        .collect()
}
