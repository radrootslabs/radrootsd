#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use nostr::nips::nip46::NostrConnectMessage;

use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_nostr::prelude::RadrootsNostrEventBuilder;

#[derive(Debug, Deserialize)]
struct Nip46SessionAuthorizeParams {
    session_id: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46SessionAuthorizeResponse {
    authorized: bool,
    replayed: bool,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.session.authorize");
    m.register_async_method("nip46.session.authorize", |params, ctx, _| async move {
        let Nip46SessionAuthorizeParams { session_id } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let outcome = ctx
            .state
            .nip46_sessions
            .authorize(&session_id)
            .await
            .ok_or_else(|| RpcError::InvalidParams("unknown session".to_string()))?;
        let mut replayed = false;
        if let Some(pending) = outcome.pending {
            let response = crate::transport::nostr::listener::handle_request(
                &ctx.state,
                &pending.client_pubkey,
                &pending.request_id,
                pending.request,
            )
            .await;
            let message = NostrConnectMessage::response(pending.request_id, response);
            let response_event = RadrootsNostrEventBuilder::nostr_connect(
                &ctx.state.keys,
                pending.client_pubkey,
                message,
            )
            .map_err(|err| RpcError::Other(format!("nip46 response build failed: {err}")))?;
            let _ = ctx.state.client.send_event_builder(response_event).await;
            replayed = true;
        }
        Ok::<Nip46SessionAuthorizeResponse, RpcError>(Nip46SessionAuthorizeResponse {
            authorized: true,
            replayed,
        })
    })?;
    Ok(())
}
