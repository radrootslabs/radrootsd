use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::core::nip46::session::Nip46Session;
use crate::transport::jsonrpc::nip46::client;
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use nostr::UnsignedEvent;

#[derive(Debug, Deserialize)]
struct Nip46SignEventParams {
    session_id: String,
    event: UnsignedEvent,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46SignEventResponse {
    event: nostr::Event,
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
        if !has_permission(&session, "sign_event") {
            return Err(RpcError::Other("unauthorized sign_event".to_string()));
        }
        if event.pubkey != session.remote_signer_pubkey {
            return Err(RpcError::InvalidParams(
                "event pubkey does not match remote signer".to_string(),
            ));
        }
        let event = client::sign_event(&session, event, "sign_event").await?;
        Ok::<Nip46SignEventResponse, RpcError>(Nip46SignEventResponse { event })
    })?;
    Ok(())
}

fn has_permission(session: &Nip46Session, perm: &str) -> bool {
    session.perms.iter().any(|entry| entry == perm)
}
