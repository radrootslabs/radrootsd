use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::transport::jsonrpc::nip46::{client, session};
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
        let session = session::get_session(ctx.as_ref(), &session_id).await?;
        session::require_permission(&session, "sign_event")?;
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
