use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct Nip46SessionStatusParams {
    session_id: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46SessionStatusResponse {
    session_id: String,
    client_pubkey: String,
    remote_signer_pubkey: String,
    user_pubkey: Option<String>,
    relays: Vec<String>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.session.status");
    m.register_async_method("nip46.session.status", |params, ctx, _| async move {
        let Nip46SessionStatusParams { session_id } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let session = ctx
            .state
            .nip46_sessions
            .get(&session_id)
            .await
            .ok_or_else(|| RpcError::InvalidParams("unknown session".to_string()))?;
        Ok::<Nip46SessionStatusResponse, RpcError>(Nip46SessionStatusResponse {
            session_id,
            client_pubkey: session.client_pubkey.to_hex(),
            remote_signer_pubkey: session.remote_signer_pubkey.to_hex(),
            user_pubkey: session.user_pubkey.map(|pubkey| pubkey.to_hex()),
            relays: session.relays,
        })
    })?;
    Ok(())
}
