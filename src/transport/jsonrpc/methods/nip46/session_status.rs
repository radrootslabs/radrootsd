use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::transport::jsonrpc::nip46::session;
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

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
    perms: Vec<String>,
    name: Option<String>,
    url: Option<String>,
    image: Option<String>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.session.status");
    m.register_async_method("nip46.session.status", |params, ctx, _| async move {
        let Nip46SessionStatusParams { session_id } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let session = session::get_session(ctx.as_ref(), &session_id).await?;
        Ok::<Nip46SessionStatusResponse, RpcError>(Nip46SessionStatusResponse {
            session_id,
            client_pubkey: session.client_pubkey.to_hex(),
            remote_signer_pubkey: session.remote_signer_pubkey.to_hex(),
            user_pubkey: session.user_pubkey.map(|pubkey| pubkey.to_hex()),
            relays: session.relays,
            perms: session.perms,
            name: session.name,
            url: session.url,
            image: session.image,
        })
    })?;
    Ok(())
}
