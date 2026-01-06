use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct Nip46SessionCloseParams {
    session_id: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46SessionCloseResponse {
    session_id: String,
    closed: bool,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.session.close");
    m.register_async_method("nip46.session.close", |params, ctx, _| async move {
        let Nip46SessionCloseParams { session_id } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let closed = ctx.state.nip46_sessions.remove(&session_id).await;
        if !closed {
            return Err(RpcError::InvalidParams("unknown session".to_string()));
        }
        Ok::<Nip46SessionCloseResponse, RpcError>(Nip46SessionCloseResponse {
            session_id,
            closed,
        })
    })?;
    Ok(())
}
