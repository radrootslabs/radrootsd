use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::core::nip46::session::Nip46SessionView;
use crate::transport::jsonrpc::nip46::session;
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct Nip46SessionStatusParams {
    session_id: String,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.session.status");
    m.register_async_method("nip46.session.status", |params, ctx, _| async move {
        let Nip46SessionStatusParams { session_id } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let session = session::get_session(ctx.as_ref(), &session_id).await?;
        Ok::<Nip46SessionView, RpcError>(session.public_view())
    })?;
    Ok(())
}
