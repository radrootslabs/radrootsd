use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;

use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Clone, Debug, Serialize)]
struct Nip46StatusResponse {
    ready: bool,
    session_ttl_secs: u64,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.status");
    m.register_method("nip46.status", |_p, ctx, _| {
        Ok::<Nip46StatusResponse, RpcError>(Nip46StatusResponse {
            ready: true,
            session_ttl_secs: ctx.state.nip46_config.session_ttl_secs,
        })
    })?;
    Ok(())
}
