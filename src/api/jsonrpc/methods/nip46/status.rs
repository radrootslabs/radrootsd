use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Clone, Debug, Serialize)]
struct Nip46StatusResponse {
    ready: bool,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.status");
    m.register_method("nip46.status", |_p, _ctx, _| {
        Ok::<Nip46StatusResponse, RpcError>(Nip46StatusResponse { ready: true })
    })?;
    Ok(())
}
