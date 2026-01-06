use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use crate::nip46::connection::{parse_connect_url, Nip46ConnectInfo};

#[derive(Debug, Deserialize)]
struct Nip46ConnectParams {
    url: String,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.connect");
    m.register_method("nip46.connect", |params, _ctx, _| {
        let Nip46ConnectParams { url } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let info: Nip46ConnectInfo = parse_connect_url(&url)?;
        Ok::<Nip46ConnectInfo, RpcError>(info)
    })?;
    Ok(())
}
