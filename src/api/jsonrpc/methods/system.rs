use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;

use crate::api::jsonrpc::{MethodRegistry, RpcContext};

#[derive(Clone, Debug, Serialize)]
struct SystemInfoResponse {
    version: Option<serde_json::Value>,
    build: Option<serde_json::Value>,
    uptime_secs: u64,
}

pub fn module(ctx: RpcContext, registry: MethodRegistry) -> Result<RpcModule<RpcContext>> {
    let mut m = RpcModule::new(ctx);

    registry.track("system.ping");
    m.register_method("system.ping", |_p, _ctx, _| "pong")?;

    registry.track("system.get_info");
    m.register_method("system.get_info", |_p, ctx, _| {
        let uptime = ctx.state.started.elapsed().as_secs();
        Ok::<SystemInfoResponse, crate::api::jsonrpc::RpcError>(SystemInfoResponse {
            version: ctx.state.info.get("version").cloned(),
            build: ctx.state.info.get("build").cloned(),
            uptime_secs: uptime,
        })
    })?;

    registry.track("system.help");
    m.register_method("system.help", |_p, ctx, _| ctx.methods.list())?;

    Ok(m)
}
