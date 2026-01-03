use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde_json::json;

use crate::api::jsonrpc::{MethodRegistry, RpcContext};

pub fn module(ctx: RpcContext, registry: MethodRegistry) -> Result<RpcModule<RpcContext>> {
    let mut m = RpcModule::new(ctx);

    registry.track("system.ping");
    m.register_method("system.ping", |_p, _ctx, _| "pong")?;

    registry.track("system.get_info");
    m.register_method("system.get_info", |_p, ctx, _| {
        let uptime = ctx.state.started.elapsed().as_secs();
        json!({
            "version": ctx.state.info.get("version"),
            "build": ctx.state.info.get("build"),
            "uptime_secs": uptime,
        })
    })?;

    registry.track("system.help");
    m.register_method("system.help", |_p, ctx, _| ctx.methods.list())?;

    Ok(m)
}
