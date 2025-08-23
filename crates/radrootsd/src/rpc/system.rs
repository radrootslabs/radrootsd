use anyhow::Result;
use jsonrpsee::RpcModule;
use serde_json::json;

use crate::radrootsd::Radrootsd;

pub fn module(radrootsd: Radrootsd) -> Result<RpcModule<Radrootsd>> {
    let mut m = RpcModule::new(radrootsd);

    m.register_method("system.ping", |_p, _ctx, _| "pong")?;

    m.register_method("system.get_info", |_p, ctx, _| {
        let uptime = ctx.started.elapsed().as_secs();
        json!({
            "version": ctx.info.get("version"),
            "build": ctx.info.get("build"),
            "uptime_secs": uptime,
        })
    })?;

    m.register_method("system.help", |_p, _ctx, _| {
        vec![
            /* %% radrootsd-methods %% */
            "system.get_info",
            "system.help",
            "system.ping",
            "events.note.list",
            "events.note.publish",
            "events.profile.list",
            "events.profile.publish",
            "relays.add",
            "relays.connect",
            "relays.list",
            "relays.remove",
            "relays.status",
            /* %% radrootsd-methods %% */
        ]
    })?;

    Ok(m)
}
