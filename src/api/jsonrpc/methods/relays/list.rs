use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde_json::{Value as JsonValue, json};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("relays.list");
    m.register_async_method("relays.list", |_p, ctx, _| async move {
        let relays = ctx.state.client.relays().await;
        Ok::<JsonValue, RpcError>(json!(
            relays.keys().map(|u| u.to_string()).collect::<Vec<_>>()
        ))
    })?;
    Ok(())
}
