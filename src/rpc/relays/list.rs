use anyhow::Result;
use jsonrpsee::RpcModule;
use serde_json::{Value as JsonValue, json};

use crate::radrootsd::Radrootsd;
use crate::rpc::RpcError;

pub fn register(m: &mut RpcModule<Radrootsd>) -> Result<()> {
    m.register_async_method("relays.list", |_p, ctx, _| async move {
        let relays = ctx.client.relays().await;
        Ok::<JsonValue, RpcError>(json!(
            relays.keys().map(|u| u.to_string()).collect::<Vec<_>>()
        ))
    })?;
    Ok(())
}
