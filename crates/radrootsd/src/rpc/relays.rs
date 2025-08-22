use anyhow::Result;
use jsonrpsee::RpcModule;
use serde::Deserialize;
// note: bring JsonValue into scope to turbofish Ok later
use serde_json::{Value as JsonValue, json};

use crate::radrootsd::Radrootsd;
use crate::rpc::RpcError;

#[derive(Debug, Deserialize)]
struct AddParams {
    url: String,
}

pub fn module(radrootsd: Radrootsd) -> Result<RpcModule<Radrootsd>> {
    let mut m = RpcModule::new(radrootsd);

    // relays.add
    m.register_async_method("relays.add", |params, ctx, _| async move {
        let AddParams { url } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        ctx.client
            .add_relay(&url)
            .await
            .map_err(|e| RpcError::AddRelay(url.clone(), e.to_string()))?;

        Ok::<JsonValue, RpcError>(json!({ "added": url }))
    })?;

    // relays.list
    m.register_async_method("relays.list", |_p, ctx, _| async move {
        let relays = ctx.client.relays().await;
        Ok::<JsonValue, RpcError>(json!(
            relays.keys().map(|u| u.to_string()).collect::<Vec<_>>()
        ))
    })?;

    // relays.connect
    m.register_async_method("relays.connect", |_p, ctx, _| async move {
        let relays = ctx.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }
        let client = ctx.client.clone();
        tokio::spawn(async move { client.connect().await });

        Ok::<JsonValue, RpcError>(json!({ "connecting": relays.len() }))
    })?;

    Ok(m)
}
