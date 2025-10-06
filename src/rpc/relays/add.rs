use anyhow::Result;
use jsonrpsee::RpcModule;
use radroots_nostr::prelude::add_relay;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::radrootsd::Radrootsd;
use crate::rpc::RpcError;

#[derive(Debug, Deserialize)]
struct AddParams {
    url: String,
}

pub fn register(m: &mut RpcModule<Radrootsd>) -> Result<()> {
    m.register_async_method("relays.add", |params, ctx, _| async move {
        let AddParams { url } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        add_relay(&ctx.client, &url)
            .await
            .map_err(|e| RpcError::AddRelay(url.clone(), e.to_string()))?;

        Ok::<JsonValue, RpcError>(json!({ "added": url }))
    })?;
    Ok(())
}
