use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_nostr::prelude::radroots_nostr_add_relay;
use serde::Deserialize;

use crate::api::jsonrpc::relays::RelayAddedResponse;
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct AddParams {
    url: String,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("relays.add");
    m.register_async_method("relays.add", |params, ctx, _| async move {
        let AddParams { url } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        radroots_nostr_add_relay(&ctx.state.client, &url)
            .await
            .map_err(|e| RpcError::AddRelay(url.clone(), e.to_string()))?;

        Ok::<RelayAddedResponse, RpcError>(RelayAddedResponse { added: url })
    })?;
    Ok(())
}
