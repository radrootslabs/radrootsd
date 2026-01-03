use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_nostr::prelude::radroots_nostr_remove_relay;
use serde::Deserialize;

use crate::api::jsonrpc::relays::RelayRemovedResponse;
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct RemoveParams {
    url: String,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("relays.remove");
    m.register_async_method("relays.remove", |params, ctx, _| async move {
        let RemoveParams { url } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        radroots_nostr_remove_relay(&ctx.state.client, &url)
            .await
            .map_err(|e| RpcError::Other(format!("failed to remove relay {url}: {e}")))?;

        Ok::<RelayRemovedResponse, RpcError>(RelayRemovedResponse { removed: url })
    })?;
    Ok(())
}
