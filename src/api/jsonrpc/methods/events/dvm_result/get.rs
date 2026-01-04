#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::is_result_kind;
use radroots_nostr::prelude::{RadrootsNostrEventId, RadrootsNostrFilter};

use super::list::{build_dvm_result_rows, DvmResultRow};
use crate::api::jsonrpc::methods::events::helpers::{fetch_latest_event, require_non_empty};

#[derive(Debug, Deserialize)]
struct DvmResultGetParams {
    id: String,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct DvmResultGetResponse {
    result: Option<DvmResultRow>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.dvm_result.get");
    m.register_async_method("events.dvm_result.get", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let DvmResultGetParams { id, timeout_secs } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let id = require_non_empty("id", id)?;
        let event_id = RadrootsNostrEventId::parse(&id)
            .map_err(|e| RpcError::InvalidParams(format!("invalid id: {e}")))?;

        let filter = RadrootsNostrFilter::new().id(event_id);

        let event = fetch_latest_event(&ctx.state.client, filter, timeout_secs).await?;
        let result = event.and_then(|event| {
            let kind = event.kind.as_u16() as u32;
            if !is_result_kind(kind) {
                return None;
            }
            build_dvm_result_rows(vec![event]).into_iter().next()
        });

        Ok::<DvmResultGetResponse, RpcError>(DvmResultGetResponse { result })
    })?;
    Ok(())
}
