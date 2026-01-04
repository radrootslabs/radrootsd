#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_nostr::prelude::{
    RadrootsNostrEventId,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

use super::list::{build_post_rows, PostRow};
use crate::api::jsonrpc::methods::events::helpers::{
    fetch_latest_event,
    require_non_empty,
};

#[derive(Debug, Deserialize)]
struct PostGetParams {
    id: String,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct PostGetResponse {
    post: Option<PostRow>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.post.get");
    m.register_async_method("events.post.get", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PostGetParams { id, timeout_secs } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let id = require_non_empty("id", id)?;
        let event_id = RadrootsNostrEventId::parse(&id)
            .map_err(|e| RpcError::InvalidParams(format!("invalid id: {e}")))?;

        let filter = RadrootsNostrFilter::new()
            .kind(RadrootsNostrKind::TextNote)
            .id(event_id);

        let event = fetch_latest_event(&ctx.state.client, filter, timeout_secs).await?;
        let post = event.and_then(|event| build_post_rows(vec![event]).into_iter().next());

        Ok::<PostGetResponse, RpcError>(PostGetResponse { post })
    })?;
    Ok(())
}
