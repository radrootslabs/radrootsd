#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::KIND_COMMENT;
use radroots_nostr::prelude::{
    RadrootsNostrEventId,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

use super::list::{build_comment_rows, CommentRow};
use crate::api::jsonrpc::methods::events::helpers::{
    fetch_latest_event,
    require_non_empty,
};

#[derive(Debug, Deserialize)]
struct CommentGetParams {
    id: String,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct CommentGetResponse {
    comment: Option<CommentRow>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.comment.get");
    m.register_async_method("events.comment.get", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let CommentGetParams { id, timeout_secs } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let id = require_non_empty("id", id)?;
        let event_id = RadrootsNostrEventId::parse(&id)
            .map_err(|e| RpcError::InvalidParams(format!("invalid id: {e}")))?;

        let filter = RadrootsNostrFilter::new()
            .kind(RadrootsNostrKind::Custom(KIND_COMMENT as u16))
            .id(event_id);

        let event = fetch_latest_event(&ctx.state.client, filter, timeout_secs).await?;
        let comment = event.and_then(|event| build_comment_rows(vec![event]).into_iter().next());

        Ok::<CommentGetResponse, RpcError>(CommentGetResponse { comment })
    })?;
    Ok(())
}
