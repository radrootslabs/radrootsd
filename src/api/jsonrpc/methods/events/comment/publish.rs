#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::nostr::{publish_response, PublishResponse};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::comment::RadrootsComment;
use radroots_events::kinds::KIND_COMMENT;
use radroots_events_codec::comment::encode::to_wire_parts;
use radroots_nostr::prelude::{radroots_nostr_build_event, radroots_nostr_send_event};

#[derive(Debug, Deserialize)]
struct PublishCommentParams {
    comment: RadrootsComment,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.comment.publish");
    m.register_async_method("events.comment.publish", |params, ctx, _| async move {
        let relays = ctx.state.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishCommentParams { comment, tags } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let parts = to_wire_parts(&comment)
            .map_err(|e| RpcError::InvalidParams(format!("invalid comment: {e}")))?;
        let mut tag_slices = parts.tags;
        if let Some(extra_tags) = tags {
            tag_slices.extend(extra_tags);
        }

        let builder = radroots_nostr_build_event(KIND_COMMENT, parts.content, tag_slices)
            .map_err(|e| RpcError::Other(format!("failed to build comment: {e}")))?;

        let output = radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish comment: {e}")))?;

        Ok::<PublishResponse, RpcError>(publish_response(output))
    })?;

    Ok(())
}
