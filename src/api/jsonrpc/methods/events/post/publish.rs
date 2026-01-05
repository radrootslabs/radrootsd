use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::methods::events::helpers::send_event_with_options;
use crate::api::jsonrpc::nostr::{publish_response, PublishResponse};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::KIND_POST;
use radroots_nostr::prelude::radroots_nostr_build_event;

#[derive(Debug, Deserialize)]
struct PublishPostParams {
    content: String,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
    #[serde(default)]
    author_secret_key: Option<String>,
    #[serde(default)]
    created_at: Option<u64>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.post.publish");
    m.register_async_method("events.post.publish", |params, ctx, _| async move {
        let relays = ctx.state.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishPostParams {
            content,
            tags,
            author_secret_key,
            created_at,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        if content.trim().is_empty() {
            return Err(RpcError::InvalidParams("content must not be empty".into()));
        }

        let builder = radroots_nostr_build_event(KIND_POST, content, tags.unwrap_or_default())
            .map_err(|e| RpcError::Other(format!("failed to build note: {e}")))?;

        let output = send_event_with_options(&ctx, builder, author_secret_key, created_at).await?;

        Ok::<PublishResponse, RpcError>(publish_response(output))
    })?;

    Ok(())
}
