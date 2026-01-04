#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::nostr::{publish_response, PublishResponse};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::job_result::RadrootsJobResult;
use radroots_events_codec::job::encode::canonicalize_tags;
use radroots_events_codec::job::result::encode::to_wire_parts;
use radroots_nostr::prelude::{radroots_nostr_build_event, radroots_nostr_send_event};

#[derive(Debug, Deserialize)]
struct PublishDvmResultParams {
    result: RadrootsJobResult,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.dvm_result.publish");
    m.register_async_method("events.dvm_result.publish", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishDvmResultParams { result, tags } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let content = result.content.clone().unwrap_or_default();
        let mut parts = to_wire_parts(&result, &content)
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        if let Some(extra_tags) = tags {
            parts.tags.extend(extra_tags);
            canonicalize_tags(&mut parts.tags);
        }

        let builder = radroots_nostr_build_event(parts.kind, parts.content, parts.tags)
            .map_err(|e| RpcError::Other(format!("failed to build dvm result event: {e}")))?;

        let output = radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish dvm result: {e}")))?;

        Ok::<PublishResponse, RpcError>(publish_response(output))
    })?;

    Ok(())
}
