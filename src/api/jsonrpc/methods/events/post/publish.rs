use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_nostr::prelude::{radroots_nostr_build_event, radroots_nostr_send_event};

#[derive(Debug, Deserialize)]
struct PublishProfileParams {
    content: String,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.post.publish");
    m.register_async_method("events.post.publish", |params, ctx, _| async move {
        let relays = ctx.state.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishProfileParams { content, tags } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        if content.trim().is_empty() {
            return Err(RpcError::InvalidParams("content must not be empty".into()));
        }

        let builder = radroots_nostr_build_event(1, content, tags.unwrap_or_default())
            .map_err(|e| RpcError::Other(format!("failed to build note: {e}")))?;

        let output = radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish note: {e}")))?;

        let id_hex = output.id().to_string();
        let sent: Vec<String> = output.success.into_iter().map(|u| u.to_string()).collect();
        let failed: Vec<(String, String)> = output
            .failed
            .into_iter()
            .map(|(u, e)| (u.to_string(), e.to_string()))
            .collect();

        Ok::<JsonValue, RpcError>(json!({
            "id": id_hex,
            "sent": sent,
            "failed": failed
        }))
    })?;

    Ok(())
}
