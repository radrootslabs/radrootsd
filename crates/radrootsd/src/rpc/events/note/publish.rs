use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::{radrootsd::Radrootsd, rpc::RpcError};
use radroots_nostr::prelude::{build_nostr_event, nostr_send_event};

#[derive(Debug, Deserialize)]
struct PublishNoteParams {
    content: String,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
}

pub fn register(m: &mut RpcModule<Radrootsd>) -> Result<()> {
    m.register_async_method("events.note.publish", |params, ctx, _| async move {
        let relays = ctx.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishNoteParams { content, tags } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        if content.trim().is_empty() {
            return Err(RpcError::InvalidParams("content must not be empty".into()));
        }

        let builder = build_nostr_event(1, content, tags.unwrap_or_default())
            .map_err(|e| RpcError::Other(format!("failed to build note: {e}")))?;

        let output = nostr_send_event(&ctx.client, builder)
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
