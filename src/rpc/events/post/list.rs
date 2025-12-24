use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use std::time::Duration;

use crate::{radrootsd::Radrootsd, rpc::RpcError};
use radroots_nostr::prelude::{
    radroots_nostr_parse_pubkeys,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Debug, Default, Deserialize)]
struct ListProfilesParams {
    #[serde(default)]
    authors: Option<Vec<String>>,
    #[serde(default)]
    limit: Option<u64>,
}

pub fn register(m: &mut RpcModule<Radrootsd>) -> Result<()> {
    m.register_async_method("events.post.list", |params, ctx, _| async move {
        if ctx.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let ListProfilesParams { authors, limit } = params.parse().unwrap_or_default();
        let limit = limit.unwrap_or(50);

        let mut filter = RadrootsNostrFilter::new()
            .kind(RadrootsNostrKind::TextNote)
            .limit(limit.try_into().unwrap());
        if let Some(auths) = authors {
            let pks = radroots_nostr_parse_pubkeys(&auths)
                .map_err(|e| RpcError::InvalidParams(format!("invalid author: {e}")))?;
            filter = filter.authors(pks);
        } else {
            filter = filter.author(ctx.pubkey);
        }

        let events = ctx
            .client
            .fetch_events(filter, Duration::from_secs(10))
            .await
            .map_err(|e| RpcError::Other(format!("fetch failed: {e}")))?;

        let items: Vec<JsonValue> = events
            .into_iter()
            .map(|ev| {
                let tags: Vec<Vec<String>> =
                    ev.tags.iter().map(|t| t.as_slice().to_vec()).collect();
                json!({
                    "id": ev.id.to_string(),
                    "author": ev.pubkey.to_string(),
                    "created_at": ev.created_at.as_u64(),
                    "kind": ev.kind.as_u16() as u32,
                    "tags": tags,
                    "content": ev.content,
                    "sig": ev.sig.to_string(),
                })
            })
            .collect();

        Ok::<JsonValue, RpcError>(json!({ "Profiles": items }))
    })?;

    Ok(())
}
