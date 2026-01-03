use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde_json::{Value as JsonValue, json};
use std::time::Duration;

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_nostr::prelude::{
    radroots_nostr_fetch_metadata_for_author,
    radroots_nostr_npub_string,
};

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.profile.list");
    m.register_async_method("events.profile.list", |_params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let me_pk = ctx.state.pubkey;

        let latest = radroots_nostr_fetch_metadata_for_author(&ctx.state.client, me_pk, Duration::from_secs(10))
            .await
            .map_err(|e| RpcError::Other(format!("metadata fetch failed: {e}")))?;

        let npub = radroots_nostr_npub_string(&me_pk)
            .ok_or_else(|| RpcError::Other("bech32 encode failed".into()))?;

        let row = if let Some(ev) = latest {
            let parsed: Option<serde_json::Value> = serde_json::from_str(&ev.content).ok();
            let profile: Option<radroots_events::profile::RadrootsProfile> =
                serde_json::from_str(&ev.content).ok();

            json!({
                "author_hex": me_pk.to_string(),
                "author_npub": npub,
                "event_id": ev.id.to_string(),
                "created_at": ev.created_at.as_secs(),
                "content": ev.content,
                "metadata_json": parsed,
                "radroots_profile": profile,
            })
        } else {
            json!({
                "author_hex": me_pk.to_string(),
                "author_npub": npub,
                "event_id": null,
                "created_at": null,
                "content": null,
                "metadata_json": null,
                "radroots_profile": null
            })
        };

        Ok::<JsonValue, RpcError>(json!({ "profiles": [row] }))
    })?;

    Ok(())
}
