use std::time::Duration;

use anyhow::Result;
use jsonrpsee::RpcModule;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::radrootsd::Radrootsd;
use crate::rpc::RpcError;

use radroots_events::profile::models::RadrootsProfile;
use radroots_events_codec::profile::encode::to_metadata;

use radroots_nostr::prelude::{
    build_metadata_event, fetch_latest_metadata_for_author, nostr_send_event, npub_string,
};

#[derive(Debug, Deserialize)]
struct PublishProfileParams {
    profile: RadrootsProfile,
}

pub fn module(radrootsd: Radrootsd) -> Result<RpcModule<Radrootsd>> {
    let mut m = RpcModule::new(radrootsd);

    m.register_async_method("events.profile.list", |_params, ctx, _| async move {
        if ctx.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let me_pk = ctx.pubkey;

        let latest = fetch_latest_metadata_for_author(&ctx.client, me_pk, Duration::from_secs(10))
            .await
            .map_err(|e| RpcError::Other(format!("metadata fetch failed: {e}")))?;

        let npub =
            npub_string(&me_pk).ok_or_else(|| RpcError::Other("bech32 encode failed".into()))?;

        let row = if let Some(ev) = latest {
            let parsed: Option<serde_json::Value> = serde_json::from_str(&ev.content).ok();
            let profile: Option<radroots_events::profile::models::RadrootsProfile> =
                serde_json::from_str(&ev.content).ok();

            json!({
                "author_hex": me_pk.to_string(),
                "author_npub": npub,
                "event_id": ev.id.to_string(),
                "created_at": ev.created_at.as_u64(),
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

    m.register_async_method("events.profile.publish", |params, ctx, _| async move {
        let relays = ctx.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishProfileParams { profile } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let metadata = to_metadata(&profile).map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let builder = build_metadata_event(&metadata);

        let output = nostr_send_event(&ctx.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish metadata: {e}")))?;

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

    Ok(m)
}
