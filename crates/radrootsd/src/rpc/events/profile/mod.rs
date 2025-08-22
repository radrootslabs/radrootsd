use std::time::Duration;

use anyhow::Result;
use jsonrpsee::RpcModule;
use nostr::nips::nip19::ToBech32;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::radrootsd::Radrootsd;
use crate::rpc::RpcError;

use radroots_events::profile::models::RadrootsProfile;
use radroots_events_codec::profile::encode::to_metadata;

use nostr_sdk::prelude::EventBuilder;

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

        let ctx_pk = ctx.pubkey;

        let filter = nostr::Filter::new()
            .authors(vec![ctx_pk])
            .kind(nostr::Kind::Metadata);

        let stored = ctx
            .client
            .database()
            .query(filter.clone())
            .await
            .map_err(|e| RpcError::Other(format!("database query failed: {e}")))?;
        let fetched = ctx
            .client
            .fetch_events(filter, Duration::from_secs(10))
            .await
            .map_err(|e| RpcError::Other(format!("network fetch failed: {e}")))?;

        let mut latest: Option<nostr::Event> = None;

        let mut consider = |ev: nostr::Event| {
            if ev.kind != nostr::Kind::Metadata {
                return;
            }
            if let Some(cur) = &latest {
                if ev.created_at > cur.created_at {
                    latest = Some(ev);
                }
            } else {
                latest = Some(ev);
            }
        };

        for ev in stored.into_iter() {
            consider(ev);
        }
        for ev in fetched.into_iter() {
            consider(ev);
        }

        let ctx_npub = ctx_pk
            .to_bech32()
            .map_err(|e| RpcError::Other(format!("bech32 encode failed: {e}")))?;

        let row = if let Some(ev) = latest {
            let parsed: Option<serde_json::Value> = serde_json::from_str(&ev.content).ok();
            let profile: Option<radroots_events::profile::models::RadrootsProfile> =
                serde_json::from_str(&ev.content).ok();

            json!({
                "author_hex": ctx_pk.to_string(),
                "author_npub": ctx_npub,
                "event_id": ev.id.to_string(),
                "created_at": ev.created_at.as_u64(),
                "content": ev.content,
                "metadata_json": parsed,
                "radroots_profile": profile,
            })
        } else {
            json!({
                "author_hex": ctx_pk.to_string(),
                "author_npub": ctx_npub,
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

        let builder = EventBuilder::metadata(&metadata);

        let output = ctx
            .client
            .send_event_builder(builder)
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
