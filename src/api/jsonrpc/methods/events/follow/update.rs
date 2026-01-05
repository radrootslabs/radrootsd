#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::nostr::{event_tags, publish_response, PublishResponse};
use crate::api::jsonrpc::params::parse_pubkeys;
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use crate::api::jsonrpc::methods::events::helpers::fetch_latest_event;
use radroots_events::follow::RadrootsFollow;
use radroots_events_codec::follow::decode::follow_from_tags;
use radroots_events_codec::follow::encode::{follow_apply, FollowMutation, to_wire_parts};
use radroots_nostr::prelude::{
    radroots_nostr_build_event,
    radroots_nostr_send_event,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FollowUpdateAction {
    Follow,
    Unfollow,
    Toggle,
}

#[derive(Debug, Deserialize)]
struct FollowUpdateParams {
    public_key: String,
    #[serde(default)]
    relay_url: Option<String>,
    #[serde(default)]
    contact_name: Option<String>,
    #[serde(default)]
    action: Option<FollowUpdateAction>,
    #[serde(default)]
    timeout_secs: Option<u64>,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.follow.update");
    m.register_async_method("events.follow.update", |params, ctx, _| async move {
        let relays = ctx.state.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let FollowUpdateParams {
            public_key,
            relay_url,
            contact_name,
            action,
            timeout_secs,
            tags,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let public_key = normalize_pubkey(public_key)?;
        let relay_url = normalize_optional(relay_url);
        let contact_name = normalize_optional(contact_name);

        let base_follow = load_latest_follow(&ctx, timeout_secs).await?;

        let mutation = match action.unwrap_or(FollowUpdateAction::Toggle) {
            FollowUpdateAction::Follow => FollowMutation::Follow {
                public_key,
                relay_url,
                contact_name,
            },
            FollowUpdateAction::Unfollow => FollowMutation::Unfollow { public_key },
            FollowUpdateAction::Toggle => FollowMutation::Toggle {
                public_key,
                relay_url,
                contact_name,
            },
        };

        let updated = follow_apply(&base_follow, mutation)
            .map_err(|e| RpcError::InvalidParams(format!("invalid follow mutation: {e}")))?;
        let mut parts = to_wire_parts(&updated)
            .map_err(|e| RpcError::InvalidParams(format!("invalid follow: {e}")))?;
        if let Some(extra_tags) = tags {
            parts.tags.extend(extra_tags);
        }

        let builder = radroots_nostr_build_event(parts.kind, parts.content, parts.tags)
            .map_err(|e| RpcError::Other(format!("failed to build follow: {e}")))?;

        let output = radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish follow: {e}")))?;

        Ok::<PublishResponse, RpcError>(publish_response(output))
    })?;

    Ok(())
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn normalize_pubkey(value: String) -> Result<String, RpcError> {
    let mut parsed = parse_pubkeys("public_key", &[value])?;
    parsed
        .pop()
        .map(|key| key.to_string())
        .ok_or_else(|| RpcError::InvalidParams("public_key cannot be empty".to_string()))
}

async fn load_latest_follow(
    ctx: &RpcContext,
    timeout_secs: Option<u64>,
) -> Result<RadrootsFollow, RpcError> {
    let filter = RadrootsNostrFilter::new()
        .kind(RadrootsNostrKind::ContactList)
        .author(ctx.state.pubkey);

    let event = fetch_latest_event(&ctx.state.client, filter, timeout_secs).await?;
    match event {
        Some(event) => {
            let tags = event_tags(&event);
            let published_at = u32::try_from(event.created_at.as_secs()).map_err(|_| {
                RpcError::Other("follow event created_at overflow".to_string())
            })?;
            follow_from_tags(event.kind.as_u16() as u32, &tags, published_at)
                .map_err(|e| RpcError::Other(format!("invalid follow event: {e}")))
        }
        None => Ok(RadrootsFollow { list: Vec::new() }),
    }
}
