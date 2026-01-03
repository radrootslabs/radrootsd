use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::time::Duration;

use crate::api::jsonrpc::params::{
    apply_time_bounds,
    limit_or,
    parse_pubkeys_opt,
    timeout_or,
    EventListParams,
};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::profile::RadrootsProfile;
use radroots_nostr::prelude::{
    radroots_nostr_npub_string,
    RadrootsNostrFilter,
    RadrootsNostrKind,
    RadrootsNostrEvent,
    RadrootsNostrPublicKey,
};

#[derive(Clone, Debug, Serialize)]
struct ProfileListRow {
    author_hex: String,
    author_npub: String,
    event_id: Option<String>,
    created_at: Option<u64>,
    content: Option<String>,
    metadata_json: Option<JsonValue>,
    radroots_profile: Option<RadrootsProfile>,
}

#[derive(Clone, Debug, Serialize)]
struct ProfileListResponse {
    profiles: Vec<ProfileListRow>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.profile.list");
    m.register_async_method("events.profile.list", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let EventListParams {
            authors,
            limit,
            since,
            until,
            timeout_secs,
        } = params
            .parse::<Option<EventListParams>>()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?
            .unwrap_or_default();

        let authors = match parse_pubkeys_opt("author", authors)? {
            Some(authors) => authors,
            None => vec![ctx.state.pubkey],
        };

        let mut filter = RadrootsNostrFilter::new()
            .kind(RadrootsNostrKind::Metadata)
            .authors(authors.clone())
            .limit(limit_or(limit));
        filter = apply_time_bounds(filter, since, until);

        let mut latest_by_author: HashMap<RadrootsNostrPublicKey, RadrootsNostrEvent> =
            HashMap::new();
        let stored = ctx
            .state
            .client
            .database()
            .query(filter.clone())
            .await
            .map_err(|e| RpcError::Other(format!("metadata query failed: {e}")))?;
        let fetched = ctx
            .state
            .client
            .fetch_events(filter, Duration::from_secs(timeout_or(timeout_secs)))
            .await
            .map_err(|e| RpcError::Other(format!("metadata fetch failed: {e}")))?;

        for event in stored.into_iter().chain(fetched.into_iter()) {
            match latest_by_author.get(&event.pubkey) {
                Some(cur) if event.created_at <= cur.created_at => {}
                _ => {
                    latest_by_author.insert(event.pubkey, event);
                }
            }
        }

        let profiles = authors
            .into_iter()
            .map(|author| {
                let npub = radroots_nostr_npub_string(&author)
                    .ok_or_else(|| RpcError::Other("bech32 encode failed".into()))?;
                let row = match latest_by_author.get(&author) {
                    Some(event) => {
                        let parsed: Option<JsonValue> = serde_json::from_str(&event.content).ok();
                        let profile: Option<RadrootsProfile> =
                            serde_json::from_str(&event.content).ok();
                        ProfileListRow {
                            author_hex: author.to_string(),
                            author_npub: npub,
                            event_id: Some(event.id.to_string()),
                            created_at: Some(event.created_at.as_secs()),
                            content: Some(event.content.clone()),
                            metadata_json: parsed,
                            radroots_profile: profile,
                        }
                    }
                    None => ProfileListRow {
                        author_hex: author.to_string(),
                        author_npub: npub,
                        event_id: None,
                        created_at: None,
                        content: None,
                        metadata_json: None,
                        radroots_profile: None,
                    },
                };
                Ok(row)
            })
            .collect::<Result<Vec<_>, RpcError>>()?;

        Ok::<ProfileListResponse, RpcError>(ProfileListResponse { profiles })
    })?;

    Ok(())
}
