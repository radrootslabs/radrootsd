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
pub(crate) struct ProfileListRow {
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

pub(crate) fn build_profile_rows<I>(
    authors: Vec<RadrootsNostrPublicKey>,
    events: I,
) -> Result<Vec<ProfileListRow>, RpcError>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut latest_by_author: HashMap<RadrootsNostrPublicKey, RadrootsNostrEvent> = HashMap::new();
    for event in events {
        match latest_by_author.get(&event.pubkey) {
            Some(cur) if event.created_at <= cur.created_at => {}
            _ => {
                latest_by_author.insert(event.pubkey, event);
            }
        }
    }

    authors
        .into_iter()
        .map(|author| {
            let npub = radroots_nostr_npub_string(&author)
                .ok_or_else(|| RpcError::Other("bech32 encode failed".into()))?;
            let row = match latest_by_author.get(&author) {
                Some(event) => {
                    let parsed: Option<JsonValue> = serde_json::from_str(&event.content).ok();
                    let profile: Option<RadrootsProfile> = serde_json::from_str(&event.content).ok();
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
        .collect::<Result<Vec<_>, RpcError>>()
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

        let profiles = build_profile_rows(authors, stored.into_iter().chain(fetched.into_iter()))?;

        Ok::<ProfileListResponse, RpcError>(ProfileListResponse { profiles })
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_profile_rows;
    use radroots_nostr::prelude::{RadrootsNostrEvent, RadrootsNostrPublicKey};
    use serde_json::json;

    fn parse_pubkey(hex: &str) -> RadrootsNostrPublicKey {
        RadrootsNostrPublicKey::from_hex(hex).expect("pubkey")
    }

    fn event_with_profile(
        pubkey: &RadrootsNostrPublicKey,
        created_at: u64,
        name: &str,
        id: &str,
    ) -> RadrootsNostrEvent {
        let content = serde_json::to_string(&json!({ "name": name })).expect("content");
        let sig = format!("{:0128x}", 2);
        let event_json = json!({
            "id": id,
            "pubkey": pubkey.to_string(),
            "created_at": created_at,
            "kind": 0,
            "tags": [],
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    #[test]
    fn profile_list_picks_latest_per_author() {
        let author = parse_pubkey(
            "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4",
        );
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let older = event_with_profile(&author, 100, "old", &old_id);
        let newer = event_with_profile(&author, 200, "new", &new_id);

        let profiles = build_profile_rows(vec![author], vec![older, newer]).expect("profiles");

        assert_eq!(profiles.len(), 1);
        let row = &profiles[0];
        assert_eq!(row.created_at, Some(200));
        assert_eq!(row.event_id.as_deref(), Some(new_id.as_str()));
        assert_eq!(row.radroots_profile.as_ref().unwrap().name, "new");
        assert_eq!(row.metadata_json.as_ref().unwrap()["name"], "new");
    }

    #[test]
    fn profile_list_preserves_author_order_and_missing_rows() {
        let author_a = parse_pubkey(
            "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4",
        );
        let author_b = parse_pubkey(
            "3bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4",
        );
        let event_id = format!("{:064x}", 3);
        let event_b = event_with_profile(&author_b, 300, "b", &event_id);

        let profiles =
            build_profile_rows(vec![author_a, author_b], vec![event_b]).expect("profiles");

        assert_eq!(profiles.len(), 2);
        assert_eq!(profiles[0].author_hex, author_a.to_string());
        assert!(profiles[0].event_id.is_none());
        assert_eq!(profiles[1].author_hex, author_b.to_string());
        assert_eq!(profiles[1].event_id.as_deref(), Some(event_id.as_str()));
    }
}
