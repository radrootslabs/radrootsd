#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;

use crate::api::jsonrpc::nostr::{event_tags, event_view_with_tags};
use crate::api::jsonrpc::params::{
    apply_time_bounds,
    limit_or,
    parse_pubkeys_opt,
    timeout_or,
    EventListParams,
};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::follow::RadrootsFollow;
use radroots_events_codec::follow::decode::follow_from_tags;
use radroots_nostr::prelude::{
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrKind,
    RadrootsNostrPublicKey,
};

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FollowRow {
    id: String,
    author: String,
    created_at: u64,
    kind: u32,
    tags: Vec<Vec<String>>,
    content: String,
    sig: String,
    follow: Option<RadrootsFollow>,
}

#[derive(Clone, Debug, Serialize)]
struct FollowListResponse {
    follows: Vec<FollowRow>,
}

pub(crate) fn build_follow_rows<I>(events: I) -> Vec<FollowRow>
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

    let mut items = latest_by_author
        .into_values()
        .map(|ev| {
            let tags = event_tags(&ev);
            let follow = parse_follow_event(&ev, &tags);
            let event = event_view_with_tags(&ev, tags);
            FollowRow {
                id: event.id,
                author: event.author,
                created_at: event.created_at,
                kind: event.kind,
                tags: event.tags,
                content: event.content,
                sig: event.sig,
                follow,
            }
        })
        .collect::<Vec<_>>();

    items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    items
}

fn parse_follow_event(event: &RadrootsNostrEvent, tags: &[Vec<String>]) -> Option<RadrootsFollow> {
    let kind = event.kind.as_u16() as u32;
    let published_at = u32::try_from(event.created_at.as_secs()).ok()?;
    follow_from_tags(kind, tags, published_at).ok()
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.follow.list");
    m.register_async_method("events.follow.list", |params, ctx, _| async move {
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

        let limit = limit_or(limit);

        let mut filter = RadrootsNostrFilter::new()
            .kind(RadrootsNostrKind::ContactList)
            .limit(limit);

        if let Some(authors) = parse_pubkeys_opt("author", authors)? {
            filter = filter.authors(authors);
        } else {
            filter = filter.author(ctx.state.pubkey);
        }

        filter = apply_time_bounds(filter, since, until);

        let stored = ctx
            .state
            .client
            .database()
            .query(filter.clone())
            .await
            .map_err(|e| RpcError::Other(format!("query failed: {e}")))?;
        let fetched = ctx
            .state
            .client
            .fetch_events(filter, Duration::from_secs(timeout_or(timeout_secs)))
            .await
            .map_err(|e| RpcError::Other(format!("fetch failed: {e}")))?;

        let mut items = build_follow_rows(stored.into_iter().chain(fetched.into_iter()));
        if items.len() > limit {
            items.truncate(limit);
        }

        Ok::<FollowListResponse, RpcError>(FollowListResponse { follows: items })
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_follow_rows;
    use radroots_events::follow::{RadrootsFollow, RadrootsFollowProfile};
    use radroots_events::kinds::KIND_FOLLOW;
    use radroots_events_codec::follow::encode::follow_build_tags;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn follow_event(
        id: &str,
        pubkey: &str,
        created_at: u64,
        tags: Vec<Vec<String>>,
    ) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 9);
        let event_json = json!({
            "id": id,
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": KIND_FOLLOW,
            "tags": tags,
            "content": "",
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    #[test]
    fn follow_list_picks_latest_per_author() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let tags = vec![vec!["p".to_string(), "target".to_string()]];
        let older = follow_event(&old_id, pubkey, 100, tags.clone());
        let newer = follow_event(&new_id, pubkey, 200, tags.clone());

        let follows = build_follow_rows(vec![older, newer]);

        assert_eq!(follows.len(), 1);
        assert_eq!(follows[0].id, new_id);
        assert_eq!(follows[0].created_at, 200);
    }

    #[test]
    fn follow_list_decodes_follow_entries() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let follow = RadrootsFollow {
            list: vec![RadrootsFollowProfile {
                published_at: 0,
                public_key: "pubkey".to_string(),
                relay_url: Some("wss://relay".to_string()),
                contact_name: Some("alice".to_string()),
            }],
        };
        let tags = follow_build_tags(&follow).expect("tags");
        let id = format!("{:064x}", 3);
        let event = follow_event(&id, pubkey, 300, tags.clone());

        let follows = build_follow_rows(vec![event]);

        assert_eq!(follows.len(), 1);
        assert_eq!(follows[0].tags, tags);
        let parsed = follows[0].follow.as_ref().expect("follow");
        assert_eq!(parsed.list.len(), 1);
        assert_eq!(parsed.list[0].public_key, "pubkey");
        assert_eq!(parsed.list[0].relay_url.as_deref(), Some("wss://relay"));
        assert_eq!(parsed.list[0].contact_name.as_deref(), Some("alice"));
        assert_eq!(parsed.list[0].published_at, 300);
    }
}
