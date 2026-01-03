use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;
use std::time::Duration;

use crate::api::jsonrpc::nostr::{event_view, NostrEventView};
use crate::api::jsonrpc::params::{
    apply_time_bounds,
    limit_or,
    parse_pubkeys_opt,
    timeout_or,
    EventListParams,
};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_nostr::prelude::{
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Debug, Serialize)]
struct PostListResponse {
    posts: Vec<NostrEventView>,
}

fn build_post_rows<I>(events: I) -> Vec<NostrEventView>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut items = events.into_iter().map(|ev| event_view(&ev)).collect::<Vec<_>>();
    items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    items
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.post.list");
    m.register_async_method("events.post.list", |params, ctx, _| async move {
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
            .kind(RadrootsNostrKind::TextNote)
            .limit(limit);
        if let Some(authors) = parse_pubkeys_opt("author", authors)? {
            filter = filter.authors(authors);
        } else {
            filter = filter.author(ctx.state.pubkey);
        }
        filter = apply_time_bounds(filter, since, until);

        let events = ctx
            .state
            .client
            .fetch_events(filter, Duration::from_secs(timeout_or(timeout_secs)))
            .await
            .map_err(|e| RpcError::Other(format!("fetch failed: {e}")))?;

        let items = build_post_rows(events);

        Ok::<PostListResponse, RpcError>(PostListResponse { posts: items })
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_post_rows;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn post_event(
        id: &str,
        pubkey: &str,
        created_at: u64,
        content: &str,
        tags: Vec<Vec<String>>,
    ) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 4);
        let event_json = json!({
            "id": id,
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": 1,
            "tags": tags,
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    #[test]
    fn post_list_sorts_by_created_at_desc() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let older = post_event(&old_id, pubkey, 100, "old", Vec::new());
        let newer = post_event(&new_id, pubkey, 200, "new", Vec::new());

        let posts = build_post_rows(vec![older, newer]);

        assert_eq!(posts.len(), 2);
        assert_eq!(posts[0].id, new_id);
        assert_eq!(posts[0].created_at, 200);
        assert_eq!(posts[1].id, old_id);
        assert_eq!(posts[1].created_at, 100);
    }

    #[test]
    fn post_list_preserves_content_and_tags() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let id = format!("{:064x}", 3);
        let tags = vec![vec!["t".to_string(), "radroots".to_string()]];
        let event = post_event(&id, pubkey, 300, "hello", tags.clone());

        let posts = build_post_rows(vec![event]);

        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].content, "hello");
        assert_eq!(posts[0].tags, tags);
    }
}
