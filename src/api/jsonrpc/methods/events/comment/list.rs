#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;
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
use crate::api::jsonrpc::methods::events::helpers::require_non_empty;
use radroots_events::comment::RadrootsComment;
use radroots_events::kinds::KIND_COMMENT;
use radroots_events_codec::comment::decode::comment_from_tags;
use radroots_nostr::prelude::{
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CommentRow {
    id: String,
    author: String,
    created_at: u64,
    kind: u32,
    tags: Vec<Vec<String>>,
    content: String,
    sig: String,
    comment: Option<RadrootsComment>,
}

#[derive(Clone, Debug, Serialize)]
struct CommentListResponse {
    comments: Vec<CommentRow>,
}

#[derive(Debug, Default, Deserialize)]
struct CommentListParams {
    #[serde(flatten)]
    base: EventListParams,
    #[serde(default)]
    root_id: Option<String>,
    #[serde(default)]
    parent_id: Option<String>,
}

pub(crate) fn build_comment_rows<I>(events: I) -> Vec<CommentRow>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut items = events
        .into_iter()
        .map(|ev| {
            let tags = event_tags(&ev);
            let comment = parse_comment_event(&ev, &tags);
            let event = event_view_with_tags(&ev, tags);
            CommentRow {
                id: event.id,
                author: event.author,
                created_at: event.created_at,
                kind: event.kind,
                tags: event.tags,
                content: event.content,
                sig: event.sig,
                comment,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    items
}

fn parse_comment_event(event: &RadrootsNostrEvent, tags: &[Vec<String>]) -> Option<RadrootsComment> {
    let kind = event.kind.as_u16() as u32;
    comment_from_tags(kind, tags, &event.content).ok()
}

fn comment_matches_filter(
    comment: &RadrootsComment,
    root_id: Option<&str>,
    parent_id: Option<&str>,
) -> bool {
    if let Some(root_id) = root_id {
        if comment.root.id != root_id {
            return false;
        }
    }
    if let Some(parent_id) = parent_id {
        if comment.parent.id != parent_id {
            return false;
        }
    }
    true
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.comment.list");
    m.register_async_method("events.comment.list", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let CommentListParams {
            base,
            root_id,
            parent_id,
        } = params
            .parse::<Option<CommentListParams>>()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?
            .unwrap_or_default();

        let EventListParams {
            authors,
            limit,
            since,
            until,
            timeout_secs,
        } = base;

        let root_id = match root_id {
            Some(value) => Some(require_non_empty("root_id", value)?),
            None => None,
        };
        let parent_id = match parent_id {
            Some(value) => Some(require_non_empty("parent_id", value)?),
            None => None,
        };

        let limit = limit_or(limit);

        let mut filter = RadrootsNostrFilter::new()
            .limit(limit)
            .kind(RadrootsNostrKind::Custom(KIND_COMMENT as u16));

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

        let mut items = build_comment_rows(events);
        if root_id.is_some() || parent_id.is_some() {
            items.retain(|row| {
                row.comment
                    .as_ref()
                    .map(|comment| comment_matches_filter(comment, root_id.as_deref(), parent_id.as_deref()))
                    .unwrap_or(false)
            });
        }
        if items.len() > limit {
            items.truncate(limit);
        }

        Ok::<CommentListResponse, RpcError>(CommentListResponse { comments: items })
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_comment_rows;
    use radroots_events::comment::RadrootsComment;
    use radroots_events::kinds::{KIND_COMMENT, KIND_POST};
    use radroots_events::RadrootsNostrEventRef;
    use radroots_events_codec::comment::encode::comment_build_tags;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn comment_event(
        id: &str,
        pubkey: &str,
        created_at: u64,
        tags: Vec<Vec<String>>,
        content: &str,
    ) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 10);
        let event_json = json!({
            "id": id,
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": KIND_COMMENT,
            "tags": tags,
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    fn sample_comment(root_id: &str, parent_id: &str, author: &str, content: &str) -> RadrootsComment {
        let root = RadrootsNostrEventRef {
            id: root_id.to_string(),
            author: author.to_string(),
            kind: KIND_POST,
            d_tag: None,
            relays: None,
        };
        let parent = RadrootsNostrEventRef {
            id: parent_id.to_string(),
            author: author.to_string(),
            kind: KIND_POST,
            d_tag: None,
            relays: None,
        };
        RadrootsComment {
            root,
            parent,
            content: content.to_string(),
        }
    }

    #[test]
    fn comment_list_sorts_by_created_at_desc() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let comment = sample_comment("root-1", "parent-1", pubkey, "hello");
        let tags = comment_build_tags(&comment).expect("tags");
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let older = comment_event(&old_id, pubkey, 100, tags.clone(), &comment.content);
        let newer = comment_event(&new_id, pubkey, 200, tags.clone(), &comment.content);

        let comments = build_comment_rows(vec![older, newer]);

        assert_eq!(comments.len(), 2);
        assert_eq!(comments[0].id, new_id);
        assert_eq!(comments[0].created_at, 200);
        assert_eq!(comments[1].id, old_id);
        assert_eq!(comments[1].created_at, 100);
    }

    #[test]
    fn comment_list_decodes_comment() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let comment = sample_comment("root-1", "parent-1", pubkey, "hello");
        let tags = comment_build_tags(&comment).expect("tags");
        let id = format!("{:064x}", 3);
        let event = comment_event(&id, pubkey, 300, tags.clone(), &comment.content);

        let comments = build_comment_rows(vec![event]);

        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].tags, tags);
        let parsed = comments[0].comment.as_ref().expect("comment");
        assert_eq!(parsed.content, "hello");
        assert_eq!(parsed.root.id, "root-1");
        assert_eq!(parsed.parent.id, "parent-1");
    }

    #[test]
    fn comment_filters_match_root_and_parent() {
        let pubkey = "3bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let comment = sample_comment("root-1", "parent-1", pubkey, "hello");

        assert!(super::comment_matches_filter(&comment, Some("root-1"), None));
        assert!(super::comment_matches_filter(&comment, None, Some("parent-1")));
        assert!(super::comment_matches_filter(&comment, Some("root-1"), Some("parent-1")));
        assert!(!super::comment_matches_filter(&comment, Some("root-2"), None));
        assert!(!super::comment_matches_filter(&comment, None, Some("parent-2")));
    }
}
