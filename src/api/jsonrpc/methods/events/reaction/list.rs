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
use radroots_events::kinds::KIND_REACTION;
use radroots_events::reaction::RadrootsReaction;
use radroots_events_codec::reaction::decode::reaction_from_tags;
use radroots_nostr::prelude::{
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ReactionRow {
    id: String,
    author: String,
    created_at: u64,
    kind: u32,
    tags: Vec<Vec<String>>,
    content: String,
    sig: String,
    reaction: Option<RadrootsReaction>,
}

#[derive(Clone, Debug, Serialize)]
struct ReactionListResponse {
    reactions: Vec<ReactionRow>,
}

pub(crate) fn build_reaction_rows<I>(events: I) -> Vec<ReactionRow>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut items = events
        .into_iter()
        .map(|ev| {
            let tags = event_tags(&ev);
            let reaction = parse_reaction_event(&ev, &tags);
            let event = event_view_with_tags(&ev, tags);
            ReactionRow {
                id: event.id,
                author: event.author,
                created_at: event.created_at,
                kind: event.kind,
                tags: event.tags,
                content: event.content,
                sig: event.sig,
                reaction,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    items
}

fn parse_reaction_event(event: &RadrootsNostrEvent, tags: &[Vec<String>]) -> Option<RadrootsReaction> {
    let kind = event.kind.as_u16() as u32;
    reaction_from_tags(kind, tags, &event.content).ok()
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.reaction.list");
    m.register_async_method("events.reaction.list", |params, ctx, _| async move {
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
            .limit(limit)
            .kind(RadrootsNostrKind::Custom(KIND_REACTION as u16));

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

        let items = build_reaction_rows(events);

        Ok::<ReactionListResponse, RpcError>(ReactionListResponse { reactions: items })
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_reaction_rows;
    use radroots_events::kinds::{KIND_REACTION, KIND_POST};
    use radroots_events::reaction::RadrootsReaction;
    use radroots_events::RadrootsNostrEventRef;
    use radroots_events_codec::reaction::encode::reaction_build_tags;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn reaction_event(
        id: &str,
        pubkey: &str,
        created_at: u64,
        tags: Vec<Vec<String>>,
        content: &str,
    ) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 11);
        let event_json = json!({
            "id": id,
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": KIND_REACTION,
            "tags": tags,
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    fn sample_reaction(event_id: &str, author: &str, content: &str) -> RadrootsReaction {
        let root = RadrootsNostrEventRef {
            id: event_id.to_string(),
            author: author.to_string(),
            kind: KIND_POST,
            d_tag: None,
            relays: None,
        };
        RadrootsReaction {
            root,
            content: content.to_string(),
        }
    }

    #[test]
    fn reaction_list_sorts_by_created_at_desc() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let reaction = sample_reaction("root-1", pubkey, "+");
        let tags = reaction_build_tags(&reaction).expect("tags");
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let older = reaction_event(&old_id, pubkey, 100, tags.clone(), &reaction.content);
        let newer = reaction_event(&new_id, pubkey, 200, tags.clone(), &reaction.content);

        let reactions = build_reaction_rows(vec![older, newer]);

        assert_eq!(reactions.len(), 2);
        assert_eq!(reactions[0].id, new_id);
        assert_eq!(reactions[0].created_at, 200);
        assert_eq!(reactions[1].id, old_id);
        assert_eq!(reactions[1].created_at, 100);
    }

    #[test]
    fn reaction_list_decodes_reaction() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let reaction = sample_reaction("root-1", pubkey, "+");
        let tags = reaction_build_tags(&reaction).expect("tags");
        let id = format!("{:064x}", 3);
        let event = reaction_event(&id, pubkey, 300, tags.clone(), &reaction.content);

        let reactions = build_reaction_rows(vec![event]);

        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0].tags, tags);
        let parsed = reactions[0].reaction.as_ref().expect("reaction");
        assert_eq!(parsed.content, "+");
        assert_eq!(parsed.root.id, "root-1");
    }
}
