#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::api::jsonrpc::nostr::{event_tags, event_view_with_tags, NostrEventView};
use crate::api::jsonrpc::params::{apply_time_bounds, limit_or, parse_pubkeys_opt, timeout_or};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::{is_nip51_list_set_kind, KIND_LIST_SET_GENERIC};
use radroots_events::list_set::RadrootsListSet;
use radroots_events_codec::list_set::decode::list_set_from_tags;
use radroots_nostr::prelude::{
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Debug, Serialize)]
struct ListSetEventFlat {
    #[serde(flatten)]
    event: NostrEventView,
    list_set: Option<RadrootsListSet>,
}

#[derive(Clone, Debug, Serialize)]
struct ListSetListResponse {
    list_sets: Vec<ListSetEventFlat>,
}

#[derive(Debug, Default, Deserialize)]
struct ListSetListParams {
    #[serde(default)]
    authors: Option<Vec<String>>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    since: Option<u64>,
    #[serde(default)]
    until: Option<u64>,
    #[serde(default)]
    timeout_secs: Option<u64>,
    #[serde(default)]
    kinds: Option<Vec<u32>>,
    #[serde(default)]
    d_tags: Option<Vec<String>>,
}

fn list_set_kinds_or(kinds: Option<Vec<u32>>) -> Result<Vec<RadrootsNostrKind>, RpcError> {
    let kinds = kinds.unwrap_or_else(|| vec![KIND_LIST_SET_GENERIC]);
    if kinds.is_empty() {
        return Err(RpcError::InvalidParams(
            "list_set kinds cannot be empty".to_string(),
        ));
    }
    let mut out = Vec::with_capacity(kinds.len());
    for kind in kinds {
        if !is_nip51_list_set_kind(kind) {
            return Err(RpcError::InvalidParams(format!(
                "invalid list_set kind: {kind}"
            )));
        }
        let kind = u16::try_from(kind).map_err(|_| {
            RpcError::InvalidParams(format!("list_set kind out of range: {kind}"))
        })?;
        out.push(RadrootsNostrKind::Custom(kind));
    }
    Ok(out)
}

fn build_list_set_rows<I>(events: I) -> Vec<ListSetEventFlat>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut items = events
        .into_iter()
        .map(|ev| {
            let tags = event_tags(&ev);
            let kind = ev.kind.as_u16() as u32;
            let list_set = list_set_from_tags(kind, ev.content.clone(), &tags).ok();
            ListSetEventFlat {
                event: event_view_with_tags(&ev, tags),
                list_set,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.event.created_at.cmp(&a.event.created_at));
    items
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.list_set.list");
    m.register_async_method("events.list_set.list", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let ListSetListParams {
            authors,
            limit,
            since,
            until,
            timeout_secs,
            kinds,
            d_tags,
        } = params
            .parse::<Option<ListSetListParams>>()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?
            .unwrap_or_default();

        let limit = limit_or(limit);
        let kinds = list_set_kinds_or(kinds)?;

        let mut filter = RadrootsNostrFilter::new().limit(limit).kinds(kinds);

        if let Some(authors) = parse_pubkeys_opt("author", authors)? {
            filter = filter.authors(authors);
        } else {
            filter = filter.author(ctx.state.pubkey);
        }

        if let Some(d_tags) = d_tags {
            if !d_tags.is_empty() {
                filter = filter.identifiers(d_tags);
            }
        }

        filter = apply_time_bounds(filter, since, until);

        let events = ctx
            .state
            .client
            .fetch_events(filter, Duration::from_secs(timeout_or(timeout_secs)))
            .await
            .map_err(|e| RpcError::Other(format!("fetch failed: {e}")))?;

        let items = build_list_set_rows(events);

        Ok::<ListSetListResponse, RpcError>(ListSetListResponse { list_sets: items })
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_list_set_rows;
    use radroots_events::kinds::KIND_LIST_SET_GENERIC;
    use radroots_events::list::RadrootsListEntry;
    use radroots_events::list_set::RadrootsListSet;
    use radroots_events_codec::list_set::encode::list_set_build_tags;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn list_set_event(
        id: &str,
        pubkey: &str,
        created_at: u64,
        tags: Vec<Vec<String>>,
        content: &str,
    ) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 12);
        let event_json = json!({
            "id": id,
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": KIND_LIST_SET_GENERIC,
            "tags": tags,
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    fn sample_list_set(d_tag: &str, pubkey: &str) -> RadrootsListSet {
        RadrootsListSet {
            d_tag: d_tag.to_string(),
            content: String::new(),
            entries: vec![RadrootsListEntry {
                tag: "p".to_string(),
                values: vec![pubkey.to_string()],
            }],
            title: None,
            description: None,
            image: None,
        }
    }

    #[test]
    fn list_set_list_sorts_by_created_at_desc() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let list_set = sample_list_set("member_of.farms", pubkey);
        let content = list_set.content.clone();
        let tags = list_set_build_tags(&list_set).expect("tags");
        let older = list_set_event(&old_id, pubkey, 100, tags.clone(), &content);
        let newer = list_set_event(&new_id, pubkey, 200, tags.clone(), &content);

        let list_sets = build_list_set_rows(vec![older, newer]);

        assert_eq!(list_sets.len(), 2);
        assert_eq!(list_sets[0].event.id, new_id);
        assert_eq!(list_sets[0].event.created_at, 200);
        assert_eq!(list_sets[1].event.id, old_id);
        assert_eq!(list_sets[1].event.created_at, 100);
    }

    #[test]
    fn list_set_list_decodes_entries() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let list_set = sample_list_set("member_of.farms", pubkey);
        let content = list_set.content.clone();
        let tags = list_set_build_tags(&list_set).expect("tags");
        let id = format!("{:064x}", 3);
        let event = list_set_event(&id, pubkey, 300, tags.clone(), &content);

        let list_sets = build_list_set_rows(vec![event]);

        assert_eq!(list_sets.len(), 1);
        assert_eq!(list_sets[0].event.tags, tags);
        let parsed = list_sets[0].list_set.as_ref().expect("list set");
        assert_eq!(parsed.d_tag, "member_of.farms");
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.entries[0].tag, "p");
        assert_eq!(parsed.entries[0].values, vec![pubkey.to_string()]);
    }
}
