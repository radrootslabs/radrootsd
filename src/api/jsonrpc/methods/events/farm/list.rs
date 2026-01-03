use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;
use std::time::Duration;

use crate::api::jsonrpc::nostr::{event_tags, event_view_with_tags, NostrEventView};
use crate::api::jsonrpc::params::{
    apply_time_bounds,
    limit_or,
    parse_pubkeys_opt,
    timeout_or,
    EventListParams,
};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::farm::RadrootsFarm;
use radroots_events::kinds::KIND_FARM;
use radroots_events_codec::farm::decode::farm_from_event;
use radroots_nostr::prelude::{
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Debug, Serialize)]
struct FarmEventFlat {
    #[serde(flatten)]
    event: NostrEventView,
    farm: Option<RadrootsFarm>,
}

#[derive(Clone, Debug, Serialize)]
struct FarmListResponse {
    farms: Vec<FarmEventFlat>,
}

fn build_farm_rows<I>(events: I) -> Vec<FarmEventFlat>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut items = events
        .into_iter()
        .map(|ev| {
            let tags = event_tags(&ev);
            let kind = ev.kind.as_u16() as u32;
            let farm = farm_from_event(kind, &tags, &ev.content).ok();
            FarmEventFlat {
                event: event_view_with_tags(&ev, tags),
                farm,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.event.created_at.cmp(&a.event.created_at));
    items
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.farm.list");
    m.register_async_method("events.farm.list", |params, ctx, _| async move {
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
            .kind(RadrootsNostrKind::Custom(KIND_FARM as u16));

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

        let items = build_farm_rows(events);

        Ok::<FarmListResponse, RpcError>(FarmListResponse { farms: items })
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_farm_rows;
    use radroots_events::farm::RadrootsFarm;
    use radroots_events::kinds::KIND_FARM;
    use radroots_events_codec::farm::encode::farm_build_tags;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn farm_event(
        id: &str,
        pubkey: &str,
        created_at: u64,
        tags: Vec<Vec<String>>,
        content: &str,
    ) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 7);
        let event_json = json!({
            "id": id,
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": KIND_FARM,
            "tags": tags,
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    fn sample_farm(d_tag: &str, name: &str) -> RadrootsFarm {
        RadrootsFarm {
            d_tag: d_tag.to_string(),
            name: name.to_string(),
            about: None,
            website: None,
            picture: None,
            banner: None,
            location: None,
            tags: None,
        }
    }

    #[test]
    fn farm_list_sorts_by_created_at_desc() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let farm = sample_farm("farm-1", "Farm One");
        let content = serde_json::to_string(&farm).expect("content");
        let tags = farm_build_tags(&farm).expect("tags");
        let older = farm_event(&old_id, pubkey, 100, tags.clone(), &content);
        let newer = farm_event(&new_id, pubkey, 200, tags.clone(), &content);

        let farms = build_farm_rows(vec![older, newer]);

        assert_eq!(farms.len(), 2);
        assert_eq!(farms[0].event.id, new_id);
        assert_eq!(farms[0].event.created_at, 200);
        assert_eq!(farms[1].event.id, old_id);
        assert_eq!(farms[1].event.created_at, 100);
    }

    #[test]
    fn farm_list_uses_tag_d_when_missing_in_content() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let farm = sample_farm("farm-1", "Farm One");
        let tags = farm_build_tags(&farm).expect("tags");
        let content_farm = sample_farm("", "Farm One");
        let content = serde_json::to_string(&content_farm).expect("content");
        let id = format!("{:064x}", 3);
        let event = farm_event(&id, pubkey, 300, tags.clone(), &content);

        let farms = build_farm_rows(vec![event]);

        assert_eq!(farms.len(), 1);
        assert_eq!(farms[0].event.tags, tags);
        let parsed = farms[0].farm.as_ref().expect("farm");
        assert_eq!(parsed.d_tag, "farm-1");
        assert_eq!(parsed.name, "Farm One");
    }
}
