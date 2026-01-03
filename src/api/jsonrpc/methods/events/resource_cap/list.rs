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
use radroots_events::kinds::KIND_RESOURCE_HARVEST_CAP;
use radroots_events::resource_cap::RadrootsResourceHarvestCap;
use radroots_events_codec::resource_cap::decode::resource_harvest_cap_from_event;
use radroots_nostr::prelude::{
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Debug, Serialize)]
struct ResourceCapEventFlat {
    #[serde(flatten)]
    event: NostrEventView,
    resource_cap: Option<RadrootsResourceHarvestCap>,
}

#[derive(Clone, Debug, Serialize)]
struct ResourceCapListResponse {
    resource_caps: Vec<ResourceCapEventFlat>,
}

fn build_resource_cap_rows<I>(events: I) -> Vec<ResourceCapEventFlat>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut items = events
        .into_iter()
        .map(|ev| {
            let tags = event_tags(&ev);
            let kind = ev.kind.as_u16() as u32;
            let resource_cap = resource_harvest_cap_from_event(kind, &tags, &ev.content).ok();
            ResourceCapEventFlat {
                event: event_view_with_tags(&ev, tags),
                resource_cap,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.event.created_at.cmp(&a.event.created_at));
    items
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.resource_cap.list");
    m.register_async_method("events.resource_cap.list", |params, ctx, _| async move {
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
            .kind(RadrootsNostrKind::Custom(KIND_RESOURCE_HARVEST_CAP as u16));

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

        let items = build_resource_cap_rows(events);

        Ok::<ResourceCapListResponse, RpcError>(ResourceCapListResponse {
            resource_caps: items,
        })
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_resource_cap_rows;
    use radroots_core::{RadrootsCoreDecimal, RadrootsCoreQuantity, RadrootsCoreUnit};
    use radroots_events::kinds::KIND_RESOURCE_HARVEST_CAP;
    use radroots_events::resource_area::RadrootsResourceAreaRef;
    use radroots_events::resource_cap::{RadrootsResourceHarvestCap, RadrootsResourceHarvestProduct};
    use radroots_events_codec::resource_cap::encode::resource_harvest_cap_build_tags;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn resource_cap_event(
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
            "kind": KIND_RESOURCE_HARVEST_CAP,
            "tags": tags,
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    fn sample_cap(d_tag: &str, area_pubkey: &str, area_d_tag: &str) -> RadrootsResourceHarvestCap {
        let quantity = RadrootsCoreQuantity::new(
            RadrootsCoreDecimal::from(100_u64),
            RadrootsCoreUnit::MassG,
        );
        RadrootsResourceHarvestCap {
            d_tag: d_tag.to_string(),
            resource_area: RadrootsResourceAreaRef {
                pubkey: area_pubkey.to_string(),
                d_tag: area_d_tag.to_string(),
            },
            product: RadrootsResourceHarvestProduct {
                key: "coffee".to_string(),
                category: None,
            },
            start: 100,
            end: 200,
            cap_quantity: quantity,
            display_amount: None,
            display_unit: None,
            display_label: None,
            tags: None,
        }
    }

    #[test]
    fn resource_cap_list_sorts_by_created_at_desc() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let cap = sample_cap("cap-1", pubkey, "area-1");
        let content = serde_json::to_string(&cap).expect("content");
        let tags = resource_harvest_cap_build_tags(&cap).expect("tags");
        let older = resource_cap_event(&old_id, pubkey, 100, tags.clone(), &content);
        let newer = resource_cap_event(&new_id, pubkey, 200, tags.clone(), &content);

        let caps = build_resource_cap_rows(vec![older, newer]);

        assert_eq!(caps.len(), 2);
        assert_eq!(caps[0].event.id, new_id);
        assert_eq!(caps[0].event.created_at, 200);
        assert_eq!(caps[1].event.id, old_id);
        assert_eq!(caps[1].event.created_at, 100);
    }

    #[test]
    fn resource_cap_list_uses_tag_d_when_missing_in_content() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let cap = sample_cap("cap-1", pubkey, "area-1");
        let tags = resource_harvest_cap_build_tags(&cap).expect("tags");
        let mut content_cap = sample_cap("", pubkey, "area-1");
        content_cap.display_label = Some("display".to_string());
        let content = serde_json::to_string(&content_cap).expect("content");
        let id = format!("{:064x}", 3);
        let event = resource_cap_event(&id, pubkey, 300, tags.clone(), &content);

        let caps = build_resource_cap_rows(vec![event]);

        assert_eq!(caps.len(), 1);
        assert_eq!(caps[0].event.tags, tags);
        let parsed = caps[0].resource_cap.as_ref().expect("cap");
        assert_eq!(parsed.d_tag, "cap-1");
        assert_eq!(parsed.resource_area.d_tag, "area-1");
        assert_eq!(parsed.product.key, "coffee");
    }
}
