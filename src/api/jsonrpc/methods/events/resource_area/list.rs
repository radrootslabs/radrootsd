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
use radroots_events::kinds::KIND_RESOURCE_AREA;
use radroots_events::resource_area::RadrootsResourceArea;
use radroots_events_codec::resource_area::decode::resource_area_from_event;
use radroots_nostr::prelude::{
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Debug, Serialize)]
struct ResourceAreaRow {
    id: String,
    author: String,
    created_at: u64,
    kind: u32,
    tags: Vec<Vec<String>>,
    content: String,
    sig: String,
    resource_area: Option<RadrootsResourceArea>,
}

#[derive(Clone, Debug, Serialize)]
struct ResourceAreaListResponse {
    resource_areas: Vec<ResourceAreaRow>,
}

fn build_resource_area_rows<I>(events: I) -> Vec<ResourceAreaRow>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut items = events
        .into_iter()
        .map(|ev| {
            let tags = event_tags(&ev);
            let resource_area = parse_resource_area_event(&ev, &tags);
            let event = event_view_with_tags(&ev, tags);
            ResourceAreaRow {
                id: event.id,
                author: event.author,
                created_at: event.created_at,
                kind: event.kind,
                tags: event.tags,
                content: event.content,
                sig: event.sig,
                resource_area,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    items
}

fn parse_resource_area_event(
    event: &RadrootsNostrEvent,
    tags: &[Vec<String>],
) -> Option<RadrootsResourceArea> {
    let kind = event.kind.as_u16() as u32;
    resource_area_from_event(kind, tags, &event.content).ok()
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.resource_area.list");
    m.register_async_method("events.resource_area.list", |params, ctx, _| async move {
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
            .kind(RadrootsNostrKind::Custom(KIND_RESOURCE_AREA as u16));

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

        let items = build_resource_area_rows(events);

        Ok::<ResourceAreaListResponse, RpcError>(ResourceAreaListResponse {
            resource_areas: items,
        })
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_resource_area_rows;
    use radroots_events::farm::{RadrootsGcsLocation, RadrootsGeoJsonPoint, RadrootsGeoJsonPolygon};
    use radroots_events::kinds::KIND_RESOURCE_AREA;
    use radroots_events::resource_area::{
        RadrootsResourceArea, RadrootsResourceAreaLocation,
    };
    use radroots_events_codec::resource_area::encode::resource_area_build_tags;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn resource_area_event(
        id: &str,
        pubkey: &str,
        created_at: u64,
        tags: Vec<Vec<String>>,
        content: &str,
    ) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 9);
        let event_json = json!({
            "id": id,
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": KIND_RESOURCE_AREA,
            "tags": tags,
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    fn sample_location() -> RadrootsResourceAreaLocation {
        let point = RadrootsGeoJsonPoint {
            r#type: "Point".to_string(),
            coordinates: [-76.9714, -6.0346],
        };
        let polygon = RadrootsGeoJsonPolygon {
            r#type: "Polygon".to_string(),
            coordinates: vec![vec![
                [-76.9714, -6.0346],
                [-76.9712, -6.0346],
                [-76.9712, -6.0344],
                [-76.9714, -6.0344],
                [-76.9714, -6.0346],
            ]],
        };
        let gcs = RadrootsGcsLocation {
            lat: -6.0346,
            lng: -76.9714,
            geohash: "6m6t5x".to_string(),
            point,
            polygon,
            accuracy: None,
            altitude: None,
            tag_0: None,
            label: None,
            area: None,
            elevation: None,
            soil: None,
            climate: None,
            gc_id: None,
            gc_name: None,
            gc_admin1_id: None,
            gc_admin1_name: None,
            gc_country_id: None,
            gc_country_name: None,
        };
        RadrootsResourceAreaLocation {
            primary: Some("Moyobamba".to_string()),
            city: None,
            region: None,
            country: None,
            gcs,
        }
    }

    fn sample_resource_area(d_tag: &str, name: &str) -> RadrootsResourceArea {
        RadrootsResourceArea {
            d_tag: d_tag.to_string(),
            name: name.to_string(),
            about: None,
            location: sample_location(),
            tags: None,
        }
    }

    #[test]
    fn resource_area_list_sorts_by_created_at_desc() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let area = sample_resource_area("area-1", "Area One");
        let content = serde_json::to_string(&area).expect("content");
        let tags = resource_area_build_tags(&area).expect("tags");
        let older = resource_area_event(&old_id, pubkey, 100, tags.clone(), &content);
        let newer = resource_area_event(&new_id, pubkey, 200, tags.clone(), &content);

        let areas = build_resource_area_rows(vec![older, newer]);

        assert_eq!(areas.len(), 2);
        assert_eq!(areas[0].id, new_id);
        assert_eq!(areas[0].created_at, 200);
        assert_eq!(areas[1].id, old_id);
        assert_eq!(areas[1].created_at, 100);
    }

    #[test]
    fn resource_area_list_uses_tag_d_when_missing_in_content() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let area = sample_resource_area("area-1", "Area One");
        let tags = resource_area_build_tags(&area).expect("tags");
        let content_area = sample_resource_area("", "Area One");
        let content = serde_json::to_string(&content_area).expect("content");
        let id = format!("{:064x}", 3);
        let event = resource_area_event(&id, pubkey, 300, tags.clone(), &content);

        let areas = build_resource_area_rows(vec![event]);

        assert_eq!(areas.len(), 1);
        assert_eq!(areas[0].tags, tags);
        let parsed = areas[0].resource_area.as_ref().expect("area");
        assert_eq!(parsed.d_tag, "area-1");
        assert_eq!(parsed.name, "Area One");
    }
}
