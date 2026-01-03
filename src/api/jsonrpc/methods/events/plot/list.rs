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
use radroots_events::kinds::KIND_PLOT;
use radroots_events::plot::RadrootsPlot;
use radroots_events_codec::plot::decode::plot_from_event;
use radroots_nostr::prelude::{
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Debug, Serialize)]
struct PlotEventFlat {
    #[serde(flatten)]
    event: NostrEventView,
    plot: Option<RadrootsPlot>,
}

#[derive(Clone, Debug, Serialize)]
struct PlotListResponse {
    plots: Vec<PlotEventFlat>,
}

fn build_plot_rows<I>(events: I) -> Vec<PlotEventFlat>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut items = events
        .into_iter()
        .map(|ev| {
            let tags = event_tags(&ev);
            let kind = ev.kind.as_u16() as u32;
            let plot = plot_from_event(kind, &tags, &ev.content).ok();
            PlotEventFlat {
                event: event_view_with_tags(&ev, tags),
                plot,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.event.created_at.cmp(&a.event.created_at));
    items
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.plot.list");
    m.register_async_method("events.plot.list", |params, ctx, _| async move {
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
            .kind(RadrootsNostrKind::Custom(KIND_PLOT as u16));

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

        let items = build_plot_rows(events);

        Ok::<PlotListResponse, RpcError>(PlotListResponse { plots: items })
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_plot_rows;
    use radroots_events::farm::RadrootsFarmRef;
    use radroots_events::kinds::KIND_PLOT;
    use radroots_events::plot::RadrootsPlot;
    use radroots_events_codec::plot::encode::plot_build_tags;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn plot_event(
        id: &str,
        pubkey: &str,
        created_at: u64,
        tags: Vec<Vec<String>>,
        content: &str,
    ) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 8);
        let event_json = json!({
            "id": id,
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": KIND_PLOT,
            "tags": tags,
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    fn sample_plot(d_tag: &str, name: &str, farm_pubkey: &str, farm_d_tag: &str) -> RadrootsPlot {
        RadrootsPlot {
            d_tag: d_tag.to_string(),
            farm: RadrootsFarmRef {
                pubkey: farm_pubkey.to_string(),
                d_tag: farm_d_tag.to_string(),
            },
            name: name.to_string(),
            about: None,
            location: None,
            tags: None,
        }
    }

    #[test]
    fn plot_list_sorts_by_created_at_desc() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let farm_pubkey = pubkey;
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let plot = sample_plot("plot-1", "Plot One", farm_pubkey, "farm-1");
        let content = serde_json::to_string(&plot).expect("content");
        let tags = plot_build_tags(&plot).expect("tags");
        let older = plot_event(&old_id, pubkey, 100, tags.clone(), &content);
        let newer = plot_event(&new_id, pubkey, 200, tags.clone(), &content);

        let plots = build_plot_rows(vec![older, newer]);

        assert_eq!(plots.len(), 2);
        assert_eq!(plots[0].event.id, new_id);
        assert_eq!(plots[0].event.created_at, 200);
        assert_eq!(plots[1].event.id, old_id);
        assert_eq!(plots[1].event.created_at, 100);
    }

    #[test]
    fn plot_list_uses_tag_fields_when_missing_in_content() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let plot = sample_plot("plot-1", "Plot One", pubkey, "farm-1");
        let tags = plot_build_tags(&plot).expect("tags");
        let content_plot = sample_plot("", "Plot One", "", "");
        let content = serde_json::to_string(&content_plot).expect("content");
        let id = format!("{:064x}", 3);
        let event = plot_event(&id, pubkey, 300, tags.clone(), &content);

        let plots = build_plot_rows(vec![event]);

        assert_eq!(plots.len(), 1);
        assert_eq!(plots[0].event.tags, tags);
        let parsed = plots[0].plot.as_ref().expect("plot");
        assert_eq!(parsed.d_tag, "plot-1");
        assert_eq!(parsed.farm.pubkey, pubkey);
        assert_eq!(parsed.farm.d_tag, "farm-1");
    }
}
