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
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Debug, Serialize)]
struct PostListResponse {
    posts: Vec<NostrEventView>,
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

        let mut items = events.into_iter().map(|ev| event_view(&ev)).collect::<Vec<_>>();
        items.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        Ok::<PostListResponse, RpcError>(PostListResponse { posts: items })
    })?;

    Ok(())
}
