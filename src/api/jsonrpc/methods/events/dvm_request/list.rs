#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::api::jsonrpc::nostr::{event_tags, event_view_with_tags};
use crate::api::jsonrpc::params::{
    apply_time_bounds,
    limit_or,
    parse_pubkeys_opt,
    timeout_or,
};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::job_request::RadrootsJobRequest;
use radroots_events::kinds::{is_request_kind, KIND_JOB_REQUEST_MAX, KIND_JOB_REQUEST_MIN};
use radroots_events_codec::job::request::decode::job_request_from_tags;
use radroots_nostr::prelude::{
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DvmRequestRow {
    id: String,
    author: String,
    created_at: u64,
    kind: u32,
    tags: Vec<Vec<String>>,
    content: String,
    sig: String,
    request: Option<RadrootsJobRequest>,
}

#[derive(Clone, Debug, Serialize)]
struct DvmRequestListResponse {
    requests: Vec<DvmRequestRow>,
}

#[derive(Debug, Default, Deserialize)]
struct DvmRequestListParams {
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
}

fn dvm_request_kinds_or(kinds: Option<Vec<u32>>) -> Result<Vec<RadrootsNostrKind>, RpcError> {
    let kinds = match kinds {
        Some(kinds) => {
            if kinds.is_empty() {
                return Err(RpcError::InvalidParams(
                    "dvm request kinds cannot be empty".to_string(),
                ));
            }
            kinds
        }
        None => (KIND_JOB_REQUEST_MIN..=KIND_JOB_REQUEST_MAX).collect(),
    };

    let mut out = Vec::with_capacity(kinds.len());
    for kind in kinds {
        if !is_request_kind(kind) {
            return Err(RpcError::InvalidParams(format!(
                "invalid dvm request kind: {kind}",
            )));
        }
        let kind = u16::try_from(kind)
            .map_err(|_| RpcError::InvalidParams(format!("dvm request kind out of range: {kind}")))?;
        out.push(RadrootsNostrKind::Custom(kind));
    }
    Ok(out)
}

pub(crate) fn build_dvm_request_rows<I>(events: I) -> Vec<DvmRequestRow>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut items = events
        .into_iter()
        .map(|ev| {
            let tags = event_tags(&ev);
            let request = parse_dvm_request_event(&ev, &tags);
            let event = event_view_with_tags(&ev, tags);
            DvmRequestRow {
                id: event.id,
                author: event.author,
                created_at: event.created_at,
                kind: event.kind,
                tags: event.tags,
                content: event.content,
                sig: event.sig,
                request,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    items
}

fn parse_dvm_request_event(
    event: &RadrootsNostrEvent,
    tags: &[Vec<String>],
) -> Option<RadrootsJobRequest> {
    let kind = event.kind.as_u16() as u32;
    if !is_request_kind(kind) {
        return None;
    }
    job_request_from_tags(kind, tags).ok()
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.dvm_request.list");
    m.register_async_method("events.dvm_request.list", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let DvmRequestListParams {
            authors,
            limit,
            since,
            until,
            timeout_secs,
            kinds,
        } = params
            .parse::<Option<DvmRequestListParams>>()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?
            .unwrap_or_default();

        let limit = limit_or(limit);
        let kinds = dvm_request_kinds_or(kinds)?;

        let mut filter = RadrootsNostrFilter::new().limit(limit).kinds(kinds);

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

        let items = build_dvm_request_rows(events);

        Ok::<DvmRequestListResponse, RpcError>(DvmRequestListResponse { requests: items })
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_dvm_request_rows;
    use radroots_events::job::JobInputType;
    use radroots_events::job_request::{RadrootsJobInput, RadrootsJobRequest};
    use radroots_events::kinds::KIND_JOB_REQUEST_MIN;
    use radroots_events_codec::job::request::encode::job_request_build_tags;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn dvm_request_event(
        id: &str,
        pubkey: &str,
        created_at: u64,
        kind: u32,
        tags: Vec<Vec<String>>,
        content: &str,
    ) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 9);
        let event_json = json!({
            "id": id,
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": kind,
            "tags": tags,
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    fn sample_request() -> RadrootsJobRequest {
        RadrootsJobRequest {
            kind: (KIND_JOB_REQUEST_MIN + 1) as u16,
            inputs: vec![RadrootsJobInput {
                data: "https://example.com".to_string(),
                input_type: JobInputType::Url,
                relay: None,
                marker: None,
            }],
            output: None,
            params: Vec::new(),
            bid_sat: None,
            relays: Vec::new(),
            providers: Vec::new(),
            topics: Vec::new(),
            encrypted: false,
        }
    }

    #[test]
    fn dvm_request_list_sorts_by_created_at_desc() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let req = sample_request();
        let tags = job_request_build_tags(&req);
        let older = dvm_request_event(&old_id, pubkey, 100, KIND_JOB_REQUEST_MIN + 1, tags.clone(), "");
        let newer = dvm_request_event(&new_id, pubkey, 200, KIND_JOB_REQUEST_MIN + 1, tags.clone(), "");

        let requests = build_dvm_request_rows(vec![older, newer]);

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].id, new_id);
        assert_eq!(requests[0].created_at, 200);
        assert_eq!(requests[1].id, old_id);
        assert_eq!(requests[1].created_at, 100);
    }

    #[test]
    fn dvm_request_list_decodes_request() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let req = sample_request();
        let tags = job_request_build_tags(&req);
        let id = format!("{:064x}", 3);
        let event = dvm_request_event(&id, pubkey, 300, KIND_JOB_REQUEST_MIN + 1, tags.clone(), "payload");

        let requests = build_dvm_request_rows(vec![event]);

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].tags, tags);
        let decoded = requests[0].request.as_ref().expect("request");
        assert_eq!(decoded, &req);
    }
}
