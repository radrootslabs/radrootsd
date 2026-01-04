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
use radroots_events::job_result::RadrootsJobResult;
use radroots_events::kinds::{is_result_kind, KIND_JOB_RESULT_MAX, KIND_JOB_RESULT_MIN};
use radroots_events_codec::job::result::decode::job_result_from_tags;
use radroots_nostr::prelude::{
    RadrootsNostrEvent,
    RadrootsNostrEventId,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DvmResultRow {
    id: String,
    author: String,
    created_at: u64,
    kind: u32,
    tags: Vec<Vec<String>>,
    content: String,
    sig: String,
    result: Option<RadrootsJobResult>,
}

#[derive(Clone, Debug, Serialize)]
struct DvmResultListResponse {
    results: Vec<DvmResultRow>,
}

#[derive(Debug, Default, Deserialize)]
struct DvmResultListParams {
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
    request_id: Option<String>,
}

fn dvm_result_kinds_or(kinds: Option<Vec<u32>>) -> Result<Vec<RadrootsNostrKind>, RpcError> {
    let kinds = match kinds {
        Some(kinds) => {
            if kinds.is_empty() {
                return Err(RpcError::InvalidParams(
                    "dvm result kinds cannot be empty".to_string(),
                ));
            }
            kinds
        }
        None => (KIND_JOB_RESULT_MIN..=KIND_JOB_RESULT_MAX).collect(),
    };

    let mut out = Vec::with_capacity(kinds.len());
    for kind in kinds {
        if !is_result_kind(kind) {
            return Err(RpcError::InvalidParams(format!(
                "invalid dvm result kind: {kind}",
            )));
        }
        let kind = u16::try_from(kind)
            .map_err(|_| RpcError::InvalidParams(format!("dvm result kind out of range: {kind}")))?;
        out.push(RadrootsNostrKind::Custom(kind));
    }
    Ok(out)
}

pub(crate) fn build_dvm_result_rows<I>(events: I) -> Vec<DvmResultRow>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut items = events
        .into_iter()
        .map(|ev| {
            let tags = event_tags(&ev);
            let result = parse_dvm_result_event(&ev, &tags);
            let event = event_view_with_tags(&ev, tags);
            DvmResultRow {
                id: event.id,
                author: event.author,
                created_at: event.created_at,
                kind: event.kind,
                tags: event.tags,
                content: event.content,
                sig: event.sig,
                result,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    items
}

fn parse_dvm_result_event(
    event: &RadrootsNostrEvent,
    tags: &[Vec<String>],
) -> Option<RadrootsJobResult> {
    let kind = event.kind.as_u16() as u32;
    if !is_result_kind(kind) {
        return None;
    }
    job_result_from_tags(kind, tags, &event.content).ok()
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.dvm_result.list");
    m.register_async_method("events.dvm_result.list", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let DvmResultListParams {
            authors,
            limit,
            since,
            until,
            timeout_secs,
            kinds,
            request_id,
        } = params
            .parse::<Option<DvmResultListParams>>()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?
            .unwrap_or_default();

        let limit = limit_or(limit);
        let kinds = dvm_result_kinds_or(kinds)?;

        let mut filter = RadrootsNostrFilter::new().limit(limit).kinds(kinds);

        if let Some(authors) = parse_pubkeys_opt("author", authors)? {
            filter = filter.authors(authors);
        } else {
            filter = filter.author(ctx.state.pubkey);
        }
        filter = apply_time_bounds(filter, since, until);

        if let Some(request_id) = request_id {
            let request_id = request_id.trim();
            if request_id.is_empty() {
                return Err(RpcError::InvalidParams(
                    "request_id cannot be empty".to_string(),
                ));
            }
            let request_id = RadrootsNostrEventId::parse(request_id)
                .map_err(|e| RpcError::InvalidParams(format!("invalid request_id: {e}")))?;
            filter = filter.event(request_id);
        }

        let events = ctx
            .state
            .client
            .fetch_events(filter, Duration::from_secs(timeout_or(timeout_secs)))
            .await
            .map_err(|e| RpcError::Other(format!("fetch failed: {e}")))?;

        let items = build_dvm_result_rows(events);

        Ok::<DvmResultListResponse, RpcError>(DvmResultListResponse { results: items })
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_dvm_result_rows;
    use radroots_events::job_request::RadrootsJobInput;
    use radroots_events::job_result::RadrootsJobResult;
    use radroots_events::kinds::KIND_JOB_RESULT_MIN;
    use radroots_events::RadrootsNostrEventPtr;
    use radroots_events_codec::job::result::encode::job_result_build_tags;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn dvm_result_event(
        id: &str,
        pubkey: &str,
        created_at: u64,
        kind: u32,
        tags: Vec<Vec<String>>,
        content: &str,
    ) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 10);
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

    fn sample_result() -> RadrootsJobResult {
        RadrootsJobResult {
            kind: (KIND_JOB_RESULT_MIN + 1) as u16,
            request_event: RadrootsNostrEventPtr {
                id: "req".to_string(),
                relays: None,
            },
            request_json: None,
            inputs: vec![RadrootsJobInput {
                data: "https://example.com".to_string(),
                input_type: radroots_events::job::JobInputType::Url,
                relay: None,
                marker: None,
            }],
            customer_pubkey: None,
            payment: None,
            content: Some("payload".to_string()),
            encrypted: false,
        }
    }

    #[test]
    fn dvm_result_list_sorts_by_created_at_desc() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let result = sample_result();
        let tags = job_result_build_tags(&result);
        let older = dvm_result_event(&old_id, pubkey, 100, KIND_JOB_RESULT_MIN + 1, tags.clone(), "payload");
        let newer = dvm_result_event(&new_id, pubkey, 200, KIND_JOB_RESULT_MIN + 1, tags.clone(), "payload");

        let results = build_dvm_result_rows(vec![older, newer]);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, new_id);
        assert_eq!(results[0].created_at, 200);
        assert_eq!(results[1].id, old_id);
        assert_eq!(results[1].created_at, 100);
    }

    #[test]
    fn dvm_result_list_decodes_result() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let result = sample_result();
        let content = result.content.clone().unwrap();
        let tags = job_result_build_tags(&result);
        let id = format!("{:064x}", 3);
        let event = dvm_result_event(&id, pubkey, 300, KIND_JOB_RESULT_MIN + 1, tags.clone(), &content);

        let results = build_dvm_result_rows(vec![event]);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].tags, tags);
        let decoded = results[0].result.as_ref().expect("result");
        assert_eq!(decoded, &result);
    }
}
