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
use radroots_events::job_feedback::RadrootsJobFeedback;
use radroots_events::kinds::KIND_JOB_FEEDBACK;
use radroots_events_codec::job::feedback::decode::job_feedback_from_tags;
use radroots_nostr::prelude::{
    RadrootsNostrEvent,
    RadrootsNostrEventId,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DvmFeedbackRow {
    id: String,
    author: String,
    created_at: u64,
    kind: u32,
    tags: Vec<Vec<String>>,
    content: String,
    sig: String,
    feedback: Option<RadrootsJobFeedback>,
}

#[derive(Clone, Debug, Serialize)]
struct DvmFeedbackListResponse {
    feedbacks: Vec<DvmFeedbackRow>,
}

#[derive(Debug, Default, Deserialize)]
struct DvmFeedbackListParams {
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
    request_id: Option<String>,
}

pub(crate) fn build_dvm_feedback_rows<I>(events: I) -> Vec<DvmFeedbackRow>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut items = events
        .into_iter()
        .map(|ev| {
            let tags = event_tags(&ev);
            let feedback = parse_dvm_feedback_event(&ev, &tags);
            let event = event_view_with_tags(&ev, tags);
            DvmFeedbackRow {
                id: event.id,
                author: event.author,
                created_at: event.created_at,
                kind: event.kind,
                tags: event.tags,
                content: event.content,
                sig: event.sig,
                feedback,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    items
}

fn parse_dvm_feedback_event(
    event: &RadrootsNostrEvent,
    tags: &[Vec<String>],
) -> Option<RadrootsJobFeedback> {
    let kind = event.kind.as_u16() as u32;
    if kind != KIND_JOB_FEEDBACK {
        return None;
    }
    job_feedback_from_tags(kind, tags, &event.content).ok()
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.dvm_feedback.list");
    m.register_async_method("events.dvm_feedback.list", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let DvmFeedbackListParams {
            authors,
            limit,
            since,
            until,
            timeout_secs,
            request_id,
        } = params
            .parse::<Option<DvmFeedbackListParams>>()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?
            .unwrap_or_default();

        let limit = limit_or(limit);

        let mut filter = RadrootsNostrFilter::new()
            .limit(limit)
            .kind(RadrootsNostrKind::Custom(KIND_JOB_FEEDBACK as u16));

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

        let items = build_dvm_feedback_rows(events);

        Ok::<DvmFeedbackListResponse, RpcError>(DvmFeedbackListResponse { feedbacks: items })
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_dvm_feedback_rows;
    use radroots_events::job::JobFeedbackStatus;
    use radroots_events::job_feedback::RadrootsJobFeedback;
    use radroots_events::kinds::KIND_JOB_FEEDBACK;
    use radroots_events::RadrootsNostrEventPtr;
    use radroots_events_codec::job::feedback::encode::job_feedback_build_tags;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn dvm_feedback_event(
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
            "kind": KIND_JOB_FEEDBACK,
            "tags": tags,
            "content": content,
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    fn sample_feedback() -> RadrootsJobFeedback {
        RadrootsJobFeedback {
            kind: KIND_JOB_FEEDBACK as u16,
            status: JobFeedbackStatus::Success,
            extra_info: Some("ok".to_string()),
            request_event: RadrootsNostrEventPtr {
                id: "req".to_string(),
                relays: None,
            },
            customer_pubkey: None,
            payment: None,
            content: Some("payload".to_string()),
            encrypted: false,
        }
    }

    #[test]
    fn dvm_feedback_list_sorts_by_created_at_desc() {
        let pubkey = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let old_id = format!("{:064x}", 1);
        let new_id = format!("{:064x}", 2);
        let feedback = sample_feedback();
        let tags = job_feedback_build_tags(&feedback);
        let older = dvm_feedback_event(&old_id, pubkey, 100, tags.clone(), "payload");
        let newer = dvm_feedback_event(&new_id, pubkey, 200, tags.clone(), "payload");

        let feedbacks = build_dvm_feedback_rows(vec![older, newer]);

        assert_eq!(feedbacks.len(), 2);
        assert_eq!(feedbacks[0].id, new_id);
        assert_eq!(feedbacks[0].created_at, 200);
        assert_eq!(feedbacks[1].id, old_id);
        assert_eq!(feedbacks[1].created_at, 100);
    }

    #[test]
    fn dvm_feedback_list_decodes_feedback() {
        let pubkey = "2bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let feedback = sample_feedback();
        let content = feedback.content.clone().unwrap();
        let tags = job_feedback_build_tags(&feedback);
        let id = format!("{:064x}", 3);
        let event = dvm_feedback_event(&id, pubkey, 300, tags.clone(), &content);

        let feedbacks = build_dvm_feedback_rows(vec![event]);

        assert_eq!(feedbacks.len(), 1);
        assert_eq!(feedbacks[0].tags, tags);
        let decoded = feedbacks[0].feedback.as_ref().expect("feedback");
        assert_eq!(decoded, &feedback);
    }
}
