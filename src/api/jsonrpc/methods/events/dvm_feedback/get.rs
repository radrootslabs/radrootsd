#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::KIND_JOB_FEEDBACK;
use radroots_nostr::prelude::{RadrootsNostrEventId, RadrootsNostrFilter};

use super::list::{build_dvm_feedback_rows, DvmFeedbackRow};
use crate::api::jsonrpc::methods::events::helpers::{fetch_latest_event, require_non_empty};

#[derive(Debug, Deserialize)]
struct DvmFeedbackGetParams {
    id: String,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct DvmFeedbackGetResponse {
    feedback: Option<DvmFeedbackRow>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.dvm_feedback.get");
    m.register_async_method("events.dvm_feedback.get", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let DvmFeedbackGetParams { id, timeout_secs } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let id = require_non_empty("id", id)?;
        let event_id = RadrootsNostrEventId::parse(&id)
            .map_err(|e| RpcError::InvalidParams(format!("invalid id: {e}")))?;

        let filter = RadrootsNostrFilter::new().id(event_id);

        let event = fetch_latest_event(&ctx.state.client, filter, timeout_secs).await?;
        let feedback = event.and_then(|event| {
            let kind = event.kind.as_u16() as u32;
            if kind != KIND_JOB_FEEDBACK {
                return None;
            }
            build_dvm_feedback_rows(vec![event]).into_iter().next()
        });

        Ok::<DvmFeedbackGetResponse, RpcError>(DvmFeedbackGetResponse { feedback })
    })?;
    Ok(())
}
