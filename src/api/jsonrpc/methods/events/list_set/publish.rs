#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::nostr::{publish_response, PublishResponse};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::{is_nip51_list_set_kind, KIND_LIST_SET_GENERIC};
use radroots_events::list_set::RadrootsListSet;
use radroots_events_codec::list_set::encode::list_set_build_tags;
use radroots_nostr::prelude::{radroots_nostr_build_event, radroots_nostr_send_event};

#[derive(Debug, Deserialize)]
struct PublishListSetParams {
    list_set: RadrootsListSet,
    #[serde(default)]
    kind: Option<u32>,
    #[serde(default)]
    tags: Option<Vec<Vec<String>>>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.list_set.publish");
    m.register_async_method("events.list_set.publish", |params, ctx, _| async move {
        let relays = ctx.state.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let PublishListSetParams {
            list_set,
            kind,
            tags,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let kind = kind.unwrap_or(KIND_LIST_SET_GENERIC);
        if !is_nip51_list_set_kind(kind) {
            return Err(RpcError::InvalidParams(format!(
                "invalid list_set kind: {kind}"
            )));
        }

        let content = list_set.content.clone();
        let mut tag_slices =
            list_set_build_tags(&list_set).map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        if let Some(extra_tags) = tags {
            tag_slices.extend(extra_tags);
        }

        let builder = radroots_nostr_build_event(kind, content, tag_slices)
            .map_err(|e| RpcError::Other(format!("failed to build list_set event: {e}")))?;

        let output = radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish list_set: {e}")))?;

        Ok::<PublishResponse, RpcError>(publish_response(output))
    })?;

    Ok(())
}
