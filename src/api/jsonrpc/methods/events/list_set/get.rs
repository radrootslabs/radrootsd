#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::{is_nip51_list_set_kind, KIND_LIST_SET_GENERIC};
use radroots_nostr::prelude::{RadrootsNostrFilter, RadrootsNostrKind};

use super::list::{build_list_set_rows, ListSetRow};
use crate::api::jsonrpc::methods::events::helpers::{
    fetch_latest_event,
    parse_author_or_default,
    require_non_empty,
};

#[derive(Debug, Deserialize)]
struct ListSetGetParams {
    d_tag: String,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    kind: Option<u32>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct ListSetGetResponse {
    list_set: Option<ListSetRow>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.list_set.get");
    m.register_async_method("events.list_set.get", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let ListSetGetParams {
            d_tag,
            author,
            kind,
            timeout_secs,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let author = parse_author_or_default(author, ctx.state.pubkey)?;
        let d_tag = require_non_empty("d_tag", d_tag)?;

        let kind = kind.unwrap_or(KIND_LIST_SET_GENERIC);
        if !is_nip51_list_set_kind(kind) {
            return Err(RpcError::InvalidParams(format!(
                "invalid list_set kind: {kind}"
            )));
        }
        let kind = u16::try_from(kind)
            .map_err(|_| RpcError::InvalidParams(format!("list_set kind out of range: {kind}")))?;

        let filter = RadrootsNostrFilter::new()
            .kind(RadrootsNostrKind::Custom(kind))
            .author(author)
            .identifiers([d_tag]);

        let event = fetch_latest_event(&ctx.state.client, filter, timeout_secs).await?;
        let list_set = event.and_then(|event| build_list_set_rows(vec![event]).into_iter().next());

        Ok::<ListSetGetResponse, RpcError>(ListSetGetResponse { list_set })
    })?;
    Ok(())
}
