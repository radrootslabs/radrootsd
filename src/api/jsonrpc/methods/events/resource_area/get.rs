#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_events::kinds::KIND_RESOURCE_AREA;
use radroots_nostr::prelude::{RadrootsNostrFilter, RadrootsNostrKind};

use super::list::{build_resource_area_rows, ResourceAreaRow};
use crate::api::jsonrpc::methods::events::helpers::{
    fetch_latest_event,
    parse_author_or_default,
    require_non_empty,
};

#[derive(Debug, Deserialize)]
struct ResourceAreaGetParams {
    d_tag: String,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct ResourceAreaGetResponse {
    resource_area: Option<ResourceAreaRow>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.resource_area.get");
    m.register_async_method("events.resource_area.get", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let ResourceAreaGetParams {
            d_tag,
            author,
            timeout_secs,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let author = parse_author_or_default(author, ctx.state.pubkey)?;
        let d_tag = require_non_empty("d_tag", d_tag)?;

        let filter = RadrootsNostrFilter::new()
            .kind(RadrootsNostrKind::Custom(KIND_RESOURCE_AREA as u16))
            .author(author)
            .identifiers([d_tag]);

        let event = fetch_latest_event(&ctx.state.client, filter, timeout_secs).await?;
        let resource_area =
            event.and_then(|event| build_resource_area_rows(vec![event]).into_iter().next());

        Ok::<ResourceAreaGetResponse, RpcError>(ResourceAreaGetResponse { resource_area })
    })?;
    Ok(())
}
