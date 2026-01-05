#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_nostr::prelude::{RadrootsNostrFilter, RadrootsNostrKind};

use super::list::{build_follow_rows, FollowRow};
use crate::api::jsonrpc::methods::events::helpers::{
    fetch_latest_event,
    parse_author_or_default,
};

#[derive(Debug, Deserialize)]
struct FollowGetParams {
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct FollowGetResponse {
    follow: Option<FollowRow>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.follow.get");
    m.register_async_method("events.follow.get", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let FollowGetParams {
            author,
            timeout_secs,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let author = parse_author_or_default(author, ctx.state.pubkey)?;

        let filter = RadrootsNostrFilter::new()
            .kind(RadrootsNostrKind::ContactList)
            .author(author);

        let event = fetch_latest_event(&ctx.state.client, filter, timeout_secs).await?;
        let follow = event.and_then(|event| build_follow_rows(vec![event]).into_iter().next());

        Ok::<FollowGetResponse, RpcError>(FollowGetResponse { follow })
    })?;

    Ok(())
}
