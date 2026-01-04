#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_nostr::prelude::{RadrootsNostrFilter, RadrootsNostrKind};

use super::list::{build_profile_rows, ProfileListRow};
use crate::api::jsonrpc::methods::events::helpers::{
    fetch_latest_event,
    parse_author_or_default,
};

#[derive(Debug, Deserialize)]
struct ProfileGetParams {
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct ProfileGetResponse {
    profile: Option<ProfileListRow>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.profile.get");
    m.register_async_method("events.profile.get", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let ProfileGetParams {
            author,
            timeout_secs,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let author = parse_author_or_default(author, ctx.state.pubkey)?;

        let filter = RadrootsNostrFilter::new()
            .kind(RadrootsNostrKind::Metadata)
            .author(author);

        let event = fetch_latest_event(&ctx.state.client, filter, timeout_secs).await?;
        let profile = match event {
            Some(event) => build_profile_rows(vec![author], vec![event])?
                .into_iter()
                .next(),
            None => None,
        };

        Ok::<ProfileGetResponse, RpcError>(ProfileGetResponse { profile })
    })?;
    Ok(())
}
