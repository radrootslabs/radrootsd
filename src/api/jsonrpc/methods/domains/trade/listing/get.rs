#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};

use super::helpers::{fetch_latest_listing_event, listing_view, parse_listing_addr};
use super::types::ListingEventView;

#[derive(Debug, Deserialize)]
struct TradeListingGetParams {
    listing_addr: String,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct TradeListingGetResponse {
    listing: Option<ListingEventView>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("trade.listing.get");
    m.register_async_method("trade.listing.get", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let TradeListingGetParams {
            listing_addr,
            timeout_secs,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let addr = parse_listing_addr(&listing_addr)?;
        let latest = fetch_latest_listing_event(&ctx.state.client, &addr, timeout_secs.unwrap_or(10)).await?;
        let listing = latest.as_ref().map(listing_view);
        Ok::<TradeListingGetResponse, RpcError>(TradeListingGetResponse { listing })
    })?;
    Ok(())
}
