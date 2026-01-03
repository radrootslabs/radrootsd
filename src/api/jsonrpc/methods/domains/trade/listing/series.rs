#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};
use crate::api::jsonrpc::params::timeout_or;
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_trade::listing::dvm_kinds::TRADE_LISTING_DVM_KINDS;

use super::helpers::{
    fetch_dvm_events, fetch_latest_listing_event, listing_view, order_summaries, parse_listing_addr,
};
use super::types::{TradeListingOrderSummary, TradeListingSeriesView};

#[derive(Debug, Deserialize)]
struct TradeListingSeriesParams {
    listing_addr: String,
    #[serde(default)]
    order_id: Option<String>,
    #[serde(default)]
    include_listing: Option<bool>,
    #[serde(default)]
    include_dvm: Option<bool>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    since: Option<u64>,
    #[serde(default)]
    until: Option<u64>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct TradeListingSeriesResponse {
    series: TradeListingSeriesView,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("trade.listing.series.get");
    m.register_async_method("trade.listing.series.get", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let TradeListingSeriesParams {
            listing_addr,
            order_id,
            include_listing,
            include_dvm,
            limit,
            since,
            until,
            timeout_secs,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let addr = parse_listing_addr(&listing_addr)?;
        let include_listing = include_listing.unwrap_or(true);
        let include_dvm = include_dvm.unwrap_or(true);

        let listing = if include_listing {
            fetch_latest_listing_event(&ctx.state.client, &addr, timeout_or(timeout_secs))
                .await?
                .as_ref()
                .map(listing_view)
        } else {
            None
        };

        let dvm_events = if include_dvm {
            fetch_dvm_events(
                &ctx.state.client,
                &addr,
                &TRADE_LISTING_DVM_KINDS,
                order_id.as_deref(),
                None,
                None,
                since,
                until,
                limit,
                timeout_or(timeout_secs),
            )
            .await?
        } else {
            Vec::new()
        };

        let orders = if include_dvm {
            order_summaries(&dvm_events, &listing_addr)
        } else {
            Vec::<TradeListingOrderSummary>::new()
        };

        let series = TradeListingSeriesView {
            listing,
            dvm_events,
            orders,
        };

        Ok::<TradeListingSeriesResponse, RpcError>(TradeListingSeriesResponse { series })
    })?;
    Ok(())
}
