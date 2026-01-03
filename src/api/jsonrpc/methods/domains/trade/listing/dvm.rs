#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::params::{parse_pubkeys_opt, timeout_or, EventListParams};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_trade::listing::dvm_kinds::TRADE_LISTING_DVM_KINDS;

use super::helpers::{fetch_dvm_events, parse_listing_addr};
use super::types::DvmEventView;

#[derive(Debug, Deserialize)]
struct TradeListingDvmListParams {
    listing_addr: String,
    #[serde(default)]
    order_id: Option<String>,
    #[serde(default)]
    recipients: Option<Vec<String>>,
    #[serde(default)]
    kinds: Option<Vec<u16>>,
    #[serde(default, flatten)]
    query: EventListParams,
}

#[derive(Clone, Debug, Serialize)]
struct TradeListingDvmListResponse {
    events: Vec<DvmEventView>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("trade.listing.dvm.list");
    m.register_async_method("trade.listing.dvm.list", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let TradeListingDvmListParams {
            listing_addr,
            order_id,
            recipients,
            kinds,
            query,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let EventListParams {
            authors,
            limit,
            since,
            until,
            timeout_secs,
        } = query;

        let addr = parse_listing_addr(&listing_addr)?;
        let kinds = kinds.unwrap_or_else(|| TRADE_LISTING_DVM_KINDS.to_vec());
        let authors = parse_pubkeys_opt("author", authors)?;
        let recipients = parse_pubkeys_opt("recipient", recipients)?;

        let events = fetch_dvm_events(
            &ctx.state.client,
            &addr,
            &kinds,
            order_id.as_deref(),
            authors.as_deref(),
            recipients.as_deref(),
            since,
            until,
            limit,
            timeout_or(timeout_secs),
        )
        .await?;

        Ok::<TradeListingDvmListResponse, RpcError>(TradeListingDvmListResponse { events })
    })?;
    Ok(())
}
