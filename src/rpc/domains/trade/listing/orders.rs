#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::{radrootsd::Radrootsd, rpc::RpcError};
use radroots_nostr::prelude::radroots_nostr_parse_pubkeys;
use radroots_trade::listing::dvm_kinds::TRADE_LISTING_DVM_KINDS;

use super::helpers::{fetch_dvm_events, order_summaries, parse_listing_addr};
use super::types::TradeListingOrderSummary;

#[derive(Debug, Deserialize)]
struct TradeListingOrdersParams {
    listing_addr: String,
    #[serde(default)]
    authors: Option<Vec<String>>,
    #[serde(default)]
    recipients: Option<Vec<String>>,
    #[serde(default)]
    kinds: Option<Vec<u16>>,
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
struct TradeListingOrdersResponse {
    orders: Vec<TradeListingOrderSummary>,
}

pub fn register(m: &mut RpcModule<Radrootsd>) -> Result<()> {
    m.register_async_method("trade.listing.orders.list", |params, ctx, _| async move {
        if ctx.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let TradeListingOrdersParams {
            listing_addr,
            authors,
            recipients,
            kinds,
            limit,
            since,
            until,
            timeout_secs,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let addr = parse_listing_addr(&listing_addr)?;
        let kinds = kinds.unwrap_or_else(|| TRADE_LISTING_DVM_KINDS.to_vec());
        let authors = match authors {
            Some(authors) => Some(
                radroots_nostr_parse_pubkeys(&authors)
                    .map_err(|e| RpcError::InvalidParams(format!("invalid author: {e}")))?,
            ),
            None => None,
        };
        let recipients = match recipients {
            Some(recipients) => Some(
                radroots_nostr_parse_pubkeys(&recipients)
                    .map_err(|e| RpcError::InvalidParams(format!("invalid recipient: {e}")))?,
            ),
            None => None,
        };

        let events = fetch_dvm_events(
            &ctx.client,
            &addr,
            &kinds,
            None,
            authors.as_deref(),
            recipients.as_deref(),
            since,
            until,
            limit,
            timeout_secs.unwrap_or(10),
        )
        .await?;

        let orders = order_summaries(&events, &listing_addr);

        Ok::<TradeListingOrdersResponse, RpcError>(TradeListingOrdersResponse { orders })
    })?;
    Ok(())
}
