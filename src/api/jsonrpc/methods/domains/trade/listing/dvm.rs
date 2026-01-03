#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_nostr::prelude::radroots_nostr_parse_pubkeys;
use radroots_trade::listing::dvm_kinds::TRADE_LISTING_DVM_KINDS;

use super::helpers::{fetch_dvm_events, parse_listing_addr};
use super::types::DvmEventView;

#[derive(Debug, Deserialize)]
struct TradeListingDvmListParams {
    listing_addr: String,
    #[serde(default)]
    order_id: Option<String>,
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
            &ctx.state.client,
            &addr,
            &kinds,
            order_id.as_deref(),
            authors.as_deref(),
            recipients.as_deref(),
            since,
            until,
            limit,
            timeout_secs.unwrap_or(10),
        )
        .await?;

        Ok::<TradeListingDvmListResponse, RpcError>(TradeListingDvmListResponse { events })
    })?;
    Ok(())
}
