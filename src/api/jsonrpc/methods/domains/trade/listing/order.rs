#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use radroots_nostr::prelude::{
    radroots_nostr_build_event,
    radroots_nostr_parse_pubkey,
    radroots_nostr_send_event,
    RadrootsNostrPublicKey,
};
use radroots_trade::listing::dvm::{
    TradeListingEnvelope,
    TradeListingMessageType,
    TradeOrderResponse,
};
use radroots_trade::listing::order::TradeOrder;

use crate::api::jsonrpc::nostr::{publish_response, PublishResponse};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};

use super::helpers::parse_listing_addr;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum TradeListingOrderPayload {
    OrderRequest { order: TradeOrder },
    OrderResponse {
        order_id: String,
        accepted: bool,
        #[serde(default)]
        reason: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
struct TradeListingOrderParams {
    listing_addr: String,
    recipient_pubkey: String,
    #[serde(flatten)]
    payload: TradeListingOrderPayload,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("trade.listing.order");
    m.register_async_method("trade.listing.order", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let TradeListingOrderParams {
            listing_addr,
            recipient_pubkey,
            payload,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let addr = parse_listing_addr(&listing_addr)?;
        let listing_addr = addr.as_str();

        let recipient = radroots_nostr_parse_pubkey(&recipient_pubkey)
            .map_err(|e| RpcError::InvalidParams(format!("invalid recipient_pubkey: {e}")))?;
        let recipient_pubkey = recipient.to_string();

        let (message_type, order_id, content) = match payload {
            TradeListingOrderPayload::OrderRequest { order } => {
                validate_order_request(&order, &addr, &ctx.state.pubkey, &listing_addr)?;
                let order_id = order.order_id.trim().to_string();
                let envelope = TradeListingEnvelope::new(
                    TradeListingMessageType::OrderRequest,
                    listing_addr.clone(),
                    Some(order_id.clone()),
                    order,
                );
                envelope
                    .validate()
                    .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
                let content = serde_json::to_string(&envelope)
                    .map_err(|e| RpcError::Other(format!("failed to encode envelope: {e}")))?;
                (TradeListingMessageType::OrderRequest, order_id, content)
            }
            TradeListingOrderPayload::OrderResponse {
                order_id,
                accepted,
                reason,
            } => {
                validate_order_response(&order_id, &addr, &ctx.state.pubkey)?;
                let order_id = order_id.trim().to_string();
                let response = TradeOrderResponse { accepted, reason };
                let envelope = TradeListingEnvelope::new(
                    TradeListingMessageType::OrderResponse,
                    listing_addr.clone(),
                    Some(order_id.clone()),
                    response,
                );
                envelope
                    .validate()
                    .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
                let content = serde_json::to_string(&envelope)
                    .map_err(|e| RpcError::Other(format!("failed to encode envelope: {e}")))?;
                (TradeListingMessageType::OrderResponse, order_id, content)
            }
        };

        let tags = vec![
            vec!["p".to_string(), recipient_pubkey],
            vec!["a".to_string(), listing_addr.clone()],
            vec!["d".to_string(), order_id],
        ];

        let builder = radroots_nostr_build_event(message_type.kind() as u32, content, tags)
            .map_err(|e| RpcError::Other(format!("failed to build order event: {e}")))?;

        let output = radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish order event: {e}")))?;

        Ok::<PublishResponse, RpcError>(publish_response(output))
    })?;
    Ok(())
}

fn validate_order_request(
    order: &TradeOrder,
    addr: &radroots_trade::listing::dvm::TradeListingAddress,
    runtime_pubkey: &RadrootsNostrPublicKey,
    listing_addr: &str,
) -> Result<(), RpcError> {
    let order_id = order.order_id.trim();
    if order_id.is_empty() {
        return Err(RpcError::InvalidParams("order_id must not be empty".to_string()));
    }

    if order.listing_addr.trim() != listing_addr {
        return Err(RpcError::InvalidParams(
            "order listing_addr must match listing_addr".to_string(),
        ));
    }

    let buyer_pubkey = radroots_nostr_parse_pubkey(&order.buyer_pubkey)
        .map_err(|e| RpcError::InvalidParams(format!("invalid buyer_pubkey: {e}")))?;
    if &buyer_pubkey != runtime_pubkey {
        return Err(RpcError::InvalidParams(
            "buyer_pubkey must match runtime key".to_string(),
        ));
    }

    let seller_pubkey = radroots_nostr_parse_pubkey(&order.seller_pubkey)
        .map_err(|e| RpcError::InvalidParams(format!("invalid seller_pubkey: {e}")))?;
    let listing_seller = radroots_nostr_parse_pubkey(&addr.seller_pubkey)
        .map_err(|e| RpcError::InvalidParams(format!("invalid listing author: {e}")))?;
    if seller_pubkey != listing_seller {
        return Err(RpcError::InvalidParams(
            "seller_pubkey must match listing_addr seller".to_string(),
        ));
    }

    Ok(())
}

fn validate_order_response(
    order_id: &str,
    addr: &radroots_trade::listing::dvm::TradeListingAddress,
    runtime_pubkey: &RadrootsNostrPublicKey,
) -> Result<(), RpcError> {
    if order_id.trim().is_empty() {
        return Err(RpcError::InvalidParams("order_id must not be empty".to_string()));
    }

    let listing_seller = radroots_nostr_parse_pubkey(&addr.seller_pubkey)
        .map_err(|e| RpcError::InvalidParams(format!("invalid listing author: {e}")))?;
    if &listing_seller != runtime_pubkey {
        return Err(RpcError::InvalidParams(
            "order_response must be authored by the listing seller".to_string(),
        ));
    }

    Ok(())
}
