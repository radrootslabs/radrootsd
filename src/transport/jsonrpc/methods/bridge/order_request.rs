use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_events::kinds::KIND_LISTING;
use radroots_events::trade::{
    RadrootsTradeEnvelope as TradeListingEnvelope,
    RadrootsTradeMessageType as TradeListingMessageType, RadrootsTradeOrder as TradeOrder,
    RadrootsTradeOrderStatus as TradeOrderStatus,
};
use radroots_events_codec::trade::{
    RadrootsTradeListingAddress as TradeListingAddress,
    trade_envelope_event_build as trade_listing_envelope_event_build,
};
use radroots_nostr::prelude::{radroots_nostr_build_event, radroots_nostr_parse_pubkey};
use serde::Deserialize;
use uuid::Uuid;

use crate::core::bridge::publish::{
    BridgePublishSettings, connect_and_publish_event, failed_prepublish_execution,
};
use crate::core::bridge::store::new_order_request_job;
use crate::transport::jsonrpc::auth::require_bridge_auth;
use crate::transport::jsonrpc::methods::bridge::shared::{
    BridgePublishResponse, ensure_bridge_enabled, fingerprint_bridge_request,
    normalize_idempotency_key, reserve_bridge_job, resolve_bridge_signer,
    sign_bridge_event_builder,
};
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct BridgeOrderRequestParams {
    order: TradeOrder,
    #[serde(default)]
    signer_session_id: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("bridge.order.request");
    m.register_async_method(
        "bridge.order.request",
        |params, ctx, extensions| async move {
            require_bridge_auth(&extensions)?;
            let params: BridgeOrderRequestParams = params
                .parse()
                .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
            let response = publish_order_request(ctx.as_ref().clone(), params).await?;
            Ok::<BridgePublishResponse, RpcError>(response)
        },
    )?;
    Ok(())
}

async fn publish_order_request(
    ctx: RpcContext,
    params: BridgeOrderRequestParams,
) -> Result<BridgePublishResponse, RpcError> {
    ensure_bridge_enabled(&ctx)?;

    let idempotency_key = normalize_idempotency_key(params.idempotency_key)?;
    let signer = resolve_bridge_signer(
        &ctx,
        params.signer_session_id.as_deref(),
        u32::from(TradeListingMessageType::OrderRequest.kind()),
    )
    .await?;
    let signer_pubkey = signer.signer_pubkey_hex();
    let order = canonicalize_order_request_for_signer(params.order, signer_pubkey.as_str())?;
    let request_fingerprint = fingerprint_bridge_request("bridge.order.request", &signer, &order)?;
    let envelope = TradeListingEnvelope::new(
        TradeListingMessageType::OrderRequest,
        order.listing_addr.clone(),
        Some(order.order_id.clone()),
        order.clone(),
    );
    envelope.validate().map_err(|error| {
        RpcError::InvalidParams(format!("invalid order request envelope: {error}"))
    })?;
    let built = trade_listing_envelope_event_build(
        order.seller_pubkey.clone(),
        TradeListingMessageType::OrderRequest,
        order.listing_addr.clone(),
        Some(order.order_id.clone()),
        &order,
    )
    .map_err(|error| RpcError::Other(format!("failed to build order request event: {error}")))?;
    let builder = radroots_nostr_build_event(u32::from(built.kind), built.content, built.tags)
        .map_err(|error| {
            RpcError::Other(format!("failed to build order request event: {error}"))
        })?;

    let reserved = reserve_bridge_job(
        &ctx,
        new_order_request_job(
            Uuid::new_v4().to_string(),
            idempotency_key,
            signer.signer_mode(),
            u32::from(TradeListingMessageType::OrderRequest.kind()),
            None,
            order.listing_addr.clone(),
            ctx.state.bridge_config.delivery_policy,
            ctx.state.bridge_config.delivery_quorum,
        ),
        request_fingerprint,
        "bridge order",
    )?;
    let job = match reserved {
        crate::core::bridge::store::BridgeJobReservation::Accepted(job) => job,
        crate::core::bridge::store::BridgeJobReservation::Duplicate(existing) => {
            return Ok(BridgePublishResponse {
                deduplicated: true,
                job: existing.into(),
            });
        }
    };

    let publish_settings = BridgePublishSettings::from_config(&ctx.state.bridge_config);
    let event =
        match sign_bridge_event_builder(&ctx, &signer, builder, "bridge.order.request").await {
            Ok(event) => event,
            Err(error) => {
                let _ = ctx.state.bridge_jobs.complete(
                    &job.job_id,
                    None,
                    failed_prepublish_execution(&publish_settings, error.to_string()),
                );
                return Err(error);
            }
        };

    let execution = connect_and_publish_event(&ctx.state.client, &publish_settings, &event).await;
    let job = ctx
        .state
        .bridge_jobs
        .complete(&job.job_id, Some(event.id.to_hex()), execution)
        .map_err(|error| RpcError::Other(format!("failed to persist bridge order job: {error}")))?
        .ok_or_else(|| RpcError::Other("bridge job disappeared during completion".to_string()))?;

    Ok(BridgePublishResponse {
        deduplicated: false,
        job: job.into(),
    })
}

fn canonicalize_order_request_for_signer(
    mut order: TradeOrder,
    signer_pubkey: &str,
) -> Result<TradeOrder, RpcError> {
    let order_id =
        normalized_required_string(std::mem::take(&mut order.order_id), "order.order_id")?;
    let listing_addr_raw = normalized_required_string(
        std::mem::take(&mut order.listing_addr),
        "order.listing_addr",
    )?;
    let listing_addr = TradeListingAddress::parse(&listing_addr_raw)
        .map_err(|error| RpcError::InvalidParams(format!("invalid order.listing_addr: {error}")))?;
    if u32::from(listing_addr.kind) != KIND_LISTING {
        return Err(RpcError::InvalidParams(
            "order.listing_addr must reference a public NIP-99 listing".to_string(),
        ));
    }

    let buyer_pubkey = if order.buyer_pubkey.trim().is_empty() {
        signer_pubkey.to_string()
    } else {
        normalized_required_string(
            std::mem::take(&mut order.buyer_pubkey),
            "order.buyer_pubkey",
        )?
    };
    if buyer_pubkey != signer_pubkey {
        return Err(RpcError::InvalidParams(
            "order.buyer_pubkey must match the requested bridge signer identity".to_string(),
        ));
    }

    let seller_pubkey = if order.seller_pubkey.trim().is_empty() {
        listing_addr.seller_pubkey.clone()
    } else {
        normalized_required_string(
            std::mem::take(&mut order.seller_pubkey),
            "order.seller_pubkey",
        )?
    };
    if seller_pubkey != listing_addr.seller_pubkey {
        return Err(RpcError::InvalidParams(
            "order.seller_pubkey must match order.listing_addr seller".to_string(),
        ));
    }

    radroots_nostr_parse_pubkey(&buyer_pubkey)
        .map_err(|error| RpcError::InvalidParams(format!("invalid order.buyer_pubkey: {error}")))?;
    radroots_nostr_parse_pubkey(&seller_pubkey).map_err(|error| {
        RpcError::InvalidParams(format!("invalid order.seller_pubkey: {error}"))
    })?;

    if order.items.is_empty() {
        return Err(RpcError::InvalidParams(
            "order.items must contain at least one item".to_string(),
        ));
    }
    for (index, item) in order.items.iter_mut().enumerate() {
        item.bin_id = normalized_required_string(item.bin_id.clone(), "order.items[].bin_id")?;
        if item.bin_count == 0 {
            return Err(RpcError::InvalidParams(format!(
                "order.items[{index}].bin_count must be greater than zero"
            )));
        }
    }

    if order.status != TradeOrderStatus::Requested {
        return Err(RpcError::InvalidParams(
            "order.status must be requested for bridge.order.request".to_string(),
        ));
    }

    order.order_id = order_id;
    order.listing_addr = listing_addr.as_str();
    order.buyer_pubkey = buyer_pubkey;
    order.seller_pubkey = seller_pubkey;
    order.notes = normalize_optional_string(order.notes);
    if order.discounts.as_ref().is_some_and(Vec::is_empty) {
        order.discounts = None;
    }
    Ok(order)
}

fn normalized_required_string(value: String, field: &str) -> Result<String, RpcError> {
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(RpcError::InvalidParams(format!("{field} cannot be empty")));
    }
    Ok(value)
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_string();
        if value.is_empty() { None } else { Some(value) }
    })
}

#[cfg(test)]
mod tests {
    use radroots_core::RadrootsCoreDiscountValue;
    use radroots_events::trade::{
        RadrootsTradeOrder as TradeOrder, RadrootsTradeOrderItem as TradeOrderItem,
        RadrootsTradeOrderStatus as TradeOrderStatus,
    };
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::RadrootsNostrMetadata;

    use crate::app::config::{BridgeConfig, Nip46Config};
    use crate::core::Radrootsd;
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

    use super::{
        BridgeOrderRequestParams, canonicalize_order_request_for_signer, normalize_optional_string,
        publish_order_request,
    };

    #[test]
    fn canonicalize_order_request_sets_missing_buyer_and_seller_pubkeys() {
        let order = canonicalize_order_request_for_signer(
            base_order("", "", TradeOrderStatus::Requested),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .expect("canonicalize");

        assert_eq!(
            order.buyer_pubkey,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            order.seller_pubkey,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
    }

    #[test]
    fn canonicalize_order_request_rejects_non_requested_status() {
        let err = canonicalize_order_request_for_signer(
            base_order(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "",
                TradeOrderStatus::Draft,
            ),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .expect_err("status should fail");
        assert!(err.to_string().contains("order.status"));
    }

    #[test]
    fn canonicalize_order_request_rejects_items_with_zero_bin_count() {
        let mut order = base_order(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "",
            TradeOrderStatus::Requested,
        );
        order.items[0].bin_count = 0;
        let err = canonicalize_order_request_for_signer(
            order,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .expect_err("zero bin count");
        assert!(err.to_string().contains("bin_count"));
    }

    #[test]
    fn normalize_optional_string_trims_blank_values() {
        assert_eq!(normalize_optional_string(Some("  ".to_string())), None);
        assert_eq!(
            normalize_optional_string(Some(" note ".to_string())),
            Some("note".to_string())
        );
    }

    #[tokio::test]
    async fn publish_order_request_is_job_backed_and_idempotent() {
        let identity = RadrootsIdentity::generate();
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let state = Radrootsd::new(
            identity,
            metadata,
            BridgeConfig {
                enabled: true,
                bearer_token: Some("secret".to_string()),
                ..BridgeConfig::default()
            },
            Nip46Config::default(),
        )
        .expect("state");
        let ctx = RpcContext::new(state, MethodRegistry::default());
        let params = BridgeOrderRequestParams {
            order: base_order("", "", TradeOrderStatus::Requested),
            signer_session_id: None,
            idempotency_key: Some("same-key".to_string()),
        };

        let first = publish_order_request(ctx.clone(), params)
            .await
            .expect("first");
        assert!(!first.deduplicated);
        assert_eq!(first.job.command, "bridge.order.request");
        assert_eq!(first.job.event_addr.as_deref(), Some(base_listing_addr()));

        let second = publish_order_request(
            ctx,
            BridgeOrderRequestParams {
                order: base_order("", "", TradeOrderStatus::Requested),
                signer_session_id: None,
                idempotency_key: Some("same-key".to_string()),
            },
        )
        .await
        .expect("second");
        assert!(second.deduplicated);
        assert_eq!(second.job.job_id, first.job.job_id);
    }

    #[tokio::test]
    async fn publish_order_request_rejects_conflicting_idempotency_key_reuse() {
        let identity = RadrootsIdentity::generate();
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let state = Radrootsd::new(
            identity,
            metadata,
            BridgeConfig {
                enabled: true,
                bearer_token: Some("secret".to_string()),
                ..BridgeConfig::default()
            },
            Nip46Config::default(),
        )
        .expect("state");
        let ctx = RpcContext::new(state, MethodRegistry::default());
        publish_order_request(
            ctx.clone(),
            BridgeOrderRequestParams {
                order: base_order("", "", TradeOrderStatus::Requested),
                signer_session_id: None,
                idempotency_key: Some("same-key".to_string()),
            },
        )
        .await
        .expect("first");

        let mut conflicting = base_order("", "", TradeOrderStatus::Requested);
        conflicting.order_id = "order-2".to_string();
        let err = publish_order_request(
            ctx,
            BridgeOrderRequestParams {
                order: conflicting,
                signer_session_id: None,
                idempotency_key: Some("same-key".to_string()),
            },
        )
        .await
        .expect_err("conflicting idempotency");
        assert!(err.to_string().contains("conflicts"));
    }

    fn base_listing_addr() -> &'static str {
        "30402:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:AAAAAAAAAAAAAAAAAAAAAg"
    }

    fn base_order(buyer_pubkey: &str, seller_pubkey: &str, status: TradeOrderStatus) -> TradeOrder {
        TradeOrder {
            order_id: "order-1".to_string(),
            listing_addr: base_listing_addr().to_string(),
            buyer_pubkey: buyer_pubkey.to_string(),
            seller_pubkey: seller_pubkey.to_string(),
            items: vec![TradeOrderItem {
                bin_id: "bin-1".to_string(),
                bin_count: 2,
            }],
            discounts: Some(Vec::<RadrootsCoreDiscountValue>::new()),
            notes: Some("  note  ".to_string()),
            status,
        }
    }
}
