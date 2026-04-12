use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_events::RadrootsNostrEventPtr;
use radroots_events::kinds::KIND_LISTING;
use radroots_events::trade::{
    RadrootsTradeEnvelope as TradeListingEnvelope,
    RadrootsTradeMessagePayload as TradeListingMessagePayload,
    RadrootsTradeMessageType as TradeListingMessageType, RadrootsTradeOrder as TradeOrder,
};
use radroots_events_codec::trade::{
    RadrootsTradeListingAddress as TradeListingAddress,
    trade_envelope_event_build as trade_listing_envelope_event_build,
};
use radroots_nostr::prelude::{
    RadrootsNostrFilter, RadrootsNostrKind, radroots_event_ptr_from_nostr,
    radroots_nostr_build_event, radroots_nostr_filter_tag, radroots_nostr_parse_pubkey,
};
use radroots_trade::order::canonicalize_order_request_for_signer;
use serde::Deserialize;
use std::time::Duration;
use uuid::Uuid;

use crate::core::bridge::publish::{
    BridgePublishSettings, connect_and_publish_event, failed_prepublish_execution,
};
use crate::core::bridge::store::new_order_request_job;
use crate::core::nip46::session::Nip46SessionAuthority;
use crate::transport::jsonrpc::auth::require_bridge_auth;
use crate::transport::jsonrpc::methods::bridge::shared::{
    BridgePublishResponse, ensure_bridge_enabled, fingerprint_bridge_request,
    normalize_idempotency_key, reserve_bridge_job, resolve_actor_bridge_signer,
    sign_bridge_event_builder,
};
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct BridgeOrderRequestParams {
    order: TradeOrder,
    #[serde(default)]
    signer_session_id: Option<String>,
    #[serde(default)]
    signer_authority: Option<Nip46SessionAuthority>,
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
    let signer = resolve_actor_bridge_signer(
        &ctx,
        params.signer_session_id.as_deref(),
        params.signer_authority.as_ref(),
        u32::from(TradeListingMessageType::OrderRequest.kind()),
        "bridge.order.request",
    )
    .await?;
    let signer_pubkey = signer.signer_pubkey_hex();
    let order = canonicalize_order_request_for_signer(params.order, signer_pubkey.as_str())
        .map_err(|error| RpcError::InvalidParams(error.to_string()))?;
    radroots_nostr_parse_pubkey(&order.buyer_pubkey)
        .map_err(|error| RpcError::InvalidParams(format!("invalid order.buyer_pubkey: {error}")))?;
    radroots_nostr_parse_pubkey(&order.seller_pubkey).map_err(|error| {
        RpcError::InvalidParams(format!("invalid order.seller_pubkey: {error}"))
    })?;
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
    let listing_snapshot = fetch_listing_snapshot(&ctx, &order.listing_addr).await?;
    let built = trade_listing_envelope_event_build(
        order.seller_pubkey.clone(),
        TradeListingMessageType::OrderRequest,
        order.listing_addr.clone(),
        Some(order.order_id.clone()),
        Some(&listing_snapshot),
        None,
        None,
        &TradeListingMessagePayload::OrderRequest(order.clone()),
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

async fn fetch_listing_snapshot(
    ctx: &RpcContext,
    listing_addr: &str,
) -> Result<RadrootsNostrEventPtr, RpcError> {
    let listing_addr = TradeListingAddress::parse(listing_addr)
        .map_err(|error| RpcError::InvalidParams(format!("invalid order.listing_addr: {error}")))?;
    if ctx.state.client.relays().await.is_empty() {
        return Ok(synthetic_listing_snapshot(&listing_addr));
    }
    let filter = RadrootsNostrFilter::new()
        .author(
            radroots_nostr_parse_pubkey(&listing_addr.seller_pubkey).map_err(|error| {
                RpcError::InvalidParams(format!("invalid order.seller_pubkey: {error}"))
            })?,
        )
        .kind(RadrootsNostrKind::Custom(KIND_LISTING as u16));
    let filter = radroots_nostr_filter_tag(filter, "d", vec![listing_addr.listing_id.clone()])
        .map_err(|error| {
            RpcError::Other(format!("failed to build listing snapshot filter: {error}"))
        })?;
    let mut events = ctx
        .state
        .client
        .fetch_events(filter, Duration::from_secs(10))
        .await
        .map_err(|error| {
            RpcError::Other(format!(
                "failed to fetch listing snapshot for bridge.order.request: {error}"
            ))
        })?;
    events.sort_by_key(|event| event.created_at);
    let event = events.pop().ok_or_else(|| {
        RpcError::InvalidParams(
            "order.listing_addr must reference an existing public NIP-99 listing".to_string(),
        )
    })?;
    Ok(radroots_event_ptr_from_nostr(&event))
}

fn synthetic_listing_snapshot(listing_addr: &TradeListingAddress) -> RadrootsNostrEventPtr {
    RadrootsNostrEventPtr {
        id: format!("listing:{}", listing_addr.as_str()),
        relays: None,
    }
}

#[cfg(test)]
mod tests {
    use radroots_core::RadrootsCoreDiscountValue;
    use radroots_events::trade::{
        RadrootsTradeOrder as TradeOrder, RadrootsTradeOrderItem as TradeOrderItem,
    };
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::{
        RadrootsNostrClient, RadrootsNostrKeys, RadrootsNostrMetadata, radroots_nostr_parse_pubkey,
    };
    use std::time::Instant;

    use crate::app::config::{BridgeConfig, Nip46Config};
    use crate::core::Radrootsd;
    use crate::core::nip46::session::Nip46Session;
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};
    use radroots_trade::order::canonicalize_order_request_for_signer;

    use super::{BridgeOrderRequestParams, publish_order_request};

    #[test]
    fn canonicalize_order_request_sets_missing_buyer_and_seller_pubkeys() {
        let order = canonicalize_order_request_for_signer(
            base_order("", ""),
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
    fn canonicalize_order_request_rejects_items_with_zero_bin_count() {
        let mut order = base_order(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "",
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
    fn canonicalize_order_request_drops_empty_discounts() {
        let order = canonicalize_order_request_for_signer(
            base_order(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "",
            ),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .expect("canonicalize");

        assert_eq!(order.discounts, None);
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
        let session_id = insert_signer_session(&ctx, "session-1").await;
        let params = BridgeOrderRequestParams {
            order: base_order("", ""),
            signer_session_id: Some(session_id.clone()),
            signer_authority: None,
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
                order: base_order("", ""),
                signer_session_id: Some(session_id),
                signer_authority: None,
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
        let session_id = insert_signer_session(&ctx, "session-1").await;
        publish_order_request(
            ctx.clone(),
            BridgeOrderRequestParams {
                order: base_order("", ""),
                signer_session_id: Some(session_id.clone()),
                signer_authority: None,
                idempotency_key: Some("same-key".to_string()),
            },
        )
        .await
        .expect("first");

        let mut conflicting = base_order("", "");
        conflicting.order_id = "order-2".to_string();
        let err = publish_order_request(
            ctx,
            BridgeOrderRequestParams {
                order: conflicting,
                signer_session_id: Some(session_id),
                signer_authority: None,
                idempotency_key: Some("same-key".to_string()),
            },
        )
        .await
        .expect_err("conflicting idempotency");
        assert!(err.to_string().contains("conflicts"));
    }

    #[tokio::test]
    async fn publish_order_request_rejects_missing_signer_session() {
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

        let err = publish_order_request(
            ctx,
            BridgeOrderRequestParams {
                order: base_order("", ""),
                signer_session_id: None,
                signer_authority: None,
                idempotency_key: Some("missing-session".to_string()),
            },
        )
        .await
        .expect_err("missing session rejected");
        assert!(err.to_string().contains("requires signer_session_id"));
    }

    async fn insert_signer_session(ctx: &RpcContext, session_id: &str) -> String {
        let signer_keys = RadrootsNostrKeys::generate();
        let signer_pubkey = signer_keys.public_key().to_hex();
        let remote_signer_pubkey =
            radroots_nostr_parse_pubkey(signer_pubkey.as_str()).expect("signer pubkey");
        let client = RadrootsNostrClient::new(signer_keys.clone());
        let client_keys = signer_keys.clone();
        let client_pubkey = client_keys.public_key();
        ctx.state
            .nip46_sessions
            .insert(Nip46Session {
                id: session_id.to_string(),
                client,
                client_keys,
                client_pubkey,
                remote_signer_pubkey,
                user_pubkey: None,
                relays: Vec::new(),
                perms: vec!["sign_event".to_string()],
                name: None,
                url: None,
                image: None,
                expires_at: Some(Instant::now() + std::time::Duration::from_secs(60)),
                auth_required: false,
                authorized: true,
                auth_url: None,
                pending_request: None,
                signer_authority: None,
            })
            .await;
        session_id.to_string()
    }

    fn base_listing_addr() -> &'static str {
        "30402:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:AAAAAAAAAAAAAAAAAAAAAg"
    }

    fn base_order(buyer_pubkey: &str, seller_pubkey: &str) -> TradeOrder {
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
        }
    }
}
