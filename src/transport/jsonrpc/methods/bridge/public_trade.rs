use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_events::RadrootsNostrEventPtr;
use radroots_events::kinds::KIND_LISTING;
use radroots_events::trade::{
    RadrootsTradeDiscountDecision as TradeDiscountDecision,
    RadrootsTradeMessagePayload as TradeListingMessagePayload,
    RadrootsTradeMessageType as TradeListingMessageType,
};
use radroots_events_codec::trade::{
    RadrootsTradeListingAddress as TradeListingAddress,
    trade_envelope_event_build as trade_listing_envelope_event_build,
};
use radroots_nostr::prelude::{
    radroots_event_from_nostr, radroots_nostr_build_event, radroots_nostr_fetch_event_by_id,
    radroots_nostr_parse_pubkey,
};
use radroots_trade::listing::validation::validate_listing_event;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::core::bridge::publish::{
    BridgePublishSettings, connect_and_publish_event, failed_prepublish_execution,
};
use crate::core::bridge::store::new_publish_job;
use crate::transport::jsonrpc::auth::require_bridge_auth;
use crate::transport::jsonrpc::methods::bridge::shared::{
    BridgePublishResponse, ensure_bridge_enabled, fingerprint_bridge_request,
    normalize_idempotency_key, reserve_bridge_job, resolve_bridge_signer,
    sign_bridge_event_builder,
};
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BridgePublicTradeParams<T> {
    listing_addr: String,
    order_id: String,
    counterparty_pubkey: String,
    #[serde(default)]
    listing_event: Option<RadrootsNostrEventPtr>,
    #[serde(default)]
    root_event_id: Option<String>,
    #[serde(default)]
    prev_event_id: Option<String>,
    payload: T,
    #[serde(default)]
    signer_session_id: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalBridgePublicTradeRequest<T> {
    listing_addr: String,
    order_id: String,
    counterparty_pubkey: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    listing_event: Option<RadrootsNostrEventPtr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    root_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prev_event_id: Option<String>,
    payload: T,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpectedPublicTradeAuthor {
    Buyer,
    Seller,
    Either,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    register_public_trade_method(
        m,
        registry,
        "bridge.order.response",
        TradeListingMessageType::OrderResponse,
        TradeListingMessagePayload::OrderResponse,
    )?;
    register_public_trade_method(
        m,
        registry,
        "bridge.order.revision",
        TradeListingMessageType::OrderRevision,
        TradeListingMessagePayload::OrderRevision,
    )?;
    register_public_trade_method(
        m,
        registry,
        "bridge.order.revision.accept",
        TradeListingMessageType::OrderRevisionAccept,
        TradeListingMessagePayload::OrderRevisionAccept,
    )?;
    register_public_trade_method(
        m,
        registry,
        "bridge.order.revision.decline",
        TradeListingMessageType::OrderRevisionDecline,
        TradeListingMessagePayload::OrderRevisionDecline,
    )?;
    register_public_trade_method(
        m,
        registry,
        "bridge.order.question",
        TradeListingMessageType::Question,
        TradeListingMessagePayload::Question,
    )?;
    register_public_trade_method(
        m,
        registry,
        "bridge.order.answer",
        TradeListingMessageType::Answer,
        TradeListingMessagePayload::Answer,
    )?;
    register_public_trade_method(
        m,
        registry,
        "bridge.order.discount.request",
        TradeListingMessageType::DiscountRequest,
        TradeListingMessagePayload::DiscountRequest,
    )?;
    register_public_trade_method(
        m,
        registry,
        "bridge.order.discount.offer",
        TradeListingMessageType::DiscountOffer,
        TradeListingMessagePayload::DiscountOffer,
    )?;
    register_public_trade_method(
        m,
        registry,
        "bridge.order.discount.accept",
        TradeListingMessageType::DiscountAccept,
        TradeListingMessagePayload::DiscountAccept,
    )?;
    register_public_trade_method(
        m,
        registry,
        "bridge.order.discount.decline",
        TradeListingMessageType::DiscountDecline,
        TradeListingMessagePayload::DiscountDecline,
    )?;
    register_public_trade_method(
        m,
        registry,
        "bridge.order.cancel",
        TradeListingMessageType::Cancel,
        TradeListingMessagePayload::Cancel,
    )?;
    register_public_trade_method(
        m,
        registry,
        "bridge.order.fulfillment.update",
        TradeListingMessageType::FulfillmentUpdate,
        TradeListingMessagePayload::FulfillmentUpdate,
    )?;
    register_public_trade_method(
        m,
        registry,
        "bridge.order.receipt",
        TradeListingMessageType::Receipt,
        TradeListingMessagePayload::Receipt,
    )?;
    Ok(())
}

fn register_public_trade_method<T>(
    m: &mut RpcModule<RpcContext>,
    registry: &MethodRegistry,
    method_name: &'static str,
    message_type: TradeListingMessageType,
    payload_into: fn(T) -> TradeListingMessagePayload,
) -> Result<()>
where
    T: DeserializeOwned + Serialize + Clone + Send + Sync + 'static,
{
    registry.track(method_name);
    m.register_async_method(method_name, move |params, ctx, extensions| async move {
        require_bridge_auth(&extensions)?;
        let params: BridgePublicTradeParams<T> = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let response = publish_public_trade(
            ctx.as_ref().clone(),
            method_name,
            message_type,
            params,
            payload_into,
        )
        .await?;
        Ok::<BridgePublishResponse, RpcError>(response)
    })?;
    Ok(())
}

async fn publish_public_trade<T>(
    ctx: RpcContext,
    command: &'static str,
    message_type: TradeListingMessageType,
    params: BridgePublicTradeParams<T>,
    payload_into: fn(T) -> TradeListingMessagePayload,
) -> Result<BridgePublishResponse, RpcError>
where
    T: Serialize + Clone,
{
    ensure_bridge_enabled(&ctx)?;

    let idempotency_key = normalize_idempotency_key(params.idempotency_key.clone())?;
    let signer = resolve_bridge_signer(
        &ctx,
        params.signer_session_id.as_deref(),
        message_type.kind(),
    )
    .await?;
    let signer_pubkey = signer.signer_pubkey_hex();
    let (mut canonical, listing_addr) =
        canonicalize_public_trade_params(params, signer_pubkey.as_str(), message_type)?;
    canonical.listing_event =
        resolve_listing_snapshot(&ctx, &listing_addr, message_type, canonical.listing_event)
            .await?;

    let request_fingerprint = fingerprint_bridge_request(command, &signer, &canonical)?;
    let payload = payload_into(canonical.payload.clone());
    validate_payload_for_message_type(&payload, message_type)?;
    let built = trade_listing_envelope_event_build(
        canonical.counterparty_pubkey.clone(),
        message_type,
        canonical.listing_addr.clone(),
        Some(canonical.order_id.clone()),
        canonical.listing_event.as_ref(),
        canonical.root_event_id.as_deref(),
        canonical.prev_event_id.as_deref(),
        &payload,
    )
    .map_err(|error| RpcError::InvalidParams(format!("invalid {command} envelope: {error}")))?;
    let builder = radroots_nostr_build_event(built.kind, built.content, built.tags)
        .map_err(|error| RpcError::Other(format!("failed to build {command} event: {error}")))?;

    let reserved = reserve_bridge_job(
        &ctx,
        new_publish_job(
            command,
            Uuid::new_v4().to_string(),
            idempotency_key,
            signer.signer_mode(),
            message_type.kind(),
            None,
            Some(canonical.listing_addr.clone()),
            ctx.state.bridge_config.delivery_policy,
            ctx.state.bridge_config.delivery_quorum,
        ),
        request_fingerprint,
        command,
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
    let event = match sign_bridge_event_builder(&ctx, &signer, builder, command).await {
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
        .map_err(|error| RpcError::Other(format!("failed to persist {command} job: {error}")))?
        .ok_or_else(|| RpcError::Other("bridge job disappeared during completion".to_string()))?;

    Ok(BridgePublishResponse {
        deduplicated: false,
        job: job.into(),
    })
}

fn canonicalize_public_trade_params<T>(
    params: BridgePublicTradeParams<T>,
    signer_pubkey: &str,
    message_type: TradeListingMessageType,
) -> Result<(CanonicalBridgePublicTradeRequest<T>, TradeListingAddress), RpcError> {
    let listing_addr = normalized_required_string(params.listing_addr, "listing_addr")?;
    let parsed_listing_addr = TradeListingAddress::parse(&listing_addr)
        .map_err(|error| RpcError::InvalidParams(format!("invalid listing_addr: {error}")))?;
    if u32::from(parsed_listing_addr.kind) != KIND_LISTING {
        return Err(RpcError::InvalidParams(
            "listing_addr must reference a public NIP-99 listing".to_string(),
        ));
    }

    let order_id = normalized_required_string(params.order_id, "order_id")?;
    let counterparty_pubkey =
        normalized_required_string(params.counterparty_pubkey, "counterparty_pubkey")?;
    radroots_nostr_parse_pubkey(&counterparty_pubkey).map_err(|error| {
        RpcError::InvalidParams(format!("invalid counterparty_pubkey: {error}"))
    })?;

    if counterparty_pubkey == signer_pubkey {
        return Err(RpcError::InvalidParams(
            "counterparty_pubkey must not match the requested bridge signer identity".to_string(),
        ));
    }

    validate_expected_author(
        &parsed_listing_addr,
        message_type,
        signer_pubkey,
        &counterparty_pubkey,
    )?;

    let listing_event = if message_type.requires_listing_snapshot() {
        Some(normalize_listing_event_ptr(
            params.listing_event.ok_or_else(|| {
                RpcError::InvalidParams(
                    "listing_event is required for this trade message".to_string(),
                )
            })?,
        )?)
    } else {
        None
    };

    let (root_event_id, prev_event_id) = if message_type.requires_trade_chain() {
        (
            Some(normalized_required_string(
                params.root_event_id.unwrap_or_default(),
                "root_event_id",
            )?),
            Some(normalized_required_string(
                params.prev_event_id.unwrap_or_default(),
                "prev_event_id",
            )?),
        )
    } else {
        (None, None)
    };

    Ok((
        CanonicalBridgePublicTradeRequest {
            listing_addr,
            order_id,
            counterparty_pubkey,
            listing_event,
            root_event_id,
            prev_event_id,
            payload: params.payload,
        },
        parsed_listing_addr,
    ))
}

fn validate_expected_author(
    listing_addr: &TradeListingAddress,
    message_type: TradeListingMessageType,
    signer_pubkey: &str,
    counterparty_pubkey: &str,
) -> Result<(), RpcError> {
    match expected_author(message_type) {
        ExpectedPublicTradeAuthor::Seller => {
            if signer_pubkey != listing_addr.seller_pubkey {
                return Err(RpcError::InvalidParams(format!(
                    "{message_type:?} must be authored by the listing seller"
                )));
            }
        }
        ExpectedPublicTradeAuthor::Buyer => {
            if signer_pubkey == listing_addr.seller_pubkey {
                return Err(RpcError::InvalidParams(format!(
                    "{message_type:?} must be authored by the buyer, not the listing seller"
                )));
            }
            if counterparty_pubkey != listing_addr.seller_pubkey {
                return Err(RpcError::InvalidParams(
                    "counterparty_pubkey must match the listing seller for buyer-authored trade messages"
                        .to_string(),
                ));
            }
        }
        ExpectedPublicTradeAuthor::Either => {}
    }
    Ok(())
}

fn expected_author(message_type: TradeListingMessageType) -> ExpectedPublicTradeAuthor {
    match message_type {
        TradeListingMessageType::OrderResponse
        | TradeListingMessageType::OrderRevision
        | TradeListingMessageType::Answer
        | TradeListingMessageType::DiscountOffer
        | TradeListingMessageType::FulfillmentUpdate => ExpectedPublicTradeAuthor::Seller,
        TradeListingMessageType::OrderRequest
        | TradeListingMessageType::OrderRevisionAccept
        | TradeListingMessageType::OrderRevisionDecline
        | TradeListingMessageType::Question
        | TradeListingMessageType::DiscountRequest
        | TradeListingMessageType::DiscountAccept
        | TradeListingMessageType::DiscountDecline
        | TradeListingMessageType::Receipt => ExpectedPublicTradeAuthor::Buyer,
        TradeListingMessageType::Cancel => ExpectedPublicTradeAuthor::Either,
        TradeListingMessageType::ListingValidateRequest
        | TradeListingMessageType::ListingValidateResult => ExpectedPublicTradeAuthor::Either,
    }
}

fn normalize_listing_event_ptr(
    ptr: RadrootsNostrEventPtr,
) -> Result<RadrootsNostrEventPtr, RpcError> {
    if ptr.id.trim().is_empty() {
        return Err(RpcError::InvalidParams(
            "listing_event.id cannot be empty".to_string(),
        ));
    }
    if ptr
        .relays
        .as_ref()
        .is_some_and(|relay| relay.trim().is_empty())
    {
        return Err(RpcError::InvalidParams(
            "listing_event.relays cannot be empty".to_string(),
        ));
    }
    Ok(ptr)
}

async fn resolve_listing_snapshot(
    ctx: &RpcContext,
    listing_addr: &TradeListingAddress,
    message_type: TradeListingMessageType,
    listing_event: Option<RadrootsNostrEventPtr>,
) -> Result<Option<RadrootsNostrEventPtr>, RpcError> {
    if !message_type.requires_listing_snapshot() {
        return Ok(None);
    }
    let Some(listing_event) = listing_event else {
        return Err(RpcError::InvalidParams(
            "listing_event is required for this trade message".to_string(),
        ));
    };
    if ctx.state.client.relays().await.is_empty() {
        return Ok(Some(listing_event));
    }
    let event = radroots_nostr_fetch_event_by_id(&ctx.state.client, &listing_event.id)
        .await
        .map_err(|error| {
            RpcError::Other(format!(
                "failed to fetch listing_event `{}`: {error}",
                listing_event.id
            ))
        })?;
    let validated = validate_listing_event(&radroots_event_from_nostr(&event))
        .map_err(|error| RpcError::InvalidParams(format!("invalid listing_event: {error}")))?;
    if validated.listing_addr != listing_addr.as_str() {
        return Err(RpcError::InvalidParams(
            "listing_event must match listing_addr".to_string(),
        ));
    }
    Ok(Some(listing_event))
}

fn validate_payload_for_message_type(
    payload: &TradeListingMessagePayload,
    message_type: TradeListingMessageType,
) -> Result<(), RpcError> {
    match (message_type, payload) {
        (
            TradeListingMessageType::OrderRevisionAccept,
            TradeListingMessagePayload::OrderRevisionAccept(response),
        ) => {
            if !response.accepted {
                return Err(RpcError::InvalidParams(
                    "bridge.order.revision.accept payload.accepted must be true".to_string(),
                ));
            }
        }
        (
            TradeListingMessageType::OrderRevisionDecline,
            TradeListingMessagePayload::OrderRevisionDecline(response),
        ) => {
            if response.accepted {
                return Err(RpcError::InvalidParams(
                    "bridge.order.revision.decline payload.accepted must be false".to_string(),
                ));
            }
        }
        (
            TradeListingMessageType::DiscountAccept,
            TradeListingMessagePayload::DiscountAccept(TradeDiscountDecision::Accept { .. }),
        )
        | (
            TradeListingMessageType::DiscountDecline,
            TradeListingMessagePayload::DiscountDecline(TradeDiscountDecision::Decline { .. }),
        ) => {}
        (TradeListingMessageType::DiscountAccept, _) => {
            return Err(RpcError::InvalidParams(
                "bridge.order.discount.accept payload must be an accept decision".to_string(),
            ));
        }
        (TradeListingMessageType::DiscountDecline, _) => {
            return Err(RpcError::InvalidParams(
                "bridge.order.discount.decline payload must be a decline decision".to_string(),
            ));
        }
        _ => {}
    }
    Ok(())
}

fn normalized_required_string(value: String, field: &str) -> Result<String, RpcError> {
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(RpcError::InvalidParams(format!("{field} cannot be empty")));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use radroots_core::{RadrootsCoreDecimal, RadrootsCoreDiscountValue, RadrootsCorePercent};
    use radroots_events::trade::{
        RadrootsTradeDiscountRequest as TradeDiscountRequest,
        RadrootsTradeOrderResponse as TradeOrderResponse,
        RadrootsTradeOrderRevisionResponse as TradeOrderRevisionResponse,
        RadrootsTradeQuestion as TradeQuestion,
    };
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::RadrootsNostrMetadata;

    use crate::app::config::{BridgeConfig, Nip46Config};
    use crate::core::Radrootsd;
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

    use super::*;

    #[tokio::test]
    async fn publish_order_response_is_job_backed_and_idempotent() {
        let identity = RadrootsIdentity::generate();
        let seller_pubkey = identity.public_key_hex();
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
        let params = BridgePublicTradeParams {
            listing_addr: base_listing_addr(&seller_pubkey),
            order_id: "order-1".to_string(),
            counterparty_pubkey: base_buyer_pubkey().to_string(),
            listing_event: None,
            root_event_id: Some("order-request-event".to_string()),
            prev_event_id: Some("order-request-event".to_string()),
            payload: TradeOrderResponse {
                accepted: true,
                reason: None,
            },
            signer_session_id: None,
            idempotency_key: Some("same-key".to_string()),
        };

        let first = publish_public_trade(
            ctx.clone(),
            "bridge.order.response",
            TradeListingMessageType::OrderResponse,
            params.clone(),
            TradeListingMessagePayload::OrderResponse,
        )
        .await
        .expect("first");
        assert!(!first.deduplicated);
        assert_eq!(first.job.command, "bridge.order.response");
        assert_eq!(
            first.job.event_kind,
            TradeListingMessageType::OrderResponse.kind()
        );
        assert_eq!(
            first.job.event_addr.as_deref(),
            Some(base_listing_addr(&seller_pubkey).as_str())
        );
        assert_eq!(first.job.signer_mode, "embedded_service_identity");

        let second = publish_public_trade(
            ctx,
            "bridge.order.response",
            TradeListingMessageType::OrderResponse,
            params,
            TradeListingMessagePayload::OrderResponse,
        )
        .await
        .expect("second");
        assert!(second.deduplicated);
        assert_eq!(second.job.job_id, first.job.job_id);
    }

    #[tokio::test]
    async fn publish_snapshot_message_requires_listing_event() {
        let ctx = buyer_ctx().expect("ctx");
        let err = publish_public_trade(
            ctx,
            "bridge.order.discount.request",
            TradeListingMessageType::DiscountRequest,
            BridgePublicTradeParams {
                listing_addr: base_listing_addr(base_seller_pubkey()),
                order_id: "order-1".to_string(),
                counterparty_pubkey: base_seller_pubkey().to_string(),
                listing_event: None,
                root_event_id: Some("root".to_string()),
                prev_event_id: Some("prev".to_string()),
                payload: TradeDiscountRequest {
                    discount_id: "discount-1".to_string(),
                    value: RadrootsCoreDiscountValue::Percent(RadrootsCorePercent::new(
                        RadrootsCoreDecimal::from(5u32),
                    )),
                },
                signer_session_id: None,
                idempotency_key: None,
            },
            TradeListingMessagePayload::DiscountRequest,
        )
        .await
        .expect_err("missing listing_event");
        assert!(err.to_string().contains("listing_event"));
    }

    #[tokio::test]
    async fn publish_chain_message_requires_root_and_prev() {
        let identity = RadrootsIdentity::generate();
        let seller_pubkey = identity.public_key_hex();
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
        let err = publish_public_trade(
            ctx,
            "bridge.order.response",
            TradeListingMessageType::OrderResponse,
            BridgePublicTradeParams {
                listing_addr: base_listing_addr(&seller_pubkey),
                order_id: "order-1".to_string(),
                counterparty_pubkey: base_buyer_pubkey().to_string(),
                listing_event: None,
                root_event_id: None,
                prev_event_id: Some("prev".to_string()),
                payload: TradeOrderResponse {
                    accepted: true,
                    reason: None,
                },
                signer_session_id: None,
                idempotency_key: None,
            },
            TradeListingMessagePayload::OrderResponse,
        )
        .await
        .expect_err("missing root_event_id");
        assert!(err.to_string().contains("root_event_id"));
    }

    #[tokio::test]
    async fn publish_buyer_message_rejects_listing_seller_signer() {
        let identity = RadrootsIdentity::generate();
        let seller_pubkey = identity.public_key_hex();
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
        let err = publish_public_trade(
            ctx,
            "bridge.order.question",
            TradeListingMessageType::Question,
            BridgePublicTradeParams {
                listing_addr: base_listing_addr(&seller_pubkey),
                order_id: "order-1".to_string(),
                counterparty_pubkey: base_buyer_pubkey().to_string(),
                listing_event: None,
                root_event_id: Some("root".to_string()),
                prev_event_id: Some("prev".to_string()),
                payload: TradeQuestion {
                    question_id: "q-1".to_string(),
                },
                signer_session_id: None,
                idempotency_key: None,
            },
            TradeListingMessagePayload::Question,
        )
        .await
        .expect_err("seller signed buyer message");
        assert!(err.to_string().contains("buyer"));
    }

    #[tokio::test]
    async fn publish_revision_accept_rejects_decline_payload() {
        let ctx = buyer_ctx().expect("ctx");
        let err = publish_public_trade(
            ctx,
            "bridge.order.revision.accept",
            TradeListingMessageType::OrderRevisionAccept,
            BridgePublicTradeParams {
                listing_addr: base_listing_addr(base_seller_pubkey()),
                order_id: "order-1".to_string(),
                counterparty_pubkey: base_seller_pubkey().to_string(),
                listing_event: None,
                root_event_id: Some("root".to_string()),
                prev_event_id: Some("prev".to_string()),
                payload: TradeOrderRevisionResponse {
                    accepted: false,
                    reason: Some("no".to_string()),
                },
                signer_session_id: None,
                idempotency_key: None,
            },
            TradeListingMessagePayload::OrderRevisionAccept,
        )
        .await
        .expect_err("decline payload");
        assert!(err.to_string().contains("payload.accepted"));
    }

    fn buyer_ctx() -> Result<RpcContext, RpcError> {
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
        .map_err(|error| RpcError::Other(format!("build state: {error}")))?;
        Ok(RpcContext::new(state, MethodRegistry::default()))
    }

    fn base_seller_pubkey() -> &'static str {
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    }

    fn base_buyer_pubkey() -> &'static str {
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }

    fn base_listing_addr(seller_pubkey: &str) -> String {
        format!("{KIND_LISTING}:{seller_pubkey}:AAAAAAAAAAAAAAAAAAAAAg")
    }

    #[test]
    fn validate_discount_decline_payload_shape() {
        let payload = TradeListingMessagePayload::DiscountDecline(TradeDiscountDecision::Decline {
            reason: Some("no".to_string()),
        });
        validate_payload_for_message_type(&payload, TradeListingMessageType::DiscountDecline)
            .expect("decline");

        let err = validate_payload_for_message_type(
            &TradeListingMessagePayload::DiscountDecline(TradeDiscountDecision::Accept {
                value: RadrootsCoreDiscountValue::Percent(RadrootsCorePercent::new(
                    RadrootsCoreDecimal::from(5u32),
                )),
            }),
            TradeListingMessageType::DiscountDecline,
        )
        .expect_err("accept");
        assert!(err.to_string().contains("decline"));
    }
}
