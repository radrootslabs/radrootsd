use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_events::RadrootsNostrEvent;
use radroots_events::kinds::{KIND_LISTING, KIND_LISTING_DRAFT, is_listing_kind};
use radroots_events::listing::RadrootsListing;
use radroots_events_codec::listing::encode::to_wire_parts_with_kind;
use radroots_events_codec::wire::WireEventParts;
use radroots_nostr::prelude::radroots_nostr_build_event;
use radroots_trade::listing::validation::validate_listing_event;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::core::bridge::publish::{
    BridgePublishSettings, connect_and_publish_event, failed_prepublish_execution,
};
use crate::core::bridge::store::new_listing_publish_job;
use crate::transport::jsonrpc::auth::require_bridge_auth;
use crate::transport::jsonrpc::methods::bridge::shared::{
    BridgePublishResponse, ensure_bridge_enabled, fingerprint_bridge_request, normalize_idempotency_key,
    reserve_bridge_job, resolve_actor_bridge_signer, sign_bridge_event_builder,
};
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct BridgeListingPublishParams {
    listing: RadrootsListing,
    #[serde(default)]
    kind: Option<u32>,
    #[serde(default)]
    signer_session_id: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalBridgeListingPublishRequest {
    kind: u32,
    listing: RadrootsListing,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("bridge.listing.publish");
    m.register_async_method(
        "bridge.listing.publish",
        |params, ctx, extensions| async move {
            require_bridge_auth(&extensions)?;
            let params: BridgeListingPublishParams = params
                .parse()
                .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
            let response = publish_listing(ctx.as_ref().clone(), params).await?;
            Ok::<BridgePublishResponse, RpcError>(response)
        },
    )?;
    Ok(())
}

async fn publish_listing(
    ctx: RpcContext,
    params: BridgeListingPublishParams,
) -> Result<BridgePublishResponse, RpcError> {
    ensure_bridge_enabled(&ctx)?;
    let idempotency_key = normalize_idempotency_key(params.idempotency_key)?;
    let kind = resolve_listing_kind(params.kind)?;
    let signer = resolve_actor_bridge_signer(
        &ctx,
        params.signer_session_id.as_deref(),
        kind,
        "bridge.listing.publish",
    )
    .await?;
    let signer_pubkey = signer.signer_pubkey_hex();
    let listing = canonicalize_listing_for_signer(params.listing, signer_pubkey.as_str());
    let canonical = CanonicalBridgeListingPublishRequest { kind, listing };
    let request_fingerprint =
        fingerprint_bridge_request("bridge.listing.publish", &signer, &canonical)?;
    let parts = to_wire_parts_with_kind(&canonical.listing, canonical.kind)
        .map_err(|error| RpcError::InvalidParams(format!("invalid listing contract: {error}")))?;
    let validated = validate_canonical_listing_contract_for_signer(
        &canonical.listing,
        signer_pubkey.as_str(),
        &parts,
    )?;
    let builder = radroots_nostr_build_event(parts.kind, parts.content, parts.tags)
        .map_err(|error| RpcError::Other(format!("failed to build listing event: {error}")))?;

    let reserved = reserve_bridge_job(
        &ctx,
        new_listing_publish_job(
            Uuid::new_v4().to_string(),
            idempotency_key,
            signer.signer_mode(),
            parts.kind,
            None,
            validated.listing_addr.clone(),
            ctx.state.bridge_config.delivery_policy,
            ctx.state.bridge_config.delivery_quorum,
        ),
        request_fingerprint,
        "bridge listing",
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
        match sign_bridge_event_builder(&ctx, &signer, builder, "bridge.listing.publish").await {
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
        .map_err(|error| RpcError::Other(format!("failed to persist bridge listing job: {error}")))?
        .ok_or_else(|| RpcError::Other("bridge job disappeared during completion".to_string()))?;
    debug_assert_eq!(
        job.event_addr.as_deref(),
        Some(validated.listing_addr.as_str())
    );

    Ok(BridgePublishResponse {
        deduplicated: false,
        job: job.into(),
    })
}

fn resolve_listing_kind(kind: Option<u32>) -> Result<u32, RpcError> {
    let kind = kind.unwrap_or(KIND_LISTING);
    if !is_listing_kind(kind) {
        return Err(RpcError::InvalidParams(format!(
            "listing kind must be {KIND_LISTING} or {KIND_LISTING_DRAFT}"
        )));
    }
    Ok(kind)
}

fn canonicalize_listing_for_signer(
    mut listing: RadrootsListing,
    signer_pubkey: &str,
) -> RadrootsListing {
    if listing.farm.pubkey.trim().is_empty() {
        listing.farm.pubkey = signer_pubkey.to_string();
    }
    listing
}

fn validate_canonical_listing_contract_for_signer(
    listing: &RadrootsListing,
    signer_pubkey: &str,
    parts: &WireEventParts,
) -> Result<radroots_trade::listing::validation::RadrootsTradeListing, RpcError> {
    let canonical = RadrootsNostrEvent {
        id: String::new(),
        author: signer_pubkey.to_string(),
        created_at: 0,
        kind: parts.kind,
        tags: parts.tags.clone(),
        content: parts.content.clone(),
        sig: String::new(),
    };
    let validated = validate_listing_event(&canonical)
        .map_err(|error| RpcError::InvalidParams(format!("invalid listing contract: {error}")))?;
    debug_assert_eq!(validated.listing.d_tag, listing.d_tag);
    Ok(validated)
}

#[cfg(test)]
mod tests {
    use radroots_core::{
        RadrootsCoreCurrency, RadrootsCoreDecimal, RadrootsCoreMoney, RadrootsCoreQuantity,
        RadrootsCoreQuantityPrice, RadrootsCoreUnit,
    };
    use radroots_events::kinds::{KIND_LISTING, KIND_LISTING_DRAFT};
    use radroots_events::listing::{
        RadrootsListing, RadrootsListingAvailability, RadrootsListingBin,
        RadrootsListingDeliveryMethod, RadrootsListingFarmRef, RadrootsListingLocation,
        RadrootsListingProduct,
    };
    use radroots_events_codec::listing::encode::to_wire_parts_with_kind;
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::{
        RadrootsNostrClient, RadrootsNostrKeys, RadrootsNostrMetadata, radroots_nostr_parse_pubkey,
    };
    use std::time::Instant;

    use crate::app::config::{BridgeConfig, Nip46Config};
    use crate::core::Radrootsd;
    use crate::core::nip46::session::Nip46Session;
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

    use super::{
        BridgeListingPublishParams, canonicalize_listing_for_signer, publish_listing,
        validate_canonical_listing_contract_for_signer,
    };

    #[test]
    fn canonicalize_listing_sets_missing_farm_pubkey() {
        let listing = canonicalize_listing_for_signer(base_listing(), "abc123");
        assert_eq!(listing.farm.pubkey, "abc123");
    }

    #[test]
    fn validate_canonical_listing_contract_rejects_mismatched_seller_before_sign() {
        let listing = canonicalize_listing_for_signer(base_listing(), "abc123");
        let mut invalid = listing.clone();
        invalid.farm.pubkey = "other".to_string();
        let parts = to_wire_parts_with_kind(&invalid, KIND_LISTING).expect("wire parts");
        let err =
            validate_canonical_listing_contract_for_signer(&invalid, "abc123", &parts).unwrap_err();
        assert!(err.to_string().contains("invalid listing contract"));
    }

    #[tokio::test]
    async fn publish_listing_is_job_backed_and_idempotent() {
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
        let params = BridgeListingPublishParams {
            listing: base_listing(),
            kind: None,
            signer_session_id: Some(session_id.clone()),
            idempotency_key: Some("same-key".to_string()),
        };

        let first = publish_listing(ctx.clone(), params).await.expect("first");
        assert!(!first.deduplicated);
        assert_eq!(first.job.command, "bridge.listing.publish");
        assert!(first.job.event_addr.is_some());

        let second = publish_listing(
            ctx,
            BridgeListingPublishParams {
                listing: base_listing(),
                kind: None,
                signer_session_id: Some(session_id),
                idempotency_key: Some("same-key".to_string()),
            },
        )
        .await
        .expect("second");
        assert!(second.deduplicated);
        assert_eq!(second.job.job_id, first.job.job_id);
    }

    #[tokio::test]
    async fn publish_listing_rejects_invalid_seller_before_job_reserve() {
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
        let mut listing = base_listing();
        listing.farm.pubkey =
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
        let err = publish_listing(
            ctx.clone(),
            BridgeListingPublishParams {
                listing,
                kind: None,
                signer_session_id: Some(session_id),
                idempotency_key: Some("bad-listing".to_string()),
            },
        )
        .await
        .expect_err("invalid seller rejected");
        assert!(err.to_string().contains("invalid listing contract"));
        assert_eq!(ctx.state.bridge_jobs.snapshot().retained_jobs, 0);
    }

    #[tokio::test]
    async fn publish_listing_allows_draft_kind() {
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

        let response = publish_listing(
            ctx,
            BridgeListingPublishParams {
                listing: base_listing(),
                kind: Some(KIND_LISTING_DRAFT),
                signer_session_id: Some(session_id),
                idempotency_key: Some("draft-kind".to_string()),
            },
        )
        .await
        .expect("draft listing");

        assert_eq!(response.job.event_kind, KIND_LISTING_DRAFT);
        assert!(
            response
                .job
                .event_addr
                .as_deref()
                .is_some_and(|addr| addr.starts_with("30403:"))
        );
    }

    #[tokio::test]
    async fn publish_listing_rejects_missing_signer_session() {
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

        let err = publish_listing(
            ctx,
            BridgeListingPublishParams {
                listing: base_listing(),
                kind: None,
                signer_session_id: None,
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
        ctx.state.nip46_sessions.insert(Nip46Session {
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
        }).await;
        session_id.to_string()
    }

    fn base_listing() -> RadrootsListing {
        RadrootsListing {
            d_tag: "AAAAAAAAAAAAAAAAAAAAAg".to_string(),
            farm: RadrootsListingFarmRef {
                pubkey: String::new(),
                d_tag: "AAAAAAAAAAAAAAAAAAAAAw".to_string(),
            },
            product: RadrootsListingProduct {
                key: "coffee".to_string(),
                title: "Coffee".to_string(),
                category: "coffee".to_string(),
                summary: Some("Single origin coffee".to_string()),
                process: None,
                lot: None,
                location: None,
                profile: None,
                year: None,
            },
            primary_bin_id: "bin-1".to_string(),
            bins: vec![RadrootsListingBin {
                bin_id: "bin-1".to_string(),
                quantity: RadrootsCoreQuantity::new(
                    RadrootsCoreDecimal::from(1000u32),
                    RadrootsCoreUnit::MassG,
                ),
                price_per_canonical_unit: RadrootsCoreQuantityPrice::new(
                    RadrootsCoreMoney::new(
                        RadrootsCoreDecimal::from(20u32),
                        RadrootsCoreCurrency::USD,
                    ),
                    RadrootsCoreQuantity::new(
                        RadrootsCoreDecimal::from(1u32),
                        RadrootsCoreUnit::MassG,
                    ),
                ),
                display_amount: None,
                display_unit: None,
                display_label: None,
                display_price: None,
                display_price_unit: None,
            }],
            resource_area: None,
            plot: None,
            discounts: None,
            inventory_available: Some(RadrootsCoreDecimal::from(5u32)),
            availability: Some(RadrootsListingAvailability::Status {
                status: radroots_events::listing::RadrootsListingStatus::Active,
            }),
            delivery_method: Some(RadrootsListingDeliveryMethod::Pickup),
            location: Some(RadrootsListingLocation {
                primary: "Farm".to_string(),
                city: None,
                region: None,
                country: None,
                lat: None,
                lng: None,
                geohash: None,
            }),
            images: None,
        }
    }
}
