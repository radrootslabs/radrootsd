use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_events::listing::RadrootsListing;
use radroots_events_codec::listing::encode::to_wire_parts;
use radroots_nostr::prelude::{radroots_event_from_nostr, radroots_nostr_build_event};
use radroots_trade::listing::validation::validate_listing_event;
use serde::Deserialize;
use uuid::Uuid;

use crate::core::bridge::publish::{
    BridgePublishSettings, connect_and_publish_event, failed_prepublish_execution,
};
use crate::core::bridge::store::new_listing_publish_job;
use crate::transport::jsonrpc::auth::require_bridge_auth;
use crate::transport::jsonrpc::methods::bridge::shared::{
    BridgePublishResponse, ensure_bridge_enabled, fingerprint_bridge_request,
    normalize_idempotency_key, reserve_bridge_job, resolve_bridge_signer,
    sign_bridge_event_builder,
};
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct BridgeListingPublishParams {
    listing: RadrootsListing,
    #[serde(default)]
    signer_session_id: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
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
    let signer = resolve_bridge_signer(&ctx, params.signer_session_id.as_deref(), 30402).await?;
    let signer_pubkey = signer.signer_pubkey_hex();
    let listing = canonicalize_listing_for_signer(params.listing, signer_pubkey.as_str());
    let request_fingerprint =
        fingerprint_bridge_request("bridge.listing.publish", &signer, &listing)?;
    let parts = to_wire_parts(&listing)
        .map_err(|error| RpcError::InvalidParams(format!("invalid listing contract: {error}")))?;
    let builder = radroots_nostr_build_event(parts.kind, parts.content, parts.tags)
        .map_err(|error| RpcError::Other(format!("failed to build listing event: {error}")))?;
    let listing_addr = format!("{}:{}:{}", parts.kind, signer_pubkey, listing.d_tag.trim());

    let reserved = reserve_bridge_job(
        &ctx,
        new_listing_publish_job(
            Uuid::new_v4().to_string(),
            idempotency_key,
            signer.signer_mode(),
            parts.kind,
            None,
            listing_addr,
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
                job: existing,
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
    let canonical = radroots_event_from_nostr(&event);
    let validated = match validate_listing_event(&canonical) {
        Ok(validated) => validated,
        Err(error) => {
            let _ = ctx.state.bridge_jobs.complete(
                &job.job_id,
                Some(event.id.to_hex()),
                failed_prepublish_execution(
                    &publish_settings,
                    format!("invalid listing contract: {error}"),
                ),
            );
            return Err(RpcError::InvalidParams(format!(
                "invalid listing contract: {error}"
            )));
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
        job,
    })
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

#[cfg(test)]
mod tests {
    use radroots_core::{
        RadrootsCoreCurrency, RadrootsCoreDecimal, RadrootsCoreMoney, RadrootsCoreQuantity,
        RadrootsCoreQuantityPrice, RadrootsCoreUnit,
    };
    use radroots_events::listing::{
        RadrootsListing, RadrootsListingAvailability, RadrootsListingBin,
        RadrootsListingDeliveryMethod, RadrootsListingFarmRef, RadrootsListingLocation,
        RadrootsListingProduct,
    };
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::RadrootsNostrMetadata;

    use crate::app::config::{BridgeConfig, Nip46Config};
    use crate::core::Radrootsd;
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

    use super::{BridgeListingPublishParams, canonicalize_listing_for_signer, publish_listing};

    #[test]
    fn canonicalize_listing_sets_missing_farm_pubkey() {
        let listing = canonicalize_listing_for_signer(base_listing(), "abc123");
        assert_eq!(listing.farm.pubkey, "abc123");
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
        let params = BridgeListingPublishParams {
            listing: base_listing(),
            signer_session_id: None,
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
                signer_session_id: None,
                idempotency_key: Some("same-key".to_string()),
            },
        )
        .await
        .expect("second");
        assert!(second.deduplicated);
        assert_eq!(second.job.job_id, first.job.job_id);
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
