use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_events::listing::RadrootsListing;
use radroots_events_codec::listing::encode::to_wire_parts;
use radroots_nostr::prelude::{radroots_event_from_nostr, radroots_nostr_build_event};
use radroots_nostr_signer::prelude::RadrootsNostrSignerBackend;
use radroots_trade::listing::validation::validate_listing_event;
use serde::Deserialize;
use uuid::Uuid;

use crate::core::bridge::publish::{BridgePublishSettings, connect_and_publish_event};
use crate::core::bridge::store::new_listing_publish_job;
use crate::transport::jsonrpc::auth::require_bridge_auth;
use crate::transport::jsonrpc::methods::bridge::shared::{
    BridgePublishResponse, bridge_signer_pubkey_hex, ensure_bridge_enabled,
    normalize_idempotency_key,
};
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct BridgeListingPublishParams {
    listing: RadrootsListing,
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
    let signer_pubkey = bridge_signer_pubkey_hex(&ctx)?;
    let listing = canonicalize_listing_for_embedded_signer(params.listing, signer_pubkey.as_str());
    let parts = to_wire_parts(&listing)
        .map_err(|error| RpcError::InvalidParams(format!("invalid listing contract: {error}")))?;
    let builder = radroots_nostr_build_event(parts.kind, parts.content, parts.tags)
        .map_err(|error| RpcError::Other(format!("failed to build listing event: {error}")))?;
    let signed = ctx
        .state
        .bridge_signer
        .sign_event_builder(builder)
        .map_err(|error| RpcError::Other(format!("failed to sign listing event: {error}")))?;
    let event = signed.event;
    let canonical = radroots_event_from_nostr(&event);
    let validated = validate_listing_event(&canonical)
        .map_err(|error| RpcError::InvalidParams(format!("invalid listing contract: {error}")))?;

    let reserved = ctx.state.bridge_jobs.reserve(new_listing_publish_job(
        Uuid::new_v4().to_string(),
        idempotency_key,
        parts.kind,
        event.id.to_hex(),
        validated.listing_addr,
        ctx.state.bridge_config.delivery_policy,
        ctx.state.bridge_config.delivery_quorum,
    ));
    let job = match reserved {
        Ok(job) => job,
        Err(existing) => {
            return Ok(BridgePublishResponse {
                deduplicated: true,
                job: existing,
            });
        }
    };

    let execution = connect_and_publish_event(
        &ctx.state.client,
        &BridgePublishSettings::from_config(&ctx.state.bridge_config),
        &event,
    )
    .await;
    let job = ctx
        .state
        .bridge_jobs
        .complete(&job.job_id, execution)
        .ok_or_else(|| RpcError::Other("bridge job disappeared during completion".to_string()))?;

    Ok(BridgePublishResponse {
        deduplicated: false,
        job,
    })
}

fn canonicalize_listing_for_embedded_signer(
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

    use super::canonicalize_listing_for_embedded_signer;

    #[test]
    fn canonicalize_listing_sets_missing_farm_pubkey() {
        let listing = canonicalize_listing_for_embedded_signer(base_listing(), "abc123");
        assert_eq!(listing.farm.pubkey, "abc123");
    }

    fn base_listing() -> RadrootsListing {
        RadrootsListing {
            d_tag: "fresh-carrots".to_string(),
            farm: RadrootsListingFarmRef {
                pubkey: String::new(),
                d_tag: "farm-1".to_string(),
            },
            product: RadrootsListingProduct {
                key: "carrot".to_string(),
                title: "Fresh carrots".to_string(),
                category: "vegetable".to_string(),
                summary: Some("Sweet carrots".to_string()),
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
                    RadrootsCoreDecimal::from(25),
                    RadrootsCoreUnit::MassKg,
                ),
                price_per_canonical_unit: RadrootsCoreQuantityPrice {
                    amount: RadrootsCoreMoney {
                        amount: RadrootsCoreDecimal::from(4),
                        currency: RadrootsCoreCurrency::USD,
                    },
                    quantity: RadrootsCoreQuantity::new(
                        RadrootsCoreDecimal::from(1),
                        RadrootsCoreUnit::MassKg,
                    ),
                },
                display_amount: None,
                display_unit: None,
                display_label: None,
                display_price: None,
                display_price_unit: None,
            }],
            resource_area: None,
            plot: None,
            discounts: None,
            inventory_available: Some(RadrootsCoreDecimal::from(25)),
            availability: Some(RadrootsListingAvailability::Status {
                status: radroots_events::listing::RadrootsListingStatus::Active,
            }),
            delivery_method: Some(RadrootsListingDeliveryMethod::Pickup),
            location: Some(RadrootsListingLocation {
                primary: "Shed 1".to_string(),
                city: Some("Portland".to_string()),
                region: Some("OR".to_string()),
                country: Some("US".to_string()),
                lat: None,
                lng: None,
                geohash: None,
            }),
            images: None,
        }
    }
}
