#![forbid(unsafe_code)]

use std::time::Duration;

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use radroots_events::kinds::KIND_FARM;
use radroots_events::listing::RadrootsListingFarmRef;
use radroots_events::{RadrootsNostrEvent as RadrootsWireEvent, RadrootsNostrEventPtr};
use radroots_nostr::prelude::{
    radroots_nostr_build_event,
    radroots_nostr_parse_pubkey,
    radroots_nostr_send_event,
    RadrootsNostrClient,
    RadrootsNostrEvent as RadrootsRawEvent,
    RadrootsNostrEventId,
    RadrootsNostrFilter,
    RadrootsNostrKind,
};
use radroots_nostr::util::event_created_at_u32_saturating;
use radroots_trade::listing::dvm::{
    TradeListingEnvelope,
    TradeListingMessageType,
    TradeListingValidateRequest,
    TradeListingValidateResult,
};
use radroots_trade::listing::validation::{validate_listing_event, TradeListingValidationError};

use crate::api::jsonrpc::nostr::{event_tags, publish_response, PublishResponse};
use crate::api::jsonrpc::params::timeout_or;
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};

use super::helpers::{fetch_latest_listing_event, parse_listing_addr};

#[derive(Debug, Deserialize)]
struct TradeListingValidateParams {
    listing_addr: String,
    #[serde(default)]
    listing_event: Option<RadrootsNostrEventPtr>,
    #[serde(default)]
    timeout_secs: Option<u64>,
    #[serde(default)]
    recipient_pubkey: Option<String>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("trade.listing.validate.request");
    m.register_async_method("trade.listing.validate.request", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let TradeListingValidateRequestParams {
            listing_addr,
            recipient_pubkey,
            listing_event,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let addr = parse_listing_addr(&listing_addr)?;
        let listing_addr = addr.as_str();

        let recipient = radroots_nostr_parse_pubkey(&recipient_pubkey)
            .map_err(|e| RpcError::InvalidParams(format!("invalid recipient_pubkey: {e}")))?;

        let payload = TradeListingValidateRequest { listing_event };
        let envelope = TradeListingEnvelope::new(
            TradeListingMessageType::ListingValidateRequest,
            listing_addr.clone(),
            None,
            payload,
        );
        envelope
            .validate()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let content = serde_json::to_string(&envelope)
            .map_err(|e| RpcError::Other(format!("failed to encode envelope: {e}")))?;
        let tags = vec![
            vec!["p".to_string(), recipient.to_string()],
            vec!["a".to_string(), listing_addr.clone()],
        ];

        let builder = radroots_nostr_build_event(
            TradeListingMessageType::ListingValidateRequest.kind() as u32,
            content,
            tags,
        )
        .map_err(|e| RpcError::Other(format!("failed to build validate request event: {e}")))?;

        let output = radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish validate request: {e}")))?;

        Ok::<PublishResponse, RpcError>(publish_response(output))
    })?;

    registry.track("trade.listing.validate");
    m.register_async_method("trade.listing.validate", |params, ctx, _| async move {
        if ctx.state.client.relays().await.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let TradeListingValidateParams {
            listing_addr,
            listing_event,
            timeout_secs,
            recipient_pubkey,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let addr = parse_listing_addr(&listing_addr)?;
        let listing_addr = addr.as_str();
        let timeout_secs = timeout_or(timeout_secs);

        let listing_event = if let Some(ptr) = listing_event {
            let event_id = RadrootsNostrEventId::parse(&ptr.id)
                .map_err(|e| RpcError::InvalidParams(format!("invalid listing_event id: {e}")))?;
            match fetch_event_by_id(&ctx.state.client, event_id, timeout_secs).await {
                Ok(event) => event,
                Err(_) => {
                    let errors = vec![TradeListingValidationError::ListingEventFetchFailed {
                        listing_addr: listing_addr.clone(),
                    }];
                    let result = TradeListingValidateResult {
                        valid: false,
                        errors,
                    };
                    return Ok::<TradeListingValidateResult, RpcError>(result);
                }
            }
        } else {
            match fetch_latest_listing_event(&ctx.state.client, &addr, timeout_secs).await {
                Ok(event) => event,
                Err(err) => {
                    if matches!(err, RpcError::InvalidParams(_)) {
                        return Err(err);
                    }
                    let errors = vec![TradeListingValidationError::ListingEventFetchFailed {
                        listing_addr: listing_addr.clone(),
                    }];
                    let result = TradeListingValidateResult {
                        valid: false,
                        errors,
                    };
                    return Ok::<TradeListingValidateResult, RpcError>(result);
                }
            }
        };

        let errors = if let Some(event) = listing_event {
            let rr_event = radroots_event_from_nostr(&event);
            match validate_listing_event(&rr_event) {
                Ok(listing) => validate_farm_dependencies(&ctx.state.client, &listing.listing.farm, timeout_secs).await,
                Err(err) => vec![err],
            }
        } else {
            vec![TradeListingValidationError::ListingEventNotFound {
                listing_addr: listing_addr.clone(),
            }]
        };

        let result = TradeListingValidateResult {
            valid: errors.is_empty(),
            errors,
        };

        if let Some(recipient_pubkey) = recipient_pubkey {
            publish_validate_result(
                &ctx.state.client,
                &listing_addr,
                &recipient_pubkey,
                &result,
            )
            .await?;
        }
        Ok::<TradeListingValidateResult, RpcError>(result)
    })?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct TradeListingValidateRequestParams {
    listing_addr: String,
    recipient_pubkey: String,
    #[serde(default)]
    listing_event: Option<RadrootsNostrEventPtr>,
}

async fn publish_validate_result(
    client: &RadrootsNostrClient,
    listing_addr: &str,
    recipient_pubkey: &str,
    result: &TradeListingValidateResult,
) -> Result<(), RpcError> {
    let recipient = radroots_nostr_parse_pubkey(recipient_pubkey)
        .map_err(|e| RpcError::InvalidParams(format!("invalid recipient_pubkey: {e}")))?;
    let envelope = TradeListingEnvelope::new(
        TradeListingMessageType::ListingValidateResult,
        listing_addr.to_string(),
        None,
        result.clone(),
    );
    envelope
        .validate()
        .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
    let content = serde_json::to_string(&envelope)
        .map_err(|e| RpcError::Other(format!("failed to encode envelope: {e}")))?;
    let tags = vec![
        vec!["p".to_string(), recipient.to_string()],
        vec!["a".to_string(), listing_addr.to_string()],
    ];

    let builder = radroots_nostr_build_event(
        TradeListingMessageType::ListingValidateResult.kind() as u32,
        content,
        tags,
    )
    .map_err(|e| RpcError::Other(format!("failed to build validate result event: {e}")))?;

    let output = radroots_nostr_send_event(client, builder)
        .await
        .map_err(|e| RpcError::Other(format!("failed to publish validate result: {e}")))?;
    if !output.failed.is_empty() {
        return Err(RpcError::Other(format!(
            "validate result delivery failed: {:?}",
            output.failed
        )));
    }
    Ok(())
}

async fn fetch_event_by_id(
    client: &RadrootsNostrClient,
    event_id: RadrootsNostrEventId,
    timeout_secs: u64,
) -> Result<Option<RadrootsRawEvent>, RpcError> {
    let filter = RadrootsNostrFilter::new().id(event_id);
    let events = client
        .fetch_events(filter, Duration::from_secs(timeout_secs))
        .await
        .map_err(|e| RpcError::Other(format!("fetch failed: {e}")))?;
    Ok(events.into_iter().next())
}

async fn fetch_latest_event_by_kind(
    client: &RadrootsNostrClient,
    filter: RadrootsNostrFilter,
    kind: RadrootsNostrKind,
    timeout_secs: u64,
) -> Result<Option<RadrootsRawEvent>, RpcError> {
    let events = client
        .fetch_events(filter, Duration::from_secs(timeout_secs))
        .await
        .map_err(|e| RpcError::Other(format!("fetch failed: {e}")))?;
    let mut latest: Option<RadrootsRawEvent> = None;
    for ev in events {
        if ev.kind != kind {
            continue;
        }
        match &latest {
            Some(cur) if ev.created_at <= cur.created_at => {}
            _ => latest = Some(ev),
        }
    }
    Ok(latest)
}

async fn validate_farm_dependencies(
    client: &RadrootsNostrClient,
    farm: &RadrootsListingFarmRef,
    timeout_secs: u64,
) -> Vec<TradeListingValidationError> {
    let mut errors = Vec::new();
    let farm_pubkey = farm.pubkey.trim();
    let farm_d_tag = farm.d_tag.trim();
    let author = match radroots_nostr_parse_pubkey(farm_pubkey) {
        Ok(author) => author,
        Err(_) => {
            errors.push(TradeListingValidationError::MissingFarmProfile);
            errors.push(TradeListingValidationError::MissingFarmRecord);
            return errors;
        }
    };

    let profile_filter = RadrootsNostrFilter::new()
        .kind(RadrootsNostrKind::Metadata)
        .author(author.clone());
    let profile_event =
        match fetch_latest_event_by_kind(client, profile_filter, RadrootsNostrKind::Metadata, timeout_secs).await {
            Ok(event) => event,
            Err(_) => None,
        };
    let has_profile = profile_event
        .map(|event| tag_has_value(&event_tags(&event), "t", "radroots:type:farm"))
        .unwrap_or(false);
    if !has_profile {
        errors.push(TradeListingValidationError::MissingFarmProfile);
    }

    if !farm_d_tag.is_empty() {
        let record_filter = RadrootsNostrFilter::new()
            .kind(RadrootsNostrKind::Custom(KIND_FARM as u16))
            .author(author)
            .identifier(farm_d_tag.to_string());
        let record_event = match fetch_latest_event_by_kind(
            client,
            record_filter,
            RadrootsNostrKind::Custom(KIND_FARM as u16),
            timeout_secs,
        )
        .await
        {
            Ok(event) => event,
            Err(_) => None,
        };
        if record_event.is_none() {
            errors.push(TradeListingValidationError::MissingFarmRecord);
        }
    } else {
        errors.push(TradeListingValidationError::MissingFarmRecord);
    }

    errors
}

fn radroots_event_from_nostr(event: &RadrootsRawEvent) -> RadrootsWireEvent {
    RadrootsWireEvent {
        id: event.id.to_string(),
        author: event.pubkey.to_string(),
        created_at: event_created_at_u32_saturating(event),
        kind: event.kind.as_u16() as u32,
        tags: event_tags(event),
        content: event.content.clone(),
        sig: event.sig.to_string(),
    }
}

fn tag_has_value(tags: &[Vec<String>], key: &str, value: &str) -> bool {
    tags.iter().any(|tag| {
        tag.get(0).map(|k| k.as_str()) == Some(key)
            && tag.get(1).map(|v| v.as_str()) == Some(value)
    })
}

#[cfg(test)]
mod tests {
    use super::tag_has_value;

    #[test]
    fn tag_has_value_matches_exact() {
        let tags = vec![
            vec!["t".to_string(), "radroots:type:farm".to_string()],
            vec!["d".to_string(), "listing-1".to_string()],
        ];
        assert!(tag_has_value(&tags, "t", "radroots:type:farm"));
        assert!(!tag_has_value(&tags, "t", "radroots:type:individual"));
    }

}
