#![forbid(unsafe_code)]

use std::time::Duration;

use crate::api::jsonrpc::params::{parse_pubkeys, timeout_or};
use crate::api::jsonrpc::{RpcContext, RpcError};
use radroots_nostr::prelude::{
    radroots_nostr_send_event,
    RadrootsNostrClient,
    RadrootsNostrEvent,
    RadrootsNostrEventBuilder,
    RadrootsNostrEventId,
    RadrootsNostrFilter,
    RadrootsNostrKeys,
    RadrootsNostrOutput,
    RadrootsNostrPublicKey,
    RadrootsNostrTimestamp,
};

pub(crate) fn parse_author_or_default(
    author: Option<String>,
    default: RadrootsNostrPublicKey,
) -> Result<RadrootsNostrPublicKey, RpcError> {
    match author {
        Some(author) => {
            let authors = vec![author];
            let mut parsed = parse_pubkeys("author", &authors)?;
            parsed
                .pop()
                .ok_or_else(|| RpcError::InvalidParams("author cannot be empty".to_string()))
        }
        None => Ok(default),
    }
}

pub(crate) fn require_non_empty(label: &str, value: String) -> Result<String, RpcError> {
    if value.trim().is_empty() {
        Err(RpcError::InvalidParams(format!("{label} cannot be empty")))
    } else {
        Ok(value)
    }
}

pub(crate) fn validate_test_event_options(
    allow_test_events: bool,
    author_secret_key: Option<String>,
    created_at: Option<u64>,
) -> Result<(Option<String>, Option<u64>), RpcError> {
    let has_test_options = author_secret_key.is_some() || created_at.is_some();
    if has_test_options && !allow_test_events {
        return Err(RpcError::InvalidParams(
            "test event overrides require config.rpc.allow_test_events = true".to_string(),
        ));
    }
    let author_secret_key = match author_secret_key {
        Some(value) => {
            let value = value.trim().to_string();
            if value.is_empty() {
                return Err(RpcError::InvalidParams(
                    "author_secret_key cannot be empty".to_string(),
                ));
            }
            Some(value)
        }
        None => None,
    };
    Ok((author_secret_key, created_at))
}

pub(crate) async fn send_event_with_options(
    ctx: &RpcContext,
    builder: RadrootsNostrEventBuilder,
    author_secret_key: Option<String>,
    created_at: Option<u64>,
) -> Result<RadrootsNostrOutput<RadrootsNostrEventId>, RpcError> {
    let (author_secret_key, created_at) = validate_test_event_options(
        ctx.state.allow_test_events,
        author_secret_key,
        created_at,
    )?;
    let builder = match created_at {
        Some(created_at) => {
            builder.custom_created_at(RadrootsNostrTimestamp::from_secs(created_at))
        }
        None => builder,
    };

    if let Some(author_secret_key) = author_secret_key {
        let keys = RadrootsNostrKeys::parse(&author_secret_key)
            .map_err(|e| RpcError::InvalidParams(format!("invalid author_secret_key: {e}")))?;
        let event = builder
            .sign_with_keys(&keys)
            .map_err(|e| RpcError::Other(format!("failed to sign event: {e}")))?;
        ctx.state
            .client
            .send_event(&event)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish event: {e}")))
    } else {
        radroots_nostr_send_event(&ctx.state.client, builder)
            .await
            .map_err(|e| RpcError::Other(format!("failed to publish event: {e}")))
    }
}

pub(crate) async fn fetch_latest_event(
    client: &RadrootsNostrClient,
    filter: RadrootsNostrFilter,
    timeout_secs: Option<u64>,
) -> Result<Option<RadrootsNostrEvent>, RpcError> {
    let stored = client
        .database()
        .query(filter.clone())
        .await
        .map_err(|e| RpcError::Other(format!("query failed: {e}")))?;
    let fetched = client
        .fetch_events(filter, Duration::from_secs(timeout_or(timeout_secs)))
        .await
        .map_err(|e| RpcError::Other(format!("fetch failed: {e}")))?;
    Ok(select_latest_event(
        stored.into_iter().chain(fetched.into_iter()),
    ))
}

fn select_latest_event<I>(events: I) -> Option<RadrootsNostrEvent>
where
    I: IntoIterator<Item = RadrootsNostrEvent>,
{
    let mut latest: Option<RadrootsNostrEvent> = None;
    for event in events {
        let replace = match latest.as_ref() {
            Some(current) => event.created_at > current.created_at,
            None => true,
        };
        if replace {
            latest = Some(event);
        }
    }
    latest
}

#[cfg(test)]
mod tests {
    use super::{select_latest_event, validate_test_event_options};
    use crate::api::jsonrpc::RpcError;
    use radroots_nostr::prelude::RadrootsNostrEvent;
    use serde_json::json;

    fn event_with_created_at(id: &str, created_at: u64) -> RadrootsNostrEvent {
        let sig = format!("{:0128x}", 1);
        let event_json = json!({
            "id": id,
            "pubkey": "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4",
            "created_at": created_at,
            "kind": 1,
            "tags": [],
            "content": "content",
            "sig": sig,
        });
        serde_json::from_value(event_json).expect("event")
    }

    #[test]
    fn select_latest_event_picks_newest() {
        let older_id = format!("{:064x}", 1);
        let newer_id = format!("{:064x}", 2);
        let older = event_with_created_at(&older_id, 100);
        let newer = event_with_created_at(&newer_id, 200);
        let latest = select_latest_event(vec![older, newer]).expect("latest");
        assert_eq!(latest.id.to_string(), newer_id);
        assert_eq!(latest.created_at.as_secs(), 200);
    }

    #[test]
    fn test_event_options_require_flag() {
        let err =
            validate_test_event_options(false, Some("deadbeef".to_string()), None).unwrap_err();
        match err {
            RpcError::InvalidParams(message) => {
                assert!(message.contains("allow_test_events"));
            }
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn test_event_options_reject_empty_secret() {
        let err = validate_test_event_options(true, Some("  ".to_string()), None).unwrap_err();
        match err {
            RpcError::InvalidParams(message) => {
                assert!(message.contains("author_secret_key"));
            }
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn test_event_options_pass_through() {
        let (secret_key, created_at) =
            validate_test_event_options(true, Some("deadbeef".to_string()), Some(42))
                .expect("options");
        assert_eq!(secret_key, Some("deadbeef".to_string()));
        assert_eq!(created_at, Some(42));
    }
}
