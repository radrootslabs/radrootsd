#![forbid(unsafe_code)]

use std::time::Duration;

use crate::api::jsonrpc::params::{parse_pubkeys, timeout_or};
use crate::api::jsonrpc::RpcError;
use radroots_nostr::prelude::{
    RadrootsNostrClient,
    RadrootsNostrEvent,
    RadrootsNostrFilter,
    RadrootsNostrPublicKey,
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
    use super::select_latest_event;
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

}
