#![forbid(unsafe_code)]

use serde::Deserialize;

use crate::api::jsonrpc::RpcError;
use radroots_nostr::prelude::{
    radroots_nostr_parse_pubkeys,
    RadrootsNostrFilter,
    RadrootsNostrPublicKey,
    RadrootsNostrTimestamp,
};

pub const DEFAULT_LIMIT: u64 = 50;
pub const MAX_LIMIT: u64 = 1000;
pub const DEFAULT_TIMEOUT_SECS: u64 = 10;

#[derive(Debug, Default, Deserialize)]
pub struct EventListParams {
    #[serde(default)]
    pub authors: Option<Vec<String>>,
    #[serde(default)]
    pub limit: Option<u64>,
    #[serde(default)]
    pub since: Option<u64>,
    #[serde(default)]
    pub until: Option<u64>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

pub(crate) fn limit_or(limit: Option<u64>) -> usize {
    limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT) as usize
}

pub(crate) fn timeout_or(timeout_secs: Option<u64>) -> u64 {
    timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS)
}

pub(crate) fn parse_pubkeys(
    label: &str,
    values: &[String],
) -> Result<Vec<RadrootsNostrPublicKey>, RpcError> {
    radroots_nostr_parse_pubkeys(values)
        .map_err(|e| RpcError::InvalidParams(format!("invalid {label}: {e}")))
}

pub(crate) fn parse_pubkeys_opt(
    label: &str,
    values: Option<Vec<String>>,
) -> Result<Option<Vec<RadrootsNostrPublicKey>>, RpcError> {
    match values {
        Some(values) => Ok(Some(parse_pubkeys(label, &values)?)),
        None => Ok(None),
    }
}

pub(crate) fn apply_time_bounds(
    mut filter: RadrootsNostrFilter,
    since: Option<u64>,
    until: Option<u64>,
) -> RadrootsNostrFilter {
    if let Some(since) = since {
        filter = filter.since(RadrootsNostrTimestamp::from_secs(since));
    }
    if let Some(until) = until {
        filter = filter.until(RadrootsNostrTimestamp::from_secs(until));
    }
    filter
}

#[cfg(test)]
mod tests {
    use super::{
        apply_time_bounds,
        limit_or,
        parse_pubkeys_opt,
        timeout_or,
        DEFAULT_LIMIT,
        DEFAULT_TIMEOUT_SECS,
        MAX_LIMIT,
    };
    use crate::api::jsonrpc::RpcError;
    use radroots_nostr::prelude::RadrootsNostrFilter;

    #[test]
    fn limit_or_defaults_and_caps() {
        assert_eq!(limit_or(None), DEFAULT_LIMIT as usize);
        assert_eq!(limit_or(Some(MAX_LIMIT + 1)), MAX_LIMIT as usize);
        assert_eq!(limit_or(Some(0)), 0);
    }

    #[test]
    fn timeout_or_defaults() {
        assert_eq!(timeout_or(None), DEFAULT_TIMEOUT_SECS);
        assert_eq!(timeout_or(Some(3)), 3);
    }

    #[test]
    fn apply_time_bounds_sets_since_until() {
        let filter = RadrootsNostrFilter::new();
        let filter = apply_time_bounds(filter, Some(10), Some(20));
        assert_eq!(filter.since.map(|t| t.as_secs()), Some(10));
        assert_eq!(filter.until.map(|t| t.as_secs()), Some(20));
    }

    #[test]
    fn apply_time_bounds_noop_when_empty() {
        let filter = RadrootsNostrFilter::new();
        let filter = apply_time_bounds(filter, None, None);
        assert!(filter.since.is_none());
        assert!(filter.until.is_none());
    }

    #[test]
    fn parse_pubkeys_opt_accepts_valid() {
        let key = "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";
        let out = parse_pubkeys_opt("author", Some(vec![key.to_string()])).expect("pubkey");
        let out = out.expect("some");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].to_string(), key);
    }

    #[test]
    fn parse_pubkeys_opt_rejects_invalid() {
        let err = parse_pubkeys_opt("author", Some(vec!["nope".to_string()]))
            .expect_err("error");
        match err {
            RpcError::InvalidParams(msg) => assert!(msg.contains("invalid author")),
            _ => panic!("unexpected error"),
        }
    }
}
