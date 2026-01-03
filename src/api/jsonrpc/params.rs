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
