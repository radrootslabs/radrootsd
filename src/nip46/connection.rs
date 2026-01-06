#![forbid(unsafe_code)]

use serde::Serialize;
use url::Url;

use crate::api::jsonrpc::RpcError;
use radroots_nostr::prelude::radroots_nostr_parse_pubkey;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Nip46ConnectMode {
    Bunker,
    Nostrconnect,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Nip46ConnectInfo {
    pub mode: Nip46ConnectMode,
    pub remote_signer_pubkey: Option<String>,
    pub client_pubkey: Option<String>,
    pub relays: Vec<String>,
    pub secret: Option<String>,
    pub perms: Vec<String>,
    pub name: Option<String>,
    pub url: Option<String>,
    pub image: Option<String>,
}

pub fn parse_connect_url(raw: &str) -> Result<Nip46ConnectInfo, RpcError> {
    let url = Url::parse(raw)
        .map_err(|e| RpcError::InvalidParams(format!("invalid connect url: {e}")))?;
    match url.scheme() {
        "bunker" => parse_bunker_url(&url),
        "nostrconnect" => parse_nostrconnect_url(&url),
        scheme => Err(RpcError::InvalidParams(format!(
            "unsupported connect scheme: {scheme}"
        ))),
    }
}

fn parse_bunker_url(url: &Url) -> Result<Nip46ConnectInfo, RpcError> {
    let username = url.username();
    let host = url
        .host_str()
        .ok_or_else(|| RpcError::InvalidParams("missing remote signer".to_string()))?;
    let remote_signer_raw = if username.is_empty() { host } else { username };
    let remote_signer = radroots_nostr_parse_pubkey(remote_signer_raw)
        .map_err(|e| RpcError::InvalidParams(format!("invalid remote signer: {e}")))?
        .to_hex();

    let mut relays = parse_relays(url);
    if !username.is_empty() {
        if let Some(relay_host) = host_to_relay(url) {
            relays.insert(0, relay_host);
        }
    }

    Ok(Nip46ConnectInfo {
        mode: Nip46ConnectMode::Bunker,
        remote_signer_pubkey: Some(remote_signer),
        client_pubkey: None,
        relays,
        secret: parse_optional_param(url, "secret"),
        perms: parse_perms(url),
        name: parse_optional_param(url, "name"),
        url: parse_optional_param(url, "url"),
        image: parse_optional_param(url, "image"),
    })
}

fn parse_nostrconnect_url(url: &Url) -> Result<Nip46ConnectInfo, RpcError> {
    let host = url
        .host_str()
        .ok_or_else(|| RpcError::InvalidParams("missing client pubkey".to_string()))?;
    let client_pubkey = radroots_nostr_parse_pubkey(host)
        .map_err(|e| RpcError::InvalidParams(format!("invalid client pubkey: {e}")))?
        .to_hex();

    let relays = parse_relays(url);
    if relays.is_empty() {
        return Err(RpcError::InvalidParams("missing relay".to_string()));
    }

    let secret = parse_optional_param(url, "secret")
        .ok_or_else(|| RpcError::InvalidParams("missing secret".to_string()))?;

    Ok(Nip46ConnectInfo {
        mode: Nip46ConnectMode::Nostrconnect,
        remote_signer_pubkey: None,
        client_pubkey: Some(client_pubkey),
        relays,
        secret: Some(secret),
        perms: parse_perms(url),
        name: parse_optional_param(url, "name"),
        url: parse_optional_param(url, "url"),
        image: parse_optional_param(url, "image"),
    })
}

fn parse_relays(url: &Url) -> Vec<String> {
    url.query_pairs()
        .filter_map(|(key, value)| {
            if key == "relay" && !value.trim().is_empty() {
                Some(value.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn parse_optional_param(url: &Url, key: &str) -> Option<String> {
    url.query_pairs()
        .find_map(|(k, value)| {
            if k == key {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            } else {
                None
            }
        })
}

fn parse_perms(url: &Url) -> Vec<String> {
    parse_optional_param(url, "perms")
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(|entry| entry.to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn host_to_relay(url: &Url) -> Option<String> {
    let host = url.host_str()?;
    let port = url.port();
    let base = match port {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    Some(format!("wss://{base}"))
}

#[cfg(test)]
mod tests {
    use super::{parse_connect_url, Nip46ConnectMode};

    const HEX_PUBKEY: &str =
        "1bdebe7b23fccb167fc8843280b789839dfa296ae9fd86cc9769b4813d76d8a4";

    #[test]
    fn parse_bunker_with_relay_host() {
        let url = format!("bunker://{HEX_PUBKEY}@relay.example.com");
        let info = parse_connect_url(&url).expect("info");
        assert_eq!(info.mode, Nip46ConnectMode::Bunker);
        assert_eq!(info.remote_signer_pubkey.as_deref(), Some(HEX_PUBKEY));
        assert_eq!(info.relays, vec!["wss://relay.example.com"]);
    }

    #[test]
    fn parse_bunker_with_query_relay() {
        let url = format!("bunker://{HEX_PUBKEY}?relay=wss%3A%2F%2Frelay.example.com&secret=abc");
        let info = parse_connect_url(&url).expect("info");
        assert_eq!(info.mode, Nip46ConnectMode::Bunker);
        assert_eq!(info.remote_signer_pubkey.as_deref(), Some(HEX_PUBKEY));
        assert_eq!(info.relays, vec!["wss://relay.example.com"]);
        assert_eq!(info.secret.as_deref(), Some("abc"));
    }

    #[test]
    fn parse_nostrconnect_requires_secret_and_relay() {
        let url = format!("nostrconnect://{HEX_PUBKEY}?relay=wss%3A%2F%2Frelay.example.com&secret=token&perms=sign_event%3A1,nip44_encrypt");
        let info = parse_connect_url(&url).expect("info");
        assert_eq!(info.mode, Nip46ConnectMode::Nostrconnect);
        assert_eq!(info.client_pubkey.as_deref(), Some(HEX_PUBKEY));
        assert_eq!(info.relays, vec!["wss://relay.example.com"]);
        assert_eq!(info.secret.as_deref(), Some("token"));
        assert_eq!(info.perms.len(), 2);
    }
}
