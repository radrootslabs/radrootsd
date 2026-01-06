use radroots_nostr::prelude::radroots_nostr_parse_pubkey;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::transport::jsonrpc::RpcError;

#[derive(Clone, Debug, Serialize)]
pub enum Nip46ConnectMode {
    Bunker,
    Nostrconnect,
}

#[derive(Clone, Debug)]
pub struct Nip46ConnectInfo {
    pub mode: Nip46ConnectMode,
    pub relays: Vec<String>,
    pub remote_signer_pubkey: Option<String>,
    pub client_pubkey: Option<String>,
    pub secret: Option<String>,
    pub perms: Vec<String>,
    pub name: Option<String>,
    pub url: Option<String>,
    pub image: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RelayParam {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Deserialize)]
struct Nip46ConnectQuery {
    relay: Option<RelayParam>,
    secret: Option<String>,
    perms: Option<String>,
    name: Option<String>,
    url: Option<String>,
    image: Option<String>,
}

pub fn parse_connect_url(raw: &str) -> Result<Nip46ConnectInfo, RpcError> {
    let url = Url::parse(raw).map_err(|e| RpcError::InvalidParams(e.to_string()))?;
    match url.scheme() {
        "bunker" => parse_bunker_url(&url),
        "nostrconnect" => parse_nostrconnect_url(&url),
        _ => Err(RpcError::InvalidParams("unsupported scheme".to_string())),
    }
}

fn parse_bunker_url(url: &Url) -> Result<Nip46ConnectInfo, RpcError> {
    let remote_signer_pubkey = url.host_str().map(|host| host.to_string());
    let query: Nip46ConnectQuery =
        serde_qs::from_str(url.query().unwrap_or_default())
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
    let relays = relay_list(query.relay);
    let perms = parse_perms(query.perms);

    Ok(Nip46ConnectInfo {
        mode: Nip46ConnectMode::Bunker,
        relays,
        remote_signer_pubkey,
        client_pubkey: None,
        secret: query.secret,
        perms,
        name: query.name,
        url: query.url,
        image: query.image,
    })
}

fn parse_nostrconnect_url(url: &Url) -> Result<Nip46ConnectInfo, RpcError> {
    let client_pubkey = url
        .host_str()
        .map(|host| host.to_string())
        .ok_or_else(|| RpcError::InvalidParams("missing client pubkey".to_string()))?;
    radroots_nostr_parse_pubkey(&client_pubkey)
        .map_err(|e| RpcError::InvalidParams(format!("invalid client pubkey: {e}")))?;
    let query: Nip46ConnectQuery =
        serde_qs::from_str(url.query().unwrap_or_default())
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
    let relays = relay_list(query.relay);
    let perms = parse_perms(query.perms);

    Ok(Nip46ConnectInfo {
        mode: Nip46ConnectMode::Nostrconnect,
        relays,
        remote_signer_pubkey: None,
        client_pubkey: Some(client_pubkey),
        secret: query.secret,
        perms,
        name: query.name,
        url: query.url,
        image: query.image,
    })
}

fn parse_perms(perms: Option<String>) -> Vec<String> {
    perms
        .unwrap_or_default()
        .split(',')
        .map(|entry| entry.trim().to_string())
        .filter(|entry| !entry.is_empty())
        .collect()
}

fn relay_list(relay: Option<RelayParam>) -> Vec<String> {
    let relays = match relay {
        Some(RelayParam::One(value)) => vec![value],
        Some(RelayParam::Many(values)) => values,
        None => Vec::new(),
    };
    relays
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect()
}
