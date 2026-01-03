#![forbid(unsafe_code)]

use serde::Serialize;

use radroots_events::relay_document::RadrootsRelayDocument;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RelayAddedResponse {
    pub added: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RelayRemovedResponse {
    pub removed: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RelayConnectResponse {
    pub connected: usize,
    pub connecting: usize,
    pub disconnected: usize,
    pub spawned_connect: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RelayStatusRow {
    pub url: String,
    pub status: String,
    pub scheme: Option<String>,
    pub host: Option<String>,
    pub onion: Option<bool>,
    pub port: Option<u16>,
    pub nip11: Option<RadrootsRelayDocument>,
}
