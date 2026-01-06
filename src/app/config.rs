use radroots_nostr::prelude::RadrootsNostrMetadata;
use serde::{Deserialize, Serialize};

fn default_rpc_addr() -> String {
    "127.0.0.1:7070".to_string()
}

fn default_max_request_body_size() -> u32 {
    10 * 1024 * 1024
}

fn default_max_response_body_size() -> u32 {
    10 * 1024 * 1024
}

fn default_max_connections() -> u32 {
    100
}

fn default_max_subscriptions_per_connection() -> u32 {
    1024
}

fn default_message_buffer_capacity() -> u32 {
    1024
}

fn default_nip46_session_ttl_secs() -> u64 {
    900
}

fn default_nip46_perms() -> Vec<String> {
    Vec::new()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Nip46Config {
    #[serde(default = "default_nip46_session_ttl_secs")]
    pub session_ttl_secs: u64,
    #[serde(default = "default_nip46_perms")]
    pub perms: Vec<String>,
}

impl Default for Nip46Config {
    fn default() -> Self {
        Self {
            session_ttl_secs: default_nip46_session_ttl_secs(),
            perms: default_nip46_perms(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RpcConfig {
    #[serde(default = "default_rpc_addr")]
    pub addr: String,
    #[serde(default = "default_max_request_body_size")]
    pub max_request_body_size: u32,
    #[serde(default = "default_max_response_body_size")]
    pub max_response_body_size: u32,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_max_subscriptions_per_connection")]
    pub max_subscriptions_per_connection: u32,
    #[serde(default = "default_message_buffer_capacity")]
    pub message_buffer_capacity: u32,
    #[serde(default)]
    pub batch_request_limit: Option<u32>,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            addr: default_rpc_addr(),
            max_request_body_size: default_max_request_body_size(),
            max_response_body_size: default_max_response_body_size(),
            max_connections: default_max_connections(),
            max_subscriptions_per_connection: default_max_subscriptions_per_connection(),
            message_buffer_capacity: default_message_buffer_capacity(),
            batch_request_limit: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Configuration {
    pub logs_dir: String,
    #[serde(default)]
    pub rpc: RpcConfig,
    #[serde(default)]
    pub rpc_addr: Option<String>,
    #[serde(default)]
    pub relays: Vec<String>,
    #[serde(default)]
    pub nip46: Nip46Config,
}

impl Configuration {
    pub fn rpc_addr(&self) -> &str {
        self.rpc_addr
            .as_deref()
            .unwrap_or(self.rpc.addr.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub metadata: RadrootsNostrMetadata,
    pub config: Configuration,
}
