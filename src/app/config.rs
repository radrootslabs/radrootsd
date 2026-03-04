use radroots_nostr::prelude::RadrootsNostrMetadata;
use radroots_runtime::RadrootsNostrServiceConfig;
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
    #[serde(default)]
    pub nostrconnect_url: Option<String>,
}

impl Default for Nip46Config {
    fn default() -> Self {
        Self {
            session_ttl_secs: default_nip46_session_ttl_secs(),
            perms: default_nip46_perms(),
            nostrconnect_url: None,
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
    #[serde(flatten)]
    pub service: RadrootsNostrServiceConfig,
    #[serde(default)]
    pub rpc: RpcConfig,
    #[serde(default)]
    pub rpc_addr: Option<String>,
    #[serde(default)]
    pub nip46: Nip46Config,
}

impl Configuration {
    pub fn rpc_addr(&self) -> &str {
        self.rpc_addr.as_deref().unwrap_or(self.rpc.addr.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub metadata: RadrootsNostrMetadata,
    pub config: Configuration,
}

#[cfg(test)]
mod tests {
    use super::{Configuration, Nip46Config, RpcConfig};
    use radroots_runtime::RadrootsNostrServiceConfig;

    fn service_config() -> RadrootsNostrServiceConfig {
        RadrootsNostrServiceConfig {
            logs_dir: "logs".to_string(),
            relays: Vec::new(),
            nip89_identifier: Some("radrootsd".to_string()),
            nip89_extra_tags: Vec::new(),
        }
    }

    #[test]
    fn nip46_defaults_are_expected() {
        let cfg = Nip46Config::default();
        assert_eq!(cfg.session_ttl_secs, 900);
        assert!(cfg.perms.is_empty());
        assert!(cfg.nostrconnect_url.is_none());
    }

    #[test]
    fn rpc_defaults_are_expected() {
        let cfg = RpcConfig::default();
        assert_eq!(cfg.addr, "127.0.0.1:7070");
        assert_eq!(cfg.max_request_body_size, 10 * 1024 * 1024);
        assert_eq!(cfg.max_response_body_size, 10 * 1024 * 1024);
        assert_eq!(cfg.max_connections, 100);
        assert_eq!(cfg.max_subscriptions_per_connection, 1024);
        assert_eq!(cfg.message_buffer_capacity, 1024);
        assert!(cfg.batch_request_limit.is_none());
    }

    #[test]
    fn rpc_addr_prefers_override() {
        let mut cfg = Configuration {
            service: service_config(),
            rpc: RpcConfig {
                addr: "127.0.0.1:1111".to_string(),
                ..RpcConfig::default()
            },
            rpc_addr: None,
            nip46: Nip46Config::default(),
        };
        assert_eq!(cfg.rpc_addr(), "127.0.0.1:1111");
        cfg.rpc_addr = Some("127.0.0.1:2222".to_string());
        assert_eq!(cfg.rpc_addr(), "127.0.0.1:2222");
    }
}
