use anyhow::{Result, bail};
use radroots_nostr::prelude::RadrootsNostrMetadata;
use radroots_runtime::RadrootsNostrServiceConfig;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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

fn default_nip46_public_jsonrpc_enabled() -> bool {
    false
}

fn default_bridge_enabled() -> bool {
    false
}

fn default_bridge_connect_timeout_secs() -> u64 {
    10
}

fn default_bridge_delivery_policy() -> BridgeDeliveryPolicy {
    BridgeDeliveryPolicy::Any
}

fn default_bridge_publish_max_attempts() -> usize {
    1
}

fn default_bridge_publish_initial_backoff_millis() -> u64 {
    250
}

fn default_bridge_publish_max_backoff_millis() -> u64 {
    2_000
}

fn default_bridge_job_status_retention() -> usize {
    256
}

fn default_bridge_state_path() -> PathBuf {
    PathBuf::from("state/bridge-jobs.json")
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Nip46Config {
    #[serde(default = "default_nip46_session_ttl_secs")]
    pub session_ttl_secs: u64,
    #[serde(default = "default_nip46_perms")]
    pub perms: Vec<String>,
    #[serde(default = "default_nip46_public_jsonrpc_enabled")]
    pub public_jsonrpc_enabled: bool,
    #[serde(default)]
    pub nostrconnect_url: Option<String>,
}

impl Default for Nip46Config {
    fn default() -> Self {
        Self {
            session_ttl_secs: default_nip46_session_ttl_secs(),
            perms: default_nip46_perms(),
            public_jsonrpc_enabled: default_nip46_public_jsonrpc_enabled(),
            nostrconnect_url: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BridgeDeliveryPolicy {
    Any,
    Quorum,
    All,
}

impl BridgeDeliveryPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::Quorum => "quorum",
            Self::All => "all",
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BridgeConfig {
    #[serde(default = "default_bridge_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub bearer_token: Option<String>,
    #[serde(default = "default_bridge_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_bridge_delivery_policy")]
    pub delivery_policy: BridgeDeliveryPolicy,
    #[serde(default)]
    pub delivery_quorum: Option<usize>,
    #[serde(default = "default_bridge_publish_max_attempts")]
    pub publish_max_attempts: usize,
    #[serde(default = "default_bridge_publish_initial_backoff_millis")]
    pub publish_initial_backoff_millis: u64,
    #[serde(default = "default_bridge_publish_max_backoff_millis")]
    pub publish_max_backoff_millis: u64,
    #[serde(default = "default_bridge_job_status_retention")]
    pub job_status_retention: usize,
    #[serde(default = "default_bridge_state_path")]
    pub state_path: PathBuf,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            enabled: default_bridge_enabled(),
            bearer_token: None,
            connect_timeout_secs: default_bridge_connect_timeout_secs(),
            delivery_policy: default_bridge_delivery_policy(),
            delivery_quorum: None,
            publish_max_attempts: default_bridge_publish_max_attempts(),
            publish_initial_backoff_millis: default_bridge_publish_initial_backoff_millis(),
            publish_max_backoff_millis: default_bridge_publish_max_backoff_millis(),
            job_status_retention: default_bridge_job_status_retention(),
            state_path: default_bridge_state_path(),
        }
    }
}

impl BridgeConfig {
    pub fn bearer_token(&self) -> Option<&str> {
        self.bearer_token
            .as_deref()
            .map(str::trim)
            .filter(|token| !token.is_empty())
    }

    pub fn validate(&self) -> Result<()> {
        if self.enabled && self.bearer_token().is_none() {
            bail!("bridge bearer_token is required when bridge ingress is enabled");
        }
        Ok(())
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
    #[serde(default)]
    pub bridge: BridgeConfig,
}

impl Configuration {
    pub fn rpc_addr(&self) -> &str {
        self.rpc_addr.as_deref().unwrap_or(self.rpc.addr.as_str())
    }

    pub fn validate(&self) -> Result<()> {
        self.bridge.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub metadata: RadrootsNostrMetadata,
    pub config: Configuration,
}

#[cfg(test)]
mod tests {
    use super::{BridgeConfig, BridgeDeliveryPolicy, Configuration, Nip46Config, RpcConfig};
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
        assert!(!cfg.public_jsonrpc_enabled);
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
    fn bridge_defaults_are_expected() {
        let cfg = BridgeConfig::default();
        assert!(!cfg.enabled);
        assert!(cfg.bearer_token.is_none());
        assert_eq!(cfg.connect_timeout_secs, 10);
        assert_eq!(cfg.delivery_policy, BridgeDeliveryPolicy::Any);
        assert_eq!(cfg.delivery_quorum, None);
        assert_eq!(cfg.publish_max_attempts, 1);
        assert_eq!(cfg.publish_initial_backoff_millis, 250);
        assert_eq!(cfg.publish_max_backoff_millis, 2_000);
        assert_eq!(cfg.job_status_retention, 256);
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
            bridge: BridgeConfig::default(),
        };
        assert_eq!(cfg.rpc_addr(), "127.0.0.1:1111");
        cfg.rpc_addr = Some("127.0.0.1:2222".to_string());
        assert_eq!(cfg.rpc_addr(), "127.0.0.1:2222");
    }

    #[test]
    fn bridge_validation_requires_bearer_token_when_enabled() {
        let err = BridgeConfig {
            enabled: true,
            ..BridgeConfig::default()
        }
        .validate()
        .expect_err("missing token should fail");
        assert!(err.to_string().contains("bearer_token"));
    }

    #[test]
    fn bridge_validation_accepts_enabled_bridge_with_bearer_token() {
        BridgeConfig {
            enabled: true,
            bearer_token: Some("secret".to_string()),
            ..BridgeConfig::default()
        }
        .validate()
        .expect("valid bridge config");
    }
}
