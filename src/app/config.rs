use anyhow::{Context, Result, bail};
use radroots_nostr::prelude::RadrootsNostrMetadata;
use radroots_runtime::RadrootsNostrServiceConfig;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use super::paths::{
    RadrootsdRuntimePaths, default_bridge_state_path, process_path_selection,
    resolve_runtime_paths_with_resolver,
};

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

#[derive(Debug, Deserialize, Clone, Default)]
struct RawServiceConfig {
    #[serde(default)]
    pub logs_dir: Option<String>,
    #[serde(default)]
    pub relays: Vec<String>,
    #[serde(default)]
    pub nip89_identifier: Option<String>,
    #[serde(default)]
    pub nip89_extra_tags: Vec<Vec<String>>,
}

impl RawServiceConfig {
    fn into_service_config(self, paths: &RadrootsdRuntimePaths) -> RadrootsNostrServiceConfig {
        RadrootsNostrServiceConfig {
            logs_dir: self
                .logs_dir
                .unwrap_or_else(|| paths.logs_dir.display().to_string()),
            relays: self.relays,
            nip89_identifier: self.nip89_identifier,
            nip89_extra_tags: self.nip89_extra_tags,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
struct RawBridgeConfig {
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
    #[serde(default)]
    pub state_path: Option<PathBuf>,
}

impl Default for RawBridgeConfig {
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
            state_path: None,
        }
    }
}

impl RawBridgeConfig {
    fn into_bridge_config(self, paths: &RadrootsdRuntimePaths) -> BridgeConfig {
        BridgeConfig {
            enabled: self.enabled,
            bearer_token: self.bearer_token,
            connect_timeout_secs: self.connect_timeout_secs,
            delivery_policy: self.delivery_policy,
            delivery_quorum: self.delivery_quorum,
            publish_max_attempts: self.publish_max_attempts,
            publish_initial_backoff_millis: self.publish_initial_backoff_millis,
            publish_max_backoff_millis: self.publish_max_backoff_millis,
            job_status_retention: self.job_status_retention,
            state_path: self
                .state_path
                .unwrap_or_else(|| paths.bridge_state_path.clone()),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
struct RawConfiguration {
    #[serde(flatten)]
    pub service: RawServiceConfig,
    #[serde(default)]
    pub rpc: RpcConfig,
    #[serde(default)]
    pub rpc_addr: Option<String>,
    #[serde(default)]
    pub nip46: Nip46Config,
    #[serde(default)]
    pub bridge: RawBridgeConfig,
}

#[derive(Debug, Deserialize, Clone)]
struct RawSettings {
    pub metadata: RadrootsNostrMetadata,
    pub config: RawConfiguration,
}

impl RawSettings {
    fn into_settings(self, paths: &RadrootsdRuntimePaths) -> Settings {
        Settings {
            metadata: self.metadata,
            config: Configuration {
                service: self.config.service.into_service_config(paths),
                rpc: self.config.rpc,
                rpc_addr: self.config.rpc_addr,
                nip46: self.config.nip46,
                bridge: self.config.bridge.into_bridge_config(paths),
            },
        }
    }
}

fn load_settings_from_path_with_resolver(
    path: &Path,
    resolver: &radroots_runtime_paths::RadrootsPathResolver,
    profile: radroots_runtime_paths::RadrootsPathProfile,
    repo_local_root: Option<&Path>,
) -> Result<Settings> {
    let raw: RawSettings = radroots_runtime::load_required_file(path)
        .with_context(|| format!("load configuration from {}", path.display()))?;
    let paths = resolve_runtime_paths_with_resolver(resolver, profile, repo_local_root)?;
    let settings = raw.into_settings(&paths);
    settings.validate()?;
    Ok(settings)
}

pub fn load_settings_from_path(path: impl AsRef<Path>) -> Result<Settings> {
    let path = path.as_ref();
    let (profile, repo_local_root) = process_path_selection()?;
    load_settings_from_path_with_resolver(
        path,
        &radroots_runtime_paths::RadrootsPathResolver::current(),
        profile,
        repo_local_root.as_deref(),
    )
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

impl Settings {
    pub fn validate(&self) -> Result<()> {
        self.config.validate()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::app::paths::{
        default_runtime_paths_for_process, resolve_runtime_paths_with_resolver,
        runtime_contract_with_resolver,
    };
    use super::{
        BridgeConfig, BridgeDeliveryPolicy, Configuration, Nip46Config, RpcConfig,
        load_settings_from_path_with_resolver,
    };
    use radroots_runtime::RadrootsNostrServiceConfig;
    use radroots_runtime_paths::{
        RadrootsHostEnvironment, RadrootsPathProfile, RadrootsPathResolver, RadrootsPlatform,
    };

    fn linux_resolver(home: &str) -> RadrootsPathResolver {
        RadrootsPathResolver::new(
            RadrootsPlatform::Linux,
            RadrootsHostEnvironment {
                home_dir: Some(PathBuf::from(home)),
                ..RadrootsHostEnvironment::default()
            },
        )
    }

    fn service_config() -> RadrootsNostrServiceConfig {
        let paths = resolve_runtime_paths_with_resolver(
            &linux_resolver("/home/treesap"),
            RadrootsPathProfile::InteractiveUser,
            None,
        )
        .expect("resolve interactive-user paths");
        RadrootsNostrServiceConfig {
            logs_dir: paths.logs_dir.display().to_string(),
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
        let paths = default_runtime_paths_for_process().expect("resolve process runtime paths");
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
        assert_eq!(cfg.state_path, paths.bridge_state_path);
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

    #[test]
    fn runtime_paths_follow_interactive_user_contract() {
        let paths = resolve_runtime_paths_with_resolver(
            &linux_resolver("/home/treesap"),
            RadrootsPathProfile::InteractiveUser,
            None,
        )
        .expect("resolve interactive-user paths");

        assert_eq!(
            paths.config_path,
            PathBuf::from("/home/treesap/.radroots/config/services/radrootsd/config.toml")
        );
        assert_eq!(
            paths.logs_dir,
            PathBuf::from("/home/treesap/.radroots/logs/services/radrootsd")
        );
        assert_eq!(
            paths.identity_path,
            PathBuf::from(
                "/home/treesap/.radroots/secrets/services/radrootsd/identity.secret.json"
            )
        );
        assert_eq!(
            paths.bridge_state_path,
            PathBuf::from(
                "/home/treesap/.radroots/data/services/radrootsd/bridge/bridge-jobs.json"
            )
        );
    }

    #[test]
    fn runtime_paths_follow_service_host_contract() {
        let paths = resolve_runtime_paths_with_resolver(
            &linux_resolver("/home/treesap"),
            RadrootsPathProfile::ServiceHost,
            None,
        )
        .expect("resolve service-host paths");

        assert_eq!(
            paths.config_path,
            PathBuf::from("/etc/radroots/services/radrootsd/config.toml")
        );
        assert_eq!(
            paths.logs_dir,
            PathBuf::from("/var/log/radroots/services/radrootsd")
        );
        assert_eq!(
            paths.identity_path,
            PathBuf::from("/etc/radroots/secrets/services/radrootsd/identity.secret.json")
        );
        assert_eq!(
            paths.bridge_state_path,
            PathBuf::from("/var/lib/radroots/services/radrootsd/bridge/bridge-jobs.json")
        );
    }

    #[test]
    fn runtime_paths_follow_repo_local_contract() {
        let repo_local_root = PathBuf::from("/repo/.local/radroots/dev/radrootsd");
        let paths = resolve_runtime_paths_with_resolver(
            &linux_resolver("/home/treesap"),
            RadrootsPathProfile::RepoLocal,
            Some(repo_local_root.as_path()),
        )
        .expect("resolve repo-local paths");

        assert_eq!(
            paths.config_path,
            repo_local_root.join("config/services/radrootsd/config.toml")
        );
        assert_eq!(
            paths.logs_dir,
            repo_local_root.join("logs/services/radrootsd")
        );
        assert_eq!(
            paths.identity_path,
            repo_local_root.join("secrets/services/radrootsd/identity.secret.json")
        );
        assert_eq!(
            paths.bridge_state_path,
            repo_local_root.join("data/services/radrootsd/bridge/bridge-jobs.json")
        );
    }

    #[test]
    fn load_settings_materializes_profile_defaults_when_paths_are_omitted() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("radrootsd.toml");
        std::fs::write(
            &config_path,
            r#"
[metadata]
name = "radrootsd-test"

[config]
relays = ["ws://127.0.0.1:8080"]

[config.rpc]
addr = "127.0.0.1:7070"

[config.bridge]
enabled = true
bearer_token = "change-me"
"#,
        )
        .expect("write config");

        let settings = load_settings_from_path_with_resolver(
            &config_path,
            &linux_resolver("/home/treesap"),
            RadrootsPathProfile::InteractiveUser,
            None,
        )
        .expect("load settings");

        assert_eq!(
            settings.config.service.logs_dir,
            "/home/treesap/.radroots/logs/services/radrootsd"
        );
        assert_eq!(
            settings.config.bridge.state_path,
            PathBuf::from(
                "/home/treesap/.radroots/data/services/radrootsd/bridge/bridge-jobs.json"
            )
        );
    }

    #[test]
    fn runtime_contract_output_matches_interactive_user_contract() {
        let contract = runtime_contract_with_resolver(
            &linux_resolver("/home/treesap"),
            RadrootsPathProfile::InteractiveUser,
            None,
        )
        .expect("interactive-user contract");

        assert_eq!(contract.active_profile, "interactive_user");
        assert_eq!(
            contract.allowed_profiles,
            vec![
                "interactive_user".to_owned(),
                "service_host".to_owned(),
                "repo_local".to_owned(),
            ]
        );
        assert_eq!(contract.default_shared_secret_backend, "encrypted_file");
        assert_eq!(
            contract.allowed_shared_secret_backends,
            vec!["encrypted_file".to_owned()]
        );
        assert_eq!(
            contract.canonical_config_path,
            PathBuf::from("/home/treesap/.radroots/config/services/radrootsd/config.toml")
        );
        assert_eq!(
            contract.canonical_logs_dir,
            PathBuf::from("/home/treesap/.radroots/logs/services/radrootsd")
        );
        assert_eq!(
            contract.canonical_identity_path,
            PathBuf::from(
                "/home/treesap/.radroots/secrets/services/radrootsd/identity.secret.json"
            )
        );
        assert_eq!(
            contract.canonical_bridge_state_path,
            PathBuf::from(
                "/home/treesap/.radroots/data/services/radrootsd/bridge/bridge-jobs.json"
            )
        );
    }

    #[test]
    fn runtime_contract_output_matches_service_host_contract() {
        let contract = runtime_contract_with_resolver(
            &linux_resolver("/home/treesap"),
            RadrootsPathProfile::ServiceHost,
            None,
        )
        .expect("service-host contract");

        assert_eq!(contract.active_profile, "service_host");
        assert_eq!(
            contract.canonical_config_path,
            PathBuf::from("/etc/radroots/services/radrootsd/config.toml")
        );
        assert_eq!(
            contract.canonical_logs_dir,
            PathBuf::from("/var/log/radroots/services/radrootsd")
        );
        assert_eq!(
            contract.canonical_identity_path,
            PathBuf::from("/etc/radroots/secrets/services/radrootsd/identity.secret.json")
        );
        assert_eq!(
            contract.canonical_bridge_state_path,
            PathBuf::from("/var/lib/radroots/services/radrootsd/bridge/bridge-jobs.json")
        );
    }
}
