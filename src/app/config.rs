use anyhow::{Context, Result, bail};
use radroots_nostr::prelude::RadrootsNostrMetadata;
use radroots_runtime::RadrootsNostrServiceConfig;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use super::paths::{
    RadrootsdRuntimePaths, default_transport_publish_database_path, process_path_selection,
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

fn default_rpc_batch_request_limit() -> Option<u32> {
    Some(0)
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

fn default_transport_publish_enabled() -> bool {
    true
}

fn default_transport_publish_connect_timeout_secs() -> u64 {
    10
}

fn default_transport_publish_max_event_bytes() -> usize {
    128 * 1024
}

fn default_transport_publish_max_targets_per_request() -> usize {
    20
}

fn default_transport_publish_job_list_limit() -> usize {
    100
}

fn default_transport_publish_max_concurrent_publish_jobs() -> usize {
    8
}

fn default_nostr_relay_url_policy() -> NostrRelayUrlPolicy {
    NostrRelayUrlPolicy::Public
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
#[serde(deny_unknown_fields)]
struct RawTransportPublishConfig {
    #[serde(default = "default_transport_publish_enabled")]
    pub enabled: bool,
    #[serde(default = "default_transport_publish_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_transport_publish_max_event_bytes")]
    pub max_event_bytes: usize,
    #[serde(default = "default_transport_publish_max_targets_per_request")]
    pub max_targets_per_request: usize,
    #[serde(default = "default_transport_publish_job_list_limit")]
    pub job_list_limit: usize,
    #[serde(default = "default_transport_publish_max_concurrent_publish_jobs")]
    pub max_concurrent_publish_jobs: usize,
    #[serde(default)]
    pub database_path: Option<PathBuf>,
    #[serde(default)]
    pub nostr: TransportPublishNostrConfig,
}

impl Default for RawTransportPublishConfig {
    fn default() -> Self {
        Self {
            enabled: default_transport_publish_enabled(),
            connect_timeout_secs: default_transport_publish_connect_timeout_secs(),
            max_event_bytes: default_transport_publish_max_event_bytes(),
            max_targets_per_request: default_transport_publish_max_targets_per_request(),
            job_list_limit: default_transport_publish_job_list_limit(),
            max_concurrent_publish_jobs: default_transport_publish_max_concurrent_publish_jobs(),
            database_path: None,
            nostr: TransportPublishNostrConfig::default(),
        }
    }
}

impl RawTransportPublishConfig {
    fn into_transport_publish_config(
        self,
        paths: &RadrootsdRuntimePaths,
    ) -> TransportPublishConfig {
        TransportPublishConfig {
            enabled: self.enabled,
            connect_timeout_secs: self.connect_timeout_secs,
            max_event_bytes: self.max_event_bytes,
            max_targets_per_request: self.max_targets_per_request,
            job_list_limit: self.job_list_limit,
            max_concurrent_publish_jobs: self.max_concurrent_publish_jobs,
            database_path: self
                .database_path
                .unwrap_or_else(|| paths.transport_publish_database_path.clone()),
            nostr: self.nostr,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
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
    pub transport_publish: RawTransportPublishConfig,
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
                transport_publish: self
                    .config
                    .transport_publish
                    .into_transport_publish_config(paths),
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NostrRelayUrlPolicy {
    Public,
    Localhost,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TransportPublishNostrConfig {
    #[serde(default = "default_nostr_relay_url_policy")]
    pub relay_url_policy: NostrRelayUrlPolicy,
    #[serde(default)]
    pub author_relay_discovery_relays: Vec<String>,
    #[serde(default)]
    pub daemon_default_relays: Vec<String>,
}

impl Default for TransportPublishNostrConfig {
    fn default() -> Self {
        Self {
            relay_url_policy: default_nostr_relay_url_policy(),
            author_relay_discovery_relays: Vec::new(),
            daemon_default_relays: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TransportPublishConfig {
    #[serde(default = "default_transport_publish_enabled")]
    pub enabled: bool,
    #[serde(default = "default_transport_publish_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_transport_publish_max_event_bytes")]
    pub max_event_bytes: usize,
    #[serde(default = "default_transport_publish_max_targets_per_request")]
    pub max_targets_per_request: usize,
    #[serde(default = "default_transport_publish_job_list_limit")]
    pub job_list_limit: usize,
    #[serde(default = "default_transport_publish_max_concurrent_publish_jobs")]
    pub max_concurrent_publish_jobs: usize,
    #[serde(default = "default_transport_publish_database_path")]
    pub database_path: PathBuf,
    #[serde(default)]
    pub nostr: TransportPublishNostrConfig,
}

impl Default for TransportPublishConfig {
    fn default() -> Self {
        Self {
            enabled: default_transport_publish_enabled(),
            connect_timeout_secs: default_transport_publish_connect_timeout_secs(),
            max_event_bytes: default_transport_publish_max_event_bytes(),
            max_targets_per_request: default_transport_publish_max_targets_per_request(),
            job_list_limit: default_transport_publish_job_list_limit(),
            max_concurrent_publish_jobs: default_transport_publish_max_concurrent_publish_jobs(),
            database_path: default_transport_publish_database_path(),
            nostr: TransportPublishNostrConfig::default(),
        }
    }
}

impl TransportPublishConfig {
    pub fn validate(&self) -> Result<()> {
        if self.max_event_bytes == 0 {
            bail!("transport_publish max_event_bytes must be greater than zero");
        }
        if self.max_targets_per_request == 0 {
            bail!("transport_publish max_targets_per_request must be greater than zero");
        }
        if self.job_list_limit == 0 {
            bail!("transport_publish job_list_limit must be greater than zero");
        }
        if self.max_concurrent_publish_jobs == 0 {
            bail!("transport_publish max_concurrent_publish_jobs must be greater than zero");
        }
        if self.connect_timeout_secs == 0 {
            bail!("transport_publish connect_timeout_secs must be greater than zero");
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
    #[serde(default = "default_rpc_batch_request_limit")]
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
            batch_request_limit: default_rpc_batch_request_limit(),
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
    pub transport_publish: TransportPublishConfig,
}

impl Configuration {
    pub fn rpc_addr(&self) -> &str {
        self.rpc_addr.as_deref().unwrap_or(self.rpc.addr.as_str())
    }

    pub fn validate(&self) -> Result<()> {
        self.transport_publish.validate()?;
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

    use super::{
        Configuration, Nip46Config, NostrRelayUrlPolicy, RpcConfig, TransportPublishConfig,
        load_settings_from_path_with_resolver,
    };
    use crate::app::paths::{
        RadrootsdRuntimeContractOutput, default_runtime_paths_for_process,
        resolve_runtime_paths_with_resolver, runtime_contract_with_selection,
    };
    use radroots_runtime::RadrootsNostrServiceConfig;
    use radroots_runtime_paths::{
        RadrootsHostEnvironment, RadrootsPathProfile, RadrootsPathResolver, RadrootsPlatform,
        RadrootsRuntimePathSelection,
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

    fn runtime_contract_with_resolver(
        resolver: &RadrootsPathResolver,
        profile: RadrootsPathProfile,
        repo_local_root: Option<&std::path::Path>,
    ) -> anyhow::Result<RadrootsdRuntimeContractOutput> {
        runtime_contract_with_selection(
            resolver,
            &RadrootsRuntimePathSelection::caller(profile, repo_local_root.map(PathBuf::from)),
        )
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
    fn rpc_defaults_disable_batches() {
        let cfg = RpcConfig::default();
        assert_eq!(cfg.addr, "127.0.0.1:7070");
        assert_eq!(cfg.batch_request_limit, Some(0));
    }

    #[test]
    fn transport_publish_defaults_are_expected() {
        let paths = default_runtime_paths_for_process().expect("resolve process runtime paths");
        let cfg = TransportPublishConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.connect_timeout_secs, 10);
        assert_eq!(cfg.max_event_bytes, 128 * 1024);
        assert_eq!(cfg.max_targets_per_request, 20);
        assert_eq!(cfg.job_list_limit, 100);
        assert_eq!(cfg.max_concurrent_publish_jobs, 8);
        assert_eq!(cfg.database_path, paths.transport_publish_database_path);
        assert_eq!(cfg.nostr.relay_url_policy, NostrRelayUrlPolicy::Public);
        assert!(cfg.nostr.author_relay_discovery_relays.is_empty());
        assert!(cfg.nostr.daemon_default_relays.is_empty());
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
            transport_publish: TransportPublishConfig::default(),
        };
        assert_eq!(cfg.rpc_addr(), "127.0.0.1:1111");
        cfg.rpc_addr = Some("127.0.0.1:2222".to_string());
        assert_eq!(cfg.rpc_addr(), "127.0.0.1:2222");
    }

    #[test]
    fn transport_publish_validation_rejects_zero_limits() {
        let mut cfg = TransportPublishConfig::default();
        cfg.max_event_bytes = 0;
        assert!(cfg.validate().is_err());
        let mut cfg = TransportPublishConfig::default();
        cfg.max_targets_per_request = 0;
        assert!(cfg.validate().is_err());
        let mut cfg = TransportPublishConfig::default();
        cfg.job_list_limit = 0;
        assert!(cfg.validate().is_err());
        let mut cfg = TransportPublishConfig::default();
        cfg.max_concurrent_publish_jobs = 0;
        assert!(cfg.validate().is_err());
        let mut cfg = TransportPublishConfig::default();
        cfg.connect_timeout_secs = 0;
        assert!(cfg.validate().is_err());
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
            paths.transport_publish_database_path,
            PathBuf::from(
                "/home/treesap/.radroots/data/services/radrootsd/transport_publish.sqlite"
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
            paths.transport_publish_database_path,
            PathBuf::from("/var/lib/radroots/services/radrootsd/transport_publish.sqlite")
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
            paths.transport_publish_database_path,
            repo_local_root.join("data/services/radrootsd/transport_publish.sqlite")
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
            settings.config.transport_publish.database_path,
            PathBuf::from(
                "/home/treesap/.radroots/data/services/radrootsd/transport_publish.sqlite"
            )
        );
    }

    #[test]
    fn obsolete_transport_publish_config_is_rejected() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("radrootsd.toml");
        std::fs::write(
            &config_path,
            r#"
[metadata]
name = "radrootsd-test"

[config]
relays = []

[config.transport_publish]
relay_url_policy = "localhost"
"#,
        )
        .expect("write config");

        let err = load_settings_from_path_with_resolver(
            &config_path,
            &linux_resolver("/home/treesap"),
            RadrootsPathProfile::InteractiveUser,
            None,
        )
        .expect_err("obsolete transport_publish config should fail");
        let err_chain = format!("{err:?}");
        assert!(err_chain.contains("unknown field"));
        assert!(err_chain.contains("relay_url_policy"));
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
            contract.path_overrides.subordinate_path_override_keys,
            vec![
                "config.service.logs_dir".to_owned(),
                "config.transport_publish.database_path".to_owned(),
            ]
        );
        assert_eq!(
            contract.canonical_transport_publish_database_path,
            PathBuf::from(
                "/home/treesap/.radroots/data/services/radrootsd/transport_publish.sqlite"
            )
        );
    }
}
