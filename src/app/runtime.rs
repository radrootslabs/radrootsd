use anyhow::Result;
use jsonrpsee::server::ServerHandle;
use radroots_identity::RadrootsIdentity;
use std::time::Duration;
use tracing::{info, warn};

use crate::app::identity_storage::load_service_identity;
use crate::app::{cli, config};
use crate::core::Radrootsd;
use crate::transport::jsonrpc;
#[cfg(not(test))]
use crate::transport::nostr::listener::spawn_nip46_listener;
#[cfg(not(test))]
use anyhow::Context;
#[cfg(not(test))]
use clap::Parser;
use radroots_events::kinds::KIND_LISTING;
use radroots_events::profile::RadrootsProfileType;
use radroots_nostr::prelude::{
    RadrootsNostrApplicationHandlerSpec, RadrootsNostrKind,
    radroots_nostr_bootstrap_service_presence,
};
use std::path::PathBuf;

#[cfg(test)]
static RUN_LOAD_HOOK: std::sync::OnceLock<
    std::sync::Mutex<Option<Result<(cli::Args, config::Settings), String>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
static RUN_BOOTSTRAP_HOOK: std::sync::OnceLock<std::sync::Mutex<Option<Result<(), String>>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static RUN_WAIT_HOOK: std::sync::OnceLock<std::sync::Mutex<Option<RunWaitOutcome>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static RUN_START_RPC_HOOK: std::sync::OnceLock<
    std::sync::Mutex<Option<Result<ServerHandle, String>>>,
> = std::sync::OnceLock::new();

#[derive(Clone, Copy)]
enum RunWaitOutcome {
    Shutdown,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RadrootsdRuntimeStartupReport {
    active_profile: String,
    config_path: PathBuf,
    canonical_config_path: PathBuf,
    logs_dir: PathBuf,
    canonical_logs_dir: PathBuf,
    identity_path: PathBuf,
    canonical_identity_path: PathBuf,
    bridge_state_path: PathBuf,
    canonical_bridge_state_path: PathBuf,
    default_shared_secret_backend: String,
    allowed_shared_secret_backends: Vec<String>,
}

#[cfg(test)]
fn run_load_hook()
-> &'static std::sync::Mutex<Option<Result<(cli::Args, config::Settings), String>>> {
    RUN_LOAD_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn run_bootstrap_hook() -> &'static std::sync::Mutex<Option<Result<(), String>>> {
    RUN_BOOTSTRAP_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn run_wait_hook() -> &'static std::sync::Mutex<Option<RunWaitOutcome>> {
    RUN_WAIT_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn run_start_rpc_hook() -> &'static std::sync::Mutex<Option<Result<ServerHandle, String>>> {
    RUN_START_RPC_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn take_load_hook_result() -> Option<Result<(cli::Args, config::Settings), String>> {
    run_load_hook()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

#[cfg(test)]
fn take_bootstrap_hook_result() -> Option<Result<(), String>> {
    run_bootstrap_hook()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

#[cfg(not(test))]
fn take_bootstrap_hook_result() -> Option<Result<(), String>> {
    None
}

#[cfg(test)]
fn take_wait_hook_result() -> Option<RunWaitOutcome> {
    run_wait_hook()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

#[cfg(test)]
fn take_start_rpc_hook_result() -> Option<Result<ServerHandle, String>> {
    run_start_rpc_hook()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

fn load_args_and_settings() -> Result<(cli::Args, config::Settings)> {
    #[cfg(test)]
    {
        if let Some(result) = take_load_hook_result() {
            return result.map_err(anyhow::Error::msg);
        }
        return Err(anyhow::anyhow!("run loader hook not set"));
    }

    #[cfg(not(test))]
    {
        let args = cli::Args::try_parse().map_err(radroots_runtime::RuntimeCliError::from)?;
        let config_path = args
            .service
            .config
            .clone()
            .map(Ok)
            .unwrap_or_else(config::default_config_path_for_process)?;
        let settings =
            config::load_settings_from_path(&config_path).context("load configuration")?;
        radroots_runtime::init_with_logs_dir(
            std::path::Path::new(settings.config.service.logs_dir.as_str()),
            None,
        )?;
        Ok((args, settings))
    }
}

fn runtime_startup_report(
    args: &cli::Args,
    settings: &config::Settings,
    contract: &config::RadrootsdRuntimeContractOutput,
) -> RadrootsdRuntimeStartupReport {
    RadrootsdRuntimeStartupReport {
        active_profile: contract.active_profile.clone(),
        config_path: args
            .service
            .config
            .clone()
            .unwrap_or_else(|| contract.canonical_config_path.clone()),
        canonical_config_path: contract.canonical_config_path.clone(),
        logs_dir: PathBuf::from(settings.config.service.logs_dir.as_str()),
        canonical_logs_dir: contract.canonical_logs_dir.clone(),
        identity_path: args
            .service
            .identity
            .clone()
            .unwrap_or_else(|| contract.canonical_identity_path.clone()),
        canonical_identity_path: contract.canonical_identity_path.clone(),
        bridge_state_path: settings.config.bridge.state_path.clone(),
        canonical_bridge_state_path: contract.canonical_bridge_state_path.clone(),
        default_shared_secret_backend: contract.default_shared_secret_backend.clone(),
        allowed_shared_secret_backends: contract.allowed_shared_secret_backends.clone(),
    }
}

#[cfg(not(test))]
fn log_runtime_startup_report(report: &RadrootsdRuntimeStartupReport) {
    info!(
        active_profile = report.active_profile.as_str(),
        config_path = %report.config_path.display(),
        canonical_config_path = %report.canonical_config_path.display(),
        logs_dir = %report.logs_dir.display(),
        canonical_logs_dir = %report.canonical_logs_dir.display(),
        identity_path = %report.identity_path.display(),
        canonical_identity_path = %report.canonical_identity_path.display(),
        bridge_state_path = %report.bridge_state_path.display(),
        canonical_bridge_state_path = %report.canonical_bridge_state_path.display(),
        default_shared_secret_backend = report.default_shared_secret_backend.as_str(),
        allowed_shared_secret_backends = ?report.allowed_shared_secret_backends,
        "radrootsd runtime contract"
    );
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn bootstrap_presence(
    client: &radroots_nostr::prelude::RadrootsNostrClient,
    identity: &RadrootsIdentity,
    metadata: &radroots_nostr::prelude::RadrootsNostrMetadata,
    handler_spec: &RadrootsNostrApplicationHandlerSpec,
) -> Result<()> {
    let bootstrap_result: Result<()> = match take_bootstrap_hook_result() {
        Some(result) => result.map_err(anyhow::Error::msg),
        None => radroots_nostr_bootstrap_service_presence(
            client,
            identity,
            Some(RadrootsProfileType::Radrootsd),
            metadata,
            handler_spec,
            Duration::from_secs(5),
        )
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from),
    };
    bootstrap_result?;
    Ok(())
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn publish_service_presence(
    client: radroots_nostr::prelude::RadrootsNostrClient,
    identity: RadrootsIdentity,
    metadata: radroots_nostr::prelude::RadrootsNostrMetadata,
    service_cfg: radroots_runtime::RadrootsNostrServiceConfig,
    bridge_config: config::BridgeConfig,
    nip46_config: config::Nip46Config,
) -> Result<()> {
    let kinds = service_presence_kinds(&bridge_config);
    let handler_spec = RadrootsNostrApplicationHandlerSpec {
        kinds,
        identifier: service_cfg.nip89_identifier.clone(),
        metadata: Some(metadata.clone()),
        extra_tags: service_cfg.nip89_extra_tags.clone(),
        relays: service_cfg.relays.clone(),
        nostrconnect_url: nip46_config.nostrconnect_url.clone(),
    };
    bootstrap_presence(&client, &identity, &metadata, &handler_spec).await
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn maybe_publish_service_presence(
    client: radroots_nostr::prelude::RadrootsNostrClient,
    identity: RadrootsIdentity,
    metadata: radroots_nostr::prelude::RadrootsNostrMetadata,
    service_cfg: radroots_runtime::RadrootsNostrServiceConfig,
    bridge_config: config::BridgeConfig,
    nip46_config: config::Nip46Config,
) {
    #[cfg(test)]
    {
        let result = publish_service_presence(
            client,
            identity,
            metadata,
            service_cfg,
            bridge_config,
            nip46_config,
        )
        .await;
        if let Err(err) = result {
            warn!("Failed to publish service presence on startup: {err}");
        } else {
            info!("Published service presence on startup");
        }
        return;
    }

    #[cfg(not(test))]
    tokio::spawn(async move {
        let result = publish_service_presence(
            client,
            identity,
            metadata,
            service_cfg,
            bridge_config,
            nip46_config,
        )
        .await;
        if let Err(err) = result {
            warn!("Failed to publish service presence on startup: {err}");
        } else {
            info!("Published service presence on startup");
        }
    });
}

#[cfg(not(test))]
#[cfg_attr(coverage_nightly, coverage(off))]
fn spawn_nip46_listener_io(radrootsd: Radrootsd) {
    spawn_nip46_listener(radrootsd);
}

#[cfg(test)]
fn spawn_nip46_listener_io(_radrootsd: Radrootsd) {}

#[cfg(test)]
async fn start_rpc_io(
    state: Radrootsd,
    addr: std::net::SocketAddr,
    rpc_cfg: &config::RpcConfig,
) -> Result<ServerHandle> {
    if let Some(result) = take_start_rpc_hook_result() {
        return result.map_err(anyhow::Error::msg);
    }
    jsonrpc::start_rpc(state, addr, rpc_cfg).await
}

#[cfg(not(test))]
#[cfg_attr(coverage_nightly, coverage(off))]
async fn start_rpc_io(
    state: Radrootsd,
    addr: std::net::SocketAddr,
    rpc_cfg: &config::RpcConfig,
) -> Result<ServerHandle> {
    jsonrpc::start_rpc(state, addr, rpc_cfg).await
}

#[cfg(test)]
async fn wait_for_shutdown_or_stopped(handle: ServerHandle) -> RunWaitOutcome {
    if let Some(outcome) = take_wait_hook_result() {
        return outcome;
    }
    handle.stopped().await;
    RunWaitOutcome::Stopped
}

#[cfg(not(test))]
#[cfg_attr(coverage_nightly, coverage(off))]
async fn wait_for_shutdown_or_stopped(handle: ServerHandle) -> RunWaitOutcome {
    tokio::select! {
        _ = radroots_runtime::shutdown_signal() => RunWaitOutcome::Shutdown,
        _ = handle.stopped() => RunWaitOutcome::Stopped,
    }
}

pub async fn run() -> Result<()> {
    let (args, settings): (cli::Args, config::Settings) = load_args_and_settings()?;
    settings.config.validate()?;

    #[cfg(not(test))]
    {
        let contract =
            config::runtime_contract_for_process().context("resolve runtime contract")?;
        let report = runtime_startup_report(&args, &settings, &contract);
        log_runtime_startup_report(&report);
    }

    info!("Starting radrootsd");

    let identity = load_service_identity(
        args.service.identity.as_deref(),
        args.service.allow_generate_identity,
    )?;
    let radrootsd = Radrootsd::new(
        identity.clone(),
        settings.metadata.clone(),
        settings.config.bridge.clone(),
        settings.config.nip46.clone(),
    );
    let radrootsd = radrootsd?;

    for relay in settings.config.service.relays.iter() {
        radrootsd.client.add_relay(relay).await?;
    }

    if !settings.config.service.relays.is_empty() {
        maybe_publish_service_presence(
            radrootsd.client.clone(),
            identity.clone(),
            settings.metadata.clone(),
            settings.config.service.clone(),
            settings.config.bridge.clone(),
            settings.config.nip46.clone(),
        )
        .await;

        spawn_nip46_listener_io(radrootsd.clone());
    }

    let addr: std::net::SocketAddr = settings.config.rpc_addr().parse()?;
    let handle = start_rpc_io(radrootsd.clone(), addr, &settings.config.rpc).await?;
    info!("JSON-RPC listening on {addr}");

    let stop_handle = handle.clone();

    match wait_for_shutdown_or_stopped(handle).await {
        RunWaitOutcome::Shutdown => {
            info!("Shutting down…");
            let _ = stop_handle.stop();
        }
        RunWaitOutcome::Stopped => {}
    }

    Ok(())
}

fn service_presence_kinds(bridge_config: &config::BridgeConfig) -> Vec<u32> {
    let mut kinds = vec![RadrootsNostrKind::NostrConnect.as_u16() as u32];
    if bridge_config.enabled {
        kinds.push(KIND_LISTING);
    }
    kinds.sort_unstable();
    kinds.dedup();
    kinds
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::{
        RadrootsdRuntimeStartupReport, RunWaitOutcome, run, run_bootstrap_hook, run_load_hook,
        run_start_rpc_hook, run_wait_hook, runtime_startup_report,
    };
    use crate::app::{cli, config};
    use crate::core::Radrootsd;
    use crate::transport::jsonrpc;
    use radroots_events::kinds::KIND_LISTING;
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::RadrootsNostrMetadata;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::{Mutex, MutexGuard};

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn test_guard() -> MutexGuard<'static, ()> {
        let guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *run_load_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        *run_bootstrap_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        *run_wait_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        *run_start_rpc_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        guard
    }

    fn unique_identity_path(suffix: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("radrootsd-{suffix}-{nanos}.secret.json"))
    }

    fn cleanup_identity_artifacts(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(crate::app::identity_storage::encrypted_identity_key_path(
            path,
        ));
    }

    fn args_for_identity(path: PathBuf, allow_generate: bool) -> cli::Args {
        cli::Args {
            service: radroots_runtime::RadrootsServiceCliArgs {
                config: Some(PathBuf::from("config.toml")),
                identity: Some(path),
                allow_generate_identity: allow_generate,
            },
        }
    }

    fn settings_with_relays(relays: Vec<String>) -> config::Settings {
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        config::Settings {
            metadata,
            config: config::Configuration {
                service: radroots_runtime::RadrootsNostrServiceConfig {
                    logs_dir: "logs".to_string(),
                    relays,
                    nip89_identifier: Some("radrootsd".to_string()),
                    nip89_extra_tags: Vec::new(),
                },
                rpc: config::RpcConfig {
                    addr: "127.0.0.1:0".to_string(),
                    ..config::RpcConfig::default()
                },
                rpc_addr: Some("127.0.0.1:0".to_string()),
                bridge: config::BridgeConfig::default(),
                nip46: config::Nip46Config::default(),
            },
        }
    }

    fn sample_runtime_contract() -> config::RadrootsdRuntimeContractOutput {
        config::RadrootsdRuntimeContractOutput {
            active_profile: "interactive_user".to_string(),
            allowed_profiles: vec![
                "interactive_user".to_string(),
                "service_host".to_string(),
                "repo_local".to_string(),
            ],
            default_shared_secret_backend: "encrypted_file".to_string(),
            allowed_shared_secret_backends: vec!["encrypted_file".to_string()],
            canonical_config_path: PathBuf::from(
                "/home/treesap/.radroots/config/services/radrootsd/config.toml",
            ),
            canonical_logs_dir: PathBuf::from("/home/treesap/.radroots/logs/services/radrootsd"),
            canonical_identity_path: PathBuf::from(
                "/home/treesap/.radroots/secrets/services/radrootsd/identity.secret.json",
            ),
            canonical_bridge_state_path: PathBuf::from(
                "/home/treesap/.radroots/data/services/radrootsd/bridge/bridge-jobs.json",
            ),
        }
    }

    async fn make_handle(settings: &config::Settings) -> jsonrpsee::server::ServerHandle {
        let identity = RadrootsIdentity::generate();
        let state = Radrootsd::new(
            identity,
            settings.metadata.clone(),
            settings.config.bridge.clone(),
            settings.config.nip46.clone(),
        )
        .expect("state");
        jsonrpc::start_rpc(
            state,
            "127.0.0.1:0".parse().expect("addr"),
            &settings.config.rpc,
        )
        .await
        .expect("rpc handle")
    }

    #[tokio::test]
    async fn run_returns_error_when_hook_is_missing() {
        let _guard = test_guard();
        let err = run().await.expect_err("missing loader hook should error");
        let msg = format!("{err:#}");
        assert!(msg.contains("run loader hook not set"));
    }

    #[tokio::test]
    async fn run_returns_error_when_identity_missing() {
        let _guard = test_guard();
        let args = args_for_identity(PathBuf::from("/tmp/radrootsd-missing.secret.json"), false);
        let settings = settings_with_relays(Vec::new());
        *run_load_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok((args, settings)));
        let err = run().await.expect_err("missing identity should error");
        let msg = format!("{err:#}");
        assert!(msg.contains("identity"));
    }

    #[tokio::test]
    async fn run_returns_error_when_bridge_is_enabled_without_bearer_token() {
        let _guard = test_guard();
        let path = unique_identity_path("bridge-auth");
        let args = args_for_identity(path, true);
        let mut settings = settings_with_relays(Vec::new());
        settings.config.bridge.enabled = true;
        settings.config.bridge.bearer_token = None;
        *run_load_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok((args, settings)));
        let err = run().await.expect_err("invalid bridge config should error");
        assert!(err.to_string().contains("bearer_token"));
    }

    #[tokio::test]
    async fn run_covers_shutdown_path_and_presence_success() {
        let _guard = test_guard();
        let path = unique_identity_path("shutdown");
        let args = args_for_identity(path.clone(), true);
        let settings = settings_with_relays(vec!["wss://relay.example.com".to_string()]);
        let handle = make_handle(&settings).await;
        *run_load_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(Ok((args, settings.clone())));
        *run_start_rpc_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok(handle));
        *run_wait_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(RunWaitOutcome::Shutdown);
        *run_bootstrap_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok(()));
        assert!(run().await.is_ok());
        cleanup_identity_artifacts(&path);
    }

    #[tokio::test]
    async fn run_covers_stopped_path_and_presence_failure() {
        let _guard = test_guard();
        let path = unique_identity_path("stopped");
        let args = args_for_identity(path.clone(), true);
        let settings = settings_with_relays(vec!["wss://relay.example.com".to_string()]);
        let handle = make_handle(&settings).await;
        let _ = handle.stop();
        *run_load_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(Ok((args, settings.clone())));
        *run_start_rpc_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok(handle));
        *run_wait_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(RunWaitOutcome::Stopped);
        *run_bootstrap_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Err("boom".to_string()));
        assert!(run().await.is_ok());
        cleanup_identity_artifacts(&path);
    }

    #[tokio::test]
    async fn run_skips_presence_when_relays_empty() {
        let _guard = test_guard();
        let path = unique_identity_path("empty");
        let args = args_for_identity(path.clone(), true);
        let settings = settings_with_relays(Vec::new());
        let handle = make_handle(&settings).await;
        *run_load_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(Ok((args, settings.clone())));
        *run_start_rpc_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok(handle));
        *run_wait_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(RunWaitOutcome::Shutdown);
        assert!(run().await.is_ok());
        cleanup_identity_artifacts(&path);
    }

    #[tokio::test]
    async fn run_returns_error_when_relay_is_invalid() {
        let _guard = test_guard();
        let path = unique_identity_path("invalid-relay");
        let args = args_for_identity(path.clone(), true);
        let settings = settings_with_relays(vec!["not-a-relay".to_string()]);
        *run_load_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok((args, settings)));
        let err = run().await.expect_err("invalid relay should error");
        let msg = format!("{err:#}");
        assert!(!msg.is_empty());
        cleanup_identity_artifacts(&path);
    }

    #[tokio::test]
    async fn run_returns_error_when_rpc_addr_is_invalid() {
        let _guard = test_guard();
        let path = unique_identity_path("invalid-rpc-addr");
        let args = args_for_identity(path.clone(), true);
        let mut settings = settings_with_relays(Vec::new());
        settings.config.rpc_addr = Some("not-an-addr".to_string());
        *run_load_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok((args, settings)));
        let err = run().await.expect_err("invalid rpc addr should error");
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid"));
        cleanup_identity_artifacts(&path);
    }

    #[tokio::test]
    async fn run_returns_error_when_rpc_start_fails() {
        let _guard = test_guard();
        let path = unique_identity_path("rpc-start-fail");
        let args = args_for_identity(path.clone(), true);
        let settings = settings_with_relays(Vec::new());
        *run_load_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok((args, settings)));
        *run_start_rpc_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(Err("rpc start failed".to_string()));
        let err = run().await.expect_err("rpc start hook should fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("rpc start failed"));
        cleanup_identity_artifacts(&path);
    }

    #[tokio::test]
    async fn run_waits_for_stopped_when_wait_hook_is_not_set() {
        let _guard = test_guard();
        let path = unique_identity_path("wait-no-hook");
        let args = args_for_identity(path.clone(), true);
        let settings = settings_with_relays(Vec::new());
        let handle = make_handle(&settings).await;
        let _ = handle.stop();
        *run_load_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok((args, settings)));
        *run_start_rpc_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok(handle));
        assert!(run().await.is_ok());
        cleanup_identity_artifacts(&path);
    }

    #[tokio::test]
    async fn run_starts_rpc_when_start_hook_is_not_set() {
        let _guard = test_guard();
        let path = unique_identity_path("start-rpc-real");
        let args = args_for_identity(path.clone(), true);
        let settings = settings_with_relays(Vec::new());
        *run_load_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok((args, settings)));
        *run_wait_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(RunWaitOutcome::Shutdown);
        assert!(run().await.is_ok());
        cleanup_identity_artifacts(&path);
    }

    #[test]
    fn service_presence_kinds_include_listing_when_bridge_is_enabled() {
        let mut bridge = config::BridgeConfig::default();
        bridge.enabled = true;

        let kinds = super::service_presence_kinds(&bridge);

        assert!(
            kinds.contains(
                &(radroots_nostr::prelude::RadrootsNostrKind::NostrConnect.as_u16() as u32)
            )
        );
        assert!(kinds.contains(&KIND_LISTING));
    }

    #[test]
    fn runtime_startup_report_prefers_explicit_cli_paths() {
        let args = cli::Args {
            service: radroots_runtime::RadrootsServiceCliArgs {
                config: Some(PathBuf::from("/tmp/radrootsd/config.toml")),
                identity: Some(PathBuf::from("/tmp/radrootsd/identity.secret.json")),
                allow_generate_identity: false,
            },
        };
        let mut settings = settings_with_relays(Vec::new());
        settings.config.service.logs_dir = "/tmp/radrootsd/logs".to_string();
        settings.config.bridge.state_path = PathBuf::from("/tmp/radrootsd/bridge-jobs.json");

        let report = runtime_startup_report(&args, &settings, &sample_runtime_contract());

        assert_eq!(
            report,
            RadrootsdRuntimeStartupReport {
                active_profile: "interactive_user".to_string(),
                config_path: PathBuf::from("/tmp/radrootsd/config.toml"),
                canonical_config_path: PathBuf::from(
                    "/home/treesap/.radroots/config/services/radrootsd/config.toml"
                ),
                logs_dir: PathBuf::from("/tmp/radrootsd/logs"),
                canonical_logs_dir: PathBuf::from(
                    "/home/treesap/.radroots/logs/services/radrootsd"
                ),
                identity_path: PathBuf::from("/tmp/radrootsd/identity.secret.json"),
                canonical_identity_path: PathBuf::from(
                    "/home/treesap/.radroots/secrets/services/radrootsd/identity.secret.json"
                ),
                bridge_state_path: PathBuf::from("/tmp/radrootsd/bridge-jobs.json"),
                canonical_bridge_state_path: PathBuf::from(
                    "/home/treesap/.radroots/data/services/radrootsd/bridge/bridge-jobs.json"
                ),
                default_shared_secret_backend: "encrypted_file".to_string(),
                allowed_shared_secret_backends: vec!["encrypted_file".to_string()],
            }
        );
    }

    #[test]
    fn runtime_startup_report_falls_back_to_canonical_contract_paths() {
        let args = cli::Args {
            service: radroots_runtime::RadrootsServiceCliArgs {
                config: None,
                identity: None,
                allow_generate_identity: false,
            },
        };
        let contract = sample_runtime_contract();
        let mut settings = settings_with_relays(Vec::new());
        settings.config.service.logs_dir = contract.canonical_logs_dir.display().to_string();
        settings.config.bridge.state_path = contract.canonical_bridge_state_path.clone();

        let report = runtime_startup_report(&args, &settings, &contract);

        assert_eq!(report.config_path, contract.canonical_config_path);
        assert_eq!(report.logs_dir, contract.canonical_logs_dir);
        assert_eq!(report.identity_path, contract.canonical_identity_path);
        assert_eq!(
            report.bridge_state_path,
            contract.canonical_bridge_state_path
        );
        assert_eq!(report.default_shared_secret_backend, "encrypted_file");
        assert_eq!(
            report.allowed_shared_secret_backends,
            vec!["encrypted_file".to_string()]
        );
    }
}
