use anyhow::Result;
use jsonrpsee::server::ServerHandle;
use radroots_identity::RadrootsIdentity;
use std::time::Duration;
use tracing::{info, warn};

use crate::app::identity_storage::load_service_identity;
use crate::app::{cli, config, paths};
use crate::core::Radrootsd;
use crate::core::transport_publish::{
    PublishPrincipalInit, TransportPublishStore, generate_bearer_token, hash_bearer_token,
    parse_explicit_transport_kind, parse_nostr_source_policy, parse_target_policy,
    write_token_file,
};
use crate::transport::jsonrpc;
#[cfg(not(test))]
use crate::transport::nostr::listener::spawn_nip46_listener;
#[cfg(not(test))]
use anyhow::Context;
#[cfg(not(test))]
use clap::Parser;
use radroots_event::profile::RadrootsProfileType;
use radroots_nostr::prelude::{
    RadrootsNostrApplicationHandlerSpec, RadrootsNostrKind,
    radroots_nostr_bootstrap_service_presence,
};
use std::path::PathBuf;

#[cfg(test)]
type RunLoadHookValue = Result<(cli::Args, config::Settings), String>;

#[cfg(test)]
type RunLoadHook = std::sync::Mutex<Option<RunLoadHookValue>>;

#[cfg(test)]
static RUN_LOAD_HOOK: std::sync::OnceLock<RunLoadHook> = std::sync::OnceLock::new();

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
    config_path_source: String,
    canonical_config_path: PathBuf,
    logs_dir: PathBuf,
    logs_dir_source: String,
    canonical_logs_dir: PathBuf,
    identity_path: PathBuf,
    identity_path_source: String,
    canonical_identity_path: PathBuf,
    transport_publish_database_path: PathBuf,
    transport_publish_database_path_source: String,
    canonical_transport_publish_database_path: PathBuf,
    path_overrides: paths::RadrootsdRuntimePathOverrideContractOutput,
    default_shared_secret_backend: String,
    allowed_shared_secret_backends: Vec<String>,
}

#[cfg(test)]
fn run_load_hook() -> &'static RunLoadHook {
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
        Err(anyhow::anyhow!("run loader hook not set"))
    }

    #[cfg(not(test))]
    {
        let args = cli::Args::try_parse().map_err(radroots_runtime::RuntimeCliError::from)?;
        let config_path = args
            .service
            .config
            .clone()
            .map(Ok)
            .unwrap_or_else(paths::default_config_path_for_process)?;
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
    contract: &paths::RadrootsdRuntimeContractOutput,
) -> RadrootsdRuntimeStartupReport {
    RadrootsdRuntimeStartupReport {
        active_profile: contract.active_profile.clone(),
        config_path: args
            .service
            .config
            .clone()
            .unwrap_or_else(|| contract.canonical_config_path.clone()),
        config_path_source: cli_or_profile_path_source(
            args.service.config.is_some(),
            &args
                .service
                .config
                .clone()
                .unwrap_or_else(|| contract.canonical_config_path.clone()),
            &contract.canonical_config_path,
        ),
        canonical_config_path: contract.canonical_config_path.clone(),
        logs_dir: PathBuf::from(settings.config.service.logs_dir.as_str()),
        logs_dir_source: config_or_profile_path_source(
            &PathBuf::from(settings.config.service.logs_dir.as_str()),
            &contract.canonical_logs_dir,
        ),
        canonical_logs_dir: contract.canonical_logs_dir.clone(),
        identity_path: args
            .service
            .identity
            .clone()
            .unwrap_or_else(|| contract.canonical_identity_path.clone()),
        identity_path_source: cli_or_profile_path_source(
            args.service.identity.is_some(),
            &args
                .service
                .identity
                .clone()
                .unwrap_or_else(|| contract.canonical_identity_path.clone()),
            &contract.canonical_identity_path,
        ),
        canonical_identity_path: contract.canonical_identity_path.clone(),
        transport_publish_database_path: settings.config.transport_publish.database_path.clone(),
        transport_publish_database_path_source: config_or_profile_path_source(
            &settings.config.transport_publish.database_path,
            &contract.canonical_transport_publish_database_path,
        ),
        canonical_transport_publish_database_path: contract
            .canonical_transport_publish_database_path
            .clone(),
        path_overrides: contract.path_overrides.clone(),
        default_shared_secret_backend: contract.default_shared_secret_backend.clone(),
        allowed_shared_secret_backends: contract.allowed_shared_secret_backends.clone(),
    }
}

fn cli_or_profile_path_source(
    is_cli_arg: bool,
    actual_path: &PathBuf,
    canonical_path: &PathBuf,
) -> String {
    if is_cli_arg {
        "cli_arg".to_owned()
    } else {
        config_or_profile_path_source(actual_path, canonical_path)
    }
}

fn config_or_profile_path_source(actual_path: &PathBuf, canonical_path: &PathBuf) -> String {
    if actual_path == canonical_path {
        "profile_default".to_owned()
    } else {
        "config_artifact".to_owned()
    }
}

#[cfg(not(test))]
fn log_runtime_startup_report(report: &RadrootsdRuntimeStartupReport) {
    info!(
        active_profile = report.active_profile.as_str(),
        profile_source = report.path_overrides.profile_source.as_str(),
        root_source = report.path_overrides.root_source.as_str(),
        repo_local_root = ?report.path_overrides.repo_local_root,
        repo_local_root_source = ?report.path_overrides.repo_local_root_source,
        subordinate_path_override_source = report.path_overrides.subordinate_path_override_source.as_str(),
        config_path = %report.config_path.display(),
        config_path_source = report.config_path_source.as_str(),
        canonical_config_path = %report.canonical_config_path.display(),
        logs_dir = %report.logs_dir.display(),
        logs_dir_source = report.logs_dir_source.as_str(),
        canonical_logs_dir = %report.canonical_logs_dir.display(),
        identity_path = %report.identity_path.display(),
        identity_path_source = report.identity_path_source.as_str(),
        canonical_identity_path = %report.canonical_identity_path.display(),
        transport_publish_database_path = %report.transport_publish_database_path.display(),
        transport_publish_database_path_source = report.transport_publish_database_path_source.as_str(),
        canonical_transport_publish_database_path = %report.canonical_transport_publish_database_path.display(),
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
    nip46_config: config::Nip46Config,
) -> Result<()> {
    let kinds = service_presence_kinds();
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
    nip46_config: config::Nip46Config,
) {
    #[cfg(test)]
    {
        let result =
            publish_service_presence(client, identity, metadata, service_cfg, nip46_config).await;
        if let Err(err) = result {
            warn!("Failed to publish service presence on startup: {err}");
        } else {
            info!("Published service presence on startup");
        }
    }

    #[cfg(not(test))]
    tokio::spawn(async move {
        let result =
            publish_service_presence(client, identity, metadata, service_cfg, nip46_config).await;
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

async fn handle_command(command: cli::Command, settings: &config::Settings) -> Result<()> {
    match command {
        cli::Command::TransportPublish(command) => match command.command {
            cli::TransportPublishSubcommand::Principal(command) => match command.command {
                cli::PrincipalSubcommand::Init(args) => {
                    let token = generate_bearer_token();
                    let token_hash = hash_bearer_token(token.as_str());
                    let store = TransportPublishStore::open(
                        settings.config.transport_publish.database_path.clone(),
                    )?;
                    let allowed_target_policies = args
                        .allowed_target_policy
                        .iter()
                        .map(|policy| parse_target_policy(policy.as_str()))
                        .collect::<Result<Vec<_>, _>>()?;
                    let allowed_explicit_transport_kinds = args
                        .allowed_explicit_transport_kind
                        .iter()
                        .map(|transport_kind| {
                            parse_explicit_transport_kind(transport_kind.as_str())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let allowed_nostr_source_policies = args
                        .allowed_nostr_source_policy
                        .iter()
                        .map(|policy| parse_nostr_source_policy(policy.as_str()))
                        .collect::<Result<Vec<_>, _>>()?;
                    let principal = store.create_principal(PublishPrincipalInit {
                        label: args.label,
                        token_hash,
                        allowed_pubkeys: args.allowed_pubkey,
                        allowed_kinds: args.allowed_kind,
                        allowed_target_policies,
                        allowed_explicit_transport_kinds,
                        allowed_nostr_source_policies,
                        allow_request_targets: args.allow_request_targets,
                        job_visibility: args.job_visibility.parse()?,
                        expires_at_unix: None,
                    })?;
                    write_token_file(&args.token_file, token.as_str())?;
                    println!(
                        "{}",
                        serde_json::json!({
                            "principal_id": principal.principal_id,
                            "label": principal.label,
                            "token_file": args.token_file,
                            "database_path": settings.config.transport_publish.database_path,
                        })
                    );
                    Ok(())
                }
            },
        },
    }
}

pub async fn run() -> Result<()> {
    let (args, settings): (cli::Args, config::Settings) = load_args_and_settings()?;
    settings.config.validate()?;

    #[cfg(not(test))]
    {
        let contract = paths::runtime_contract_for_process().context("resolve runtime contract")?;
        let report = runtime_startup_report(&args, &settings, &contract);
        log_runtime_startup_report(&report);
    }

    if let Some(command) = args.command.clone() {
        return handle_command(command, &settings).await;
    }

    info!("Starting radrootsd");

    let identity = load_service_identity(
        args.service.identity.as_deref(),
        args.service.allow_generate_identity,
    )?;
    let radrootsd = Radrootsd::new(
        identity.clone(),
        settings.metadata.clone(),
        settings.config.transport_publish.clone(),
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

fn service_presence_kinds() -> Vec<u32> {
    let mut kinds = vec![RadrootsNostrKind::NostrConnect.as_u16() as u32];
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
    use crate::app::{cli, config, paths};
    use crate::core::Radrootsd;
    use crate::transport::jsonrpc;
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::RadrootsNostrMetadata;
    use std::path::Path;
    use std::path::PathBuf;
    use tokio::sync::{Mutex, MutexGuard};

    static TEST_LOCK: Mutex<()> = Mutex::const_new(());

    async fn test_guard() -> MutexGuard<'static, ()> {
        let guard = TEST_LOCK.lock().await;
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
            command: None,
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
                nip46: config::Nip46Config::default(),
                transport_publish: config::TransportPublishConfig::default(),
            },
        }
    }

    fn sample_runtime_contract() -> paths::RadrootsdRuntimeContractOutput {
        paths::RadrootsdRuntimeContractOutput {
            active_profile: "interactive_user".to_string(),
            allowed_profiles: vec![
                "interactive_user".to_string(),
                "service_host".to_string(),
                "repo_local".to_string(),
            ],
            path_overrides: paths::RadrootsdRuntimePathOverrideContractOutput {
                profile_source: "caller".to_string(),
                root_source: "host_defaults".to_string(),
                repo_local_root: None,
                repo_local_root_source: None,
                subordinate_path_override_source: "config_artifact".to_string(),
                subordinate_path_override_keys: vec![
                    "config.service.logs_dir".to_string(),
                    "config.transport_publish.database_path".to_string(),
                ],
            },
            default_shared_secret_backend: "encrypted_file".to_string(),
            allowed_shared_secret_backends: vec!["encrypted_file".to_string()],
            canonical_config_path: PathBuf::from(
                "/home/treesap/.radroots/config/services/radrootsd/config.toml",
            ),
            canonical_logs_dir: PathBuf::from("/home/treesap/.radroots/logs/services/radrootsd"),
            canonical_identity_path: PathBuf::from(
                "/home/treesap/.radroots/secrets/services/radrootsd/identity.secret.json",
            ),
            canonical_transport_publish_database_path: PathBuf::from(
                "/home/treesap/.radroots/data/services/radrootsd/transport_publish.sqlite",
            ),
        }
    }

    async fn make_handle(settings: &config::Settings) -> jsonrpsee::server::ServerHandle {
        let identity = RadrootsIdentity::generate();
        let state = Radrootsd::new(
            identity,
            settings.metadata.clone(),
            settings.config.transport_publish.clone(),
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
        let _guard = test_guard().await;
        let err = run().await.expect_err("missing loader hook should error");
        let msg = format!("{err:#}");
        assert!(msg.contains("run loader hook not set"));
    }

    #[tokio::test]
    async fn run_returns_error_when_identity_missing() {
        let _guard = test_guard().await;
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
    async fn run_covers_shutdown_path_and_presence_success() {
        let _guard = test_guard().await;
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
        let _guard = test_guard().await;
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
        let _guard = test_guard().await;
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
        let _guard = test_guard().await;
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
        let _guard = test_guard().await;
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
        let _guard = test_guard().await;
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
        let _guard = test_guard().await;
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
        let _guard = test_guard().await;
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
    fn service_presence_kinds_include_nostr_connect_only() {
        let kinds = super::service_presence_kinds();

        assert!(
            kinds.contains(
                &(radroots_nostr::prelude::RadrootsNostrKind::NostrConnect.as_u16() as u32)
            )
        );
        assert_eq!(kinds.len(), 1);
    }

    #[test]
    fn runtime_startup_report_prefers_explicit_cli_paths() {
        let args = cli::Args {
            service: radroots_runtime::RadrootsServiceCliArgs {
                config: Some(PathBuf::from("/tmp/radrootsd/config.toml")),
                identity: Some(PathBuf::from("/tmp/radrootsd/identity.secret.json")),
                allow_generate_identity: false,
            },
            command: None,
        };
        let mut settings = settings_with_relays(Vec::new());
        settings.config.service.logs_dir = "/tmp/radrootsd/logs".to_string();
        settings.config.transport_publish.database_path =
            PathBuf::from("/tmp/radrootsd/transport_publish.sqlite");

        let contract = sample_runtime_contract();
        let report = runtime_startup_report(&args, &settings, &contract);

        assert_eq!(
            report,
            RadrootsdRuntimeStartupReport {
                active_profile: "interactive_user".to_string(),
                config_path: PathBuf::from("/tmp/radrootsd/config.toml"),
                config_path_source: "cli_arg".to_string(),
                canonical_config_path: PathBuf::from(
                    "/home/treesap/.radroots/config/services/radrootsd/config.toml"
                ),
                logs_dir: PathBuf::from("/tmp/radrootsd/logs"),
                logs_dir_source: "config_artifact".to_string(),
                canonical_logs_dir: PathBuf::from(
                    "/home/treesap/.radroots/logs/services/radrootsd"
                ),
                identity_path: PathBuf::from("/tmp/radrootsd/identity.secret.json"),
                identity_path_source: "cli_arg".to_string(),
                canonical_identity_path: PathBuf::from(
                    "/home/treesap/.radroots/secrets/services/radrootsd/identity.secret.json"
                ),
                transport_publish_database_path: PathBuf::from(
                    "/tmp/radrootsd/transport_publish.sqlite"
                ),
                transport_publish_database_path_source: "config_artifact".to_string(),
                canonical_transport_publish_database_path: PathBuf::from(
                    "/home/treesap/.radroots/data/services/radrootsd/transport_publish.sqlite"
                ),
                path_overrides: sample_runtime_contract().path_overrides,
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
            command: None,
        };
        let contract = sample_runtime_contract();
        let mut settings = settings_with_relays(Vec::new());
        settings.config.service.logs_dir = contract.canonical_logs_dir.display().to_string();
        settings.config.transport_publish.database_path =
            contract.canonical_transport_publish_database_path.clone();

        let report = runtime_startup_report(&args, &settings, &contract);

        assert_eq!(report.config_path, contract.canonical_config_path);
        assert_eq!(report.config_path_source, "profile_default");
        assert_eq!(report.logs_dir, contract.canonical_logs_dir);
        assert_eq!(report.logs_dir_source, "profile_default");
        assert_eq!(report.identity_path, contract.canonical_identity_path);
        assert_eq!(report.identity_path_source, "profile_default");
        assert_eq!(
            report.transport_publish_database_path,
            contract.canonical_transport_publish_database_path
        );
        assert_eq!(
            report.transport_publish_database_path_source,
            "profile_default"
        );
        assert_eq!(report.path_overrides, contract.path_overrides);
        assert_eq!(report.default_shared_secret_backend, "encrypted_file");
        assert_eq!(
            report.allowed_shared_secret_backends,
            vec!["encrypted_file".to_string()]
        );
    }
}
