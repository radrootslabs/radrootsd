use anyhow::Result;
use jsonrpsee::server::ServerHandle;
use radroots_identity::RadrootsIdentity;
use std::time::Duration;
use tracing::{info, warn};

use crate::app::{cli, config};
use crate::core::Radrootsd;
use crate::transport::jsonrpc;
#[cfg(not(test))]
use crate::transport::nostr::listener::spawn_nip46_listener;
use radroots_events::profile::RadrootsProfileType;
use radroots_nostr::prelude::{
    RadrootsNostrApplicationHandlerSpec, RadrootsNostrKind,
    radroots_nostr_bootstrap_service_presence,
};
#[cfg(not(test))]
use anyhow::Context;

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

#[cfg(test)]
fn run_load_hook(
) -> &'static std::sync::Mutex<Option<Result<(cli::Args, config::Settings), String>>> {
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
    radroots_runtime::parse_and_load_path_with_init(
        |a: &cli::Args| Some(a.service.config.as_path()),
        |cfg: &config::Settings| cfg.config.service.logs_dir.as_str(),
        None,
    )
    .context("load configuration")
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
    let nip46_kind = RadrootsNostrKind::NostrConnect.as_u16() as u32;
    let handler_spec = RadrootsNostrApplicationHandlerSpec {
        kinds: vec![nip46_kind],
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
        let result = publish_service_presence(
            client,
            identity,
            metadata,
            service_cfg,
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

    info!("Starting radrootsd");

    let identity = RadrootsIdentity::load_or_generate(
        args.service.identity.as_ref(),
        args.service.allow_generate_identity,
    )?;
    let keys = identity.keys().clone();
    let radrootsd = Radrootsd::new(
        keys,
        settings.metadata.clone(),
        settings.config.nip46.clone(),
    );

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

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::{
        RunWaitOutcome, run, run_bootstrap_hook, run_load_hook, run_start_rpc_hook, run_wait_hook,
    };
    use crate::app::{cli, config};
    use crate::core::Radrootsd;
    use crate::transport::jsonrpc;
    use radroots_nostr::prelude::{RadrootsNostrKeys, RadrootsNostrMetadata};
    use std::path::PathBuf;
    use std::sync::{Mutex, MutexGuard};

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn test_guard() -> MutexGuard<'static, ()> {
        let guard = TEST_LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
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
        std::env::temp_dir().join(format!("radrootsd-{suffix}-{nanos}.json"))
    }

    fn args_for_identity(path: PathBuf, allow_generate: bool) -> cli::Args {
        cli::Args {
            service: radroots_runtime::RadrootsServiceCliArgs {
                config: PathBuf::from("config.toml"),
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
                nip46: config::Nip46Config::default(),
            },
        }
    }

    async fn make_handle(settings: &config::Settings) -> jsonrpsee::server::ServerHandle {
        let keys = RadrootsNostrKeys::generate();
        let state = Radrootsd::new(
            keys,
            settings.metadata.clone(),
            settings.config.nip46.clone(),
        );
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
        let args = args_for_identity(PathBuf::from("/tmp/radrootsd-missing.json"), false);
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
        let _guard = test_guard();
        let path = unique_identity_path("shutdown");
        let args = args_for_identity(path.clone(), true);
        let settings = settings_with_relays(vec!["wss://relay.example.com".to_string()]);
        let handle = make_handle(&settings).await;
        *run_load_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok((args, settings.clone())));
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
        let _ = std::fs::remove_file(path);
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
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok((args, settings.clone())));
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
        let _ = std::fs::remove_file(path);
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
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok((args, settings.clone())));
        *run_start_rpc_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok(handle));
        *run_wait_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(RunWaitOutcome::Shutdown);
        assert!(run().await.is_ok());
        let _ = std::fs::remove_file(path);
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
        let _ = std::fs::remove_file(path);
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
        let _ = std::fs::remove_file(path);
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
        let _ = std::fs::remove_file(path);
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
        let _ = std::fs::remove_file(path);
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
        let _ = std::fs::remove_file(path);
    }
}
