use anyhow::{Context, Result};
use radroots_identity::RadrootsIdentity;
use tracing::info;

use crate::app::{cli, config};
use crate::core::Radrootsd;
use crate::transport::jsonrpc;
use crate::transport::nostr::listener::spawn_nip46_listener;

pub async fn run() -> Result<()> {
    let (args, settings): (cli::Args, config::Settings) =
        radroots_runtime::parse_and_load_path_with_init(
            |a: &cli::Args| Some(a.config.as_path()),
            |cfg: &config::Settings| cfg.config.logs_dir.as_str(),
            None,
        )
        .context("load configuration")?;

    info!("Starting radrootsd");

    let identity = RadrootsIdentity::load_or_generate(
        args.identity.as_ref(),
        args.allow_generate_identity,
    )?;
    let keys = identity.keys().clone();
    let radrootsd = Radrootsd::new(keys, settings.metadata.clone());

    for relay in settings.config.relays.iter() {
        radrootsd.client.add_relay(relay).await?;
    }

    if !settings.config.relays.is_empty() {
        spawn_nip46_listener(radrootsd.clone());
    }

    let addr: std::net::SocketAddr = settings.config.rpc_addr().parse()?;
    let handle = jsonrpc::start_rpc(radrootsd.clone(), addr, &settings.config.rpc).await?;
    info!("JSON-RPC listening on {addr}");

    let stop_handle = handle.clone();

    tokio::select! {
        _ = radroots_runtime::shutdown_signal() => {
            info!("Shutting down…");
            let _ = stop_handle.stop();
        }
        _ = handle.stopped() => {}
    }

    Ok(())
}
