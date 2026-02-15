use anyhow::{Context, Result};
use radroots_identity::RadrootsIdentity;
use std::time::Duration;
use tracing::info;

use crate::app::{cli, config};
use crate::core::Radrootsd;
use crate::transport::jsonrpc;
use crate::transport::nostr::listener::spawn_nip46_listener;
use radroots_events::profile::RadrootsProfileType;
use radroots_nostr::prelude::{
    RadrootsNostrApplicationHandlerSpec, RadrootsNostrKind,
    radroots_nostr_bootstrap_service_presence,
};

pub async fn run() -> Result<()> {
    let (args, settings): (cli::Args, config::Settings) =
        radroots_runtime::parse_and_load_path_with_init(
            |a: &cli::Args| Some(a.service.config.as_path()),
            |cfg: &config::Settings| cfg.config.service.logs_dir.as_str(),
            None,
        )
        .context("load configuration")?;

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
        let client = radrootsd.client.clone();
        let md = settings.metadata.clone();
        let identity = identity.clone();
        let nip46_config = settings.config.nip46.clone();
        let service_cfg = settings.config.service.clone();

        tokio::spawn(async move {
            let nip46_kind = RadrootsNostrKind::NostrConnect.as_u16() as u32;
            let handler_spec = RadrootsNostrApplicationHandlerSpec {
                kinds: vec![nip46_kind],
                identifier: service_cfg.nip89_identifier.clone(),
                metadata: Some(md.clone()),
                extra_tags: service_cfg.nip89_extra_tags.clone(),
                relays: service_cfg.relays.clone(),
                nostrconnect_url: nip46_config.nostrconnect_url.clone(),
            };
            if let Err(e) = radroots_nostr_bootstrap_service_presence(
                &client,
                &identity,
                Some(RadrootsProfileType::Radrootsd),
                &md,
                &handler_spec,
                Duration::from_secs(5),
            )
            .await
            {
                tracing::warn!("Failed to publish service presence on startup: {e}");
            } else {
                tracing::info!("Published service presence on startup");
            }
        });

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
