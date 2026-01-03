#![forbid(unsafe_code)]

pub mod api;
pub mod cli;
pub mod config;
pub mod radrootsd;

use anyhow::Result;

pub use cli::Args as cli_args;
use tracing::info;

use crate::radrootsd::Radrootsd;
use radroots_identity::RadrootsIdentity;
use radroots_nostr::prelude::radroots_nostr_publish_identity_profile;

pub async fn run_radrootsd(settings: &config::Settings, args: &cli_args) -> Result<()> {
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
        let client = radrootsd.client.clone();
        let md = settings.metadata.clone();
        let identity = identity.clone();
        let has_metadata = serde_json::to_value(&md)
            .ok()
            .and_then(|v| v.as_object().cloned())
            .map(|o| !o.is_empty())
            .unwrap_or(false);

        tokio::spawn(async move {
            client.connect().await;
            let profile_published =
                match radroots_nostr_publish_identity_profile(&client, &identity).await {
                    Ok(Some(_)) => true,
                    Ok(None) => false,
                    Err(e) => {
                        tracing::warn!("Failed to publish identity profile: {e}");
                        false
                    }
                };
            if has_metadata && !profile_published {
                if let Err(e) = client.set_metadata(&md).await {
                    tracing::warn!("Failed to publish metadata on startup: {e}");
                } else {
                    tracing::info!("Published metadata on startup");
                }
            }
        });
    }

    let addr: std::net::SocketAddr = settings.config.rpc_addr().parse()?;
    let handle = api::jsonrpc::start_rpc(radrootsd.clone(), addr, &settings.config.rpc).await?;
    info!("JSON-RPC listening on {addr}");

    let stop_handle = handle.clone();

    tokio::select! {
        _ = radroots_runtime::shutdown_signal() => {
            tracing::info!("Shutting down…");
            let _ = stop_handle.stop();
        }
        _ = handle.stopped() => {}
    }

    Ok(())
}
