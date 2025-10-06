pub mod cli;
pub mod config;
pub mod identity;
pub mod radrootsd;
pub mod rpc;

use anyhow::Result;

pub use cli::Args as cli_args;
use tracing::info;

use crate::{identity::Identity, radrootsd::Radrootsd};

pub async fn run_radrootsd(settings: &config::Settings, args: &cli_args) -> Result<()> {
    let identity = radroots_identity::load_or_generate::<Identity, _>(
        args.identity.as_ref(),
        args.allow_generate_identity,
    )?;
    let keys = radroots_identity::to_keys(&identity.value)?;

    let radrootsd = Radrootsd::new(keys, settings.metadata.clone());

    for relay in settings.config.relays.iter() {
        radrootsd.client.add_relay(relay).await?;
    }

    if !settings.config.relays.is_empty() {
        let client = radrootsd.client.clone();
        let md = settings.metadata.clone();
        let has_metadata = serde_json::to_value(&md)
            .ok()
            .and_then(|v| v.as_object().cloned())
            .map(|o| !o.is_empty())
            .unwrap_or(false);

        tokio::spawn(async move {
            client.connect().await;
            if has_metadata {
                if let Err(e) = client.set_metadata(&md).await {
                    tracing::warn!("Failed to publish metadata on startup: {e}");
                } else {
                    tracing::info!("Published metadata on startup");
                }
            }
        });
    }

    let addr: std::net::SocketAddr = settings.config.rpc_addr.parse()?;
    let handle = rpc::start_rpc(radrootsd.clone(), addr).await?;
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
