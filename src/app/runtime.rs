use anyhow::{Context, Result};
use radroots_identity::RadrootsIdentity;
use tracing::info;

use crate::app::{cli, config};
use crate::core::Radrootsd;
use crate::transport::jsonrpc;
use crate::transport::nostr::listener::spawn_nip46_listener;
use radroots_events::kinds::KIND_APPLICATION_HANDLER;
use radroots_events::profile::RadrootsProfileType;
use radroots_events_codec::profile::encode::profile_type_tags;
use radroots_nostr::prelude::{
    radroots_nostr_build_event,
    radroots_nostr_build_metadata_event,
    radroots_nostr_publish_identity_profile_with_type,
    RadrootsNostrKind,
    RadrootsNostrTag,
    RadrootsNostrTagKind,
};

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
    let radrootsd = Radrootsd::new(
        keys,
        settings.metadata.clone(),
        settings.config.nip46.clone(),
    );

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
                match radroots_nostr_publish_identity_profile_with_type(
                    &client,
                    &identity,
                    Some(RadrootsProfileType::Radrootsd),
                )
                .await
                {
                    Ok(Some(_)) => true,
                    Ok(None) => false,
                    Err(e) => {
                        tracing::warn!("Failed to publish identity profile: {e}");
                        false
                    }
                };
            if has_metadata && !profile_published {
                let mut tags = Vec::new();
                for mut tag in profile_type_tags(RadrootsProfileType::Radrootsd) {
                    if tag.is_empty() {
                        continue;
                    }
                    let key = tag.remove(0);
                    tags.push(RadrootsNostrTag::custom(
                        RadrootsNostrTagKind::Custom(key.into()),
                        tag,
                    ));
                }
                let builder = radroots_nostr_build_metadata_event(&md).tags(tags);
                if let Err(e) = client.send_event_builder(builder).await {
                    tracing::warn!("Failed to publish metadata on startup: {e}");
                } else {
                    tracing::info!("Published metadata on startup");
                }
            }

            let nip46_kind = RadrootsNostrKind::NostrConnect.as_u16().to_string();
            let nip89_content = if has_metadata {
                serde_json::to_string(&md).unwrap_or_default()
            } else {
                String::new()
            };
            let nip89_tags = vec![
                vec!["d".to_string(), nip46_kind.clone()],
                vec!["k".to_string(), nip46_kind],
            ];
            let nip89_builder =
                radroots_nostr_build_event(KIND_APPLICATION_HANDLER, nip89_content, nip89_tags);
            match nip89_builder {
                Ok(builder) => {
                    if let Err(e) = client.send_event_builder(builder).await {
                        tracing::warn!("Failed to publish NIP-89 announcement: {e}");
                    } else {
                        tracing::info!("Published NIP-89 announcement");
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to build NIP-89 announcement: {e}");
                }
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
