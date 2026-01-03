use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::api::jsonrpc::relays::RelayStatusRow;
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use radroots_nostr::prelude::fetch_nip11;

#[derive(Debug, Default, Deserialize)]
struct StatusParams {
    #[serde(default)]
    include_nip11: bool,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("relays.status");
    m.register_async_method("relays.status", |params, ctx, _| async move {
        let StatusParams { include_nip11 } = params
            .parse::<Option<StatusParams>>()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?
            .unwrap_or_default();

        let relays = ctx.state.client.relays().await;
        let mut out = Vec::with_capacity(relays.len());

        for (relay_url, relay) in relays {
            let url_str = relay_url.to_string();
            let status_str = format!("{}", relay.status());
            let parsed = reqwest::Url::parse(&url_str).ok();

            let mut row = RelayStatusRow {
                url: url_str.clone(),
                status: status_str,
                scheme: None,
                host: None,
                onion: None,
                port: None,
                nip11: None,
            };

            if let Some(u) = &parsed {
                row.scheme = Some(u.scheme().to_string());
                if let Some(h) = u.host_str() {
                    row.host = Some(h.to_string());
                    row.onion = Some(h.ends_with(".onion"));
                }
                if let Some(p) = u.port() {
                    row.port = Some(p);
                }
            }

            if include_nip11 {
                if let Some(doc) = fetch_nip11(&row.url).await {
                    row.nip11 = Some(doc);
                }
            }

            out.push(row);
        }

        Ok::<Vec<RelayStatusRow>, RpcError>(out)
    })?;
    Ok(())
}
