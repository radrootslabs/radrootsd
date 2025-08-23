use anyhow::Result;
use jsonrpsee::RpcModule;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::radrootsd::Radrootsd;
use crate::rpc::RpcError;

use radroots_nostr::prelude::{add_relay, connect, fetch_nip11, remove_relay};

#[derive(Debug, Deserialize)]
struct AddParams {
    url: String,
}

#[derive(Debug, Deserialize)]
struct RemoveParams {
    url: String,
}

#[derive(Debug, Deserialize)]
struct StatusParams {
    #[serde(default)]
    include_nip11: bool,
}

pub fn module(radrootsd: Radrootsd) -> Result<RpcModule<Radrootsd>> {
    let mut m = RpcModule::new(radrootsd);

    m.register_async_method("relays.add", |params, ctx, _| async move {
        let AddParams { url } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        add_relay(&ctx.client, &url)
            .await
            .map_err(|e| RpcError::AddRelay(url.clone(), e.to_string()))?;

        Ok::<JsonValue, RpcError>(json!({ "added": url }))
    })?;

    m.register_async_method("relays.remove", |params, ctx, _| async move {
        let RemoveParams { url } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        remove_relay(&ctx.client, &url)
            .await
            .map_err(|e| RpcError::Other(format!("failed to remove relay {url}: {e}")))?;

        Ok::<JsonValue, RpcError>(json!({ "removed": url }))
    })?;

    m.register_async_method("relays.list", |_p, ctx, _| async move {
        let relays = ctx.client.relays().await;
        Ok::<JsonValue, RpcError>(json!(
            relays.keys().map(|u| u.to_string()).collect::<Vec<_>>()
        ))
    })?;

    m.register_async_method("relays.status", |params, ctx, _| async move {
        let StatusParams { include_nip11 } = params.parse().unwrap_or(StatusParams {
            include_nip11: false,
        });

        let relays = ctx.client.relays().await;

        let mut out = Vec::with_capacity(relays.len());

        for (relay_url, relay) in relays {
            let url_str = relay_url.to_string();
            let parsed = reqwest::Url::parse(&url_str).ok();

            let scheme = parsed.as_ref().map(|u| u.scheme().to_string());
            let host = parsed
                .as_ref()
                .and_then(|u| u.host_str())
                .map(|s| s.to_string());
            let port = parsed.as_ref().and_then(|u| u.port());
            let onion = host
                .as_deref()
                .map(|h| h.ends_with(".onion"))
                .unwrap_or(false);

            let mut row = json!({
                "url": url_str,
                "status": format!("{}", relay.status()),
                "scheme": scheme,
                "host": host,
                "port": port,
                "onion": onion
            });

            if include_nip11 {
                if let Some(doc) = fetch_nip11(row["url"].as_str().unwrap()).await {
                    row["nip11"] = json!(doc);
                }
            }

            out.push(row);
        }

        Ok::<JsonValue, RpcError>(json!(out))
    })?;

    m.register_async_method("relays.connect", |_p, ctx, _| async move {
        let relays = ctx.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }
        let client = ctx.client.clone();
        tokio::spawn(async move { connect(&client).await });

        Ok::<JsonValue, RpcError>(json!({ "connecting": relays.len() }))
    })?;

    Ok(m)
}
