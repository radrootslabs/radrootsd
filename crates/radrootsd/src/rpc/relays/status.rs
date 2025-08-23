use anyhow::Result;
use jsonrpsee::RpcModule;
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use crate::radrootsd::Radrootsd;
use crate::rpc::RpcError;
use radroots_nostr::prelude::fetch_nip11;

#[derive(Debug, Deserialize)]
struct StatusParams {
    #[serde(default)]
    include_nip11: bool,
}

pub fn register(m: &mut RpcModule<Radrootsd>) -> Result<()> {
    m.register_async_method("relays.status", |params, ctx, _| async move {
        let StatusParams { include_nip11 } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let relays = ctx.client.relays().await;
        let mut out = Vec::with_capacity(relays.len());

        for (relay_url, relay) in relays {
            let url_str = relay_url.to_string();
            let status_str = format!("{}", relay.status());
            let parsed = reqwest::Url::parse(&url_str).ok();

            // Build with locals; only insert present fields.
            let mut row = JsonMap::new();
            row.insert("url".into(), json!(url_str));
            row.insert("status".into(), json!(status_str));

            if let Some(u) = &parsed {
                row.insert("scheme".into(), json!(u.scheme()));
                if let Some(h) = u.host_str() {
                    row.insert("host".into(), json!(h));
                    row.insert("onion".into(), json!(h.ends_with(".onion")));
                }
                if let Some(p) = u.port() {
                    row.insert("port".into(), json!(p));
                }
            }

            if include_nip11 {
                if let Some(doc) = fetch_nip11(row["url"].as_str().unwrap()).await {
                    row.insert("nip11".into(), json!(doc));
                }
            }

            out.push(JsonValue::Object(row));
        }

        Ok::<JsonValue, RpcError>(json!(out))
    })?;
    Ok(())
}
