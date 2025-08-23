use anyhow::Result;
use jsonrpsee::RpcModule;
use serde_json::{Value as JsonValue, json};

use crate::radrootsd::Radrootsd;
use crate::rpc::RpcError;

use nostr_sdk::RelayStatus;
use radroots_nostr::prelude::connect;

pub fn register(m: &mut RpcModule<Radrootsd>) -> Result<()> {
    m.register_async_method("relays.connect", |_p, ctx, _| async move {
        let relays = ctx.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let mut connected = 0usize;
        let mut connecting = 0usize;
        let mut disconnected = 0usize;

        for (_, r) in &relays {
            match r.status() {
                RelayStatus::Connected => connected += 1,
                RelayStatus::Connecting => connecting += 1,
                _ => disconnected += 1,
            }
        }

        // Idempotent: only spawn if we have anything not connected/connecting
        let need_connect = disconnected > 0;
        if need_connect {
            let client = ctx.client.clone();
            tokio::spawn(async move { connect(&client).await });
        }

        Ok::<JsonValue, RpcError>(json!({
            "connected": connected,
            "connecting": connecting,
            "disconnected": disconnected,
            "spawned_connect": need_connect
        }))
    })?;
    Ok(())
}
