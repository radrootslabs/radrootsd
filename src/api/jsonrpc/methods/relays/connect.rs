use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde_json::{Value as JsonValue, json};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};

use radroots_nostr::prelude::{radroots_nostr_connect, RadrootsNostrRelayStatus};

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("relays.connect");
    m.register_async_method("relays.connect", |_p, ctx, _| async move {
        let relays = ctx.state.client.relays().await;
        if relays.is_empty() {
            return Err(RpcError::NoRelays);
        }

        let mut connected = 0usize;
        let mut connecting = 0usize;
        let mut disconnected = 0usize;

        for (_, r) in &relays {
            match r.status() {
                RadrootsNostrRelayStatus::Connected => connected += 1,
                RadrootsNostrRelayStatus::Connecting => connecting += 1,
                _ => disconnected += 1,
            }
        }

        let need_connect = disconnected > 0;
        if need_connect {
            let client = ctx.state.client.clone();
            tokio::spawn(async move { radroots_nostr_connect(&client).await });
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
