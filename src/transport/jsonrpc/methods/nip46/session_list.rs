use std::time::{Instant};

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Serialize;

use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

#[derive(Clone, Serialize)]
struct Nip46SessionListEntry {
    session_id: String,
    client_pubkey: String,
    remote_signer_pubkey: String,
    user_pubkey: Option<String>,
    relays: Vec<String>,
    perms: Vec<String>,
    name: Option<String>,
    url: Option<String>,
    image: Option<String>,
    expires_in_secs: Option<u64>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.session.list");
    m.register_async_method("nip46.session.list", |_params, ctx, _| async move {
        let sessions = ctx.state.nip46_sessions.list().await;
        let entries = sessions
            .into_iter()
            .map(|session| Nip46SessionListEntry {
                session_id: session.id,
                client_pubkey: session.client_pubkey.to_hex(),
                remote_signer_pubkey: session.remote_signer_pubkey.to_hex(),
                user_pubkey: session.user_pubkey.map(|pubkey| pubkey.to_hex()),
                relays: session.relays,
                perms: session.perms,
                name: session.name,
                url: session.url,
                image: session.image,
                expires_in_secs: session
                    .expires_at
                    .map(|expires_at| remaining_secs(expires_at)),
            })
            .collect::<Vec<_>>();
        Ok::<Vec<Nip46SessionListEntry>, crate::transport::jsonrpc::RpcError>(entries)
    })?;
    Ok(())
}

fn remaining_secs(expires_at: Instant) -> u64 {
    if expires_at <= Instant::now() {
        0
    } else {
        expires_at.saturating_duration_since(Instant::now()).as_secs()
    }
}
