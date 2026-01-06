use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use crate::nip46::session::Nip46Session;
use crate::nip46::client;
use radroots_nostr::prelude::RadrootsNostrPublicKey;
use nostr::nips::nip46::{NostrConnectMethod, NostrConnectRequest, ResponseResult};

#[derive(Debug, Deserialize)]
struct Nip46GetPublicKeyParams {
    session_id: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46GetPublicKeyResponse {
    pubkey: String,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.get_public_key");
    m.register_async_method("nip46.get_public_key", |params, ctx, _| async move {
        let Nip46GetPublicKeyParams { session_id } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let session = ctx
            .state
            .nip46_sessions
            .get(&session_id)
            .await
            .ok_or_else(|| RpcError::InvalidParams("unknown session".to_string()))?;
        let pubkey = request_public_key(&session).await?;
        let updated = ctx
            .state
            .nip46_sessions
            .set_user_pubkey(&session_id, pubkey.clone())
            .await;
        if !updated {
            return Err(RpcError::Other("nip46 session update failed".to_string()));
        }
        Ok::<Nip46GetPublicKeyResponse, RpcError>(Nip46GetPublicKeyResponse {
            pubkey: pubkey.to_hex(),
        })
    })?;
    Ok(())
}

async fn request_public_key(
    session: &Nip46Session,
) -> Result<RadrootsNostrPublicKey, RpcError> {
    let request = NostrConnectRequest::GetPublicKey;
    let response = client::request(session, request, "get_public_key").await?;
    let response = response
        .to_response(NostrConnectMethod::GetPublicKey)
        .map_err(|e| RpcError::Other(format!("nip46 get_public_key failed: {e}")))?;

    if let Some(error) = response.error {
        return Err(RpcError::Other(format!("nip46 get_public_key error: {error}")));
    }

    match response.result {
        Some(ResponseResult::GetPublicKey(pubkey)) => Ok(pubkey),
        Some(_) => Err(RpcError::Other(
            "nip46 get_public_key unexpected response".to_string(),
        )),
        None => Err(RpcError::Other(
            "nip46 get_public_key missing response".to_string(),
        )),
    }
}
