use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::core::nip46::session::Nip46Session;
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use crate::transport::jsonrpc::nip46::client;
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
        let (pubkey, updated) = request_get_public_key(&session).await?;
        if updated {
            if !ctx
                .state
                .nip46_sessions
                .set_user_pubkey(&session_id, pubkey.clone())
                .await
            {
                return Err(RpcError::Other("nip46 session update failed".to_string()));
            }
        }
        Ok::<Nip46GetPublicKeyResponse, RpcError>(Nip46GetPublicKeyResponse {
            pubkey: pubkey.to_hex(),
        })
    })?;
    Ok(())
}

async fn request_get_public_key(
    session: &Nip46Session,
) -> Result<(radroots_nostr::prelude::RadrootsNostrPublicKey, bool), RpcError> {
    let req = NostrConnectRequest::GetPublicKey;
    let response = client::request(session, req, "get_public_key").await?;
    let response = response
        .to_response(NostrConnectMethod::GetPublicKey)
        .map_err(|e| RpcError::Other(format!("nip46 get_public_key failed: {e}")))?;

    if let Some(error) = response.error {
        return Err(RpcError::Other(format!(
            "nip46 get_public_key error: {error}"
        )));
    }

    let pubkey = match response.result {
        Some(ResponseResult::GetPublicKey(pubkey)) => pubkey,
        Some(_) => {
            return Err(RpcError::Other(
                "nip46 get_public_key unexpected response".to_string(),
            ))
        }
        None => {
            return Err(RpcError::Other(
                "nip46 get_public_key missing response".to_string(),
            ))
        }
    };

    let updated = session.user_pubkey.map(|existing| existing != pubkey).unwrap_or(true);
    Ok((pubkey, updated))
}
