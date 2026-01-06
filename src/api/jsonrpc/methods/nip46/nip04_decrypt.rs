use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use crate::nip46::client;
use crate::nip46::session::Nip46Session;
use radroots_nostr::prelude::radroots_nostr_parse_pubkey;
use nostr::nips::nip46::{NostrConnectMethod, NostrConnectRequest, ResponseResult};

#[derive(Debug, Deserialize)]
struct Nip46Nip04DecryptParams {
    session_id: String,
    pubkey: String,
    ciphertext: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46Nip04DecryptResponse {
    plaintext: String,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.nip04_decrypt");
    m.register_async_method("nip46.nip04_decrypt", |params, ctx, _| async move {
        let Nip46Nip04DecryptParams {
            session_id,
            pubkey,
            ciphertext,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let session = ctx
            .state
            .nip46_sessions
            .get(&session_id)
            .await
            .ok_or_else(|| RpcError::InvalidParams("unknown session".to_string()))?;
        let plaintext = request_nip04_decrypt(&session, pubkey, ciphertext).await?;
        Ok::<Nip46Nip04DecryptResponse, RpcError>(Nip46Nip04DecryptResponse { plaintext })
    })?;
    Ok(())
}

async fn request_nip04_decrypt(
    session: &Nip46Session,
    pubkey: String,
    ciphertext: String,
) -> Result<String, RpcError> {
    let public_key = radroots_nostr_parse_pubkey(&pubkey)
        .map_err(|e| RpcError::InvalidParams(format!("invalid pubkey: {e}")))?;
    let request = NostrConnectRequest::Nip04Decrypt {
        public_key,
        ciphertext,
    };
    let response = client::request(session, request, "nip04_decrypt").await?;
    let response = response
        .to_response(NostrConnectMethod::Nip04Decrypt)
        .map_err(|e| RpcError::Other(format!("nip46 nip04_decrypt failed: {e}")))?;

    if let Some(error) = response.error {
        return Err(RpcError::Other(format!(
            "nip46 nip04_decrypt error: {error}"
        )));
    }

    match response.result {
        Some(ResponseResult::Nip04Decrypt { plaintext }) => Ok(plaintext),
        Some(_) => Err(RpcError::Other(
            "nip46 nip04_decrypt unexpected response".to_string(),
        )),
        None => Err(RpcError::Other(
            "nip46 nip04_decrypt missing response".to_string(),
        )),
    }
}
