use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::core::nip46::session::Nip46Session;
use crate::transport::jsonrpc::nip46::client;
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use nostr::nips::nip46::{NostrConnectMethod, NostrConnectRequest, ResponseResult};

#[derive(Debug, Deserialize)]
struct Nip46Nip04EncryptParams {
    session_id: String,
    public_key: String,
    text: String,
}

#[derive(Debug, Deserialize)]
struct Nip46Nip04DecryptParams {
    session_id: String,
    public_key: String,
    ciphertext: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46Nip04EncryptResponse {
    ciphertext: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46Nip04DecryptResponse {
    plaintext: String,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.nip04_encrypt");
    m.register_async_method("nip46.nip04_encrypt", |params, ctx, _| async move {
        let Nip46Nip04EncryptParams {
            session_id,
            public_key,
            text,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let session = ctx
            .state
            .nip46_sessions
            .get(&session_id)
            .await
            .ok_or_else(|| RpcError::InvalidParams("unknown session".to_string()))?;
        if !has_permission(&session, "nip04_encrypt") {
            return Err(RpcError::Other("unauthorized nip04_encrypt".to_string()));
        }
        let public_key = radroots_nostr::prelude::radroots_nostr_parse_pubkey(&public_key)
            .map_err(|e| RpcError::InvalidParams(format!("invalid public_key: {e}")))?;
        let req = NostrConnectRequest::Nip04Encrypt { public_key, text };
        let response =
            client::request(&session, req, "nip04_encrypt").await?;
        let response = response
            .to_response(NostrConnectMethod::Nip04Encrypt)
            .map_err(|e| RpcError::Other(format!("nip46 nip04_encrypt failed: {e}")))?;
        if let Some(error) = response.error {
            return Err(RpcError::Other(format!(
                "nip46 nip04_encrypt error: {error}"
            )));
        }
        let ciphertext = match response.result {
            Some(ResponseResult::Nip04Encrypt { ciphertext }) => ciphertext,
            Some(_) => {
                return Err(RpcError::Other(
                    "nip46 nip04_encrypt unexpected response".to_string(),
                ))
            }
            None => {
                return Err(RpcError::Other(
                    "nip46 nip04_encrypt missing response".to_string(),
                ))
            }
        };
        Ok::<Nip46Nip04EncryptResponse, RpcError>(Nip46Nip04EncryptResponse { ciphertext })
    })?;

    registry.track("nip46.nip04_decrypt");
    m.register_async_method("nip46.nip04_decrypt", |params, ctx, _| async move {
        let Nip46Nip04DecryptParams {
            session_id,
            public_key,
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
        if !has_permission(&session, "nip04_decrypt") {
            return Err(RpcError::Other("unauthorized nip04_decrypt".to_string()));
        }
        let public_key = radroots_nostr::prelude::radroots_nostr_parse_pubkey(&public_key)
            .map_err(|e| RpcError::InvalidParams(format!("invalid public_key: {e}")))?;
        let req = NostrConnectRequest::Nip04Decrypt {
            public_key,
            ciphertext,
        };
        let response =
            client::request(&session, req, "nip04_decrypt").await?;
        let response = response
            .to_response(NostrConnectMethod::Nip04Decrypt)
            .map_err(|e| RpcError::Other(format!("nip46 nip04_decrypt failed: {e}")))?;
        if let Some(error) = response.error {
            return Err(RpcError::Other(format!(
                "nip46 nip04_decrypt error: {error}"
            )));
        }
        let plaintext = match response.result {
            Some(ResponseResult::Nip04Decrypt { plaintext }) => plaintext,
            Some(_) => {
                return Err(RpcError::Other(
                    "nip46 nip04_decrypt unexpected response".to_string(),
                ))
            }
            None => {
                return Err(RpcError::Other(
                    "nip46 nip04_decrypt missing response".to_string(),
                ))
            }
        };
        Ok::<Nip46Nip04DecryptResponse, RpcError>(Nip46Nip04DecryptResponse { plaintext })
    })?;

    Ok(())
}

fn has_permission(session: &Nip46Session, perm: &str) -> bool {
    session.perms.iter().any(|entry| entry == perm)
}
