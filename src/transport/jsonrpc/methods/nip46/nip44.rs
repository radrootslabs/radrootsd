use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::transport::jsonrpc::nip46::{client, session};
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use nostr::nips::nip46::{NostrConnectMethod, NostrConnectRequest, ResponseResult};

#[derive(Debug, Deserialize)]
struct Nip46Nip44EncryptParams {
    session_id: String,
    public_key: String,
    text: String,
}

#[derive(Debug, Deserialize)]
struct Nip46Nip44DecryptParams {
    session_id: String,
    public_key: String,
    ciphertext: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46Nip44EncryptResponse {
    ciphertext: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46Nip44DecryptResponse {
    plaintext: String,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.nip44_encrypt");
    m.register_async_method("nip46.nip44_encrypt", |params, ctx, _| async move {
        let Nip46Nip44EncryptParams {
            session_id,
            public_key,
            text,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let session = session::get_session(ctx.as_ref(), &session_id).await?;
        session::require_permission(&session, "nip44_encrypt")?;
        let public_key = radroots_nostr::prelude::radroots_nostr_parse_pubkey(&public_key)
            .map_err(|e| RpcError::InvalidParams(format!("invalid public_key: {e}")))?;
        let req = NostrConnectRequest::Nip44Encrypt { public_key, text };
        let response =
            client::request(&session, req, "nip44_encrypt").await?;
        let response = response
            .to_response(NostrConnectMethod::Nip44Encrypt)
            .map_err(|e| RpcError::Other(format!("nip46 nip44_encrypt failed: {e}")))?;
        if let Some(error) = response.error {
            return Err(RpcError::Other(format!(
                "nip46 nip44_encrypt error: {error}"
            )));
        }
        let ciphertext = match response.result {
            Some(ResponseResult::Nip44Encrypt { ciphertext }) => ciphertext,
            Some(_) => {
                return Err(RpcError::Other(
                    "nip46 nip44_encrypt unexpected response".to_string(),
                ))
            }
            None => {
                return Err(RpcError::Other(
                    "nip46 nip44_encrypt missing response".to_string(),
                ))
            }
        };
        Ok::<Nip46Nip44EncryptResponse, RpcError>(Nip46Nip44EncryptResponse { ciphertext })
    })?;

    registry.track("nip46.nip44_decrypt");
    m.register_async_method("nip46.nip44_decrypt", |params, ctx, _| async move {
        let Nip46Nip44DecryptParams {
            session_id,
            public_key,
            ciphertext,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let session = session::get_session(ctx.as_ref(), &session_id).await?;
        session::require_permission(&session, "nip44_decrypt")?;
        let public_key = radroots_nostr::prelude::radroots_nostr_parse_pubkey(&public_key)
            .map_err(|e| RpcError::InvalidParams(format!("invalid public_key: {e}")))?;
        let req = NostrConnectRequest::Nip44Decrypt {
            public_key,
            ciphertext,
        };
        let response =
            client::request(&session, req, "nip44_decrypt").await?;
        let response = response
            .to_response(NostrConnectMethod::Nip44Decrypt)
            .map_err(|e| RpcError::Other(format!("nip46 nip44_decrypt failed: {e}")))?;
        if let Some(error) = response.error {
            return Err(RpcError::Other(format!(
                "nip46 nip44_decrypt error: {error}"
            )));
        }
        let plaintext = match response.result {
            Some(ResponseResult::Nip44Decrypt { plaintext }) => plaintext,
            Some(_) => {
                return Err(RpcError::Other(
                    "nip46 nip44_decrypt unexpected response".to_string(),
                ))
            }
            None => {
                return Err(RpcError::Other(
                    "nip46 nip44_decrypt missing response".to_string(),
                ))
            }
        };
        Ok::<Nip46Nip44DecryptResponse, RpcError>(Nip46Nip44DecryptResponse { plaintext })
    })?;

    Ok(())
}
