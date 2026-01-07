#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct Nip46SessionRequireAuthParams {
    session_id: String,
    auth_url: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46SessionRequireAuthResponse {
    required: bool,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.session.require_auth");
    m.register_async_method("nip46.session.require_auth", |params, ctx, _| async move {
        let Nip46SessionRequireAuthParams { session_id, auth_url } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        if auth_url.trim().is_empty() {
            return Err(RpcError::InvalidParams("auth_url is empty".to_string()));
        }
        let required = ctx
            .state
            .nip46_sessions
            .require_auth(&session_id, auth_url)
            .await;
        Ok::<Nip46SessionRequireAuthResponse, RpcError>(Nip46SessionRequireAuthResponse {
            required,
        })
    })?;
    Ok(())
}
