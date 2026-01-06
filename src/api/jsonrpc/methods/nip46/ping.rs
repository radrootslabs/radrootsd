use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use crate::nip46::client;
use crate::nip46::session::Nip46Session;
use nostr::nips::nip46::{NostrConnectMethod, NostrConnectRequest, ResponseResult};

#[derive(Debug, Deserialize)]
struct Nip46PingParams {
    session_id: String,
}

#[derive(Clone, Debug, Serialize)]
struct Nip46PingResponse {
    result: String,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.ping");
    m.register_async_method("nip46.ping", |params, ctx, _| async move {
        let Nip46PingParams { session_id } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let session = ctx
            .state
            .nip46_sessions
            .get(&session_id)
            .await
            .ok_or_else(|| RpcError::InvalidParams("unknown session".to_string()))?;
        let response = request_ping(&session).await?;
        Ok::<Nip46PingResponse, RpcError>(Nip46PingResponse {
            result: response,
        })
    })?;
    Ok(())
}

async fn request_ping(session: &Nip46Session) -> Result<String, RpcError> {
    let request = NostrConnectRequest::Ping;
    let response = client::request(session, request, "ping").await?;
    let response = response
        .to_response(NostrConnectMethod::Ping)
        .map_err(|e| RpcError::Other(format!("nip46 ping failed: {e}")))?;

    if let Some(error) = response.error {
        return Err(RpcError::Other(format!("nip46 ping error: {error}")));
    }

    match response.result {
        Some(ResponseResult::Pong) => Ok("pong".to_string()),
        Some(_) => Err(RpcError::Other(
            "nip46 ping unexpected response".to_string(),
        )),
        None => Err(RpcError::Other("nip46 ping missing response".to_string())),
    }
}
