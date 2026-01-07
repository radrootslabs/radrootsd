use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::{Deserialize, Serialize};

use crate::core::nip46::Nip46Session;
use crate::transport::jsonrpc::nip46::{client, session};
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};
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
        let session = session::get_session(ctx.as_ref(), &session_id).await?;
        Ok::<Nip46PingResponse, RpcError>(Nip46PingResponse {
            result: request_ping(&session).await?,
        })
    })?;
    Ok(())
}

async fn request_ping(session: &Nip46Session) -> Result<String, RpcError> {
    let req = NostrConnectRequest::Ping;
    let response = client::request(session, req, "ping").await?;
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
