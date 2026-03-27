#![forbid(unsafe_code)]

use std::net::SocketAddr;

use anyhow::Result;
use jsonrpsee::server::{
    BatchRequestConfig, HttpBody, HttpRequest, RpcModule, ServerBuilder, ServerConfigBuilder,
    ServerHandle,
};

use crate::app::config::{BridgeConfig, RpcConfig};
use crate::transport::jsonrpc::RpcContext;
use crate::transport::jsonrpc::auth;

pub async fn start_server(
    addr: SocketAddr,
    rpc_cfg: &RpcConfig,
    bridge_cfg: &BridgeConfig,
    root: RpcModule<RpcContext>,
) -> Result<ServerHandle> {
    let mut builder = ServerConfigBuilder::new()
        .max_request_body_size(rpc_cfg.max_request_body_size)
        .max_response_body_size(rpc_cfg.max_response_body_size)
        .max_connections(rpc_cfg.max_connections)
        .max_subscriptions_per_connection(rpc_cfg.max_subscriptions_per_connection)
        .set_message_buffer_capacity(rpc_cfg.message_buffer_capacity);

    if let Some(limit) = rpc_cfg.batch_request_limit {
        let cfg = if limit == 0 {
            BatchRequestConfig::Disabled
        } else {
            BatchRequestConfig::Limit(limit)
        };
        builder = builder.set_batch_request_config(cfg);
    }

    let server_cfg = builder.build();
    let bridge_bearer_token = bridge_cfg.bearer_token().map(str::to_owned);
    let server = ServerBuilder::with_config(server_cfg)
        .set_http_middleware(tower::ServiceBuilder::new().map_request(
            move |mut request: HttpRequest<HttpBody>| {
                let bridge_auth = auth::authorize_bridge_request(
                    request
                        .headers()
                        .get("authorization")
                        .and_then(|value| value.to_str().ok()),
                    bridge_bearer_token.as_deref(),
                );
                request.extensions_mut().insert(bridge_auth);
                request
            },
        ))
        .build(addr)
        .await?;
    Ok(server.start(root))
}
