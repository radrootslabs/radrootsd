#![forbid(unsafe_code)]

use std::net::SocketAddr;

use anyhow::Result;
use jsonrpsee::server::{BatchRequestConfig, Server, ServerBuilder, ServerConfigBuilder};

use crate::app::config::RpcConfig;

pub async fn build_server(addr: SocketAddr, rpc_cfg: &RpcConfig) -> Result<Server> {
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
    let server = ServerBuilder::with_config(server_cfg).build(addr).await?;
    Ok(server)
}
