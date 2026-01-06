#![forbid(unsafe_code)]

use std::net::SocketAddr;

use anyhow::Result;
use jsonrpsee::server::{RpcModule, ServerHandle};

use crate::app::config::RpcConfig;
use crate::core::Radrootsd;

mod context;
mod error;
mod params;
mod registry;
mod server;

pub mod methods;
pub mod nip46;

pub use context::RpcContext;
pub use error::RpcError;
pub use registry::MethodRegistry;
pub(crate) use params::DEFAULT_TIMEOUT_SECS;

pub async fn start_rpc(
    state: Radrootsd,
    addr: SocketAddr,
    rpc_cfg: &RpcConfig,
) -> Result<ServerHandle> {
    let registry = MethodRegistry::default();
    let ctx = RpcContext::new(state, registry.clone());
    let server = server::build_server(addr, rpc_cfg).await?;

    let mut root = RpcModule::new(ctx.clone());
    methods::register_all(&mut root, ctx, registry)?;

    let handle = server.start(root);
    Ok(handle)
}
