#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::api::jsonrpc::{MethodRegistry, RpcContext};

pub mod status;

pub fn module(ctx: RpcContext, registry: MethodRegistry) -> Result<RpcModule<RpcContext>> {
    let mut m = RpcModule::new(ctx);
    status::register(&mut m, &registry)?;
    Ok(m)
}
