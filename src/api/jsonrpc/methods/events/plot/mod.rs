use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::api::jsonrpc::{MethodRegistry, RpcContext};

pub mod list;
pub mod publish;
pub mod get;

pub fn module(ctx: RpcContext, registry: MethodRegistry) -> Result<RpcModule<RpcContext>> {
    let mut m = RpcModule::new(ctx);
    list::register(&mut m, &registry)?;
    publish::register(&mut m, &registry)?;
    get::register(&mut m, &registry)?;
    Ok(m)
}
