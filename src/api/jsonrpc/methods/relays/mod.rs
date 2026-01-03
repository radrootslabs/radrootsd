use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::api::jsonrpc::{MethodRegistry, RpcContext};

pub mod add;
pub mod connect;
pub mod list;
pub mod remove;
pub mod status;

pub fn module(ctx: RpcContext, registry: MethodRegistry) -> Result<RpcModule<RpcContext>> {
    let mut m = RpcModule::new(ctx);

    add::register(&mut m, &registry)?;
    remove::register(&mut m, &registry)?;
    list::register(&mut m, &registry)?;
    status::register(&mut m, &registry)?;
    connect::register(&mut m, &registry)?;

    Ok(m)
}
