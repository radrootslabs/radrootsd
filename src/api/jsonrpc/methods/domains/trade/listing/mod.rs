#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::api::jsonrpc::{MethodRegistry, RpcContext};

pub mod dvm;
pub mod get;
pub mod list;
pub mod orders;
pub mod series;

mod helpers;
mod types;

pub fn module(ctx: RpcContext, registry: MethodRegistry) -> Result<RpcModule<RpcContext>> {
    let mut m = RpcModule::new(ctx);
    get::register(&mut m, &registry)?;
    list::register(&mut m, &registry)?;
    dvm::register(&mut m, &registry)?;
    series::register(&mut m, &registry)?;
    orders::register(&mut m, &registry)?;
    Ok(m)
}
