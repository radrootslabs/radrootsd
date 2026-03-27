use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

mod job_status;
mod listing_publish;
mod status;

pub fn module(ctx: RpcContext, registry: MethodRegistry) -> Result<RpcModule<RpcContext>> {
    let mut m = RpcModule::new(ctx);
    status::register(&mut m, &registry)?;
    job_status::register(&mut m, &registry)?;
    listing_publish::register(&mut m, &registry)?;
    Ok(m)
}
