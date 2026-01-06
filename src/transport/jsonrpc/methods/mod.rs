#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

pub mod nip46;

pub fn register_all(
    root: &mut RpcModule<RpcContext>,
    ctx: RpcContext,
    registry: MethodRegistry,
) -> Result<()> {
    root.merge(nip46::module(ctx, registry)?)?;
    Ok(())
}
