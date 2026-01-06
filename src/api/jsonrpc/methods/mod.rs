#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;

use super::{context::RpcContext, registry::MethodRegistry};

pub mod nip46;
pub mod relays;
pub mod events;

pub fn register_all(
    root: &mut RpcModule<RpcContext>,
    ctx: RpcContext,
    registry: MethodRegistry,
) -> Result<()> {
    root.merge(relays::module(ctx.clone(), registry.clone())?)?;
    root.merge(nip46::module(ctx.clone(), registry.clone())?)?;
    root.merge(events::module(ctx, registry)?)?;
    Ok(())
}
