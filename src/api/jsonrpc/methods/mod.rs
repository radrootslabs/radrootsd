#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;

use super::{context::RpcContext, registry::MethodRegistry};

pub mod domains;
pub mod events;
pub mod relays;
pub mod system;

pub fn register_all(
    root: &mut RpcModule<RpcContext>,
    ctx: RpcContext,
    registry: MethodRegistry,
) -> Result<()> {
    root.merge(system::module(ctx.clone(), registry.clone())?)?;
    root.merge(relays::module(ctx.clone(), registry.clone())?)?;
    root.merge(events::profile::module(ctx.clone(), registry.clone())?)?;
    root.merge(events::post::module(ctx.clone(), registry.clone())?)?;
    root.merge(events::listing::module(ctx.clone(), registry.clone())?)?;
    root.merge(events::list_set::module(ctx.clone(), registry.clone())?)?;
    root.merge(events::farm::module(ctx.clone(), registry.clone())?)?;
    root.merge(events::plot::module(ctx.clone(), registry.clone())?)?;
    root.merge(events::resource_area::module(ctx.clone(), registry.clone())?)?;
    root.merge(events::resource_cap::module(ctx.clone(), registry.clone())?)?;
    root.merge(domains::trade::module(ctx, registry)?)?;
    Ok(())
}
