#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

pub mod connect;
pub mod get_public_key;
pub mod nip04;
pub mod ping;
pub mod sign_event;
pub mod session_close;
pub mod session_status;
pub mod status;

pub fn module(ctx: RpcContext, registry: MethodRegistry) -> Result<RpcModule<RpcContext>> {
    let mut m = RpcModule::new(ctx);
    status::register(&mut m, &registry)?;
    connect::register(&mut m, &registry)?;
    ping::register(&mut m, &registry)?;
    get_public_key::register(&mut m, &registry)?;
    nip04::register(&mut m, &registry)?;
    sign_event::register(&mut m, &registry)?;
    session_status::register(&mut m, &registry)?;
    session_close::register(&mut m, &registry)?;
    Ok(m)
}
