use std::net::SocketAddr;

use anyhow::Result;
use jsonrpsee::server::{RpcModule, Server, ServerHandle};

use crate::radrootsd::Radrootsd;

mod error;
mod domains;
mod events;
mod relays;
mod system;

pub use error::RpcError;

pub async fn start_rpc(radrootsd: Radrootsd, addr: SocketAddr) -> Result<ServerHandle> {
    let server = Server::builder().build(addr).await?;

    let mut root = RpcModule::new(radrootsd.clone());
    root.merge(system::module(radrootsd.clone())?)?;
    root.merge(relays::module(radrootsd.clone())?)?;
    root.merge(events::profile::module(radrootsd.clone())?)?;
    root.merge(events::post::module(radrootsd.clone())?)?;
    root.merge(events::listing::module(radrootsd.clone())?)?;
    root.merge(domains::trade::module(radrootsd.clone())?)?;

    let handle = server.start(root);
    Ok(handle)
}
