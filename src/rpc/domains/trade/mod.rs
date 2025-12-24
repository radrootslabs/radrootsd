#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::radrootsd::Radrootsd;

pub mod listing;

pub fn module(radrootsd: Radrootsd) -> Result<RpcModule<Radrootsd>> {
    let mut m = RpcModule::new(radrootsd.clone());
    m.merge(listing::module(radrootsd)?)?;
    Ok(m)
}
