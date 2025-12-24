#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::radrootsd::Radrootsd;

pub mod dvm;
pub mod get;
pub mod list;
pub mod orders;
pub mod series;

mod helpers;
mod types;

pub fn module(radrootsd: Radrootsd) -> Result<RpcModule<Radrootsd>> {
    let mut m = RpcModule::new(radrootsd);
    get::register(&mut m)?;
    list::register(&mut m)?;
    dvm::register(&mut m)?;
    series::register(&mut m)?;
    orders::register(&mut m)?;
    Ok(m)
}
