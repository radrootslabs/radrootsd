use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::radrootsd::Radrootsd;

pub mod list;
pub mod publish;

pub fn module(radrootsd: Radrootsd) -> Result<RpcModule<Radrootsd>> {
    let mut m = RpcModule::new(radrootsd);
    list::register(&mut m)?;
    publish::register(&mut m)?;
    Ok(m)
}
