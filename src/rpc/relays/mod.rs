use anyhow::Result;
use jsonrpsee::RpcModule;

use crate::radrootsd::Radrootsd;

pub mod add;
pub mod connect;
pub mod list;
pub mod remove;
pub mod status;

pub fn module(radrootsd: Radrootsd) -> Result<RpcModule<Radrootsd>> {
    let mut m = RpcModule::new(radrootsd);

    add::register(&mut m)?;
    remove::register(&mut m)?;
    list::register(&mut m)?;
    status::register(&mut m)?;
    connect::register(&mut m)?;

    Ok(m)
}
