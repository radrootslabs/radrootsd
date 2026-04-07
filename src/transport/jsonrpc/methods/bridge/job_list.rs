use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::transport::jsonrpc::auth::require_bridge_auth;
use crate::transport::jsonrpc::methods::bridge::shared::BridgeJobView;
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("bridge.job.list");
    m.register_async_method("bridge.job.list", |_params, ctx, extensions| async move {
        require_bridge_auth(&extensions)?;
        let jobs = ctx
            .state
            .bridge_jobs
            .list()
            .into_iter()
            .map(BridgeJobView::from)
            .collect::<Vec<_>>();
        Ok::<Vec<BridgeJobView>, RpcError>(jobs)
    })?;
    Ok(())
}
