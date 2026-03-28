use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;

use crate::transport::jsonrpc::auth::require_bridge_auth;
use crate::transport::jsonrpc::methods::bridge::shared::BridgeJobView;
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct BridgeJobStatusParams {
    job_id: String,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("bridge.job.status");
    m.register_async_method("bridge.job.status", |params, ctx, extensions| async move {
        require_bridge_auth(&extensions)?;
        let params: BridgeJobStatusParams = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let job_id = params.job_id.trim();
        if job_id.is_empty() {
            return Err(RpcError::InvalidParams("missing job_id".to_string()));
        }
        ctx.state
            .bridge_jobs
            .get(job_id)
            .ok_or_else(|| RpcError::Other(format!("unknown bridge job: {job_id}")))
            .map(BridgeJobView::from)
    })?;
    Ok(())
}
