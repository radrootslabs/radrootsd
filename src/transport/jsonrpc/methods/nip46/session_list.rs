use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::core::nip46::session::Nip46SessionView;
use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("nip46.session.list");
    m.register_async_method("nip46.session.list", |_params, ctx, _| async move {
        let sessions = ctx.state.nip46_sessions.list().await;
        let entries = sessions
            .into_iter()
            .map(|session| session.public_view())
            .collect::<Vec<_>>();
        Ok::<Vec<Nip46SessionView>, crate::transport::jsonrpc::RpcError>(entries)
    })?;
    Ok(())
}
