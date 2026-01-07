use crate::core::nip46::session::Nip46Session;
use crate::transport::jsonrpc::{RpcContext, RpcError};

pub async fn get_session(
    ctx: &RpcContext,
    session_id: &str,
) -> Result<Nip46Session, RpcError> {
    ctx.state
        .nip46_sessions
        .get(session_id)
        .await
        .ok_or_else(|| RpcError::InvalidParams("unknown session".to_string()))
}

pub fn require_permission(session: &Nip46Session, perm: &str) -> Result<(), RpcError> {
    if session.perms.iter().any(|entry| entry == perm) {
        Ok(())
    } else {
        Err(RpcError::Other(format!("unauthorized {perm}")))
    }
}
