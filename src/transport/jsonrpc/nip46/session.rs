use crate::core::nip46::session::{sign_event_allowed, Nip46Session};
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
    if session.auth_required && !session.authorized {
        return Err(auth_required_error(session));
    }
    if session.perms.iter().any(|entry| entry == perm) {
        Ok(())
    } else {
        Err(RpcError::Other(format!("unauthorized {perm}")))
    }
}

pub fn require_sign_event_permission(
    session: &Nip46Session,
    kind: u32,
) -> Result<(), RpcError> {
    if session.auth_required && !session.authorized {
        return Err(auth_required_error(session));
    }
    if sign_event_allowed(&session.perms, kind) {
        Ok(())
    } else {
        Err(RpcError::Other(format!("unauthorized sign_event:{kind}")))
    }
}

fn auth_required_error(session: &Nip46Session) -> RpcError {
    let url = session
        .auth_url
        .as_deref()
        .unwrap_or("auth required");
    RpcError::Other(format!("auth_url:{url}"))
}
