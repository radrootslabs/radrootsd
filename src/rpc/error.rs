use jsonrpsee::types::{ErrorObject, ErrorObjectOwned};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("failed to add relay {0}: {1}")]
    AddRelay(String, String),
    #[error("no relays configured; call relays.add first")]
    NoRelays,
    #[error("invalid params: {0}")]
    InvalidParams(String),
    #[error("method not found: {0}")]
    MethodNotFound(String),
    #[error("{0}")]
    Other(String),
}

impl From<RpcError> for ErrorObjectOwned {
    fn from(err: RpcError) -> Self {
        match err {
            RpcError::InvalidParams(msg) => ErrorObject::owned(-32602, msg, None::<()>),
            RpcError::MethodNotFound(name) => {
                ErrorObject::owned(-32601, format!("method not found: {name}"), None::<()>)
            }
            other => ErrorObject::owned(-32000, other.to_string(), None::<()>),
        }
    }
}
