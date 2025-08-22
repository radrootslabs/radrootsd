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
    #[error("{0}")]
    Other(String),
}

impl From<RpcError> for ErrorObjectOwned {
    fn from(err: RpcError) -> Self {
        ErrorObject::owned(-32000, err.to_string(), None::<()>)
    }
}
