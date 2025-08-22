use jsonrpsee::types::{ErrorObject, ErrorObjectOwned};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("{0}")]
    Other(String),
}

impl From<RpcError> for ErrorObjectOwned {
    fn from(err: RpcError) -> Self {
        ErrorObject::owned(-32000, err.to_string(), None::<()>)
    }
}
