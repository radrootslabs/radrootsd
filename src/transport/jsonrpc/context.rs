#![forbid(unsafe_code)]

use crate::core::Radrootsd;

#[derive(Clone)]
pub struct RpcContext {
    pub state: Radrootsd,
}

impl RpcContext {
    pub fn new(state: Radrootsd) -> Self {
        Self { state }
    }
}
