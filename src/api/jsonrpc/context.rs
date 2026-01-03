#![forbid(unsafe_code)]

use crate::radrootsd::Radrootsd;

use super::registry::MethodRegistry;

#[derive(Clone)]
pub struct RpcContext {
    pub state: Radrootsd,
    pub methods: MethodRegistry,
}

impl RpcContext {
    pub fn new(state: Radrootsd, methods: MethodRegistry) -> Self {
        Self { state, methods }
    }
}
