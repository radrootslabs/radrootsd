#![forbid(unsafe_code)]

use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
pub struct MethodRegistry {
    inner: Arc<RwLock<Vec<String>>>,
}

impl MethodRegistry {
    pub fn track(&self, name: &'static str) {
        let mut methods = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if methods.iter().any(|entry| entry == name) {
            return;
        }
        methods.push(name.to_string());
        methods.sort();
    }

    pub fn list(&self) -> Vec<String> {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).clone()
    }
}
