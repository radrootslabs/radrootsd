#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use radroots_nostr::prelude::{
    RadrootsNostrClient,
    RadrootsNostrKeys,
    RadrootsNostrPublicKey,
};

#[derive(Clone)]
pub struct Nip46SessionStore {
    inner: Arc<Mutex<HashMap<String, Nip46Session>>>,
}

#[derive(Clone)]
pub struct Nip46Session {
    pub id: String,
    pub client: RadrootsNostrClient,
    pub client_keys: RadrootsNostrKeys,
    pub client_pubkey: RadrootsNostrPublicKey,
    pub remote_signer_pubkey: RadrootsNostrPublicKey,
    pub user_pubkey: Option<RadrootsNostrPublicKey>,
    pub relays: Vec<String>,
}

impl Nip46SessionStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn insert(&self, session: Nip46Session) {
        let mut sessions = self.inner.lock().await;
        sessions.insert(session.id.clone(), session);
    }

    pub async fn get(&self, session_id: &str) -> Option<Nip46Session> {
        let sessions = self.inner.lock().await;
        sessions.get(session_id).cloned()
    }

    pub async fn set_user_pubkey(
        &self,
        session_id: &str,
        pubkey: RadrootsNostrPublicKey,
    ) -> bool {
        let mut sessions = self.inner.lock().await;
        match sessions.get_mut(session_id) {
            Some(session) => {
                session.user_pubkey = Some(pubkey);
                true
            }
            None => false,
        }
    }
}
