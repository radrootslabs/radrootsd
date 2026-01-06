#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::time::{Duration, Instant};
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
    pub perms: Vec<String>,
    pub name: Option<String>,
    pub url: Option<String>,
    pub image: Option<String>,
    pub expires_at: Option<Instant>,
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
        let mut sessions = self.inner.lock().await;
        let expired = sessions
            .get(session_id)
            .map(|session| session.is_expired())
            .unwrap_or(false);
        if expired {
            sessions.remove(session_id);
            return None;
        }
        sessions.get(session_id).cloned()
    }

    pub async fn remove(&self, session_id: &str) -> bool {
        let mut sessions = self.inner.lock().await;
        sessions.remove(session_id).is_some()
    }

    pub async fn set_user_pubkey(
        &self,
        session_id: &str,
        pubkey: RadrootsNostrPublicKey,
    ) -> bool {
        let mut sessions = self.inner.lock().await;
        match sessions.get_mut(session_id) {
            Some(session) => {
                if session.is_expired() {
                    sessions.remove(session_id);
                    return false;
                }
                session.user_pubkey = Some(pubkey);
                true
            }
            None => false,
        }
    }
}

impl Nip46Session {
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|expires_at| expires_at <= Instant::now())
            .unwrap_or(false)
    }
}

pub fn filter_perms(requested: &[String], allowed: &[String]) -> Vec<String> {
    if allowed.is_empty() {
        return Vec::new();
    }
    requested
        .iter()
        .filter(|perm| allowed.iter().any(|allow| allow == *perm))
        .cloned()
        .collect()
}

pub fn session_expires_at(ttl_secs: u64) -> Option<Instant> {
    if ttl_secs == 0 {
        None
    } else {
        Some(Instant::now() + Duration::from_secs(ttl_secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_session(id: &str, expires_at: Option<Instant>) -> Nip46Session {
        let keys = RadrootsNostrKeys::generate();
        let client = RadrootsNostrClient::new(keys.clone());
        let pubkey = keys.public_key();
        Nip46Session {
            id: id.to_string(),
            client,
            client_keys: keys,
            client_pubkey: pubkey,
            remote_signer_pubkey: pubkey,
            user_pubkey: None,
            relays: Vec::new(),
            perms: Vec::new(),
            name: None,
            url: None,
            image: None,
            expires_at,
        }
    }

    #[tokio::test]
    async fn session_store_removes_expired() {
        let store = Nip46SessionStore::new();
        let session = build_session(
            "expired",
            Some(Instant::now() - Duration::from_secs(1)),
        );
        store.insert(session).await;
        let found = store.get("expired").await;
        assert!(found.is_none());
        let found_again = store.get("expired").await;
        assert!(found_again.is_none());
    }

    #[tokio::test]
    async fn session_store_keeps_active() {
        let store = Nip46SessionStore::new();
        let session = build_session(
            "active",
            Some(Instant::now() + Duration::from_secs(60)),
        );
        store.insert(session).await;
        let found = store.get("active").await;
        assert!(found.is_some());
    }
}
