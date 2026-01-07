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
use nostr::nips::nip46::NostrConnectRequest;

#[derive(Clone)]
pub struct Nip46SessionStore {
    inner: Arc<Mutex<HashMap<String, Nip46Session>>>,
}

#[derive(Clone)]
pub struct PendingNostrRequest {
    pub request_id: String,
    pub client_pubkey: RadrootsNostrPublicKey,
    pub request: NostrConnectRequest,
}

pub struct Nip46AuthorizeOutcome {
    pub pending: Option<PendingNostrRequest>,
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
    pub auth_required: bool,
    pub authorized: bool,
    pub auth_url: Option<String>,
    pub pending_request: Option<PendingNostrRequest>,
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

    pub async fn require_auth(&self, session_id: &str, auth_url: String) -> bool {
        let mut sessions = self.inner.lock().await;
        match sessions.get_mut(session_id) {
            Some(session) => {
                if session.is_expired() {
                    sessions.remove(session_id);
                    return false;
                }
                session.auth_required = true;
                session.authorized = false;
                session.auth_url = Some(auth_url);
                session.pending_request = None;
                true
            }
            None => false,
        }
    }

    pub async fn authorize(&self, session_id: &str) -> Option<Nip46AuthorizeOutcome> {
        let mut sessions = self.inner.lock().await;
        match sessions.get_mut(session_id) {
            Some(session) => {
                if session.is_expired() {
                    sessions.remove(session_id);
                    return None;
                }
                session.authorized = true;
                Some(Nip46AuthorizeOutcome {
                    pending: session.pending_request.take(),
                })
            }
            None => None,
        }
    }

    pub async fn set_pending_request(
        &self,
        session_id: &str,
        pending: PendingNostrRequest,
    ) -> bool {
        let mut sessions = self.inner.lock().await;
        match sessions.get_mut(session_id) {
            Some(session) => {
                if session.is_expired() {
                    sessions.remove(session_id);
                    return false;
                }
                session.pending_request = Some(pending);
                true
            }
            None => false,
        }
    }

    pub async fn list(&self) -> Vec<Nip46Session> {
        let mut sessions = self.inner.lock().await;
        sessions.retain(|_, session| !session.is_expired());
        let mut listed: Vec<Nip46Session> = sessions.values().cloned().collect();
        listed.sort_by(|left, right| left.id.cmp(&right.id));
        listed
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
    let allows_sign_event = allowed.iter().any(|entry| entry == "sign_event");
    requested
        .iter()
        .filter_map(|perm| {
            if allowed.iter().any(|allow| allow == perm) {
                return Some(perm.clone());
            }
            if allows_sign_event && perm.starts_with("sign_event:") {
                return Some(perm.clone());
            }
            None
        })
        .collect()
}

pub fn sign_event_allowed(perms: &[String], kind: u32) -> bool {
    if perms.iter().any(|entry| entry == "sign_event") {
        return true;
    }
    let entry = format!("sign_event:{kind}");
    perms.iter().any(|perm| perm == &entry)
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
            auth_required: false,
            authorized: true,
            auth_url: None,
            pending_request: None,
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

    #[tokio::test]
    async fn session_store_list_filters_expired() {
        let store = Nip46SessionStore::new();
        store
            .insert(build_session(
                "expired",
                Some(Instant::now() - Duration::from_secs(1)),
            ))
            .await;
        store
            .insert(build_session(
                "active",
                Some(Instant::now() + Duration::from_secs(10)),
            ))
            .await;
        let listed = store.list().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, "active");
    }

    #[test]
    fn filter_perms_allows_sign_event_kinds() {
        let requested = vec![
            "sign_event:1".to_string(),
            "sign_event:4".to_string(),
            "nip04_encrypt".to_string(),
        ];
        let allowed = vec!["sign_event".to_string(), "nip04_encrypt".to_string()];
        let filtered = filter_perms(&requested, &allowed);
        assert_eq!(
            filtered,
            vec![
                "sign_event:1".to_string(),
                "sign_event:4".to_string(),
                "nip04_encrypt".to_string()
            ]
        );
    }

    #[test]
    fn sign_event_allowed_respects_kinds() {
        let perms = vec!["sign_event:1".to_string()];
        assert!(sign_event_allowed(&perms, 1));
        assert!(!sign_event_allowed(&perms, 3));
    }
}
