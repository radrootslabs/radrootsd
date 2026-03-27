#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::Mutex;

use nostr::nips::nip46::NostrConnectRequest;
use radroots_nostr::prelude::{RadrootsNostrClient, RadrootsNostrKeys, RadrootsNostrPublicKey};

#[derive(Clone)]
pub struct Nip46SessionStore {
    inner: Arc<Mutex<HashMap<String, Nip46Session>>>,
    used_secrets: Arc<Mutex<HashSet<String>>>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Nip46SessionRole {
    InboundLocalSigner,
    OutboundRemoteSigner,
}

#[derive(Clone, Debug, Serialize)]
pub struct Nip46SessionView {
    pub session_id: String,
    pub role: Nip46SessionRole,
    pub client_pubkey: String,
    pub signer_pubkey: String,
    pub user_pubkey: Option<String>,
    pub relays: Vec<String>,
    pub permissions: Vec<String>,
    pub name: Option<String>,
    pub url: Option<String>,
    pub image: Option<String>,
    pub auth_required: bool,
    pub authorized: bool,
    pub auth_url: Option<String>,
    pub expires_in_secs: Option<u64>,
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
            used_secrets: Arc::new(Mutex::new(HashSet::new())),
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

    pub async fn set_user_pubkey(&self, session_id: &str, pubkey: RadrootsNostrPublicKey) -> bool {
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

    pub async fn claim_secret(&self, secret: &str) -> bool {
        let mut secrets = self.used_secrets.lock().await;
        if secrets.contains(secret) {
            return false;
        }
        secrets.insert(secret.to_string());
        true
    }
}

impl Nip46Session {
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|expires_at| expires_at <= Instant::now())
            .unwrap_or(false)
    }

    pub fn role(&self) -> Nip46SessionRole {
        if self.client_keys.public_key() == self.remote_signer_pubkey {
            Nip46SessionRole::InboundLocalSigner
        } else {
            Nip46SessionRole::OutboundRemoteSigner
        }
    }

    pub fn public_view(&self) -> Nip46SessionView {
        Nip46SessionView {
            session_id: self.id.clone(),
            role: self.role(),
            client_pubkey: self.client_pubkey.to_hex(),
            signer_pubkey: self.remote_signer_pubkey.to_hex(),
            user_pubkey: self.user_pubkey.as_ref().map(|pubkey| pubkey.to_hex()),
            relays: self.relays.clone(),
            permissions: self.perms.clone(),
            name: self.name.clone(),
            url: self.url.clone(),
            image: self.image.clone(),
            auth_required: self.auth_required,
            authorized: self.authorized,
            auth_url: self.auth_url.clone(),
            expires_in_secs: self.expires_at.map(remaining_secs),
        }
    }
}

fn remaining_secs(expires_at: Instant) -> u64 {
    if expires_at <= Instant::now() {
        0
    } else {
        expires_at
            .saturating_duration_since(Instant::now())
            .as_secs()
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
#[cfg_attr(coverage_nightly, coverage(off))]
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
        let session = build_session("expired", Some(Instant::now() - Duration::from_secs(1)));
        store.insert(session).await;
        let found = store.get("expired").await;
        assert!(found.is_none());
        let found_again = store.get("expired").await;
        assert!(found_again.is_none());
    }

    #[test]
    fn public_view_marks_inbound_local_signer_sessions() {
        let session = build_session("inbound", None);

        let view = session.public_view();

        assert_eq!(view.session_id, "inbound");
        assert_eq!(view.role, Nip46SessionRole::InboundLocalSigner);
        assert_eq!(view.client_pubkey, session.client_pubkey.to_hex());
        assert_eq!(view.signer_pubkey, session.remote_signer_pubkey.to_hex());
        assert_eq!(view.permissions, session.perms);
    }

    #[test]
    fn public_view_marks_outbound_remote_signer_sessions() {
        let client_keys = RadrootsNostrKeys::generate();
        let remote_signer_keys = RadrootsNostrKeys::generate();
        let session = Nip46Session {
            id: "outbound".to_string(),
            client: RadrootsNostrClient::new(client_keys.clone()),
            client_keys: client_keys.clone(),
            client_pubkey: client_keys.public_key(),
            remote_signer_pubkey: remote_signer_keys.public_key(),
            user_pubkey: None,
            relays: vec!["wss://relay.example.com".to_string()],
            perms: vec!["sign_event".to_string()],
            name: Some("remote signer".to_string()),
            url: Some("https://signer.example.com".to_string()),
            image: None,
            expires_at: Some(Instant::now() + Duration::from_secs(30)),
            auth_required: true,
            authorized: false,
            auth_url: Some("https://signer.example.com/auth".to_string()),
            pending_request: None,
        };

        let view = session.public_view();

        assert_eq!(view.session_id, "outbound");
        assert_eq!(view.role, Nip46SessionRole::OutboundRemoteSigner);
        assert_eq!(view.client_pubkey, session.client_pubkey.to_hex());
        assert_eq!(view.signer_pubkey, session.remote_signer_pubkey.to_hex());
        assert_eq!(view.relays, session.relays);
        assert_eq!(view.permissions, session.perms);
        assert!(view.auth_required);
        assert!(!view.authorized);
        assert_eq!(view.auth_url, session.auth_url);
        assert!(view.expires_in_secs.is_some());
    }

    #[tokio::test]
    async fn session_store_keeps_active() {
        let store = Nip46SessionStore::new();
        let session = build_session("active", Some(Instant::now() + Duration::from_secs(60)));
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

    #[tokio::test]
    async fn claim_secret_rejects_reuse() {
        let store = Nip46SessionStore::new();
        assert!(store.claim_secret("secret").await);
        assert!(!store.claim_secret("secret").await);
    }

    #[tokio::test]
    async fn session_store_remove_reports_presence() {
        let store = Nip46SessionStore::new();
        store.insert(build_session("remove", None)).await;
        assert!(store.remove("remove").await);
        assert!(!store.remove("remove").await);
    }

    #[test]
    fn session_expires_at_handles_zero_and_positive() {
        assert!(session_expires_at(0).is_none());
        assert!(session_expires_at(10).is_some());
    }

    #[test]
    fn session_is_expired_respects_future_and_none() {
        let session = build_session("active", Some(Instant::now() + Duration::from_secs(1)));
        assert!(!session.is_expired());
        let session = build_session("never", None);
        assert!(!session.is_expired());
    }

    #[test]
    fn session_is_expired_for_past_deadline() {
        let session = build_session("expired", Some(Instant::now() - Duration::from_secs(1)));
        assert!(session.is_expired());
    }

    #[tokio::test]
    async fn session_store_set_user_pubkey_handles_missing_and_expired() {
        let store = Nip46SessionStore::new();
        let keys = RadrootsNostrKeys::generate();
        assert!(!store.set_user_pubkey("missing", keys.public_key()).await);

        let session = build_session(
            "expired-user",
            Some(Instant::now() - Duration::from_secs(1)),
        );
        store.insert(session).await;
        assert!(
            !store
                .set_user_pubkey("expired-user", keys.public_key())
                .await
        );
    }

    #[tokio::test]
    async fn session_store_set_user_pubkey_sets_value_for_active_session() {
        let store = Nip46SessionStore::new();
        let session = build_session(
            "active-user",
            Some(Instant::now() + Duration::from_secs(30)),
        );
        let keys = RadrootsNostrKeys::generate();
        let pubkey = keys.public_key();
        store.insert(session).await;
        assert!(store.set_user_pubkey("active-user", pubkey).await);
        let found = store.get("active-user").await.expect("session");
        assert_eq!(found.user_pubkey, Some(pubkey));
    }

    #[tokio::test]
    async fn session_store_require_auth_sets_flags_and_clears_pending() {
        let store = Nip46SessionStore::new();
        let mut session = build_session("auth", Some(Instant::now() + Duration::from_secs(30)));
        let keys = RadrootsNostrKeys::generate();
        session.pending_request = Some(PendingNostrRequest {
            request_id: "req-1".to_string(),
            client_pubkey: keys.public_key(),
            request: NostrConnectRequest::Ping,
        });
        store.insert(session).await;

        assert!(store.require_auth("auth", "https://auth".to_string()).await);
        let found = store.get("auth").await.expect("session");
        assert!(found.auth_required);
        assert!(!found.authorized);
        assert_eq!(found.auth_url, Some("https://auth".to_string()));
        assert!(found.pending_request.is_none());
    }

    #[tokio::test]
    async fn session_store_require_auth_handles_missing_and_expired() {
        let store = Nip46SessionStore::new();
        assert!(
            !store
                .require_auth("missing", "https://auth".to_string())
                .await
        );

        store
            .insert(build_session(
                "expired-auth",
                Some(Instant::now() - Duration::from_secs(1)),
            ))
            .await;
        assert!(
            !store
                .require_auth("expired-auth", "https://auth".to_string())
                .await
        );
    }

    #[tokio::test]
    async fn session_store_authorize_returns_pending() {
        let store = Nip46SessionStore::new();
        let mut session =
            build_session("authorize", Some(Instant::now() + Duration::from_secs(30)));
        let keys = RadrootsNostrKeys::generate();
        session.pending_request = Some(PendingNostrRequest {
            request_id: "req-2".to_string(),
            client_pubkey: keys.public_key(),
            request: NostrConnectRequest::GetPublicKey,
        });
        store.insert(session).await;

        let outcome = store.authorize("authorize").await.expect("outcome");
        assert!(outcome.pending.is_some());
        let found = store.get("authorize").await.expect("session");
        assert!(found.authorized);
    }

    #[tokio::test]
    async fn session_store_authorize_handles_missing_and_expired() {
        let store = Nip46SessionStore::new();
        assert!(store.authorize("missing").await.is_none());

        store
            .insert(build_session(
                "expired-authorize",
                Some(Instant::now() - Duration::from_secs(1)),
            ))
            .await;
        assert!(store.authorize("expired-authorize").await.is_none());
    }

    #[tokio::test]
    async fn session_store_set_pending_request_handles_missing_and_expired() {
        let store = Nip46SessionStore::new();
        let keys = RadrootsNostrKeys::generate();
        let pending = PendingNostrRequest {
            request_id: "req-3".to_string(),
            client_pubkey: keys.public_key(),
            request: NostrConnectRequest::Ping,
        };
        assert!(!store.set_pending_request("missing", pending.clone()).await);

        let session = build_session(
            "expired-pending",
            Some(Instant::now() - Duration::from_secs(1)),
        );
        store.insert(session).await;
        assert!(!store.set_pending_request("expired-pending", pending).await);
    }

    #[tokio::test]
    async fn session_store_set_pending_request_succeeds_for_active_session() {
        let store = Nip46SessionStore::new();
        store
            .insert(build_session(
                "pending",
                Some(Instant::now() + Duration::from_secs(30)),
            ))
            .await;
        let keys = RadrootsNostrKeys::generate();
        let pending = PendingNostrRequest {
            request_id: "req-active".to_string(),
            client_pubkey: keys.public_key(),
            request: NostrConnectRequest::Ping,
        };
        assert!(store.set_pending_request("pending", pending).await);
        let found = store.get("pending").await.expect("session");
        assert!(found.pending_request.is_some());
    }

    #[tokio::test]
    async fn session_store_list_sorts_ids() {
        let store = Nip46SessionStore::new();
        store
            .insert(build_session(
                "b",
                Some(Instant::now() + Duration::from_secs(10)),
            ))
            .await;
        store
            .insert(build_session(
                "a",
                Some(Instant::now() + Duration::from_secs(10)),
            ))
            .await;
        let listed = store.list().await;
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].id, "a");
        assert_eq!(listed[1].id, "b");
    }

    #[test]
    fn filter_perms_empty_allowed_returns_empty() {
        let requested = vec!["nip04_encrypt".to_string()];
        let filtered = filter_perms(&requested, &[]);
        assert!(filtered.is_empty());
    }

    #[test]
    fn filter_perms_exact_match_and_rejects_unlisted() {
        let requested = vec![
            "nip04_encrypt".to_string(),
            "nip44_encrypt".to_string(),
            "sign_event:1".to_string(),
        ];
        let allowed = vec!["nip04_encrypt".to_string()];
        let filtered = filter_perms(&requested, &allowed);
        assert_eq!(filtered, vec!["nip04_encrypt".to_string()]);
    }

    #[test]
    fn filter_perms_sign_event_global_does_not_allow_unrelated_perm() {
        let requested = vec!["nip44_encrypt".to_string()];
        let allowed = vec!["sign_event".to_string()];
        let filtered = filter_perms(&requested, &allowed);
        assert!(filtered.is_empty());
    }

    #[test]
    fn sign_event_allowed_accepts_global_permission() {
        let perms = vec!["sign_event".to_string()];
        assert!(sign_event_allowed(&perms, 4));
    }
}
