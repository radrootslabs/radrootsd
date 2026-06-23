use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use radroots_events::RadrootsNostrEvent;
use radroots_events::draft::{
    RadrootsDraftError, RadrootsSignedNostrEvent, RadrootsSignedNostrEventParts,
};
use radroots_nostr::prelude::{
    RadrootsNostrClient, RadrootsNostrEventVerification, RadrootsNostrFilter, RadrootsNostrKind,
    RadrootsNostrPublicKey, radroots_nostr_verify_event,
};
use radroots_publish_proxy_protocol::{
    PublishDeliveryPolicy, PublishEventRequest, PublishEventResponse, PublishJobStatus,
    PublishJobView, PublishRelayOutcome, PublishRelayOutcomeKind, PublishRelayPolicy,
    PublishRelaySource, SignedNostrEventWire,
};
use radroots_relay_transport::{
    RadrootsNostrClientPublishAdapter, RadrootsRelayOutcome, RadrootsRelayOutcomeKind,
    RadrootsRelayPublishAdapter, RadrootsRelayPublishRelayReceipt, RadrootsRelayPublishRequest,
    RadrootsRelayTargetSet, RadrootsRelayTransportError, RadrootsRelayUrl, RadrootsRelayUrlPolicy,
};
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::app::config::PublishProxyConfig;

const TOKEN_PREFIX: &str = "rrd_pp_";
const TOKEN_HASH_PREFIX: &str = "sha256:";
const SCHEMA_VERSION: i64 = 2;

#[derive(Debug, Error)]
pub enum PublishProxyError {
    #[error("publish proxy storage error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("publish proxy json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("publish proxy io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid publish proxy scope: {0}")]
    InvalidScope(String),
    #[error("invalid signed Nostr event: {0}")]
    InvalidSignedEvent(String),
    #[error("signed Nostr event verification failed: {0:?}")]
    SignedEventVerification(RadrootsNostrEventVerification),
    #[error("signed Nostr event conversion error: {0}")]
    Draft(#[from] RadrootsDraftError),
    #[error("publish proxy relay error: {0}")]
    Relay(#[from] RadrootsRelayTransportError),
    #[error("publish proxy transport error: {0}")]
    Transport(String),
    #[error("publish proxy concurrency limit reached")]
    ConcurrencyLimit,
    #[error("publish proxy idempotency conflict for key `{0}`")]
    IdempotencyConflict(String),
}

#[derive(Clone)]
pub struct PublishProxy {
    pub config: PublishProxyConfig,
    pub store: PublishProxyStore,
    publisher: Option<Arc<dyn RadrootsRelayPublishAdapter>>,
    resolver: Arc<dyn PublishRelayResolver>,
    author_relay_discovery: Arc<dyn PublishAuthorRelayDiscovery>,
    publish_jobs: Arc<Semaphore>,
}

impl PublishProxy {
    pub fn open(config: PublishProxyConfig) -> Result<Self, PublishProxyError> {
        let store = PublishProxyStore::open(config.database_path.clone())?;
        let publish_jobs = Arc::new(Semaphore::new(config.max_concurrent_publish_jobs));
        Ok(Self {
            config,
            store,
            publisher: None,
            resolver: Arc::new(SystemPublishRelayResolver),
            author_relay_discovery: Arc::new(NostrPublishAuthorRelayDiscovery),
            publish_jobs,
        })
    }

    pub fn memory(config: PublishProxyConfig) -> Result<Self, PublishProxyError> {
        let store = PublishProxyStore::memory()?;
        let publish_jobs = Arc::new(Semaphore::new(config.max_concurrent_publish_jobs));
        Ok(Self {
            config,
            store,
            publisher: None,
            resolver: Arc::new(SystemPublishRelayResolver),
            author_relay_discovery: Arc::new(NostrPublishAuthorRelayDiscovery),
            publish_jobs,
        })
    }

    pub fn with_publisher(mut self, publisher: Arc<dyn RadrootsRelayPublishAdapter>) -> Self {
        self.publisher = Some(publisher);
        self
    }

    #[cfg(test)]
    fn with_relay_resolver(mut self, resolver: Arc<dyn PublishRelayResolver>) -> Self {
        self.resolver = resolver;
        self
    }

    #[cfg(test)]
    fn with_author_relay_discovery(
        mut self,
        author_relay_discovery: Arc<dyn PublishAuthorRelayDiscovery>,
    ) -> Self {
        self.author_relay_discovery = author_relay_discovery;
        self
    }

    fn acquire_publish_permit(&self) -> Result<OwnedSemaphorePermit, PublishProxyError> {
        self.publish_jobs
            .clone()
            .try_acquire_owned()
            .map_err(|_| PublishProxyError::ConcurrencyLimit)
    }

    pub async fn publish_event(
        &self,
        principal: &PublishPrincipal,
        request: PublishEventRequest,
    ) -> Result<PublishEventResponse, PublishProxyError> {
        request
            .validate(self.config.max_relays_per_request)
            .map_err(|error| {
                PublishProxyError::InvalidSignedEvent(format!(
                    "publish request validation failed: {error}"
                ))
            })?;
        principal.allows_event(&request)?;
        let signed_event = signed_event_from_wire(&request.event)?;
        if signed_event.raw_json.len() > self.config.max_event_bytes {
            return Err(PublishProxyError::InvalidSignedEvent(
                "signed event exceeds publish_proxy max_event_bytes".to_owned(),
            ));
        }
        let effective_timeout_ms = effective_publish_timeout_ms(&self.config, request.timeout_ms)?;
        let _permit = self.acquire_publish_permit()?;
        let request_fingerprint = request_intent_fingerprint(
            principal.principal_id.as_str(),
            signed_event.raw_json.as_str(),
            &request,
            effective_timeout_ms,
        )?;
        let resolution = self
            .resolve_relays_for_request(signed_event.pubkey.as_str(), &request)
            .await?;
        let response = self.store.record_publish_job(PublishJobInsert {
            principal_id: principal.principal_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            request: request.clone(),
            request_fingerprint,
            effective_relay_count: resolution.targets.len(),
        })?;
        if response.deduplicated {
            return Ok(response);
        }
        let completed = self
            .complete_job_execution(
                response.job.job_id.as_str(),
                signed_event,
                request.delivery_policy.clone(),
                effective_timeout_ms,
                resolution,
            )
            .await?;
        Ok(PublishEventResponse {
            deduplicated: false,
            job: completed,
        })
    }

    pub async fn resolve_relays_for_request(
        &self,
        pubkey: &str,
        request: &PublishEventRequest,
    ) -> Result<PublishRelayResolution, PublishProxyError> {
        match request.relay_policy {
            PublishRelayPolicy::ExplicitOnly => self.resolve_request_relays(&request.relays).await,
            PublishRelayPolicy::RequestThenAuthorWriteThenDaemonDefault => {
                if !request.relays.is_empty() {
                    self.resolve_request_relays(&request.relays).await
                } else {
                    self.resolve_author_or_default_relays(pubkey).await
                }
            }
            PublishRelayPolicy::AuthorWriteThenDaemonDefault => {
                self.resolve_author_or_default_relays(pubkey).await
            }
            PublishRelayPolicy::DaemonDefaultOnly => self.resolve_daemon_default_relays().await,
        }
    }

    async fn resolve_author_or_default_relays(
        &self,
        pubkey: &str,
    ) -> Result<PublishRelayResolution, PublishProxyError> {
        let mut author_relays = self.resolve_author_write_relays(pubkey).await?;
        if author_relays.targets.is_empty() {
            let mut daemon_defaults = self.resolve_daemon_default_relays().await?;
            daemon_defaults.outcomes.append(&mut author_relays.outcomes);
            Ok(daemon_defaults)
        } else {
            Ok(author_relays)
        }
    }

    async fn resolve_request_relays(
        &self,
        relays: &[String],
    ) -> Result<PublishRelayResolution, PublishProxyError> {
        let mut targets = Vec::new();
        let mut outcomes = Vec::new();
        for relay in relays {
            match RadrootsRelayUrl::parse(relay, relay_url_policy(&self.config)) {
                Ok(url) => {
                    self.push_checked_relay_target(
                        &mut targets,
                        &mut outcomes,
                        url,
                        PublishRelaySource::Request,
                    )
                    .await;
                }
                Err(error) => outcomes.push(PublishRelayOutcome {
                    relay_url: relay.trim().to_owned(),
                    source: PublishRelaySource::Request,
                    attempted: false,
                    outcome_kind: PublishRelayOutcomeKind::RelayUrlRejected,
                    message: Some(error.to_string()),
                    latency_ms: None,
                }),
            }
        }
        Ok(PublishRelayResolution { targets, outcomes })
    }

    async fn resolve_author_write_relays(
        &self,
        pubkey: &str,
    ) -> Result<PublishRelayResolution, PublishProxyError> {
        let cached = self.store.cached_author_write_relays(pubkey)?;
        let mut cached_resolution = self.resolve_author_relay_inputs(&cached).await?;
        if !cached_resolution.targets.is_empty() {
            return Ok(cached_resolution);
        }
        if self.config.author_relay_discovery_relays.is_empty() {
            return Ok(cached_resolution);
        }
        let mut discovery_targets = self
            .resolve_config_relays(
                &self.config.author_relay_discovery_relays,
                PublishRelaySource::DaemonDefault,
            )
            .await?;
        if discovery_targets.targets.is_empty() {
            discovery_targets
                .outcomes
                .append(&mut cached_resolution.outcomes);
            return Ok(discovery_targets);
        }
        let discovered = self
            .author_relay_discovery
            .fetch_author_write_relays(
                pubkey,
                std::mem::take(&mut discovery_targets.targets),
                self.config.connect_timeout_secs,
            )
            .await?;
        self.store.cache_author_write_relays(pubkey, &discovered)?;
        let mut discovered_resolution = self.resolve_author_relay_inputs(&discovered).await?;
        discovered_resolution
            .outcomes
            .append(&mut cached_resolution.outcomes);
        discovered_resolution
            .outcomes
            .append(&mut discovery_targets.outcomes);
        Ok(discovered_resolution)
    }

    async fn resolve_author_relay_inputs(
        &self,
        relays: &[String],
    ) -> Result<PublishRelayResolution, PublishProxyError> {
        let mut targets = Vec::new();
        let mut outcomes = Vec::new();
        for relay in relays {
            match RadrootsRelayUrl::parse(relay, relay_url_policy(&self.config)) {
                Ok(url) => {
                    self.push_checked_relay_target(
                        &mut targets,
                        &mut outcomes,
                        url,
                        PublishRelaySource::AuthorWrite,
                    )
                    .await;
                }
                Err(error) => outcomes.push(PublishRelayOutcome {
                    relay_url: relay.trim().to_owned(),
                    source: PublishRelaySource::AuthorWrite,
                    attempted: false,
                    outcome_kind: PublishRelayOutcomeKind::RelayUrlRejected,
                    message: Some(error.to_string()),
                    latency_ms: None,
                }),
            }
        }
        Ok(PublishRelayResolution { targets, outcomes })
    }

    async fn resolve_daemon_default_relays(
        &self,
    ) -> Result<PublishRelayResolution, PublishProxyError> {
        self.resolve_config_relays(
            &self.config.daemon_default_publish_relays,
            PublishRelaySource::DaemonDefault,
        )
        .await
    }

    async fn resolve_config_relays(
        &self,
        relays: &[String],
        source: PublishRelaySource,
    ) -> Result<PublishRelayResolution, PublishProxyError> {
        let mut targets = Vec::new();
        let mut outcomes = Vec::new();
        for relay in relays {
            match RadrootsRelayUrl::parse(relay, relay_url_policy(&self.config)) {
                Ok(url) => {
                    self.push_checked_relay_target(&mut targets, &mut outcomes, url, source)
                        .await;
                }
                Err(error) => outcomes.push(PublishRelayOutcome {
                    relay_url: relay.trim().to_owned(),
                    source,
                    attempted: false,
                    outcome_kind: PublishRelayOutcomeKind::RelayUrlRejected,
                    message: Some(error.to_string()),
                    latency_ms: None,
                }),
            }
        }
        Ok(PublishRelayResolution { targets, outcomes })
    }

    async fn push_checked_relay_target(
        &self,
        targets: &mut Vec<ResolvedPublishRelay>,
        outcomes: &mut Vec<PublishRelayOutcome>,
        url: RadrootsRelayUrl,
        source: PublishRelaySource,
    ) {
        if relay_url_policy(&self.config) == RadrootsRelayUrlPolicy::Localhost {
            push_resolved_relay(targets, url, source);
            return;
        }
        match self.resolver.resolve(&url).await {
            Ok(addresses) if addresses.is_empty() => {
                outcomes.push(relay_resolution_connection_failure(
                    url.as_str(),
                    source,
                    "dns lookup returned no addresses",
                ));
            }
            Ok(addresses) => match url.validate_public_resolved_ip_addrs(addresses) {
                Ok(()) => push_resolved_relay(targets, url, source),
                Err(error) => outcomes.push(PublishRelayOutcome {
                    relay_url: url.as_str().to_owned(),
                    source,
                    attempted: false,
                    outcome_kind: PublishRelayOutcomeKind::RelayUrlRejected,
                    message: Some(error.to_string()),
                    latency_ms: None,
                }),
            },
            Err(error) => outcomes.push(relay_resolution_connection_failure(
                url.as_str(),
                source,
                format!("dns lookup failed: {error}"),
            )),
        }
    }

    async fn complete_job_execution(
        &self,
        job_id: &str,
        signed_event: RadrootsSignedNostrEvent,
        delivery_policy: PublishDeliveryPolicy,
        timeout_ms: u64,
        resolution: PublishRelayResolution,
    ) -> Result<PublishJobView, PublishProxyError> {
        if resolution.targets.is_empty() {
            let status = if resolution
                .outcomes
                .iter()
                .any(|outcome| outcome.outcome_kind.is_retryable())
            {
                PublishJobStatus::DeliveryUnsatisfiedRetryable
            } else {
                PublishJobStatus::Rejected
            };
            let last_error = if status == PublishJobStatus::DeliveryUnsatisfiedRetryable {
                "delivery_unsatisfied"
            } else {
                "no_publish_relays"
            };
            self.store.complete_publish_job(
                job_id,
                status,
                resolution.outcomes,
                Some(last_error.to_owned()),
            )?;
            return self.store.job_by_id(job_id);
        }
        let required_ack_count = delivery_policy.required_ack_count(resolution.targets.len());
        if required_ack_count > resolution.targets.len() {
            self.store.complete_publish_job(
                job_id,
                PublishJobStatus::Rejected,
                resolution.outcomes,
                Some("delivery_quorum_exceeds_relay_count".to_owned()),
            )?;
            return self.store.job_by_id(job_id);
        }
        let source_by_relay = resolution.source_by_relay();
        let target_set = RadrootsRelayTargetSet::from_urls(
            resolution
                .targets
                .iter()
                .map(|target| target.url.clone())
                .collect(),
        )?;
        let publish_request =
            RadrootsRelayPublishRequest::new(signed_event, target_set, current_unix_millis())
                .with_accepted_quorum(required_ack_count);
        let started = Instant::now();
        let publish_timeout = Duration::from_millis(timeout_ms);
        let receipts =
            match tokio::time::timeout(publish_timeout, self.publish_with_adapter(publish_request))
                .await
            {
                Ok(Ok(receipts)) => receipts,
                Ok(Err(error)) => transport_error_receipts(&resolution.targets, error),
                Err(_) => timeout_receipts(&resolution.targets),
            };
        let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let mut outcomes = resolution.outcomes;
        outcomes.extend(receipts.into_iter().map(|receipt| {
            publish_outcome_from_receipt(receipt, &source_by_relay, Some(latency_ms))
        }));
        let status = delivery_status(&delivery_policy, resolution.targets.len(), &outcomes);
        let last_error = if status == PublishJobStatus::DeliverySatisfied {
            None
        } else {
            Some("delivery_unsatisfied".to_owned())
        };
        self.store
            .complete_publish_job(job_id, status, outcomes, last_error)?;
        self.store.job_by_id(job_id)
    }

    async fn publish_with_adapter(
        &self,
        request: RadrootsRelayPublishRequest,
    ) -> Result<Vec<RadrootsRelayPublishRelayReceipt>, PublishProxyError> {
        if let Some(publisher) = &self.publisher {
            return publisher
                .publish(request)
                .await
                .map_err(PublishProxyError::Relay);
        }
        let adapter = RadrootsNostrClientPublishAdapter::new(RadrootsNostrClient::new_signerless());
        adapter
            .publish(request)
            .await
            .map_err(PublishProxyError::Relay)
    }
}

#[derive(Clone)]
pub struct PublishProxyStore {
    inner: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishJobVisibility {
    Own,
    Admin,
}

impl FromStr for PublishJobVisibility {
    type Err = PublishProxyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "own" => Ok(Self::Own),
            "admin" => Ok(Self::Admin),
            other => Err(PublishProxyError::InvalidScope(format!(
                "unknown job visibility `{other}`"
            ))),
        }
    }
}

impl fmt::Display for PublishJobVisibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Own => f.write_str("own"),
            Self::Admin => f.write_str("admin"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishPrincipalInit {
    pub label: String,
    pub token_hash: String,
    pub allowed_pubkeys: Vec<String>,
    pub allowed_kinds: Vec<u32>,
    pub allowed_relay_policies: Vec<PublishRelayPolicy>,
    pub allow_request_relays: bool,
    pub job_visibility: PublishJobVisibility,
    pub expires_at_unix: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishPrincipal {
    pub principal_id: String,
    pub label: String,
    pub allowed_pubkeys: Vec<String>,
    pub allowed_kinds: Vec<u32>,
    pub allowed_relay_policies: Vec<PublishRelayPolicy>,
    pub allow_request_relays: bool,
    pub job_visibility: PublishJobVisibility,
    pub expires_at_unix: Option<i64>,
}

impl PublishPrincipal {
    pub fn allows_event(&self, request: &PublishEventRequest) -> Result<(), PublishProxyError> {
        ensure_lower_hex("pubkey", request.event.pubkey.as_str(), 64)?;
        if !self
            .allowed_pubkeys
            .iter()
            .any(|pubkey| pubkey == &request.event.pubkey)
        {
            return Err(PublishProxyError::InvalidScope(
                "principal is not allowed to publish for event pubkey".to_owned(),
            ));
        }
        if !self.allowed_kinds.contains(&request.event.kind) {
            return Err(PublishProxyError::InvalidScope(
                "principal is not allowed to publish event kind".to_owned(),
            ));
        }
        if !self.allowed_relay_policies.contains(&request.relay_policy) {
            return Err(PublishProxyError::InvalidScope(
                "principal is not allowed to use requested relay policy".to_owned(),
            ));
        }
        if !self.allow_request_relays && !request.relays.is_empty() {
            return Err(PublishProxyError::InvalidScope(
                "principal is not allowed to provide request relays".to_owned(),
            ));
        }
        Ok(())
    }

    fn can_read_job(&self, principal_id: &str) -> bool {
        self.job_visibility == PublishJobVisibility::Admin || self.principal_id == principal_id
    }
}

#[derive(Debug, Clone)]
pub struct PublishJobInsert {
    pub principal_id: String,
    pub idempotency_key: Option<String>,
    pub request: PublishEventRequest,
    pub request_fingerprint: String,
    pub effective_relay_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPublishRelay {
    pub url: RadrootsRelayUrl,
    pub source: PublishRelaySource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRelayResolution {
    pub targets: Vec<ResolvedPublishRelay>,
    pub outcomes: Vec<PublishRelayOutcome>,
}

impl PublishRelayResolution {
    fn source_by_relay(&self) -> BTreeMap<String, PublishRelaySource> {
        self.targets
            .iter()
            .map(|target| (target.url.as_str().to_owned(), target.source))
            .collect()
    }
}

type PublishRelayResolveFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, std::io::Error>> + Send + 'a>>;

trait PublishRelayResolver: Send + Sync {
    fn resolve<'a>(&'a self, url: &'a RadrootsRelayUrl) -> PublishRelayResolveFuture<'a>;
}

type PublishAuthorRelayDiscoveryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<String>, PublishProxyError>> + Send + 'a>>;

trait PublishAuthorRelayDiscovery: Send + Sync {
    fn fetch_author_write_relays<'a>(
        &'a self,
        pubkey: &'a str,
        discovery_targets: Vec<ResolvedPublishRelay>,
        connect_timeout_secs: u64,
    ) -> PublishAuthorRelayDiscoveryFuture<'a>;
}

#[derive(Debug)]
struct SystemPublishRelayResolver;

impl PublishRelayResolver for SystemPublishRelayResolver {
    fn resolve<'a>(&'a self, url: &'a RadrootsRelayUrl) -> PublishRelayResolveFuture<'a> {
        Box::pin(async move {
            let (host, port) = relay_socket_target(url)?;
            let addrs = tokio::net::lookup_host((host.as_str(), port)).await?;
            Ok(addrs.map(|addr| addr.ip()).collect())
        })
    }
}

#[derive(Debug)]
struct NostrPublishAuthorRelayDiscovery;

impl PublishAuthorRelayDiscovery for NostrPublishAuthorRelayDiscovery {
    fn fetch_author_write_relays<'a>(
        &'a self,
        pubkey: &'a str,
        discovery_targets: Vec<ResolvedPublishRelay>,
        connect_timeout_secs: u64,
    ) -> PublishAuthorRelayDiscoveryFuture<'a> {
        Box::pin(async move {
            let Ok(public_key) = RadrootsNostrPublicKey::from_hex(pubkey) else {
                return Ok(Vec::new());
            };
            let client = RadrootsNostrClient::new_signerless();
            for target in discovery_targets {
                if client.add_read_relay(target.url.as_str()).await.is_err() {
                    return Ok(Vec::new());
                }
            }
            let filter = RadrootsNostrFilter::new()
                .author(public_key)
                .kind(RadrootsNostrKind::Custom(10_002))
                .limit(10);
            let timeout = Duration::from_secs(connect_timeout_secs);
            let Ok(events) = client.fetch_events(filter, timeout).await else {
                return Ok(Vec::new());
            };
            let Some(event) = events.into_iter().max_by(|left, right| {
                left.created_at
                    .as_secs()
                    .cmp(&right.created_at.as_secs())
                    .then_with(|| left.id.to_hex().cmp(&right.id.to_hex()))
            }) else {
                return Ok(Vec::new());
            };
            Ok(author_write_relays_from_nip65_event(&event))
        })
    }
}

impl PublishProxyStore {
    pub fn open(path: PathBuf) -> Result<Self, PublishProxyError> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        Self::from_connection(connection)
    }

    pub fn memory() -> Result<Self, PublishProxyError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, PublishProxyError> {
        connection.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS publish_proxy_principals (
                principal_id TEXT PRIMARY KEY NOT NULL,
                label TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                allowed_pubkeys_json TEXT NOT NULL,
                allowed_kinds_json TEXT NOT NULL,
                allowed_relay_policies_json TEXT NOT NULL,
                allow_request_relays INTEGER NOT NULL,
                job_visibility TEXT NOT NULL,
                expires_at_unix INTEGER,
                revoked_at_unix INTEGER,
                created_at_unix INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS publish_proxy_jobs (
                job_id TEXT PRIMARY KEY NOT NULL,
                principal_id TEXT NOT NULL,
                idempotency_key TEXT,
                request_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL,
                event_id TEXT NOT NULL,
                event_pubkey TEXT NOT NULL,
                event_kind INTEGER NOT NULL,
                relay_policy_json TEXT NOT NULL,
                delivery_policy_json TEXT NOT NULL,
                requested_relay_count INTEGER NOT NULL,
                effective_relay_count INTEGER NOT NULL,
                request_json TEXT NOT NULL,
                requested_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                completed_at_ms INTEGER,
                last_error TEXT,
                FOREIGN KEY(principal_id) REFERENCES publish_proxy_principals(principal_id)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS publish_proxy_jobs_principal_idempotency_idx
                ON publish_proxy_jobs(principal_id, idempotency_key)
                WHERE idempotency_key IS NOT NULL;
            CREATE TABLE IF NOT EXISTS publish_proxy_relay_results (
                job_id TEXT NOT NULL,
                relay_url TEXT NOT NULL,
                source TEXT NOT NULL,
                attempted INTEGER NOT NULL,
                outcome_kind TEXT NOT NULL,
                message TEXT,
                latency_ms INTEGER,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(job_id, relay_url),
                FOREIGN KEY(job_id) REFERENCES publish_proxy_jobs(job_id)
            );
            CREATE TABLE IF NOT EXISTS publish_proxy_relay_list_cache (
                pubkey TEXT PRIMARY KEY NOT NULL,
                relays_json TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            "#,
        )?;
        migrate_schema(&connection)?;
        recover_interrupted_publish_jobs(&connection)?;
        connection.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn create_principal(
        &self,
        input: PublishPrincipalInit,
    ) -> Result<PublishPrincipal, PublishProxyError> {
        validate_principal_init(&input)?;
        let principal_id = Uuid::new_v4().to_string();
        let now = current_unix_secs();
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        connection.execute(
            r#"
            INSERT INTO publish_proxy_principals (
                principal_id,
                label,
                token_hash,
                allowed_pubkeys_json,
                allowed_kinds_json,
                allowed_relay_policies_json,
                allow_request_relays,
                job_visibility,
                expires_at_unix,
                revoked_at_unix,
                created_at_unix
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10)
            "#,
            params![
                principal_id,
                input.label.trim(),
                input.token_hash,
                serde_json::to_string(&input.allowed_pubkeys)?,
                serde_json::to_string(&input.allowed_kinds)?,
                serde_json::to_string(&input.allowed_relay_policies)?,
                input.allow_request_relays,
                input.job_visibility.to_string(),
                input.expires_at_unix,
                now,
            ],
        )?;
        drop(connection);
        self.principal_by_id(principal_id.as_str())?
            .ok_or_else(|| PublishProxyError::InvalidScope("created principal missing".to_owned()))
    }

    pub fn principal_for_token_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<PublishPrincipal>, PublishProxyError> {
        let now = current_unix_secs();
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let principal = connection
            .query_row(
                r#"
                SELECT
                    principal_id,
                    label,
                    allowed_pubkeys_json,
                    allowed_kinds_json,
                    allowed_relay_policies_json,
                    allow_request_relays,
                    job_visibility,
                    expires_at_unix
                FROM publish_proxy_principals
                WHERE token_hash = ?1
                  AND revoked_at_unix IS NULL
                  AND (expires_at_unix IS NULL OR expires_at_unix > ?2)
                "#,
                params![token_hash, now],
                principal_from_row,
            )
            .optional()?;
        Ok(principal)
    }

    pub fn principal_by_id(
        &self,
        principal_id: &str,
    ) -> Result<Option<PublishPrincipal>, PublishProxyError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let principal = connection
            .query_row(
                r#"
                SELECT
                    principal_id,
                    label,
                    allowed_pubkeys_json,
                    allowed_kinds_json,
                    allowed_relay_policies_json,
                    allow_request_relays,
                    job_visibility,
                    expires_at_unix
                FROM publish_proxy_principals
                WHERE principal_id = ?1
                "#,
                params![principal_id],
                principal_from_row,
            )
            .optional()?;
        Ok(principal)
    }

    pub fn record_publish_job(
        &self,
        insert: PublishJobInsert,
    ) -> Result<PublishEventResponse, PublishProxyError> {
        if let Some(idempotency_key) = insert.idempotency_key.as_deref() {
            if let Some(existing) =
                self.job_for_principal_id_and_key(insert.principal_id.as_str(), idempotency_key)?
            {
                if existing.request_fingerprint != insert.request_fingerprint {
                    return Err(PublishProxyError::IdempotencyConflict(
                        idempotency_key.to_owned(),
                    ));
                }
                return Ok(PublishEventResponse {
                    deduplicated: true,
                    job: existing.view,
                });
            }
        }

        let job_id = Uuid::new_v4().to_string();
        let now = current_unix_millis();
        let request_json = serde_json::to_string(&insert.request)?;
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let insert_result = connection.execute(
            r#"
            INSERT INTO publish_proxy_jobs (
                job_id,
                principal_id,
                idempotency_key,
                request_fingerprint,
                status,
                event_id,
                event_pubkey,
                event_kind,
                relay_policy_json,
                delivery_policy_json,
                requested_relay_count,
                effective_relay_count,
                request_json,
                requested_at_ms,
                updated_at_ms
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            "#,
            params![
                job_id,
                insert.principal_id,
                insert.idempotency_key,
                insert.request_fingerprint,
                serde_json::to_string(&PublishJobStatus::Publishing)?,
                insert.request.event.id,
                insert.request.event.pubkey,
                insert.request.event.kind,
                serde_json::to_string(&insert.request.relay_policy)?,
                serde_json::to_string(&insert.request.delivery_policy)?,
                insert.request.relays.len(),
                insert.effective_relay_count,
                request_json,
                now,
                now,
            ],
        );
        match insert_result {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                return Err(PublishProxyError::IdempotencyConflict(
                    "idempotency key conflicts with an existing publish job".to_owned(),
                ));
            }
            Err(error) => return Err(error.into()),
        }
        drop(connection);
        let job = self.job_by_id(job_id.as_str())?;
        Ok(PublishEventResponse {
            deduplicated: false,
            job,
        })
    }

    pub fn job_by_id_for_principal(
        &self,
        job_id: &str,
        principal: &PublishPrincipal,
    ) -> Result<Option<PublishJobView>, PublishProxyError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sql = job_select_sql("WHERE job_id = ?1");
        let row = connection
            .query_row(sql.as_str(), params![job_id], job_from_row)
            .optional()?;
        drop(connection);
        let Some(mut job) = row else {
            return Ok(None);
        };
        if !principal.can_read_job(job.principal_id.as_str()) {
            return Ok(None);
        }
        job.view.relays = self.relay_outcomes(job.view.job_id.as_str())?;
        finalize_job_view(&mut job.view);
        Ok(Some(job.view))
    }

    pub fn list_jobs_for_principal(
        &self,
        principal: &PublishPrincipal,
        limit: usize,
    ) -> Result<Vec<PublishJobView>, PublishProxyError> {
        let limit = i64::try_from(limit.clamp(1, 200)).unwrap_or(200);
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sql = if principal.job_visibility == PublishJobVisibility::Admin {
            job_select_sql("ORDER BY requested_at_ms DESC, job_id DESC LIMIT ?1")
        } else {
            job_select_sql(
                "WHERE principal_id = ?1 ORDER BY requested_at_ms DESC, job_id DESC LIMIT ?2",
            )
        };
        let mut stmt = connection.prepare(sql.as_str())?;
        let rows = if principal.job_visibility == PublishJobVisibility::Admin {
            stmt.query_map(params![limit], job_from_row)?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(params![principal.principal_id, limit], job_from_row)?
                .collect::<Result<Vec<_>, _>>()?
        };
        drop(stmt);
        drop(connection);

        rows.into_iter()
            .map(|mut row| {
                row.view.relays = self.relay_outcomes(row.view.job_id.as_str())?;
                finalize_job_view(&mut row.view);
                Ok(row.view)
            })
            .collect()
    }

    fn job_for_principal_id_and_key(
        &self,
        principal_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<PublishJobRow>, PublishProxyError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sql = job_select_sql("WHERE principal_id = ?1 AND idempotency_key = ?2");
        let row = connection
            .query_row(
                sql.as_str(),
                params![principal_id, idempotency_key],
                job_from_row,
            )
            .optional()?;
        drop(connection);
        let Some(mut job) = row else {
            return Ok(None);
        };
        job.view.relays = self.relay_outcomes(job.view.job_id.as_str())?;
        finalize_job_view(&mut job.view);
        Ok(Some(job))
    }

    pub fn job_by_id(&self, job_id: &str) -> Result<PublishJobView, PublishProxyError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sql = job_select_sql("WHERE job_id = ?1");
        let row = connection
            .query_row(sql.as_str(), params![job_id], job_from_row)
            .optional()?;
        drop(connection);
        let Some(mut job) = row else {
            return Err(PublishProxyError::InvalidScope(
                "unknown publish job".to_owned(),
            ));
        };
        job.view.relays = self.relay_outcomes(job.view.job_id.as_str())?;
        finalize_job_view(&mut job.view);
        Ok(job.view)
    }

    pub fn complete_publish_job(
        &self,
        job_id: &str,
        status: PublishJobStatus,
        outcomes: Vec<PublishRelayOutcome>,
        last_error: Option<String>,
    ) -> Result<(), PublishProxyError> {
        let now = current_unix_millis();
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        connection.execute(
            r#"
            UPDATE publish_proxy_jobs
            SET status = ?2,
                updated_at_ms = ?3,
                completed_at_ms = ?4,
                last_error = ?5
            WHERE job_id = ?1
            "#,
            params![
                job_id,
                serde_json::to_string(&status)?,
                now,
                now,
                last_error,
            ],
        )?;
        connection.execute(
            "DELETE FROM publish_proxy_relay_results WHERE job_id = ?1",
            params![job_id],
        )?;
        for outcome in outcomes {
            connection.execute(
                r#"
                INSERT OR REPLACE INTO publish_proxy_relay_results (
                    job_id,
                    relay_url,
                    source,
                    attempted,
                    outcome_kind,
                    message,
                    latency_ms,
                    updated_at_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                "#,
                params![
                    job_id,
                    outcome.relay_url,
                    serde_json::to_string(&outcome.source)?,
                    outcome.attempted,
                    serde_json::to_string(&outcome.outcome_kind)?,
                    outcome.message,
                    outcome
                        .latency_ms
                        .and_then(|value| i64::try_from(value).ok()),
                    now,
                ],
            )?;
        }
        Ok(())
    }

    pub fn cached_author_write_relays(
        &self,
        pubkey: &str,
    ) -> Result<Vec<String>, PublishProxyError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let relays_json = connection
            .query_row(
                "SELECT relays_json FROM publish_proxy_relay_list_cache WHERE pubkey = ?1",
                params![pubkey],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        relays_json
            .map(|value| serde_json::from_str(value.as_str()).map_err(PublishProxyError::from))
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    pub fn cache_author_write_relays(
        &self,
        pubkey: &str,
        relays: &[String],
    ) -> Result<(), PublishProxyError> {
        let now = current_unix_millis();
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        connection.execute(
            r#"
            INSERT INTO publish_proxy_relay_list_cache (pubkey, relays_json, updated_at_ms)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(pubkey) DO UPDATE SET
                relays_json = excluded.relays_json,
                updated_at_ms = excluded.updated_at_ms
            "#,
            params![pubkey, serde_json::to_string(relays)?, now],
        )?;
        Ok(())
    }

    fn relay_outcomes(&self, job_id: &str) -> Result<Vec<PublishRelayOutcome>, PublishProxyError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut stmt = connection.prepare(
            r#"
            SELECT relay_url, source, attempted, outcome_kind, message, latency_ms
            FROM publish_proxy_relay_results
            WHERE job_id = ?1
            ORDER BY relay_url
            "#,
        )?;
        let outcomes = stmt
            .query_map(params![job_id], relay_outcome_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(outcomes)
    }
}

struct PublishJobRow {
    principal_id: String,
    request_fingerprint: String,
    view: PublishJobView,
}

fn migrate_schema(connection: &Connection) -> Result<(), PublishProxyError> {
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version < 2 {
        if !table_has_column(connection, "publish_proxy_jobs", "request_fingerprint")? {
            connection.execute(
                "ALTER TABLE publish_proxy_jobs ADD COLUMN request_fingerprint TEXT NOT NULL DEFAULT ''",
                [],
            )?;
        }
        if !table_has_column(connection, "publish_proxy_jobs", "effective_relay_count")? {
            connection.execute(
                "ALTER TABLE publish_proxy_jobs ADD COLUMN effective_relay_count INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
            connection.execute(
                "UPDATE publish_proxy_jobs SET effective_relay_count = requested_relay_count WHERE effective_relay_count = 0",
                [],
            )?;
        }
    }
    Ok(())
}

fn recover_interrupted_publish_jobs(connection: &Connection) -> Result<(), PublishProxyError> {
    let now = current_unix_millis();
    connection.execute(
        r#"
        UPDATE publish_proxy_jobs
        SET status = ?1,
            updated_at_ms = ?2,
            completed_at_ms = ?3,
            last_error = ?4
        WHERE status = ?5
        "#,
        params![
            serde_json::to_string(&PublishJobStatus::DeliveryUnsatisfiedRetryable)?,
            now,
            now,
            "publish_attempt_interrupted",
            serde_json::to_string(&PublishJobStatus::Publishing)?,
        ],
    )?;
    Ok(())
}

fn table_has_column(
    connection: &Connection,
    table: &str,
    column: &str,
) -> Result<bool, PublishProxyError> {
    let mut stmt = connection.prepare(format!("PRAGMA table_info({table})").as_str())?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns.iter().any(|existing| existing == column))
}

fn job_select_sql(tail: &str) -> String {
    format!(
        r#"
        SELECT
            job_id,
            principal_id,
            request_fingerprint,
            status,
            event_id,
            event_pubkey,
            event_kind,
            relay_policy_json,
            delivery_policy_json,
            effective_relay_count,
            requested_at_ms,
            completed_at_ms,
            last_error
        FROM publish_proxy_jobs
        {tail}
        "#
    )
}

fn principal_from_row(row: &Row<'_>) -> Result<PublishPrincipal, rusqlite::Error> {
    let visibility: String = row.get(6)?;
    Ok(PublishPrincipal {
        principal_id: row.get(0)?,
        label: row.get(1)?,
        allowed_pubkeys: json_column(row, 2)?,
        allowed_kinds: json_column(row, 3)?,
        allowed_relay_policies: json_column(row, 4)?,
        allow_request_relays: row.get(5)?,
        job_visibility: PublishJobVisibility::from_str(visibility.as_str())
            .map_err(|error| conversion_error(6, error))?,
        expires_at_unix: row.get(7)?,
    })
}

fn job_from_row(row: &Row<'_>) -> Result<PublishJobRow, rusqlite::Error> {
    let status: PublishJobStatus = json_text(row, 3)?;
    let relay_policy: PublishRelayPolicy = json_text(row, 7)?;
    let delivery_policy: PublishDeliveryPolicy = json_text(row, 8)?;
    let relay_count: i64 = row.get(9)?;
    Ok(PublishJobRow {
        principal_id: row.get(1)?,
        request_fingerprint: row.get(2)?,
        view: PublishJobView {
            job_id: row.get(0)?,
            status,
            terminal: false,
            delivery_satisfied: false,
            event_id: row.get(4)?,
            pubkey: row.get(5)?,
            event_kind: row.get::<_, i64>(6)? as u32,
            relay_policy,
            delivery_policy,
            relay_count: usize::try_from(relay_count).unwrap_or(0),
            acknowledged_count: 0,
            retryable_count: 0,
            terminal_count: 0,
            requested_at_ms: row.get(10)?,
            completed_at_ms: row.get(11)?,
            last_error: row.get(12)?,
            relays: Vec::new(),
        },
    })
}

fn relay_outcome_from_row(row: &Row<'_>) -> Result<PublishRelayOutcome, rusqlite::Error> {
    let source: PublishRelaySource = json_text(row, 1)?;
    let outcome_kind: PublishRelayOutcomeKind = json_text(row, 3)?;
    Ok(PublishRelayOutcome {
        relay_url: row.get(0)?,
        source,
        attempted: row.get(2)?,
        outcome_kind,
        message: row.get(4)?,
        latency_ms: row
            .get::<_, Option<i64>>(5)?
            .map(|latency| u64::try_from(latency).unwrap_or(0)),
    })
}

fn finalize_job_view(view: &mut PublishJobView) {
    view.acknowledged_count = view
        .relays
        .iter()
        .filter(|relay| relay.outcome_kind.counts_toward_quorum())
        .count();
    view.retryable_count = view
        .relays
        .iter()
        .filter(|relay| relay.outcome_kind.is_retryable())
        .count();
    view.terminal_count = view
        .relays
        .iter()
        .filter(|relay| relay.outcome_kind.is_terminal_failure())
        .count();
    view.terminal = matches!(
        view.status,
        PublishJobStatus::DeliverySatisfied
            | PublishJobStatus::DeliveryUnsatisfiedTerminal
            | PublishJobStatus::Rejected
    );
    view.delivery_satisfied = view.status == PublishJobStatus::DeliverySatisfied;
}

fn validate_principal_init(input: &PublishPrincipalInit) -> Result<(), PublishProxyError> {
    if input.label.trim().is_empty() {
        return Err(PublishProxyError::InvalidScope(
            "principal label must not be empty".to_owned(),
        ));
    }
    if !input.token_hash.starts_with(TOKEN_HASH_PREFIX) {
        return Err(PublishProxyError::InvalidScope(
            "principal token hash must use sha256 prefix".to_owned(),
        ));
    }
    if input.allowed_pubkeys.is_empty() {
        return Err(PublishProxyError::InvalidScope(
            "principal must include at least one allowed pubkey".to_owned(),
        ));
    }
    for pubkey in &input.allowed_pubkeys {
        ensure_lower_hex("allowed_pubkey", pubkey, 64)?;
    }
    if input.allowed_kinds.is_empty() {
        return Err(PublishProxyError::InvalidScope(
            "principal must include at least one allowed kind".to_owned(),
        ));
    }
    if input
        .allowed_kinds
        .iter()
        .any(|kind| *kind > u16::MAX as u32)
    {
        return Err(PublishProxyError::InvalidScope(
            "allowed kind exceeds publish proxy range".to_owned(),
        ));
    }
    if input.allowed_relay_policies.is_empty() {
        return Err(PublishProxyError::InvalidScope(
            "principal must include at least one allowed relay policy".to_owned(),
        ));
    }
    Ok(())
}

pub fn generate_bearer_token() -> String {
    let bytes: [u8; 32] = rand::random();
    format!("{TOKEN_PREFIX}{}", hex_lower(&bytes))
}

pub fn hash_bearer_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    format!("{TOKEN_HASH_PREFIX}{}", hex_lower(&hasher.finalize()))
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

pub fn parse_relay_policy(value: &str) -> Result<PublishRelayPolicy, PublishProxyError> {
    match value {
        "explicit_only" => Ok(PublishRelayPolicy::ExplicitOnly),
        "request_then_author_write_then_daemon_default" => {
            Ok(PublishRelayPolicy::RequestThenAuthorWriteThenDaemonDefault)
        }
        "author_write_then_daemon_default" => Ok(PublishRelayPolicy::AuthorWriteThenDaemonDefault),
        "daemon_default_only" => Ok(PublishRelayPolicy::DaemonDefaultOnly),
        other => Err(PublishProxyError::InvalidScope(format!(
            "unknown relay policy `{other}`"
        ))),
    }
}

fn signed_event_from_wire(
    event: &SignedNostrEventWire,
) -> Result<RadrootsSignedNostrEvent, PublishProxyError> {
    event
        .validate()
        .map_err(|error| PublishProxyError::InvalidSignedEvent(error.to_string()))?;
    let created_at = u32::try_from(event.created_at).map_err(|_| {
        PublishProxyError::InvalidSignedEvent(
            "signed event created_at exceeds daemon-supported range".to_owned(),
        )
    })?;
    let raw_json = serde_json::to_string(event)?;
    let radroots_event = RadrootsNostrEvent {
        id: event.id.clone(),
        author: event.pubkey.clone(),
        created_at,
        kind: event.kind,
        tags: event.tags.clone(),
        content: event.content.clone(),
        sig: event.sig.clone(),
    };
    match radroots_nostr_verify_event(&radroots_event) {
        RadrootsNostrEventVerification::Verified => {}
        verification => return Err(PublishProxyError::SignedEventVerification(verification)),
    }
    RadrootsSignedNostrEvent::new(RadrootsSignedNostrEventParts {
        id: event.id.clone(),
        pubkey: event.pubkey.clone(),
        created_at,
        kind: event.kind,
        tags: event.tags.clone(),
        content: event.content.clone(),
        sig: event.sig.clone(),
        raw_json,
    })
    .map_err(PublishProxyError::from)
}

fn request_intent_fingerprint(
    principal_id: &str,
    canonical_event_json: &str,
    request: &PublishEventRequest,
    effective_timeout_ms: u64,
) -> Result<String, PublishProxyError> {
    #[derive(Serialize)]
    struct FingerprintInput<'a> {
        principal_id: &'a str,
        canonical_event_json: &'a str,
        relays: Vec<String>,
        relay_policy: &'a PublishRelayPolicy,
        delivery_policy: &'a PublishDeliveryPolicy,
        effective_timeout_ms: u64,
    }

    let input = FingerprintInput {
        principal_id,
        canonical_event_json,
        relays: request
            .relays
            .iter()
            .map(|relay| relay.trim().to_owned())
            .collect(),
        relay_policy: &request.relay_policy,
        delivery_policy: &request.delivery_policy,
        effective_timeout_ms,
    };
    let bytes = serde_json::to_vec(&input)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(hex_lower(&hasher.finalize()))
}

fn effective_publish_timeout_ms(
    config: &PublishProxyConfig,
    timeout_ms: Option<u64>,
) -> Result<u64, PublishProxyError> {
    let max_timeout_ms = config.connect_timeout_secs.saturating_mul(1_000);
    match timeout_ms {
        Some(0) => Err(PublishProxyError::InvalidSignedEvent(
            "timeout_ms must be greater than zero".to_owned(),
        )),
        Some(timeout_ms) if timeout_ms > max_timeout_ms => {
            Err(PublishProxyError::InvalidSignedEvent(format!(
                "timeout_ms must be at most {max_timeout_ms}"
            )))
        }
        Some(timeout_ms) => Ok(timeout_ms),
        None => Ok(max_timeout_ms),
    }
}

fn push_resolved_relay(
    targets: &mut Vec<ResolvedPublishRelay>,
    url: RadrootsRelayUrl,
    source: PublishRelaySource,
) {
    if !targets.iter().any(|target| target.url == url) {
        targets.push(ResolvedPublishRelay { url, source });
    }
}

fn relay_resolution_connection_failure(
    relay_url: impl Into<String>,
    source: PublishRelaySource,
    message: impl Into<String>,
) -> PublishRelayOutcome {
    PublishRelayOutcome {
        relay_url: relay_url.into(),
        source,
        attempted: false,
        outcome_kind: PublishRelayOutcomeKind::ConnectionFailed,
        message: Some(message.into()),
        latency_ms: None,
    }
}

fn relay_socket_target(url: &RadrootsRelayUrl) -> Result<(String, u16), std::io::Error> {
    let parsed = url::Url::parse(url.as_str())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let host = parsed
        .host_str()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "relay URL must include a DNS host",
            )
        })?
        .to_owned();
    let port = parsed.port_or_known_default().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "relay URL scheme must have a default port",
        )
    })?;
    Ok((host, port))
}

fn relay_url_policy(config: &PublishProxyConfig) -> RadrootsRelayUrlPolicy {
    match config.relay_url_policy {
        crate::app::config::PublishProxyRelayUrlPolicy::Public => RadrootsRelayUrlPolicy::Public,
        crate::app::config::PublishProxyRelayUrlPolicy::Localhost => {
            RadrootsRelayUrlPolicy::Localhost
        }
    }
}

fn author_write_relays_from_nip65_event(
    event: &radroots_nostr::prelude::RadrootsNostrEvent,
) -> Vec<String> {
    event
        .tags
        .iter()
        .filter_map(|tag| {
            let values = tag.as_slice();
            if values.first().map(String::as_str) != Some("r") {
                return None;
            }
            let relay = values.get(1)?.trim();
            if relay.is_empty() {
                return None;
            }
            if values.get(2).map(String::as_str) == Some("read") {
                return None;
            }
            Some(relay.to_owned())
        })
        .collect()
}

fn publish_outcome_from_receipt(
    receipt: RadrootsRelayPublishRelayReceipt,
    source_by_relay: &BTreeMap<String, PublishRelaySource>,
    latency_ms: Option<u64>,
) -> PublishRelayOutcome {
    let source = source_by_relay
        .get(receipt.relay_url.as_str())
        .copied()
        .unwrap_or(PublishRelaySource::DaemonDefault);
    PublishRelayOutcome {
        relay_url: receipt.relay_url,
        source,
        attempted: receipt.attempted,
        outcome_kind: publish_outcome_kind(receipt.outcome.kind),
        message: receipt.outcome.message,
        latency_ms,
    }
}

fn publish_outcome_kind(kind: RadrootsRelayOutcomeKind) -> PublishRelayOutcomeKind {
    match kind {
        RadrootsRelayOutcomeKind::Accepted => PublishRelayOutcomeKind::Accepted,
        RadrootsRelayOutcomeKind::DuplicateAccepted => PublishRelayOutcomeKind::DuplicateAccepted,
        RadrootsRelayOutcomeKind::Blocked => PublishRelayOutcomeKind::Blocked,
        RadrootsRelayOutcomeKind::RateLimited => PublishRelayOutcomeKind::RateLimited,
        RadrootsRelayOutcomeKind::Invalid => PublishRelayOutcomeKind::Invalid,
        RadrootsRelayOutcomeKind::PowRequired => PublishRelayOutcomeKind::PowRequired,
        RadrootsRelayOutcomeKind::Restricted => PublishRelayOutcomeKind::Restricted,
        RadrootsRelayOutcomeKind::AuthRequired => PublishRelayOutcomeKind::AuthRequired,
        RadrootsRelayOutcomeKind::Muted => PublishRelayOutcomeKind::Muted,
        RadrootsRelayOutcomeKind::Unsupported => PublishRelayOutcomeKind::Unsupported,
        RadrootsRelayOutcomeKind::PaymentRequired => PublishRelayOutcomeKind::PaymentRequired,
        RadrootsRelayOutcomeKind::Error => PublishRelayOutcomeKind::Error,
        RadrootsRelayOutcomeKind::Timeout => PublishRelayOutcomeKind::Timeout,
        RadrootsRelayOutcomeKind::ConnectionFailed => PublishRelayOutcomeKind::ConnectionFailed,
        RadrootsRelayOutcomeKind::RelayUrlRejected => PublishRelayOutcomeKind::RelayUrlRejected,
        RadrootsRelayOutcomeKind::SkippedAlreadyAccepted => {
            PublishRelayOutcomeKind::SkippedAlreadyAccepted
        }
        RadrootsRelayOutcomeKind::Unknown => PublishRelayOutcomeKind::Unknown,
    }
}

fn delivery_status(
    delivery_policy: &PublishDeliveryPolicy,
    relay_count: usize,
    outcomes: &[PublishRelayOutcome],
) -> PublishJobStatus {
    let required = delivery_policy.required_ack_count(relay_count);
    let acknowledged = outcomes
        .iter()
        .filter(|outcome| outcome.outcome_kind.counts_toward_quorum())
        .count();
    if acknowledged >= required {
        return PublishJobStatus::DeliverySatisfied;
    }
    if outcomes
        .iter()
        .any(|outcome| outcome.outcome_kind.is_retryable())
    {
        PublishJobStatus::DeliveryUnsatisfiedRetryable
    } else {
        PublishJobStatus::DeliveryUnsatisfiedTerminal
    }
}

fn timeout_receipts(targets: &[ResolvedPublishRelay]) -> Vec<RadrootsRelayPublishRelayReceipt> {
    targets
        .iter()
        .map(|target| {
            RadrootsRelayPublishRelayReceipt::attempted(
                target.url.as_str(),
                RadrootsRelayOutcome::timeout("timeout: publish attempt exceeded daemon bound"),
            )
        })
        .collect()
}

fn transport_error_receipts(
    targets: &[ResolvedPublishRelay],
    error: PublishProxyError,
) -> Vec<RadrootsRelayPublishRelayReceipt> {
    let message = format!("error: {error}");
    targets
        .iter()
        .map(|target| {
            RadrootsRelayPublishRelayReceipt::attempted(
                target.url.as_str(),
                RadrootsRelayOutcome::connection_failed(message.clone()),
            )
        })
        .collect()
}

pub fn write_token_file(path: &Path, token: &str) -> Result<(), PublishProxyError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(path)?;
    file.write_all(token.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

fn ensure_lower_hex(
    field: &str,
    value: &str,
    expected_len: usize,
) -> Result<(), PublishProxyError> {
    if value.len() == expected_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(PublishProxyError::InvalidScope(format!(
            "{field} must be {expected_len} lowercase hex characters"
        )))
    }
}

fn json_column<T: for<'de> Deserialize<'de>>(
    row: &Row<'_>,
    index: usize,
) -> Result<T, rusqlite::Error> {
    let value: String = row.get(index)?;
    serde_json::from_str(value.as_str()).map_err(|error| conversion_error(index, error))
}

fn json_text<T: for<'de> Deserialize<'de>>(
    row: &Row<'_>,
    index: usize,
) -> Result<T, rusqlite::Error> {
    let value: String = row.get(index)?;
    serde_json::from_str(value.as_str()).map_err(|error| conversion_error(index, error))
}

fn conversion_error<E>(index: usize, error: E) -> rusqlite::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
}

fn current_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

fn current_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{
        PublishJobInsert, PublishJobVisibility, PublishPrincipal, PublishPrincipalInit,
        PublishProxy, PublishProxyError, PublishProxyStore, generate_bearer_token,
        hash_bearer_token, parse_relay_policy,
    };
    use crate::app::config::{PublishProxyConfig, PublishProxyRelayUrlPolicy};
    use nostr::JsonUtil;
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::{
        RadrootsNostrEventVerification, RadrootsNostrTimestamp, radroots_nostr_build_event,
    };
    use radroots_publish_proxy_protocol::{
        PublishDeliveryPolicy, PublishEventRequest, PublishJobStatus, PublishRelayOutcomeKind,
        PublishRelayPolicy, PublishRelaySource, SignedNostrEventWire,
    };
    use radroots_relay_transport::{RadrootsMockRelayPublishAdapter, RadrootsRelayOutcome};
    use std::collections::BTreeMap;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    const RELAY_PRIMARY: &str = "wss://relay.example.com";
    const RELAY_SECONDARY: &str = "wss://relay-2.example.com";
    const RELAY_FORBIDDEN: &str = "wss://forbidden-relay.example.com";

    fn event(pubkey: &str, kind: u32) -> SignedNostrEventWire {
        SignedNostrEventWire {
            id: "0".repeat(64),
            pubkey: pubkey.to_owned(),
            created_at: 1_700_000_000,
            kind,
            tags: vec![vec!["d".to_owned(), "listing-1".to_owned()]],
            content: "{}".to_owned(),
            sig: "1".repeat(128),
        }
    }

    fn request(pubkey: &str, kind: u32) -> PublishEventRequest {
        PublishEventRequest {
            event: event(pubkey, kind),
            relays: Vec::new(),
            relay_policy: PublishRelayPolicy::DaemonDefaultOnly,
            delivery_policy: PublishDeliveryPolicy::Any,
            idempotency_key: Some("idem-1".to_owned()),
            timeout_ms: None,
        }
    }

    fn signed_event(identity: &RadrootsIdentity, content: &str) -> SignedNostrEventWire {
        let event = radroots_nostr_build_event(
            30_402,
            content,
            vec![vec!["d".to_owned(), "listing-1".to_owned()]],
        )
        .expect("event builder")
        .custom_created_at(RadrootsNostrTimestamp::from_secs(1_700_000_000))
        .sign_with_keys(identity.keys())
        .expect("signed event");
        serde_json::from_str(event.as_json().as_str()).expect("event wire")
    }

    fn publish_request(
        event: SignedNostrEventWire,
        relays: Vec<String>,
        relay_policy: PublishRelayPolicy,
        delivery_policy: PublishDeliveryPolicy,
        idempotency_key: Option<&str>,
    ) -> PublishEventRequest {
        PublishEventRequest {
            event,
            relays,
            relay_policy,
            delivery_policy,
            idempotency_key: idempotency_key.map(str::to_owned),
            timeout_ms: Some(5_000),
        }
    }

    fn publish_proxy(
        config: PublishProxyConfig,
    ) -> (PublishProxy, RadrootsMockRelayPublishAdapter) {
        publish_proxy_with_resolver(config, Arc::new(StaticPublishRelayResolver::new()))
    }

    fn publish_proxy_with_resolver(
        config: PublishProxyConfig,
        resolver: Arc<dyn super::PublishRelayResolver>,
    ) -> (PublishProxy, RadrootsMockRelayPublishAdapter) {
        let adapter = RadrootsMockRelayPublishAdapter::new();
        let proxy = PublishProxy::memory(config)
            .expect("proxy")
            .with_relay_resolver(resolver)
            .with_publisher(Arc::new(adapter.clone()));
        (proxy, adapter)
    }

    fn principal(
        proxy: &PublishProxy,
        pubkey: String,
        policies: Vec<PublishRelayPolicy>,
        allow_request_relays: bool,
        visibility: PublishJobVisibility,
    ) -> PublishPrincipal {
        proxy
            .store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: hash_bearer_token(generate_bearer_token().as_str()),
                allowed_pubkeys: vec![pubkey],
                allowed_kinds: vec![30_402],
                allowed_relay_policies: policies,
                allow_request_relays,
                job_visibility: visibility,
                expires_at_unix: None,
            })
            .expect("principal")
    }

    fn config_with_defaults(relays: Vec<&str>) -> PublishProxyConfig {
        PublishProxyConfig {
            daemon_default_publish_relays: relays.into_iter().map(str::to_owned).collect(),
            ..PublishProxyConfig::default()
        }
    }

    #[derive(Default)]
    struct StaticPublishRelayResolver {
        results: BTreeMap<String, Result<Vec<IpAddr>, String>>,
    }

    impl StaticPublishRelayResolver {
        fn new() -> Self {
            Self::default()
        }

        fn with_addresses(mut self, url: &str, addresses: Vec<IpAddr>) -> Self {
            self.results.insert(url.to_owned(), Ok(addresses));
            self
        }

        fn with_failure(mut self, url: &str, error: &str) -> Self {
            self.results.insert(url.to_owned(), Err(error.to_owned()));
            self
        }
    }

    impl super::PublishRelayResolver for StaticPublishRelayResolver {
        fn resolve<'a>(
            &'a self,
            url: &'a radroots_relay_transport::RadrootsRelayUrl,
        ) -> super::PublishRelayResolveFuture<'a> {
            Box::pin(async move {
                match self.results.get(url.as_str()) {
                    Some(Ok(addresses)) => Ok(addresses.clone()),
                    Some(Err(error)) => Err(std::io::Error::other(error.clone())),
                    None => Ok(vec![IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))]),
                }
            })
        }
    }

    struct StaticPublishAuthorRelayDiscovery {
        relays: Vec<String>,
    }

    impl StaticPublishAuthorRelayDiscovery {
        fn new(relays: Vec<&str>) -> Self {
            Self {
                relays: relays.into_iter().map(str::to_owned).collect(),
            }
        }
    }

    impl super::PublishAuthorRelayDiscovery for StaticPublishAuthorRelayDiscovery {
        fn fetch_author_write_relays<'a>(
            &'a self,
            _pubkey: &'a str,
            _discovery_targets: Vec<super::ResolvedPublishRelay>,
            _connect_timeout_secs: u64,
        ) -> super::PublishAuthorRelayDiscoveryFuture<'a> {
            let relays = self.relays.clone();
            Box::pin(async move { Ok(relays) })
        }
    }

    #[test]
    fn token_generation_and_hashing_do_not_store_plaintext() {
        let token = generate_bearer_token();
        assert!(token.starts_with("rrd_pp_"));
        let hash = hash_bearer_token(token.as_str());
        assert!(hash.starts_with("sha256:"));
        assert!(!hash.contains(token.as_str()));
    }

    #[test]
    fn relay_policy_parser_accepts_contract_values() {
        assert_eq!(
            parse_relay_policy("explicit_only").expect("policy"),
            PublishRelayPolicy::ExplicitOnly
        );
        assert!(parse_relay_policy("unknown").is_err());
    }

    #[test]
    fn storage_authenticates_hashed_tokens_and_scopes_jobs() {
        let store = PublishProxyStore::memory().expect("store");
        let token = generate_bearer_token();
        let token_hash = hash_bearer_token(token.as_str());
        let principal = store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: token_hash.clone(),
                allowed_pubkeys: vec!["a".repeat(64)],
                allowed_kinds: vec![30_402],
                allowed_relay_policies: vec![PublishRelayPolicy::DaemonDefaultOnly],
                allow_request_relays: false,
                job_visibility: PublishJobVisibility::Own,
                expires_at_unix: None,
            })
            .expect("principal");
        assert_eq!(
            store
                .principal_for_token_hash(token_hash.as_str())
                .expect("lookup")
                .expect("principal")
                .principal_id,
            principal.principal_id
        );
        let denied = request("b".repeat(64).as_str(), 30_402);
        assert!(principal.allows_event(&denied).is_err());

        let accepted = request("a".repeat(64).as_str(), 30_402);
        principal.allows_event(&accepted).expect("scope");
        let response = store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id.clone(),
                idempotency_key: Some("idem-1".to_owned()),
                request: accepted.clone(),
                request_fingerprint: "fingerprint-1".to_owned(),
                effective_relay_count: 1,
            })
            .expect("record job");
        assert!(!response.deduplicated);
        let duplicate = store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id.clone(),
                idempotency_key: Some("idem-1".to_owned()),
                request: accepted,
                request_fingerprint: "fingerprint-1".to_owned(),
                effective_relay_count: 1,
            })
            .expect("dedupe");
        assert!(duplicate.deduplicated);
        assert_eq!(duplicate.job.job_id, response.job.job_id);
        assert_eq!(
            store
                .list_jobs_for_principal(&principal, 50)
                .expect("jobs")
                .len(),
            1
        );
    }

    #[test]
    fn store_open_recovers_interrupted_publishing_jobs() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory.path().join("publish-proxy.sqlite");
        let token_hash = hash_bearer_token(generate_bearer_token().as_str());
        let pubkey = "a".repeat(64);
        let request = request(pubkey.as_str(), 30_402);
        let job_id = {
            let store = PublishProxyStore::open(database_path.clone()).expect("store");
            let principal = store
                .create_principal(PublishPrincipalInit {
                    label: "tester".to_owned(),
                    token_hash,
                    allowed_pubkeys: vec![pubkey],
                    allowed_kinds: vec![30_402],
                    allowed_relay_policies: vec![PublishRelayPolicy::DaemonDefaultOnly],
                    allow_request_relays: false,
                    job_visibility: PublishJobVisibility::Own,
                    expires_at_unix: None,
                })
                .expect("principal");
            let response = store
                .record_publish_job(PublishJobInsert {
                    principal_id: principal.principal_id,
                    idempotency_key: Some("idem-interrupted".to_owned()),
                    request,
                    request_fingerprint: "fingerprint-interrupted".to_owned(),
                    effective_relay_count: 1,
                })
                .expect("record job");
            assert_eq!(response.job.status, PublishJobStatus::Publishing);
            response.job.job_id
        };

        let reopened = PublishProxyStore::open(database_path).expect("reopen store");
        let recovered = reopened.job_by_id(job_id.as_str()).expect("recovered job");
        assert_eq!(
            recovered.status,
            PublishJobStatus::DeliveryUnsatisfiedRetryable
        );
        assert_eq!(
            recovered.last_error.as_deref(),
            Some("publish_attempt_interrupted")
        );
        assert!(recovered.completed_at_ms.is_some());
        assert!(recovered.relays.is_empty());
    }

    #[tokio::test]
    async fn publish_event_verifies_and_records_daemon_default_outcome() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = publish_proxy(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let event = signed_event(&identity, "{}");
        let raw_event = serde_json::to_string(&event).expect("raw event");
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    event,
                    Vec::new(),
                    PublishRelayPolicy::DaemonDefaultOnly,
                    PublishDeliveryPolicy::Any,
                    Some("idem-valid"),
                ),
            )
            .await
            .expect("publish");

        assert!(!response.deduplicated);
        assert_eq!(response.job.status, PublishJobStatus::DeliverySatisfied);
        assert_eq!(response.job.relay_count, 1);
        assert_eq!(response.job.acknowledged_count, 1);
        assert_eq!(response.job.relays[0].relay_url, RELAY_PRIMARY);
        assert_eq!(
            response.job.relays[0].source,
            PublishRelaySource::DaemonDefault
        );
        assert_eq!(adapter.captured_raw_events(), vec![raw_event]);
    }

    #[tokio::test]
    async fn publish_event_rejects_tampered_content_before_publish() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = publish_proxy(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let mut event = signed_event(&identity, "trusted");
        event.content = "tampered".to_owned();
        let error = proxy
            .publish_event(
                &principal,
                publish_request(
                    event,
                    Vec::new(),
                    PublishRelayPolicy::DaemonDefaultOnly,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect_err("tampered event should fail");

        assert!(matches!(
            error,
            PublishProxyError::SignedEventVerification(RadrootsNostrEventVerification::IdMismatch)
        ));
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_rejects_wrong_signature_before_publish() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = publish_proxy(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let mut event = signed_event(&identity, "{}");
        let replacement = if event.sig.starts_with('0') { "1" } else { "0" };
        event.sig.replace_range(0..1, replacement);
        let error = proxy
            .publish_event(
                &principal,
                publish_request(
                    event,
                    Vec::new(),
                    PublishRelayPolicy::DaemonDefaultOnly,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect_err("wrong signature should fail");

        assert!(matches!(
            error,
            PublishProxyError::SignedEventVerification(
                RadrootsNostrEventVerification::SignatureInvalid
            )
        ));
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_rejects_malformed_wire_fields() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = publish_proxy(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let mut event = signed_event(&identity, "{}");
        event.id = event.id.to_uppercase();
        let error = proxy
            .publish_event(
                &principal,
                publish_request(
                    event,
                    Vec::new(),
                    PublishRelayPolicy::DaemonDefaultOnly,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect_err("malformed field should fail");

        assert!(matches!(error, PublishProxyError::InvalidSignedEvent(_)));
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_uses_explicit_request_relays_when_allowed() {
        let identity = RadrootsIdentity::generate();
        let (proxy, _adapter) = publish_proxy(config_with_defaults(vec![RELAY_SECONDARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::RequestThenAuthorWriteThenDaemonDefault],
            true,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    vec![RELAY_PRIMARY.to_owned()],
                    PublishRelayPolicy::RequestThenAuthorWriteThenDaemonDefault,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(response.job.status, PublishJobStatus::DeliverySatisfied);
        assert_eq!(response.job.relays[0].relay_url, RELAY_PRIMARY);
        assert_eq!(response.job.relays[0].source, PublishRelaySource::Request);
    }

    #[tokio::test]
    async fn publish_event_uses_cached_nip65_author_write_before_defaults() {
        let identity = RadrootsIdentity::generate();
        let (proxy, _adapter) = publish_proxy(config_with_defaults(vec![RELAY_SECONDARY]));
        proxy
            .store
            .cache_author_write_relays(
                identity.public_key_hex().as_str(),
                &[RELAY_PRIMARY.to_owned()],
            )
            .expect("cache author relays");
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::AuthorWriteThenDaemonDefault],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    PublishRelayPolicy::AuthorWriteThenDaemonDefault,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(response.job.relays[0].relay_url, RELAY_PRIMARY);
        assert_eq!(
            response.job.relays[0].source,
            PublishRelaySource::AuthorWrite
        );
    }

    #[tokio::test]
    async fn publish_event_records_invalid_cached_author_write_relay() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = publish_proxy(config_with_defaults(vec![RELAY_SECONDARY]));
        proxy
            .store
            .cache_author_write_relays(
                identity.public_key_hex().as_str(),
                &[RELAY_PRIMARY.to_owned(), "not a cached relay".to_owned()],
            )
            .expect("cache author relays");
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::AuthorWriteThenDaemonDefault],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    PublishRelayPolicy::AuthorWriteThenDaemonDefault,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(response.job.status, PublishJobStatus::DeliverySatisfied);
        let accepted = response
            .job
            .relays
            .iter()
            .find(|relay| relay.relay_url == RELAY_PRIMARY)
            .expect("accepted author relay");
        assert_eq!(accepted.source, PublishRelaySource::AuthorWrite);
        assert!(accepted.attempted);
        let rejected = response
            .job
            .relays
            .iter()
            .find(|relay| relay.relay_url == "not a cached relay")
            .expect("rejected cached author relay");
        assert_eq!(rejected.source, PublishRelaySource::AuthorWrite);
        assert_eq!(
            rejected.outcome_kind,
            PublishRelayOutcomeKind::RelayUrlRejected
        );
        assert!(!rejected.attempted);
        assert_eq!(adapter.captured_raw_events().len(), 1);
    }

    #[tokio::test]
    async fn publish_event_preserves_author_and_discovery_rejections_through_fallback() {
        let identity = RadrootsIdentity::generate();
        let mut config = config_with_defaults(vec![RELAY_SECONDARY]);
        config.author_relay_discovery_relays = vec!["not a discovery relay".to_owned()];
        let (proxy, adapter) = publish_proxy(config);
        proxy
            .store
            .cache_author_write_relays(
                identity.public_key_hex().as_str(),
                &["not a cached relay".to_owned()],
            )
            .expect("cache author relays");
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::AuthorWriteThenDaemonDefault],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    PublishRelayPolicy::AuthorWriteThenDaemonDefault,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(response.job.status, PublishJobStatus::DeliverySatisfied);
        let daemon_default = response
            .job
            .relays
            .iter()
            .find(|relay| relay.relay_url == RELAY_SECONDARY)
            .expect("daemon default relay");
        assert_eq!(daemon_default.source, PublishRelaySource::DaemonDefault);
        assert!(daemon_default.attempted);
        let cached = response
            .job
            .relays
            .iter()
            .find(|relay| relay.relay_url == "not a cached relay")
            .expect("cached author rejection");
        assert_eq!(cached.source, PublishRelaySource::AuthorWrite);
        assert_eq!(
            cached.outcome_kind,
            PublishRelayOutcomeKind::RelayUrlRejected
        );
        assert!(!cached.attempted);
        let discovery = response
            .job
            .relays
            .iter()
            .find(|relay| relay.relay_url == "not a discovery relay")
            .expect("discovery relay rejection");
        assert_eq!(discovery.source, PublishRelaySource::DaemonDefault);
        assert_eq!(
            discovery.outcome_kind,
            PublishRelayOutcomeKind::RelayUrlRejected
        );
        assert!(!discovery.attempted);
        assert_eq!(adapter.captured_raw_events().len(), 1);
    }

    #[tokio::test]
    async fn publish_event_preserves_discovery_and_discovered_author_rejections() {
        let identity = RadrootsIdentity::generate();
        let mut config = config_with_defaults(vec![RELAY_PRIMARY]);
        config.author_relay_discovery_relays =
            vec![RELAY_PRIMARY.to_owned(), RELAY_FORBIDDEN.to_owned()];
        let resolver = StaticPublishRelayResolver::new().with_addresses(
            RELAY_FORBIDDEN,
            vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))],
        );
        let adapter = RadrootsMockRelayPublishAdapter::new();
        let proxy = PublishProxy::memory(config)
            .expect("proxy")
            .with_relay_resolver(Arc::new(resolver))
            .with_author_relay_discovery(Arc::new(StaticPublishAuthorRelayDiscovery::new(vec![
                "not a discovered author relay",
                RELAY_SECONDARY,
            ])))
            .with_publisher(Arc::new(adapter.clone()));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::AuthorWriteThenDaemonDefault],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    PublishRelayPolicy::AuthorWriteThenDaemonDefault,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(response.job.status, PublishJobStatus::DeliverySatisfied);
        let accepted = response
            .job
            .relays
            .iter()
            .find(|relay| relay.relay_url == RELAY_SECONDARY)
            .expect("discovered author relay");
        assert_eq!(accepted.source, PublishRelaySource::AuthorWrite);
        assert!(accepted.attempted);
        let discovered = response
            .job
            .relays
            .iter()
            .find(|relay| relay.relay_url == "not a discovered author relay")
            .expect("discovered author rejection");
        assert_eq!(discovered.source, PublishRelaySource::AuthorWrite);
        assert_eq!(
            discovered.outcome_kind,
            PublishRelayOutcomeKind::RelayUrlRejected
        );
        assert!(!discovered.attempted);
        let discovery = response
            .job
            .relays
            .iter()
            .find(|relay| relay.relay_url == RELAY_FORBIDDEN)
            .expect("discovery relay rejection");
        assert_eq!(discovery.source, PublishRelaySource::DaemonDefault);
        assert_eq!(
            discovery.outcome_kind,
            PublishRelayOutcomeKind::RelayUrlRejected
        );
        assert!(!discovery.attempted);
        assert_eq!(adapter.captured_raw_events().len(), 1);
    }

    #[tokio::test]
    async fn publish_event_records_no_publish_relays_failure() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = publish_proxy(PublishProxyConfig::default());
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    PublishRelayPolicy::DaemonDefaultOnly,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(response.job.status, PublishJobStatus::Rejected);
        assert_eq!(
            response.job.last_error.as_deref(),
            Some("no_publish_relays")
        );
        assert!(response.job.relays.is_empty());
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_records_unsafe_request_relay_rejection() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = publish_proxy(PublishProxyConfig::default());
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::ExplicitOnly],
            true,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    vec!["wss://127.0.0.1:7777".to_owned()],
                    PublishRelayPolicy::ExplicitOnly,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(response.job.status, PublishJobStatus::Rejected);
        assert_eq!(response.job.relays.len(), 1);
        assert_eq!(
            response.job.relays[0].outcome_kind,
            PublishRelayOutcomeKind::RelayUrlRejected
        );
        assert!(!response.job.relays[0].attempted);
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_rejects_forbidden_public_dns_destination_before_publish() {
        let identity = RadrootsIdentity::generate();
        let resolver = StaticPublishRelayResolver::new()
            .with_addresses(RELAY_PRIMARY, vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))]);
        let (proxy, adapter) = publish_proxy_with_resolver(
            config_with_defaults(vec![RELAY_PRIMARY]),
            Arc::new(resolver),
        );
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    PublishRelayPolicy::DaemonDefaultOnly,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(response.job.status, PublishJobStatus::Rejected);
        assert_eq!(response.job.relays.len(), 1);
        assert_eq!(
            response.job.relays[0].outcome_kind,
            PublishRelayOutcomeKind::RelayUrlRejected
        );
        assert!(!response.job.relays[0].attempted);
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_records_dns_failure_as_unattempted_retryable_outcome() {
        let identity = RadrootsIdentity::generate();
        let resolver = StaticPublishRelayResolver::new().with_failure(RELAY_PRIMARY, "no records");
        let (proxy, adapter) = publish_proxy_with_resolver(
            config_with_defaults(vec![RELAY_PRIMARY]),
            Arc::new(resolver),
        );
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    PublishRelayPolicy::DaemonDefaultOnly,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            PublishJobStatus::DeliveryUnsatisfiedRetryable
        );
        assert_eq!(
            response.job.last_error.as_deref(),
            Some("delivery_unsatisfied")
        );
        assert_eq!(response.job.relays.len(), 1);
        assert_eq!(
            response.job.relays[0].outcome_kind,
            PublishRelayOutcomeKind::ConnectionFailed
        );
        assert!(!response.job.relays[0].attempted);
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_localhost_policy_skips_public_dns_guard() {
        let identity = RadrootsIdentity::generate();
        let mut config = config_with_defaults(vec!["ws://localhost:7777"]);
        config.relay_url_policy = PublishProxyRelayUrlPolicy::Localhost;
        let resolver = StaticPublishRelayResolver::new()
            .with_failure("ws://localhost:7777", "localhost resolution should not run");
        let (proxy, adapter) = publish_proxy_with_resolver(config, Arc::new(resolver));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    PublishRelayPolicy::DaemonDefaultOnly,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(response.job.status, PublishJobStatus::DeliverySatisfied);
        assert_eq!(response.job.relays[0].relay_url, "ws://localhost:7777");
        assert!(!adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_deduplicates_same_intent_and_conflicts_different_intent() {
        let identity = RadrootsIdentity::generate();
        let (proxy, _adapter) = publish_proxy(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let request = publish_request(
            signed_event(&identity, "{}"),
            Vec::new(),
            PublishRelayPolicy::DaemonDefaultOnly,
            PublishDeliveryPolicy::Any,
            Some("idem-conflict"),
        );
        let first = proxy
            .publish_event(&principal, request.clone())
            .await
            .expect("first");
        let duplicate = proxy
            .publish_event(&principal, request)
            .await
            .expect("duplicate");

        assert!(!first.deduplicated);
        assert!(duplicate.deduplicated);
        assert_eq!(duplicate.job.job_id, first.job.job_id);

        let conflict = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "changed"),
                    Vec::new(),
                    PublishRelayPolicy::DaemonDefaultOnly,
                    PublishDeliveryPolicy::Any,
                    Some("idem-conflict"),
                ),
            )
            .await
            .expect_err("conflict");
        assert!(matches!(
            conflict,
            PublishProxyError::IdempotencyConflict(_)
        ));
    }

    #[tokio::test]
    async fn publish_event_rejects_zero_and_excessive_timeout_before_job_creation() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = publish_proxy(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let mut zero = publish_request(
            signed_event(&identity, "{}"),
            Vec::new(),
            PublishRelayPolicy::DaemonDefaultOnly,
            PublishDeliveryPolicy::Any,
            Some("idem-zero-timeout"),
        );
        zero.timeout_ms = Some(0);
        let zero_error = proxy
            .publish_event(&principal, zero)
            .await
            .expect_err("zero timeout should fail");
        assert!(matches!(
            zero_error,
            PublishProxyError::InvalidSignedEvent(_)
        ));

        let mut excessive = publish_request(
            signed_event(&identity, "changed"),
            Vec::new(),
            PublishRelayPolicy::DaemonDefaultOnly,
            PublishDeliveryPolicy::Any,
            Some("idem-excessive-timeout"),
        );
        excessive.timeout_ms = Some(10_001);
        let excessive_error = proxy
            .publish_event(&principal, excessive)
            .await
            .expect_err("excessive timeout should fail");
        assert!(matches!(
            excessive_error,
            PublishProxyError::InvalidSignedEvent(_)
        ));
        assert!(
            proxy
                .store
                .list_jobs_for_principal(&principal, 50)
                .expect("jobs")
                .is_empty()
        );
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_default_timeout_fingerprints_as_effective_timeout() {
        let identity = RadrootsIdentity::generate();
        let (proxy, _adapter) = publish_proxy(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let event = signed_event(&identity, "{}");
        let mut default_timeout = publish_request(
            event.clone(),
            Vec::new(),
            PublishRelayPolicy::DaemonDefaultOnly,
            PublishDeliveryPolicy::Any,
            Some("idem-default-timeout"),
        );
        default_timeout.timeout_ms = None;
        let mut explicit_default = publish_request(
            event,
            Vec::new(),
            PublishRelayPolicy::DaemonDefaultOnly,
            PublishDeliveryPolicy::Any,
            Some("idem-default-timeout"),
        );
        explicit_default.timeout_ms = Some(10_000);

        let first = proxy
            .publish_event(&principal, default_timeout)
            .await
            .expect("first");
        let duplicate = proxy
            .publish_event(&principal, explicit_default)
            .await
            .expect("duplicate");
        assert!(!first.deduplicated);
        assert!(duplicate.deduplicated);
        assert_eq!(duplicate.job.job_id, first.job.job_id);
    }

    #[tokio::test]
    async fn publish_event_fingerprint_conflicts_on_different_effective_timeout() {
        let identity = RadrootsIdentity::generate();
        let (proxy, _adapter) = publish_proxy(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let event = signed_event(&identity, "{}");
        let first = publish_request(
            event.clone(),
            Vec::new(),
            PublishRelayPolicy::DaemonDefaultOnly,
            PublishDeliveryPolicy::Any,
            Some("idem-timeout-conflict"),
        );
        let mut conflict = publish_request(
            event,
            Vec::new(),
            PublishRelayPolicy::DaemonDefaultOnly,
            PublishDeliveryPolicy::Any,
            Some("idem-timeout-conflict"),
        );
        conflict.timeout_ms = Some(6_000);

        proxy.publish_event(&principal, first).await.expect("first");
        let error = proxy
            .publish_event(&principal, conflict)
            .await
            .expect_err("timeout conflict");
        assert!(matches!(error, PublishProxyError::IdempotencyConflict(_)));
    }

    #[tokio::test]
    async fn publish_event_concurrency_limit_rejects_without_job_creation() {
        let identity = RadrootsIdentity::generate();
        let mut config = config_with_defaults(vec![RELAY_PRIMARY]);
        config.max_concurrent_publish_jobs = 1;
        let (proxy, adapter) = publish_proxy(config);
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let _permit = proxy.acquire_publish_permit().expect("permit");
        let error = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    PublishRelayPolicy::DaemonDefaultOnly,
                    PublishDeliveryPolicy::Any,
                    Some("idem-concurrency"),
                ),
            )
            .await
            .expect_err("concurrency limit");
        assert!(matches!(error, PublishProxyError::ConcurrencyLimit));
        assert!(
            proxy
                .store
                .list_jobs_for_principal(&principal, 50)
                .expect("jobs")
                .is_empty()
        );
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_jobs_respect_own_and_admin_visibility() {
        let identity = RadrootsIdentity::generate();
        let other_identity = RadrootsIdentity::generate();
        let (proxy, _adapter) = publish_proxy(config_with_defaults(vec![RELAY_PRIMARY]));
        let owner = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let other = principal(
            &proxy,
            other_identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let admin = principal(
            &proxy,
            other_identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Admin,
        );
        let response = proxy
            .publish_event(
                &owner,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    PublishRelayPolicy::DaemonDefaultOnly,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert!(
            proxy
                .store
                .job_by_id_for_principal(response.job.job_id.as_str(), &other)
                .expect("other read")
                .is_none()
        );
        assert!(
            proxy
                .store
                .job_by_id_for_principal(response.job.job_id.as_str(), &admin)
                .expect("admin read")
                .is_some()
        );
    }

    #[tokio::test]
    async fn publish_event_records_retryable_relay_failures() {
        let identity = RadrootsIdentity::generate();
        let adapter = RadrootsMockRelayPublishAdapter::new().with_outcome(
            RELAY_PRIMARY,
            RadrootsRelayOutcome::connection_failed("error: unavailable"),
        );
        let proxy = PublishProxy::memory(config_with_defaults(vec![RELAY_PRIMARY]))
            .expect("proxy")
            .with_publisher(Arc::new(adapter));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![PublishRelayPolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    PublishRelayPolicy::DaemonDefaultOnly,
                    PublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            PublishJobStatus::DeliveryUnsatisfiedRetryable
        );
        assert_eq!(response.job.retryable_count, 1);
    }
}
