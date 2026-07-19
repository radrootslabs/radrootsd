use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use radroots_event::draft::{
    RadrootsSignatureVerificationError, RadrootsSignedEvent, RadrootsSignedEventError,
};
use radroots_event::wire::{RadrootsEventWireError, RadrootsNip01EventWire};
use radroots_nostr::prelude::{
    RadrootsNostrClient, RadrootsNostrFilter, RadrootsNostrKind, RadrootsNostrPublicKey,
};
use radroots_transport::{
    RADROOTS_RETICULUM_ENDPOINT_URI, RADROOTS_RETICULUM_UNAVAILABLE_MESSAGE, RadrootsTransportKind,
    RadrootsTransportMeshScopeId, RadrootsTransportSatisfactionClass,
    RadrootsTransportSatisfactionPolicy, RadrootsTransportTarget,
    RadrootsTransportTargetFingerprint, RadrootsTransportTargetLabel,
};
use radroots_transport_nostr::{
    RadrootsNostrClientPublishAdapter, RadrootsRelayOutcome, RadrootsRelayOutcomeKind,
    RadrootsRelayPublishAdapter, RadrootsRelayPublishRelayReceipt, RadrootsRelayPublishRequest,
    RadrootsRelayTargetSet, RadrootsRelayTransportError, RadrootsRelayUrl, RadrootsRelayUrlPolicy,
};
use radroots_transport_publish_protocol::{
    NostrPublishTargetSourcePolicy, TransportPublishDeliveryPolicy, TransportPublishEventRequest,
    TransportPublishEventResponse, TransportPublishJobStatus, TransportPublishJobView,
    TransportPublishOutcomeKind, TransportPublishTarget, TransportPublishTargetOutcome,
    TransportPublishTargetPolicy, TransportPublishTargetPolicyName, TransportPublishTargetSource,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqliteRow};
use sqlx::{Connection as _, Row};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::app::config::TransportPublishConfig;

const TOKEN_PREFIX: &str = "rrd_tp_";
const TOKEN_HASH_PREFIX: &str = "sha256:";
const SCHEMA_VERSION: i64 = 4;
const TRANSPORT_KIND_NOSTR: &str = "nostr";
const TRANSPORT_KIND_RETICULUM: &str = "reticulum";
const TRANSPORT_PUBLISH_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS transport_publish_principals (
    principal_id TEXT PRIMARY KEY NOT NULL,
    label TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    allowed_pubkeys_json TEXT NOT NULL,
    allowed_kinds_json TEXT NOT NULL,
    allowed_target_policies_json TEXT NOT NULL,
    allowed_explicit_transport_kinds_json TEXT NOT NULL,
    allowed_nostr_source_policies_json TEXT NOT NULL,
    allow_request_targets INTEGER NOT NULL,
    job_visibility TEXT NOT NULL,
    expires_at_unix INTEGER,
    revoked_at_unix INTEGER,
    created_at_unix INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS transport_publish_jobs (
    job_id TEXT PRIMARY KEY NOT NULL,
    principal_id TEXT NOT NULL,
    idempotency_key TEXT,
    request_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_pubkey TEXT NOT NULL,
    event_kind INTEGER NOT NULL,
    target_policy_json TEXT NOT NULL,
    delivery_policy_json TEXT NOT NULL,
    requested_target_count INTEGER NOT NULL,
    effective_target_count INTEGER NOT NULL,
    request_json TEXT NOT NULL,
    requested_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER,
    last_error TEXT,
    FOREIGN KEY(principal_id) REFERENCES transport_publish_principals(principal_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS transport_publish_jobs_principal_idempotency_idx
    ON transport_publish_jobs(principal_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS transport_publish_target_results (
    job_id TEXT NOT NULL,
    transport_kind TEXT NOT NULL,
    endpoint_uri TEXT NOT NULL,
    target_scope TEXT NOT NULL,
    target_label TEXT,
    source TEXT NOT NULL,
    attempted INTEGER NOT NULL,
    outcome_kind TEXT NOT NULL,
    message TEXT,
    latency_ms INTEGER,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY(job_id, transport_kind, endpoint_uri, target_scope),
    FOREIGN KEY(job_id) REFERENCES transport_publish_jobs(job_id)
);
CREATE TABLE IF NOT EXISTS transport_publish_target_snapshots (
    job_id TEXT NOT NULL,
    target_index INTEGER NOT NULL,
    transport_kind TEXT NOT NULL,
    endpoint_uri TEXT NOT NULL,
    target_scope TEXT NOT NULL,
    target_label TEXT,
    source TEXT NOT NULL,
    attempted INTEGER NOT NULL,
    outcome_kind TEXT NOT NULL,
    message TEXT,
    latency_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY(job_id, target_index),
    FOREIGN KEY(job_id) REFERENCES transport_publish_jobs(job_id)
);
CREATE TABLE IF NOT EXISTS transport_publish_nostr_author_cache (
    pubkey TEXT PRIMARY KEY NOT NULL,
    relays_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
"#;
const TRANSPORT_PUBLISH_TABLES: &[&str] = &[
    "transport_publish_principals",
    "transport_publish_jobs",
    "transport_publish_target_results",
    "transport_publish_target_snapshots",
    "transport_publish_nostr_author_cache",
];

#[derive(Debug, Error)]
pub enum TransportPublishError {
    #[error("transport publish storage error: {0}")]
    Sqlite(#[from] sqlx::Error),
    #[error("transport publish json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("transport publish io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid transport publish scope: {0}")]
    InvalidScope(String),
    #[error("invalid signed Nostr event: {0}")]
    InvalidSignedEvent(String),
    #[error("signed event wire error: {0}")]
    EventWire(#[from] RadrootsEventWireError),
    #[error("signed event conversion error: {0}")]
    SignedEvent(#[from] RadrootsSignedEventError),
    #[error("signed event signature verification failed: {0}")]
    SignedEventSignature(#[from] RadrootsSignatureVerificationError),
    #[error("transport publish relay error: {0}")]
    Relay(#[from] RadrootsRelayTransportError),
    #[error("transport publish transport error: {0}")]
    Transport(String),
    #[error("transport publish schema incompatible for table `{table}`: {detail}")]
    Schema { table: &'static str, detail: String },
    #[error("transport publish job state validation failed: {0}")]
    InvalidPublishJobState(String),
    #[error("transport publish concurrency limit reached")]
    ConcurrencyLimit,
    #[error("transport publish idempotency conflict for key `{0}`")]
    IdempotencyConflict(String),
}

#[derive(Clone)]
pub struct TransportPublish {
    pub config: TransportPublishConfig,
    pub store: TransportPublishStore,
    publisher: Option<Arc<dyn RadrootsRelayPublishAdapter>>,
    resolver: Arc<dyn PublishRelayResolver>,
    author_relay_discovery: Arc<dyn PublishAuthorRelayDiscovery>,
    publish_jobs: Arc<Semaphore>,
}

impl TransportPublish {
    pub fn open(config: TransportPublishConfig) -> Result<Self, TransportPublishError> {
        let store = TransportPublishStore::open(config.database_path.clone())?;
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

    pub fn memory(config: TransportPublishConfig) -> Result<Self, TransportPublishError> {
        let store = TransportPublishStore::memory()?;
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
    pub(crate) fn with_relay_resolver(mut self, resolver: Arc<dyn PublishRelayResolver>) -> Self {
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

    fn acquire_publish_permit(&self) -> Result<OwnedSemaphorePermit, TransportPublishError> {
        self.publish_jobs
            .clone()
            .try_acquire_owned()
            .map_err(|_| TransportPublishError::ConcurrencyLimit)
    }

    pub async fn publish_event(
        &self,
        principal: &PublishPrincipal,
        request: TransportPublishEventRequest,
    ) -> Result<TransportPublishEventResponse, TransportPublishError> {
        request
            .validate(self.config.max_targets_per_request)
            .map_err(|error| {
                TransportPublishError::InvalidSignedEvent(format!(
                    "publish request validation failed: {error}"
                ))
            })?;
        if request.raw_event_json.len() > self.config.max_event_bytes {
            return Err(TransportPublishError::InvalidSignedEvent(
                "signed event exceeds transport_publish max_event_bytes".to_owned(),
            ));
        }
        let signed_event = signed_event_from_raw_json(request.raw_event_json.as_str())?;
        principal.allows_event(&signed_event, &request)?;
        let effective_timeout_ms = effective_publish_timeout_ms(&self.config, request.timeout_ms)?;
        let _permit = self.acquire_publish_permit()?;
        let request_fingerprint = request_intent_fingerprint(
            principal.principal_id.as_str(),
            signed_event.raw_json(),
            &request,
            effective_timeout_ms,
        )?;
        let resolution = self
            .resolve_targets_for_request(signed_event.pubkey_str(), &request)
            .await?;
        validate_delivery_policy_for_resolution(&request.delivery_policy, &resolution)?;
        let target_snapshots = target_snapshots_from_resolution(&resolution);
        let response = self.store.record_publish_job(PublishJobInsert {
            principal_id: principal.principal_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            event: PublishEventMetadata::from_signed_event(&signed_event),
            request: request.clone(),
            request_fingerprint,
            effective_target_count: resolution.target_count(),
            target_snapshots,
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
        Ok(TransportPublishEventResponse {
            deduplicated: false,
            job: completed,
        })
    }

    pub async fn resolve_targets_for_request(
        &self,
        pubkey: &str,
        request: &TransportPublishEventRequest,
    ) -> Result<PublishRelayResolution, TransportPublishError> {
        match &request.target_policy {
            TransportPublishTargetPolicy::ExplicitTargets { targets } => {
                self.resolve_explicit_targets(targets).await
            }
            TransportPublishTargetPolicy::Nostr {
                source_policy,
                relay_urls,
            } => match source_policy {
                NostrPublishTargetSourcePolicy::ExplicitOnly => {
                    self.resolve_request_relays(relay_urls).await
                }
                NostrPublishTargetSourcePolicy::RequestThenAuthorWriteThenDaemonDefault => {
                    if !relay_urls.is_empty() {
                        self.resolve_request_relays(relay_urls).await
                    } else {
                        self.resolve_author_or_default_relays(pubkey).await
                    }
                }
                NostrPublishTargetSourcePolicy::AuthorWriteThenDaemonDefault => {
                    self.resolve_author_or_default_relays(pubkey).await
                }
                NostrPublishTargetSourcePolicy::DaemonDefaultOnly => {
                    self.resolve_daemon_default_relays().await
                }
            },
        }
    }

    async fn resolve_explicit_targets(
        &self,
        targets: &[TransportPublishTarget],
    ) -> Result<PublishRelayResolution, TransportPublishError> {
        let mut resolved = Vec::new();
        let mut outcomes = Vec::new();
        for (index, target) in targets.iter().enumerate() {
            match RadrootsTransportKind::parse_canonical(target.transport_kind.as_str()).map_err(
                |error| {
                    TransportPublishError::InvalidSignedEvent(format!(
                        "transport target {index} kind is invalid: {error}"
                    ))
                },
            )? {
                RadrootsTransportKind::Nostr => {
                    self.resolve_request_target(&mut resolved, &mut outcomes, target)
                        .await;
                }
                RadrootsTransportKind::Reticulum => {
                    outcomes.push(reticulum_unavailable_outcome(target));
                }
                _ => outcomes.push(unsupported_transport_outcome(target)),
            }
        }
        Ok(PublishRelayResolution {
            targets: resolved,
            outcomes,
        })
    }

    async fn resolve_request_target(
        &self,
        targets: &mut Vec<ResolvedPublishRelay>,
        outcomes: &mut Vec<TransportPublishTargetOutcome>,
        target: &TransportPublishTarget,
    ) {
        match RadrootsRelayUrl::parse(target.endpoint_uri.as_str(), relay_url_policy(&self.config))
        {
            Ok(url) => {
                self.push_checked_relay_target(
                    targets,
                    outcomes,
                    url,
                    TransportPublishTargetSource::Request,
                    PublishTargetMetadata::from_target(target),
                )
                .await;
            }
            Err(error) => outcomes.push(TransportPublishTargetOutcome {
                transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
                endpoint_uri: target.endpoint_uri.trim().to_owned(),
                target_scope: target.target_scope.clone(),
                target_label: target.target_label.clone(),
                source: TransportPublishTargetSource::Request,
                attempted: false,
                outcome_kind: TransportPublishOutcomeKind::TargetRejected,
                message: Some(error.to_string()),
                latency_ms: None,
            }),
        }
    }
    async fn resolve_author_or_default_relays(
        &self,
        pubkey: &str,
    ) -> Result<PublishRelayResolution, TransportPublishError> {
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
    ) -> Result<PublishRelayResolution, TransportPublishError> {
        let mut targets = Vec::new();
        let mut outcomes = Vec::new();
        for relay in relays {
            match RadrootsRelayUrl::parse(relay, relay_url_policy(&self.config)) {
                Ok(url) => {
                    self.push_checked_relay_target(
                        &mut targets,
                        &mut outcomes,
                        url,
                        TransportPublishTargetSource::Request,
                        PublishTargetMetadata::default(),
                    )
                    .await;
                }
                Err(error) => outcomes.push(TransportPublishTargetOutcome {
                    transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
                    endpoint_uri: relay.trim().to_owned(),
                    target_scope: None,
                    target_label: None,
                    source: TransportPublishTargetSource::Request,
                    attempted: false,
                    outcome_kind: TransportPublishOutcomeKind::TargetRejected,
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
    ) -> Result<PublishRelayResolution, TransportPublishError> {
        let cached = self.store.cached_author_write_relays(pubkey)?;
        let mut cached_resolution = self.resolve_author_relay_inputs(&cached).await?;
        if !cached_resolution.targets.is_empty() {
            return Ok(cached_resolution);
        }
        if self.config.nostr.author_relay_discovery_relays.is_empty() {
            return Ok(cached_resolution);
        }
        let mut discovery_targets = self
            .resolve_config_relays(
                &self.config.nostr.author_relay_discovery_relays,
                TransportPublishTargetSource::DaemonDefault,
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
    ) -> Result<PublishRelayResolution, TransportPublishError> {
        let mut targets = Vec::new();
        let mut outcomes = Vec::new();
        for relay in relays {
            match RadrootsRelayUrl::parse(relay, relay_url_policy(&self.config)) {
                Ok(url) => {
                    self.push_checked_relay_target(
                        &mut targets,
                        &mut outcomes,
                        url,
                        TransportPublishTargetSource::NostrAuthorWrite,
                        PublishTargetMetadata::default(),
                    )
                    .await;
                }
                Err(error) => outcomes.push(TransportPublishTargetOutcome {
                    transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
                    endpoint_uri: relay.trim().to_owned(),
                    target_scope: None,
                    target_label: None,
                    source: TransportPublishTargetSource::NostrAuthorWrite,
                    attempted: false,
                    outcome_kind: TransportPublishOutcomeKind::TargetRejected,
                    message: Some(error.to_string()),
                    latency_ms: None,
                }),
            }
        }
        Ok(PublishRelayResolution { targets, outcomes })
    }

    async fn resolve_daemon_default_relays(
        &self,
    ) -> Result<PublishRelayResolution, TransportPublishError> {
        self.resolve_config_relays(
            &self.config.nostr.daemon_default_relays,
            TransportPublishTargetSource::DaemonDefault,
        )
        .await
    }

    async fn resolve_config_relays(
        &self,
        relays: &[String],
        source: TransportPublishTargetSource,
    ) -> Result<PublishRelayResolution, TransportPublishError> {
        let mut targets = Vec::new();
        let mut outcomes = Vec::new();
        for relay in relays {
            match RadrootsRelayUrl::parse(relay, relay_url_policy(&self.config)) {
                Ok(url) => {
                    self.push_checked_relay_target(
                        &mut targets,
                        &mut outcomes,
                        url,
                        source,
                        PublishTargetMetadata::default(),
                    )
                    .await;
                }
                Err(error) => outcomes.push(TransportPublishTargetOutcome {
                    transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
                    endpoint_uri: relay.trim().to_owned(),
                    target_scope: None,
                    target_label: None,
                    source,
                    attempted: false,
                    outcome_kind: TransportPublishOutcomeKind::TargetRejected,
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
        outcomes: &mut Vec<TransportPublishTargetOutcome>,
        url: RadrootsRelayUrl,
        source: TransportPublishTargetSource,
        metadata: PublishTargetMetadata,
    ) {
        if relay_url_policy(&self.config) == RadrootsRelayUrlPolicy::Localhost {
            push_resolved_relay(targets, url, source, metadata);
            return;
        }
        match self.resolver.resolve(&url).await {
            Ok(addresses) if addresses.is_empty() => {
                outcomes.push(relay_resolution_connection_failure(
                    url.as_str(),
                    source,
                    &metadata,
                    "dns lookup returned no addresses",
                ));
            }
            Ok(addresses) => match url.validate_public_resolved_ip_addrs(addresses) {
                Ok(()) => push_resolved_relay(targets, url, source, metadata),
                Err(error) => outcomes.push(TransportPublishTargetOutcome {
                    transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
                    endpoint_uri: url.as_str().to_owned(),
                    target_scope: metadata.target_scope,
                    target_label: metadata.target_label,
                    source,
                    attempted: false,
                    outcome_kind: TransportPublishOutcomeKind::TargetRejected,
                    message: Some(error.to_string()),
                    latency_ms: None,
                }),
            },
            Err(error) => outcomes.push(relay_resolution_connection_failure(
                url.as_str(),
                source,
                &metadata,
                format!("dns lookup failed: {error}"),
            )),
        }
    }

    async fn complete_job_execution(
        &self,
        job_id: &str,
        signed_event: RadrootsSignedEvent,
        delivery_policy: TransportPublishDeliveryPolicy,
        timeout_ms: u64,
        resolution: PublishRelayResolution,
    ) -> Result<TransportPublishJobView, TransportPublishError> {
        let target_count = resolution.target_count();
        if resolution.targets.is_empty() {
            let status = if resolution.outcomes.is_empty() {
                TransportPublishJobStatus::Rejected
            } else {
                delivery_status(&delivery_policy, target_count, &resolution.outcomes)
            };
            let last_error = last_error_for_status(status);
            self.store.complete_publish_job(
                job_id,
                status,
                resolution.outcomes,
                last_error.map(str::to_owned),
            )?;
            return self.store.job_by_id(job_id);
        }
        let required_target_count = delivery_policy.required_target_count(target_count);
        if required_target_count > target_count {
            self.store.complete_publish_job(
                job_id,
                TransportPublishJobStatus::Rejected,
                resolution.outcomes,
                Some("delivery_quorum_exceeds_target_count".to_owned()),
            )?;
            return self.store.job_by_id(job_id);
        }
        let target_set = RadrootsRelayTargetSet::from_urls(
            resolution
                .targets
                .iter()
                .map(|target| target.url.clone())
                .collect(),
        )?;
        let satisfaction_policy = satisfaction_policy_from_delivery_policy(
            &delivery_policy,
            target_count,
            resolution.targets.as_slice(),
        )?;
        let publish_relays = target_set.relays().to_vec();
        let publish_request =
            RadrootsRelayPublishRequest::new(signed_event, target_set, current_unix_millis())
                .with_satisfaction_policy(satisfaction_policy);
        let started = Instant::now();
        let publish_timeout = Duration::from_millis(timeout_ms);
        let receipts =
            match tokio::time::timeout(publish_timeout, self.publish_with_adapter(publish_request))
                .await
            {
                Ok(Ok(receipts)) => receipts,
                Ok(Err(error)) => transport_error_receipts(publish_relays.as_slice(), error),
                Err(_) => timeout_receipts(publish_relays.as_slice()),
            };
        let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let mut outcomes = resolution.outcomes;
        outcomes.extend(receipts.into_iter().flat_map(|receipt| {
            publish_outcomes_from_receipt(receipt, &resolution.targets, Some(latency_ms))
        }));
        let status = delivery_status(&delivery_policy, target_count, &outcomes);
        let last_error = last_error_for_status(status).map(str::to_owned);
        self.store
            .complete_publish_job(job_id, status, outcomes, last_error)?;
        self.store.job_by_id(job_id)
    }

    async fn publish_with_adapter(
        &self,
        request: RadrootsRelayPublishRequest,
    ) -> Result<Vec<RadrootsRelayPublishRelayReceipt>, TransportPublishError> {
        if let Some(publisher) = &self.publisher {
            return publisher
                .publish(request)
                .await
                .map_err(TransportPublishError::Relay);
        }
        let adapter = RadrootsNostrClientPublishAdapter::new(RadrootsNostrClient::new_signerless());
        adapter
            .publish(request)
            .await
            .map_err(TransportPublishError::Relay)
    }
}

#[derive(Clone)]
pub struct TransportPublishStore {
    inner: Arc<Mutex<SqliteConnection>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishJobVisibility {
    Own,
    Admin,
}

impl FromStr for PublishJobVisibility {
    type Err = TransportPublishError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "own" => Ok(Self::Own),
            "admin" => Ok(Self::Admin),
            other => Err(TransportPublishError::InvalidScope(format!(
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
    pub allowed_target_policies: Vec<TransportPublishTargetPolicyName>,
    pub allowed_explicit_transport_kinds: Vec<String>,
    pub allowed_nostr_source_policies: Vec<NostrPublishTargetSourcePolicy>,
    pub allow_request_targets: bool,
    pub job_visibility: PublishJobVisibility,
    pub expires_at_unix: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishPrincipal {
    pub principal_id: String,
    pub label: String,
    pub allowed_pubkeys: Vec<String>,
    pub allowed_kinds: Vec<u32>,
    pub allowed_target_policies: Vec<TransportPublishTargetPolicyName>,
    pub allowed_explicit_transport_kinds: Vec<String>,
    pub allowed_nostr_source_policies: Vec<NostrPublishTargetSourcePolicy>,
    pub allow_request_targets: bool,
    pub job_visibility: PublishJobVisibility,
    pub expires_at_unix: Option<i64>,
}

impl PublishPrincipal {
    pub fn allows_event(
        &self,
        signed_event: &RadrootsSignedEvent,
        request: &TransportPublishEventRequest,
    ) -> Result<(), TransportPublishError> {
        let pubkey = signed_event.pubkey_str();
        ensure_lower_hex("pubkey", pubkey, 64)?;
        if !self
            .allowed_pubkeys
            .iter()
            .any(|allowed_pubkey| allowed_pubkey == pubkey)
        {
            return Err(TransportPublishError::InvalidScope(
                "principal is not allowed to publish for event pubkey".to_owned(),
            ));
        }
        if !self.allowed_kinds.contains(&signed_event.kind()) {
            return Err(TransportPublishError::InvalidScope(
                "principal is not allowed to publish event kind".to_owned(),
            ));
        }
        match &request.target_policy {
            TransportPublishTargetPolicy::ExplicitTargets { targets } => {
                if !self
                    .allowed_target_policies
                    .contains(&TransportPublishTargetPolicyName::ExplicitTargets)
                {
                    return Err(TransportPublishError::InvalidScope(
                        "principal is not allowed to use explicit transport targets".to_owned(),
                    ));
                }
                if !self.allow_request_targets && !targets.is_empty() {
                    return Err(TransportPublishError::InvalidScope(
                        "principal is not allowed to provide request targets".to_owned(),
                    ));
                }
                for target in targets {
                    let kind =
                        RadrootsTransportKind::parse_canonical(target.transport_kind.as_str())
                            .map_err(|error| {
                                TransportPublishError::InvalidScope(format!(
                                    "principal explicit target kind check failed: {error}"
                                ))
                            })?;
                    let transport_kind = kind.canonical_label();
                    if !self
                        .allowed_explicit_transport_kinds
                        .iter()
                        .any(|allowed| allowed == &transport_kind)
                    {
                        return Err(TransportPublishError::InvalidScope(format!(
                            "principal is not allowed to use explicit transport target kind `{transport_kind}`"
                        )));
                    }
                }
            }
            TransportPublishTargetPolicy::Nostr {
                source_policy,
                relay_urls,
            } => {
                if !self
                    .allowed_target_policies
                    .contains(&TransportPublishTargetPolicyName::Nostr)
                {
                    return Err(TransportPublishError::InvalidScope(
                        "principal is not allowed to use Nostr target policy".to_owned(),
                    ));
                }
                if !self.allowed_nostr_source_policies.contains(source_policy) {
                    return Err(TransportPublishError::InvalidScope(
                        "principal is not allowed to use requested Nostr source policy".to_owned(),
                    ));
                }
                if !self.allow_request_targets && !relay_urls.is_empty() {
                    return Err(TransportPublishError::InvalidScope(
                        "principal is not allowed to provide request targets".to_owned(),
                    ));
                }
            }
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
    pub event: PublishEventMetadata,
    pub request: TransportPublishEventRequest,
    pub request_fingerprint: String,
    pub effective_target_count: usize,
    pub target_snapshots: Vec<TransportPublishTargetOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishEventMetadata {
    pub event_id: String,
    pub pubkey: String,
    pub kind: u32,
}

impl PublishEventMetadata {
    fn from_signed_event(signed_event: &RadrootsSignedEvent) -> Self {
        Self {
            event_id: signed_event.id_str().to_owned(),
            pubkey: signed_event.pubkey_str().to_owned(),
            kind: signed_event.kind(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPublishRelay {
    pub url: RadrootsRelayUrl,
    pub source: TransportPublishTargetSource,
    target_scope: Option<String>,
    target_label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRelayResolution {
    pub targets: Vec<ResolvedPublishRelay>,
    pub outcomes: Vec<TransportPublishTargetOutcome>,
}

impl PublishRelayResolution {
    fn target_count(&self) -> usize {
        self.targets.len() + self.outcomes.len()
    }

    fn target_fingerprints(
        &self,
    ) -> Result<Vec<RadrootsTransportTargetFingerprint>, TransportPublishError> {
        let mut fingerprints = Vec::with_capacity(self.target_count());
        for target in &self.targets {
            fingerprints.push(target.fingerprint()?);
        }
        for (index, outcome) in self.outcomes.iter().enumerate() {
            fingerprints.push(target_outcome_fingerprint(outcome, index)?);
        }
        Ok(fingerprints)
    }
}

impl ResolvedPublishRelay {
    fn fingerprint(&self) -> Result<RadrootsTransportTargetFingerprint, TransportPublishError> {
        let scope = self
            .target_scope
            .as_deref()
            .map(RadrootsTransportMeshScopeId::parse)
            .transpose()
            .map_err(|error| TransportPublishError::Transport(error.to_string()))?;
        let label = self
            .target_label
            .as_deref()
            .map(RadrootsTransportTargetLabel::parse)
            .transpose()
            .map_err(|error| TransportPublishError::Transport(error.to_string()))?;
        let target =
            RadrootsTransportTarget::nostr_relay_with_metadata(self.url.as_str(), scope, label)
                .map_err(|error| TransportPublishError::Transport(error.to_string()))?;
        Ok(target.fingerprint)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PublishTargetMetadata {
    target_scope: Option<String>,
    target_label: Option<String>,
}

impl PublishTargetMetadata {
    fn from_target(target: &TransportPublishTarget) -> Self {
        Self {
            target_scope: target.target_scope.clone(),
            target_label: target.target_label.clone(),
        }
    }
}

pub(crate) type PublishRelayResolveFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, std::io::Error>> + Send + 'a>>;

pub(crate) trait PublishRelayResolver: Send + Sync {
    fn resolve<'a>(&'a self, url: &'a RadrootsRelayUrl) -> PublishRelayResolveFuture<'a>;
}

type PublishAuthorRelayDiscoveryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<String>, TransportPublishError>> + Send + 'a>>;

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

impl TransportPublishStore {
    pub fn open(path: PathBuf) -> Result<Self, TransportPublishError> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let connection = connect_sqlite(
            SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true),
        )?;
        Self::from_connection(connection)
    }

    pub fn memory() -> Result<Self, TransportPublishError> {
        Self::from_connection(connect_sqlite(SqliteConnectOptions::new().in_memory(true))?)
    }

    fn from_connection(mut connection: SqliteConnection) -> Result<Self, TransportPublishError> {
        execute_sql(&mut connection, "PRAGMA foreign_keys = ON")?;
        match transport_publish_schema_state(&mut connection)? {
            TransportPublishSchemaState::Fresh => {
                execute_raw_sql(&mut connection, TRANSPORT_PUBLISH_SCHEMA_SQL)?;
                execute_sql(
                    &mut connection,
                    format!("PRAGMA user_version = {SCHEMA_VERSION}").as_str(),
                )?;
                validate_transport_publish_schema(&mut connection)?;
            }
            TransportPublishSchemaState::Existing => {
                validate_transport_publish_schema_version(&mut connection)?;
                validate_transport_publish_schema(&mut connection)?;
            }
        }
        recover_interrupted_publish_jobs(&mut connection)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn create_principal(
        &self,
        input: PublishPrincipalInit,
    ) -> Result<PublishPrincipal, TransportPublishError> {
        validate_principal_init(&input)?;
        let principal_id = Uuid::new_v4().to_string();
        let now = current_unix_secs();
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut connection = connection;
        block_on_sqlite(
            sqlx::query(
                r#"
            INSERT INTO transport_publish_principals (
                principal_id,
                label,
                token_hash,
                allowed_pubkeys_json,
                allowed_kinds_json,
                allowed_target_policies_json,
                allowed_explicit_transport_kinds_json,
                allowed_nostr_source_policies_json,
                allow_request_targets,
                job_visibility,
                expires_at_unix,
                revoked_at_unix,
                created_at_unix
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, NULL, ?12)
            "#,
            )
            .bind(principal_id.as_str())
            .bind(input.label.trim())
            .bind(input.token_hash.as_str())
            .bind(serde_json::to_string(&input.allowed_pubkeys)?)
            .bind(serde_json::to_string(&input.allowed_kinds)?)
            .bind(serde_json::to_string(&input.allowed_target_policies)?)
            .bind(serde_json::to_string(
                &input.allowed_explicit_transport_kinds,
            )?)
            .bind(serde_json::to_string(&input.allowed_nostr_source_policies)?)
            .bind(input.allow_request_targets)
            .bind(input.job_visibility.to_string())
            .bind(input.expires_at_unix)
            .bind(now)
            .execute(&mut *connection),
        )?;
        drop(connection);
        self.principal_by_id(principal_id.as_str())?.ok_or_else(|| {
            TransportPublishError::InvalidScope("created principal missing".to_owned())
        })
    }

    pub fn principal_for_token_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<PublishPrincipal>, TransportPublishError> {
        let now = current_unix_secs();
        let mut connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let principal = block_on_sqlite(
            sqlx::query(
                r#"
                SELECT
                    principal_id,
                    label,
                    allowed_pubkeys_json,
                    allowed_kinds_json,
                    allowed_target_policies_json,
                    allowed_explicit_transport_kinds_json,
                    allowed_nostr_source_policies_json,
                    allow_request_targets,
                    job_visibility,
                    expires_at_unix
                FROM transport_publish_principals
                WHERE token_hash = ?1
                  AND revoked_at_unix IS NULL
                  AND (expires_at_unix IS NULL OR expires_at_unix > ?2)
                "#,
            )
            .bind(token_hash)
            .bind(now)
            .fetch_optional(&mut *connection),
        )?
        .map(|row| principal_from_row(&row))
        .transpose()?;
        Ok(principal)
    }

    pub fn principal_by_id(
        &self,
        principal_id: &str,
    ) -> Result<Option<PublishPrincipal>, TransportPublishError> {
        let mut connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let principal = block_on_sqlite(
            sqlx::query(
                r#"
                SELECT
                    principal_id,
                    label,
                    allowed_pubkeys_json,
                    allowed_kinds_json,
                    allowed_target_policies_json,
                    allowed_explicit_transport_kinds_json,
                    allowed_nostr_source_policies_json,
                    allow_request_targets,
                    job_visibility,
                    expires_at_unix
                FROM transport_publish_principals
                WHERE principal_id = ?1
                "#,
            )
            .bind(principal_id)
            .fetch_optional(&mut *connection),
        )?
        .map(|row| principal_from_row(&row))
        .transpose()?;
        Ok(principal)
    }

    pub fn record_publish_job(
        &self,
        insert: PublishJobInsert,
    ) -> Result<TransportPublishEventResponse, TransportPublishError> {
        if insert.effective_target_count != insert.target_snapshots.len() {
            return Err(TransportPublishError::InvalidScope(
                "publish job target snapshot count must match effective target count".to_owned(),
            ));
        }
        if let Some(idempotency_key) = insert.idempotency_key.as_deref()
            && let Some(existing) =
                self.job_for_principal_id_and_key(insert.principal_id.as_str(), idempotency_key)?
        {
            if existing.request_fingerprint != insert.request_fingerprint {
                return Err(TransportPublishError::IdempotencyConflict(
                    idempotency_key.to_owned(),
                ));
            }
            return Ok(TransportPublishEventResponse {
                deduplicated: true,
                job: existing.view,
            });
        }

        let job_id = Uuid::new_v4().to_string();
        let now = current_unix_millis();
        let request_json = serde_json::to_string(&insert.request)?;
        let requested_target_count = storage_count_i64(
            insert.request.target_policy.request_target_count(),
            "requested_target_count",
        )?;
        let effective_target_count =
            storage_count_i64(insert.effective_target_count, "effective_target_count")?;
        let mut connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        execute_sql(&mut connection, "BEGIN")?;
        let transaction_result = (|| -> Result<(), TransportPublishError> {
            let insert_result = block_on_sqlite(
                sqlx::query(
                    r#"
                    INSERT INTO transport_publish_jobs (
                        job_id,
                        principal_id,
                        idempotency_key,
                        request_fingerprint,
                        status,
                        event_id,
                        event_pubkey,
                        event_kind,
                        target_policy_json,
                        delivery_policy_json,
                        requested_target_count,
                        effective_target_count,
                        request_json,
                        requested_at_ms,
                        updated_at_ms
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                    "#,
                )
                .bind(job_id.as_str())
                .bind(insert.principal_id.as_str())
                .bind(insert.idempotency_key.as_deref())
                .bind(insert.request_fingerprint.as_str())
                .bind(serde_json::to_string(
                    &TransportPublishJobStatus::Publishing,
                )?)
                .bind(insert.event.event_id.as_str())
                .bind(insert.event.pubkey.as_str())
                .bind(i64::from(insert.event.kind))
                .bind(serde_json::to_string(&insert.request.target_policy)?)
                .bind(serde_json::to_string(&insert.request.delivery_policy)?)
                .bind(requested_target_count)
                .bind(effective_target_count)
                .bind(request_json.as_str())
                .bind(now)
                .bind(now)
                .execute(&mut *connection),
            );
            match insert_result {
                Ok(_) => {}
                Err(error) if is_sqlite_constraint_error(&error) => {
                    return Err(TransportPublishError::IdempotencyConflict(
                        "idempotency key conflicts with an existing publish job".to_owned(),
                    ));
                }
                Err(error) => return Err(error),
            }
            insert_target_snapshots(
                &mut connection,
                job_id.as_str(),
                &insert.target_snapshots,
                now,
            )?;
            Ok(())
        })();
        match transaction_result {
            Ok(()) => {
                execute_sql(&mut connection, "COMMIT")?;
            }
            Err(error) => {
                let _ = execute_sql(&mut connection, "ROLLBACK");
                return Err(error);
            }
        }
        drop(connection);
        let job = self.job_by_id(job_id.as_str())?;
        Ok(TransportPublishEventResponse {
            deduplicated: false,
            job,
        })
    }

    pub fn job_by_id_for_principal(
        &self,
        job_id: &str,
        principal: &PublishPrincipal,
    ) -> Result<Option<TransportPublishJobView>, TransportPublishError> {
        let mut connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sql = job_select_sql("WHERE job_id = ?1");
        let row = block_on_sqlite(
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(job_id)
                .fetch_optional(&mut *connection),
        )?
        .map(|row| job_from_row(&row))
        .transpose()?;
        drop(connection);
        let Some(job) = row else {
            return Ok(None);
        };
        if !principal.can_read_job(job.principal_id.as_str()) {
            return Ok(None);
        }
        let job = self.finalize_job_row_for_egress(job)?;
        Ok(Some(job.view))
    }

    pub fn list_jobs_for_principal(
        &self,
        principal: &PublishPrincipal,
        limit: usize,
    ) -> Result<Vec<TransportPublishJobView>, TransportPublishError> {
        let limit = i64::try_from(limit.clamp(1, 200)).unwrap_or(200);
        let mut connection = self
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
        let rows = if principal.job_visibility == PublishJobVisibility::Admin {
            block_on_sqlite(
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(limit)
                    .fetch_all(&mut *connection),
            )?
        } else {
            block_on_sqlite(
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(principal.principal_id.as_str())
                    .bind(limit)
                    .fetch_all(&mut *connection),
            )?
        };
        let rows = rows
            .iter()
            .map(job_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        drop(connection);

        rows.into_iter()
            .map(|row| {
                let row = self.finalize_job_row_for_egress(row)?;
                Ok(row.view)
            })
            .collect()
    }

    fn job_for_principal_id_and_key(
        &self,
        principal_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<PublishJobRow>, TransportPublishError> {
        let mut connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sql = job_select_sql("WHERE principal_id = ?1 AND idempotency_key = ?2");
        let row = block_on_sqlite(
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(principal_id)
                .bind(idempotency_key)
                .fetch_optional(&mut *connection),
        )?
        .map(|row| job_from_row(&row))
        .transpose()?;
        drop(connection);
        let Some(job) = row else {
            return Ok(None);
        };
        let job = self.finalize_job_row_for_egress(job)?;
        Ok(Some(job))
    }

    pub fn job_by_id(
        &self,
        job_id: &str,
    ) -> Result<TransportPublishJobView, TransportPublishError> {
        let mut connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sql = job_select_sql("WHERE job_id = ?1");
        let row = block_on_sqlite(
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(job_id)
                .fetch_optional(&mut *connection),
        )?
        .map(|row| job_from_row(&row))
        .transpose()?;
        drop(connection);
        let Some(job) = row else {
            return Err(TransportPublishError::InvalidScope(
                "unknown publish job".to_owned(),
            ));
        };
        let job = self.finalize_job_row_for_egress(job)?;
        Ok(job.view)
    }

    pub fn complete_publish_job(
        &self,
        job_id: &str,
        status: TransportPublishJobStatus,
        outcomes: Vec<TransportPublishTargetOutcome>,
        last_error: Option<String>,
    ) -> Result<(), TransportPublishError> {
        let now = current_unix_millis();
        let target_count = storage_count_i64(outcomes.len(), "effective_target_count")?;
        let mut connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        block_on_sqlite(
            sqlx::query(
                r#"
            UPDATE transport_publish_jobs
            SET status = ?2,
                updated_at_ms = ?3,
                completed_at_ms = ?4,
                last_error = ?5,
                effective_target_count = ?6
            WHERE job_id = ?1
            "#,
            )
            .bind(job_id)
            .bind(serde_json::to_string(&status)?)
            .bind(now)
            .bind(now)
            .bind(last_error.as_deref())
            .bind(target_count)
            .execute(&mut *connection),
        )?;
        replace_target_outcomes(&mut connection, job_id, &outcomes, now)?;
        Ok(())
    }

    pub fn cached_author_write_relays(
        &self,
        pubkey: &str,
    ) -> Result<Vec<String>, TransportPublishError> {
        let mut connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let relays_json = block_on_sqlite(
            sqlx::query_scalar::<_, String>(
                "SELECT relays_json FROM transport_publish_nostr_author_cache WHERE pubkey = ?1",
            )
            .bind(pubkey)
            .fetch_optional(&mut *connection),
        )?;
        relays_json
            .map(|value| serde_json::from_str(value.as_str()).map_err(TransportPublishError::from))
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    pub fn cache_author_write_relays(
        &self,
        pubkey: &str,
        relays: &[String],
    ) -> Result<(), TransportPublishError> {
        let now = current_unix_millis();
        let mut connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        block_on_sqlite(
            sqlx::query(
                r#"
            INSERT INTO transport_publish_nostr_author_cache (pubkey, relays_json, updated_at_ms)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(pubkey) DO UPDATE SET
                relays_json = excluded.relays_json,
                updated_at_ms = excluded.updated_at_ms
            "#,
            )
            .bind(pubkey)
            .bind(serde_json::to_string(relays)?)
            .bind(now)
            .execute(&mut *connection),
        )?;
        Ok(())
    }

    fn target_outcomes(
        &self,
        job_id: &str,
    ) -> Result<Vec<TransportPublishTargetOutcome>, TransportPublishError> {
        let mut connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let rows = block_on_sqlite(
            sqlx::query(
            r#"
            SELECT transport_kind, endpoint_uri, target_scope, target_label, source, attempted, outcome_kind, message, latency_ms
            FROM transport_publish_target_results
            WHERE job_id = ?1
            ORDER BY transport_kind, endpoint_uri, target_scope
            "#,
            )
            .bind(job_id)
            .fetch_all(&mut *connection),
        )?;
        let outcomes = rows
            .iter()
            .map(target_outcome_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(outcomes)
    }

    fn finalize_job_row_for_egress(
        &self,
        mut job: PublishJobRow,
    ) -> Result<PublishJobRow, TransportPublishError> {
        job.view.targets = self.target_outcomes(job.view.job_id.as_str())?;
        finalize_job_view(&mut job.view);
        job.view
            .validate()
            .map_err(|error| TransportPublishError::InvalidPublishJobState(error.to_string()))?;
        Ok(job)
    }
}

struct PublishJobRow {
    principal_id: String,
    request_fingerprint: String,
    view: TransportPublishJobView,
}

enum TransportPublishSchemaState {
    Fresh,
    Existing,
}

fn connect_sqlite(
    options: SqliteConnectOptions,
) -> Result<SqliteConnection, TransportPublishError> {
    block_on_sqlite(SqliteConnection::connect_with(&options))
}

fn block_on_sqlite<T>(
    future: impl Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, TransportPublishError> {
    Ok(futures_executor::block_on(future)?)
}

fn execute_sql(connection: &mut SqliteConnection, sql: &str) -> Result<u64, TransportPublishError> {
    Ok(block_on_sqlite(sqlx::query(sqlx::AssertSqlSafe(sql)).execute(connection))?.rows_affected())
}

fn execute_raw_sql(
    connection: &mut SqliteConnection,
    sql: &str,
) -> Result<(), TransportPublishError> {
    block_on_sqlite(sqlx::raw_sql(sqlx::AssertSqlSafe(sql)).execute(connection))?;
    Ok(())
}

fn fetch_all_sql(
    connection: &mut SqliteConnection,
    sql: &str,
) -> Result<Vec<SqliteRow>, TransportPublishError> {
    block_on_sqlite(sqlx::query(sqlx::AssertSqlSafe(sql)).fetch_all(connection))
}

fn is_sqlite_constraint_error(error: &TransportPublishError) -> bool {
    match error {
        TransportPublishError::Sqlite(sqlx::Error::Database(error)) => {
            error.is_unique_violation()
                || error
                    .code()
                    .as_deref()
                    .is_some_and(|code| matches!(code, "1555" | "2067" | "19"))
        }
        _ => false,
    }
}

fn transport_publish_schema_state(
    connection: &mut SqliteConnection,
) -> Result<TransportPublishSchemaState, TransportPublishError> {
    let rows = block_on_sqlite(
        sqlx::query(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE 'transport_publish_%'",
        )
        .fetch_all(&mut *connection),
    )?;
    let names = rows
        .iter()
        .map(|row| row.try_get::<String, _>(0))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if names.is_empty() {
        let version = transport_publish_schema_version(connection)?;
        if version == 0 {
            Ok(TransportPublishSchemaState::Fresh)
        } else {
            Err(TransportPublishError::Schema {
                table: "transport_publish_schema",
                detail: format!(
                    "fresh schema initialization requires user_version 0, got {version}"
                ),
            })
        }
    } else {
        Ok(TransportPublishSchemaState::Existing)
    }
}

fn transport_publish_schema_version(
    connection: &mut SqliteConnection,
) -> Result<i64, TransportPublishError> {
    block_on_sqlite(sqlx::query_scalar::<_, i64>("PRAGMA user_version").fetch_one(connection))
}

fn validate_transport_publish_schema_version(
    connection: &mut SqliteConnection,
) -> Result<(), TransportPublishError> {
    let version = transport_publish_schema_version(connection)?;
    if version == SCHEMA_VERSION {
        Ok(())
    } else {
        Err(TransportPublishError::Schema {
            table: "transport_publish_schema",
            detail: format!("user_version must be {SCHEMA_VERSION}, got {version}"),
        })
    }
}

fn validate_transport_publish_schema(
    connection: &mut SqliteConnection,
) -> Result<(), TransportPublishError> {
    validate_foreign_keys_enabled(connection)?;
    validate_table_columns(
        connection,
        "transport_publish_principals",
        &[
            RequiredColumn::text_not_null("principal_id"),
            RequiredColumn::text_not_null("label"),
            RequiredColumn::text_not_null("token_hash"),
            RequiredColumn::text_not_null("allowed_pubkeys_json"),
            RequiredColumn::text_not_null("allowed_kinds_json"),
            RequiredColumn::text_not_null("allowed_target_policies_json"),
            RequiredColumn::text_not_null("allowed_explicit_transport_kinds_json"),
            RequiredColumn::text_not_null("allowed_nostr_source_policies_json"),
            RequiredColumn::integer_not_null("allow_request_targets"),
            RequiredColumn::text_not_null("job_visibility"),
            RequiredColumn::integer_nullable("expires_at_unix"),
            RequiredColumn::integer_nullable("revoked_at_unix"),
            RequiredColumn::integer_not_null("created_at_unix"),
        ],
    )?;
    validate_primary_key(
        connection,
        "transport_publish_principals",
        &["principal_id"],
    )?;
    validate_unique_index(
        connection,
        "transport_publish_principals",
        &["token_hash"],
        None,
    )?;
    validate_table_columns(
        connection,
        "transport_publish_jobs",
        &[
            RequiredColumn::text_not_null("job_id"),
            RequiredColumn::text_not_null("principal_id"),
            RequiredColumn::text_nullable("idempotency_key"),
            RequiredColumn::text_not_null("request_fingerprint"),
            RequiredColumn::text_not_null("status"),
            RequiredColumn::text_not_null("event_id"),
            RequiredColumn::text_not_null("event_pubkey"),
            RequiredColumn::integer_not_null("event_kind"),
            RequiredColumn::text_not_null("target_policy_json"),
            RequiredColumn::text_not_null("delivery_policy_json"),
            RequiredColumn::integer_not_null("requested_target_count"),
            RequiredColumn::integer_not_null("effective_target_count"),
            RequiredColumn::text_not_null("request_json"),
            RequiredColumn::integer_not_null("requested_at_ms"),
            RequiredColumn::integer_not_null("updated_at_ms"),
            RequiredColumn::integer_nullable("completed_at_ms"),
            RequiredColumn::text_nullable("last_error"),
        ],
    )?;
    validate_primary_key(connection, "transport_publish_jobs", &["job_id"])?;
    validate_foreign_key(
        connection,
        "transport_publish_jobs",
        &["principal_id"],
        "transport_publish_principals",
        &["principal_id"],
    )?;
    validate_unique_index(
        connection,
        "transport_publish_jobs",
        &["principal_id", "idempotency_key"],
        Some("WHERE idempotency_key IS NOT NULL"),
    )?;
    validate_table_columns(
        connection,
        "transport_publish_target_results",
        &[
            RequiredColumn::text_not_null("job_id"),
            RequiredColumn::text_not_null("transport_kind"),
            RequiredColumn::text_not_null("endpoint_uri"),
            RequiredColumn::text_not_null("target_scope"),
            RequiredColumn::text_nullable("target_label"),
            RequiredColumn::text_not_null("source"),
            RequiredColumn::integer_not_null("attempted"),
            RequiredColumn::text_not_null("outcome_kind"),
            RequiredColumn::text_nullable("message"),
            RequiredColumn::integer_nullable("latency_ms"),
            RequiredColumn::integer_not_null("updated_at_ms"),
        ],
    )?;
    validate_primary_key(
        connection,
        "transport_publish_target_results",
        &["job_id", "transport_kind", "endpoint_uri", "target_scope"],
    )?;
    validate_foreign_key(
        connection,
        "transport_publish_target_results",
        &["job_id"],
        "transport_publish_jobs",
        &["job_id"],
    )?;
    validate_table_columns(
        connection,
        "transport_publish_target_snapshots",
        &[
            RequiredColumn::text_not_null("job_id"),
            RequiredColumn::integer_not_null("target_index"),
            RequiredColumn::text_not_null("transport_kind"),
            RequiredColumn::text_not_null("endpoint_uri"),
            RequiredColumn::text_not_null("target_scope"),
            RequiredColumn::text_nullable("target_label"),
            RequiredColumn::text_not_null("source"),
            RequiredColumn::integer_not_null("attempted"),
            RequiredColumn::text_not_null("outcome_kind"),
            RequiredColumn::text_nullable("message"),
            RequiredColumn::integer_nullable("latency_ms"),
            RequiredColumn::integer_not_null("created_at_ms"),
        ],
    )?;
    validate_primary_key(
        connection,
        "transport_publish_target_snapshots",
        &["job_id", "target_index"],
    )?;
    validate_foreign_key(
        connection,
        "transport_publish_target_snapshots",
        &["job_id"],
        "transport_publish_jobs",
        &["job_id"],
    )?;
    validate_table_columns(
        connection,
        "transport_publish_nostr_author_cache",
        &[
            RequiredColumn::text_not_null("pubkey"),
            RequiredColumn::text_not_null("relays_json"),
            RequiredColumn::integer_not_null("updated_at_ms"),
        ],
    )?;
    validate_primary_key(
        connection,
        "transport_publish_nostr_author_cache",
        &["pubkey"],
    )?;
    for table in TRANSPORT_PUBLISH_TABLES {
        validate_table_present(connection, table)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RequiredColumn {
    name: &'static str,
    column_type: &'static str,
    not_null: bool,
}

impl RequiredColumn {
    const fn text_not_null(name: &'static str) -> Self {
        Self {
            name,
            column_type: "TEXT",
            not_null: true,
        }
    }

    const fn text_nullable(name: &'static str) -> Self {
        Self {
            name,
            column_type: "TEXT",
            not_null: false,
        }
    }

    const fn integer_not_null(name: &'static str) -> Self {
        Self {
            name,
            column_type: "INTEGER",
            not_null: true,
        }
    }

    const fn integer_nullable(name: &'static str) -> Self {
        Self {
            name,
            column_type: "INTEGER",
            not_null: false,
        }
    }
}

struct TableColumnInfo {
    column_type: String,
    not_null: bool,
    primary_key_position: i64,
}

struct ForeignKeyEntry {
    target_table: String,
    from_columns: Vec<String>,
    to_columns: Vec<String>,
}

fn validate_foreign_keys_enabled(
    connection: &mut SqliteConnection,
) -> Result<(), TransportPublishError> {
    let enabled =
        block_on_sqlite(sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys").fetch_one(connection))?;
    if enabled == 1 {
        Ok(())
    } else {
        Err(TransportPublishError::Schema {
            table: "transport_publish_schema",
            detail: "foreign key enforcement must be enabled".to_owned(),
        })
    }
}

fn validate_table_present(
    connection: &mut SqliteConnection,
    table: &'static str,
) -> Result<(), TransportPublishError> {
    let exists = block_on_sqlite(
        sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1",
        )
        .bind(table)
        .fetch_optional(connection),
    )?
    .is_some();
    if exists {
        Ok(())
    } else {
        Err(TransportPublishError::Schema {
            table,
            detail: "table is missing".to_owned(),
        })
    }
}

fn table_columns(
    connection: &mut SqliteConnection,
    table: &'static str,
) -> Result<BTreeMap<String, TableColumnInfo>, TransportPublishError> {
    let sql = format!("PRAGMA table_info({table})");
    let rows = fetch_all_sql(connection, sql.as_str())?;
    Ok(rows
        .iter()
        .map(|row| {
            Ok((
                row.try_get::<String, _>(1)?,
                TableColumnInfo {
                    column_type: row.try_get::<String, _>(2)?.to_ascii_uppercase(),
                    not_null: row.try_get::<i64, _>(3)? != 0,
                    primary_key_position: row.try_get::<i64, _>(5)?,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, sqlx::Error>>()?)
}

fn validate_primary_key(
    connection: &mut SqliteConnection,
    table: &'static str,
    expected_columns: &[&'static str],
) -> Result<(), TransportPublishError> {
    let columns = table_columns(connection, table)?;
    let mut primary_key = columns
        .iter()
        .filter_map(|(name, column)| {
            (column.primary_key_position > 0)
                .then_some((column.primary_key_position, name.as_str()))
        })
        .collect::<Vec<_>>();
    primary_key.sort_by_key(|(position, _)| *position);
    let actual_columns = primary_key
        .into_iter()
        .map(|(_, name)| name)
        .collect::<Vec<_>>();
    if actual_columns == expected_columns {
        Ok(())
    } else {
        Err(TransportPublishError::Schema {
            table,
            detail: format!("primary key must be ({})", expected_columns.join(", ")),
        })
    }
}

fn validate_unique_index(
    connection: &mut SqliteConnection,
    table: &'static str,
    expected_columns: &[&'static str],
    partial_where: Option<&'static str>,
) -> Result<(), TransportPublishError> {
    let sql = format!("PRAGMA index_list({table})");
    let rows = fetch_all_sql(connection, sql.as_str())?;
    let indexes = rows
        .iter()
        .map(|row| {
            Ok((
                row.try_get::<String, _>(1)?,
                row.try_get::<i64, _>(2)? != 0,
                row.try_get::<i64, _>(4)? != 0,
            ))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    for (index_name, unique, partial) in indexes {
        if !unique {
            continue;
        }
        if partial_where.is_some() != partial {
            continue;
        }
        let index_columns = index_columns(connection, index_name.as_str())?;
        if !columns_match(index_columns.as_slice(), expected_columns) {
            continue;
        }
        let where_matches = match partial_where {
            Some(required_where) => {
                index_sql_contains_where(connection, table, index_name.as_str(), required_where)?
            }
            None => true,
        };
        if where_matches {
            return Ok(());
        }
    }
    Err(TransportPublishError::Schema {
        table,
        detail: format!(
            "missing required unique index on ({})",
            expected_columns.join(", ")
        ),
    })
}

fn index_columns(
    connection: &mut SqliteConnection,
    index_name: &str,
) -> Result<Vec<String>, TransportPublishError> {
    let sql = format!("PRAGMA index_info({index_name})");
    let rows = fetch_all_sql(connection, sql.as_str())?;
    let mut columns = rows
        .iter()
        .map(|row| Ok((row.try_get::<i64, _>(0)?, row.try_get::<String, _>(2)?)))
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    columns.sort_by_key(|(position, _)| *position);
    Ok(columns.into_iter().map(|(_, name)| name).collect())
}

fn index_sql_contains_where(
    connection: &mut SqliteConnection,
    table: &'static str,
    index_name: &str,
    required_where: &str,
) -> Result<bool, TransportPublishError> {
    let sql = block_on_sqlite(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT sql FROM sqlite_schema WHERE type = 'index' AND tbl_name = ?1 AND name = ?2",
        )
        .bind(table)
        .bind(index_name)
        .fetch_optional(connection),
    )?
    .flatten()
    .unwrap_or_default();
    Ok(normalized_sql(sql.as_str()).contains(normalized_sql(required_where).as_str()))
}

fn normalized_sql(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase()
}

fn validate_foreign_key(
    connection: &mut SqliteConnection,
    table: &'static str,
    expected_from_columns: &[&'static str],
    expected_target_table: &'static str,
    expected_to_columns: &[&'static str],
) -> Result<(), TransportPublishError> {
    for foreign_key in foreign_keys(connection, table)? {
        if foreign_key.target_table == expected_target_table
            && columns_match(foreign_key.from_columns.as_slice(), expected_from_columns)
            && columns_match(foreign_key.to_columns.as_slice(), expected_to_columns)
        {
            return Ok(());
        }
    }
    Err(TransportPublishError::Schema {
        table,
        detail: format!(
            "missing required foreign key ({}) references {}({})",
            expected_from_columns.join(", "),
            expected_target_table,
            expected_to_columns.join(", ")
        ),
    })
}

fn foreign_keys(
    connection: &mut SqliteConnection,
    table: &'static str,
) -> Result<Vec<ForeignKeyEntry>, TransportPublishError> {
    let mut groups = BTreeMap::<i64, (String, Vec<(i64, String, String)>)>::new();
    let sql = format!("PRAGMA foreign_key_list({table})");
    let rows = fetch_all_sql(connection, sql.as_str())?;
    let rows = rows
        .iter()
        .map(|row| {
            Ok((
                row.try_get::<i64, _>(0)?,
                row.try_get::<i64, _>(1)?,
                row.try_get::<String, _>(2)?,
                row.try_get::<String, _>(3)?,
                row.try_get::<String, _>(4)?,
            ))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    for (id, seq, target_table, from_column, to_column) in rows {
        let entry = groups
            .entry(id)
            .or_insert_with(|| (target_table, Vec::new()));
        entry.1.push((seq, from_column, to_column));
    }
    let mut foreign_keys = Vec::new();
    for (_, (target_table, mut columns)) in groups {
        columns.sort_by_key(|(seq, _, _)| *seq);
        foreign_keys.push(ForeignKeyEntry {
            target_table,
            from_columns: columns
                .iter()
                .map(|(_, from_column, _)| from_column.clone())
                .collect(),
            to_columns: columns
                .into_iter()
                .map(|(_, _, to_column)| to_column)
                .collect(),
        });
    }
    Ok(foreign_keys)
}

fn columns_match(actual: &[String], expected: &[&'static str]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .map(String::as_str)
            .zip(expected.iter().copied())
            .all(|(actual, expected)| actual == expected)
}

fn validate_table_columns(
    connection: &mut SqliteConnection,
    table: &'static str,
    required_columns: &[RequiredColumn],
) -> Result<(), TransportPublishError> {
    let columns = table_columns(connection, table)?;
    if columns.is_empty() {
        return Err(TransportPublishError::Schema {
            table,
            detail: "table is missing".to_owned(),
        });
    }
    for required in required_columns {
        let Some(column) = columns.get(required.name) else {
            return Err(TransportPublishError::Schema {
                table,
                detail: format!("missing required column `{}`", required.name),
            });
        };
        if column.column_type != required.column_type {
            return Err(TransportPublishError::Schema {
                table,
                detail: format!(
                    "column `{}` must have type {}, got {}",
                    required.name, required.column_type, column.column_type
                ),
            });
        }
        if column.not_null != required.not_null {
            return Err(TransportPublishError::Schema {
                table,
                detail: format!(
                    "column `{}` must be {}",
                    required.name,
                    if required.not_null {
                        "NOT NULL"
                    } else {
                        "nullable"
                    }
                ),
            });
        }
    }
    if columns.len() != required_columns.len() {
        let required = required_columns
            .iter()
            .map(|column| column.name)
            .collect::<BTreeSet<_>>();
        let extras = columns
            .keys()
            .filter(|column| !required.contains(column.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        return Err(TransportPublishError::Schema {
            table,
            detail: format!("unexpected columns: {}", extras.join(", ")),
        });
    }
    Ok(())
}

fn recover_interrupted_publish_jobs(
    connection: &mut SqliteConnection,
) -> Result<(), TransportPublishError> {
    let now = current_unix_millis();
    let publishing = serde_json::to_string(&TransportPublishJobStatus::Publishing)?;
    let sql = job_select_sql("WHERE status = ?1");
    let rows = block_on_sqlite(
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(publishing.as_str())
            .fetch_all(&mut *connection),
    )?;
    let rows = rows
        .iter()
        .map(job_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    for row in rows {
        let job_id = row.view.job_id.clone();
        let snapshots = target_snapshot_outcomes(connection, job_id.as_str())?;
        if snapshots.is_empty() {
            block_on_sqlite(
                sqlx::query(
                    r#"
                UPDATE transport_publish_jobs
                SET status = ?2,
                    updated_at_ms = ?3,
                    completed_at_ms = ?4,
                    last_error = ?5,
                    effective_target_count = 0
                WHERE job_id = ?1
                "#,
                )
                .bind(job_id.as_str())
                .bind(serde_json::to_string(&TransportPublishJobStatus::Rejected)?)
                .bind(now)
                .bind(now)
                .bind("publish_attempt_interrupted_missing_target_snapshot")
                .execute(&mut *connection),
            )?;
            replace_target_outcomes(connection, job_id.as_str(), &[], now)?;
            continue;
        }
        let status = delivery_status(&row.view.delivery_policy, snapshots.len(), &snapshots);
        let effective_target_count = storage_count_i64(snapshots.len(), "effective_target_count")?;
        let last_error = if status == TransportPublishJobStatus::DeliveryUnsatisfiedRetryable {
            Some("publish_attempt_interrupted".to_owned())
        } else {
            last_error_for_status(status).map(str::to_owned)
        };
        block_on_sqlite(
            sqlx::query(
                r#"
            UPDATE transport_publish_jobs
            SET status = ?2,
                updated_at_ms = ?3,
                completed_at_ms = ?4,
                last_error = ?5,
                effective_target_count = ?6
            WHERE job_id = ?1
            "#,
            )
            .bind(job_id.as_str())
            .bind(serde_json::to_string(&status)?)
            .bind(now)
            .bind(now)
            .bind(last_error.as_deref())
            .bind(effective_target_count)
            .execute(&mut *connection),
        )?;
        replace_target_outcomes(connection, job_id.as_str(), &snapshots, now)?;
    }
    Ok(())
}

fn insert_target_snapshots(
    connection: &mut SqliteConnection,
    job_id: &str,
    outcomes: &[TransportPublishTargetOutcome],
    now: i64,
) -> Result<(), TransportPublishError> {
    for (target_index, outcome) in outcomes.iter().enumerate() {
        block_on_sqlite(
            sqlx::query(
                r#"
            INSERT INTO transport_publish_target_snapshots (
                job_id,
                target_index,
                transport_kind,
                endpoint_uri,
                target_scope,
                target_label,
                source,
                attempted,
                outcome_kind,
                message,
                latency_ms,
                created_at_ms
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
            "#,
            )
            .bind(job_id)
            .bind(i64::try_from(target_index).unwrap_or(i64::MAX))
            .bind(outcome.transport_kind.as_str())
            .bind(outcome.endpoint_uri.as_str())
            .bind(storage_target_scope(outcome.target_scope.as_deref()))
            .bind(outcome.target_label.as_deref())
            .bind(serde_json::to_string(&outcome.source)?)
            .bind(outcome.attempted)
            .bind(serde_json::to_string(&outcome.outcome_kind)?)
            .bind(outcome.message.as_deref())
            .bind(
                outcome
                    .latency_ms
                    .and_then(|value| i64::try_from(value).ok()),
            )
            .bind(now)
            .execute(&mut *connection),
        )?;
    }
    Ok(())
}

fn replace_target_outcomes(
    connection: &mut SqliteConnection,
    job_id: &str,
    outcomes: &[TransportPublishTargetOutcome],
    now: i64,
) -> Result<(), TransportPublishError> {
    block_on_sqlite(
        sqlx::query("DELETE FROM transport_publish_target_results WHERE job_id = ?1")
            .bind(job_id)
            .execute(&mut *connection),
    )?;
    for outcome in outcomes {
        block_on_sqlite(
            sqlx::query(
                r#"
            INSERT OR REPLACE INTO transport_publish_target_results (
                job_id,
                transport_kind,
                endpoint_uri,
                target_scope,
                target_label,
                source,
                attempted,
                outcome_kind,
                message,
                latency_ms,
                updated_at_ms
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            )
            .bind(job_id)
            .bind(outcome.transport_kind.as_str())
            .bind(outcome.endpoint_uri.as_str())
            .bind(storage_target_scope(outcome.target_scope.as_deref()))
            .bind(outcome.target_label.as_deref())
            .bind(serde_json::to_string(&outcome.source)?)
            .bind(outcome.attempted)
            .bind(serde_json::to_string(&outcome.outcome_kind)?)
            .bind(outcome.message.as_deref())
            .bind(
                outcome
                    .latency_ms
                    .and_then(|value| i64::try_from(value).ok()),
            )
            .bind(now)
            .execute(&mut *connection),
        )?;
    }
    Ok(())
}

fn target_snapshot_outcomes(
    connection: &mut SqliteConnection,
    job_id: &str,
) -> Result<Vec<TransportPublishTargetOutcome>, TransportPublishError> {
    let rows = block_on_sqlite(
        sqlx::query(
        r#"
        SELECT transport_kind, endpoint_uri, target_scope, target_label, source, attempted, outcome_kind, message, latency_ms
        FROM transport_publish_target_snapshots
        WHERE job_id = ?1
        ORDER BY target_index
        "#,
        )
        .bind(job_id)
        .fetch_all(connection),
    )?;
    let outcomes = rows
        .iter()
        .map(target_outcome_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(outcomes)
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
            target_policy_json,
            delivery_policy_json,
            effective_target_count,
            requested_at_ms,
            completed_at_ms,
            last_error
        FROM transport_publish_jobs
        {tail}
        "#
    )
}

fn principal_from_row(row: &SqliteRow) -> Result<PublishPrincipal, TransportPublishError> {
    let visibility: String = row.try_get(8)?;
    Ok(PublishPrincipal {
        principal_id: row.try_get(0)?,
        label: row.try_get(1)?,
        allowed_pubkeys: json_column(row, 2, "allowed_pubkeys_json")?,
        allowed_kinds: json_column(row, 3, "allowed_kinds_json")?,
        allowed_target_policies: json_column(row, 4, "allowed_target_policies_json")?,
        allowed_explicit_transport_kinds: json_column(
            row,
            5,
            "allowed_explicit_transport_kinds_json",
        )?,
        allowed_nostr_source_policies: json_column(row, 6, "allowed_nostr_source_policies_json")?,
        allow_request_targets: row.try_get(7)?,
        job_visibility: PublishJobVisibility::from_str(visibility.as_str())?,
        expires_at_unix: row.try_get(9)?,
    })
}

fn job_from_row(row: &SqliteRow) -> Result<PublishJobRow, TransportPublishError> {
    let status: TransportPublishJobStatus = json_text(row, 3, "status")?;
    let target_policy: TransportPublishTargetPolicy = json_text(row, 7, "target_policy_json")?;
    let delivery_policy: TransportPublishDeliveryPolicy =
        json_text(row, 8, "delivery_policy_json")?;
    Ok(PublishJobRow {
        principal_id: row.try_get(1)?,
        request_fingerprint: row.try_get(2)?,
        view: TransportPublishJobView {
            job_id: row.try_get(0)?,
            status,
            terminal: false,
            delivery_satisfied: false,
            event_id: row.try_get(4)?,
            pubkey: row.try_get(5)?,
            event_kind: checked_event_kind_column(row, 6)?,
            target_policy,
            delivery_policy,
            target_count: checked_usize_column(row, 9, "effective_target_count")?,
            acknowledged_count: 0,
            retryable_count: 0,
            terminal_count: 0,
            requested_at_ms: row.try_get(10)?,
            completed_at_ms: row.try_get(11)?,
            last_error: row.try_get(12)?,
            targets: Vec::new(),
        },
    })
}

fn target_outcome_from_row(
    row: &SqliteRow,
) -> Result<TransportPublishTargetOutcome, TransportPublishError> {
    let source: TransportPublishTargetSource = json_text(row, 4, "source")?;
    let outcome_kind: TransportPublishOutcomeKind = json_text(row, 6, "outcome_kind")?;
    Ok(TransportPublishTargetOutcome {
        transport_kind: row.try_get(0)?,
        endpoint_uri: row.try_get(1)?,
        target_scope: storage_target_scope_to_protocol(row.try_get::<String, _>(2)?),
        target_label: row.try_get(3)?,
        source,
        attempted: row.try_get(5)?,
        outcome_kind,
        message: row.try_get(7)?,
        latency_ms: checked_optional_u64_column(row, 8, "latency_ms")?,
    })
}

fn finalize_job_view(view: &mut TransportPublishJobView) {
    view.acknowledged_count = view
        .targets
        .iter()
        .filter(|relay| relay.outcome_kind.counts_toward_accepted_delivery())
        .count();
    view.retryable_count = view
        .targets
        .iter()
        .filter(|relay| relay.outcome_kind.is_retryable())
        .count();
    view.terminal_count = view
        .targets
        .iter()
        .filter(|relay| relay.outcome_kind.is_terminal_failure())
        .count();
    view.terminal = matches!(
        view.status,
        TransportPublishJobStatus::DeliverySatisfied
            | TransportPublishJobStatus::DeliveryUnsatisfiedTerminal
            | TransportPublishJobStatus::DeliveryDeferred
            | TransportPublishJobStatus::DeliveryDeferredUntilImplemented
            | TransportPublishJobStatus::Rejected
    );
    view.delivery_satisfied = view.status == TransportPublishJobStatus::DeliverySatisfied;
}

fn validate_principal_init(input: &PublishPrincipalInit) -> Result<(), TransportPublishError> {
    if input.label.trim().is_empty() {
        return Err(TransportPublishError::InvalidScope(
            "principal label must not be empty".to_owned(),
        ));
    }
    if !input.token_hash.starts_with(TOKEN_HASH_PREFIX) {
        return Err(TransportPublishError::InvalidScope(
            "principal token hash must use sha256 prefix".to_owned(),
        ));
    }
    if input.allowed_pubkeys.is_empty() {
        return Err(TransportPublishError::InvalidScope(
            "principal must include at least one allowed pubkey".to_owned(),
        ));
    }
    for pubkey in &input.allowed_pubkeys {
        ensure_lower_hex("allowed_pubkey", pubkey, 64)?;
    }
    if input.allowed_kinds.is_empty() {
        return Err(TransportPublishError::InvalidScope(
            "principal must include at least one allowed kind".to_owned(),
        ));
    }
    if input.allowed_target_policies.is_empty() {
        return Err(TransportPublishError::InvalidScope(
            "principal must include at least one allowed target policy".to_owned(),
        ));
    }
    let allows_explicit_targets = input
        .allowed_target_policies
        .contains(&TransportPublishTargetPolicyName::ExplicitTargets);
    if allows_explicit_targets && input.allowed_explicit_transport_kinds.is_empty() {
        return Err(TransportPublishError::InvalidScope(
            "principal must include at least one allowed explicit transport kind".to_owned(),
        ));
    }
    if !allows_explicit_targets && !input.allowed_explicit_transport_kinds.is_empty() {
        return Err(TransportPublishError::InvalidScope(
            "principal cannot include explicit transport kinds without explicit target policy"
                .to_owned(),
        ));
    }
    let mut explicit_transport_kinds = BTreeSet::new();
    for transport_kind in &input.allowed_explicit_transport_kinds {
        let canonical = parse_explicit_transport_kind(transport_kind)?;
        if canonical != *transport_kind || !explicit_transport_kinds.insert(canonical) {
            return Err(TransportPublishError::InvalidScope(
                "allowed explicit transport kinds must be canonical and unique".to_owned(),
            ));
        }
    }
    if input
        .allowed_target_policies
        .contains(&TransportPublishTargetPolicyName::Nostr)
        && input.allowed_nostr_source_policies.is_empty()
    {
        return Err(TransportPublishError::InvalidScope(
            "principal must include at least one allowed Nostr source policy".to_owned(),
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

pub fn parse_nostr_source_policy(
    value: &str,
) -> Result<NostrPublishTargetSourcePolicy, TransportPublishError> {
    match value {
        "explicit_only" => Ok(NostrPublishTargetSourcePolicy::ExplicitOnly),
        "request_then_author_write_then_daemon_default" => {
            Ok(NostrPublishTargetSourcePolicy::RequestThenAuthorWriteThenDaemonDefault)
        }
        "author_write_then_daemon_default" => {
            Ok(NostrPublishTargetSourcePolicy::AuthorWriteThenDaemonDefault)
        }
        "daemon_default_only" => Ok(NostrPublishTargetSourcePolicy::DaemonDefaultOnly),
        other => Err(TransportPublishError::InvalidScope(format!(
            "unknown Nostr source policy `{other}`"
        ))),
    }
}

pub fn parse_target_policy(
    value: &str,
) -> Result<TransportPublishTargetPolicyName, TransportPublishError> {
    match value {
        "explicit_targets" => Ok(TransportPublishTargetPolicyName::ExplicitTargets),
        "nostr" => Ok(TransportPublishTargetPolicyName::Nostr),
        other => Err(TransportPublishError::InvalidScope(format!(
            "unknown target policy `{other}`"
        ))),
    }
}

pub fn parse_explicit_transport_kind(value: &str) -> Result<String, TransportPublishError> {
    let kind = RadrootsTransportKind::parse_canonical(value).map_err(|error| {
        TransportPublishError::InvalidScope(format!(
            "unknown explicit transport kind `{value}`: {error}"
        ))
    })?;
    Ok(kind.canonical_label())
}

fn signed_event_from_raw_json(
    raw_json: &str,
) -> Result<RadrootsSignedEvent, TransportPublishError> {
    let wire = RadrootsNip01EventWire::parse_json(raw_json)?;
    let signed_event = RadrootsSignedEvent::from_wire_verified_id(wire, raw_json.to_owned())?;
    Ok(signed_event.verify_signature()?.into_signed_event())
}

fn request_intent_fingerprint(
    principal_id: &str,
    canonical_event_json: &str,
    request: &TransportPublishEventRequest,
    effective_timeout_ms: u64,
) -> Result<String, TransportPublishError> {
    #[derive(Serialize)]
    struct FingerprintInput<'a> {
        principal_id: &'a str,
        canonical_event_json: &'a str,
        target_policy: &'a TransportPublishTargetPolicy,
        delivery_policy: &'a TransportPublishDeliveryPolicy,
        effective_timeout_ms: u64,
    }

    let input = FingerprintInput {
        principal_id,
        canonical_event_json,
        target_policy: &request.target_policy,
        delivery_policy: &request.delivery_policy,
        effective_timeout_ms,
    };
    let bytes = serde_json::to_vec(&input)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(hex_lower(&hasher.finalize()))
}

fn effective_publish_timeout_ms(
    config: &TransportPublishConfig,
    timeout_ms: Option<u64>,
) -> Result<u64, TransportPublishError> {
    let max_timeout_ms = config.connect_timeout_secs.saturating_mul(1_000);
    match timeout_ms {
        Some(0) => Err(TransportPublishError::InvalidSignedEvent(
            "timeout_ms must be greater than zero".to_owned(),
        )),
        Some(timeout_ms) if timeout_ms > max_timeout_ms => {
            Err(TransportPublishError::InvalidSignedEvent(format!(
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
    source: TransportPublishTargetSource,
    metadata: PublishTargetMetadata,
) {
    if !targets
        .iter()
        .any(|target| target.url == url && target.target_scope == metadata.target_scope)
    {
        targets.push(ResolvedPublishRelay {
            url,
            source,
            target_scope: metadata.target_scope,
            target_label: metadata.target_label,
        });
    }
}

fn reticulum_unavailable_outcome(target: &TransportPublishTarget) -> TransportPublishTargetOutcome {
    TransportPublishTargetOutcome {
        transport_kind: TRANSPORT_KIND_RETICULUM.to_owned(),
        endpoint_uri: target.endpoint_uri.trim().to_owned(),
        target_scope: target.target_scope.clone(),
        target_label: target.target_label.clone(),
        source: TransportPublishTargetSource::Reticulum,
        attempted: false,
        outcome_kind: TransportPublishOutcomeKind::DeferredUntilImplemented,
        message: Some(RADROOTS_RETICULUM_UNAVAILABLE_MESSAGE.to_owned()),
        latency_ms: None,
    }
}

fn unsupported_transport_outcome(target: &TransportPublishTarget) -> TransportPublishTargetOutcome {
    TransportPublishTargetOutcome {
        transport_kind: target.transport_kind.trim().to_owned(),
        endpoint_uri: target.endpoint_uri.trim().to_owned(),
        target_scope: target.target_scope.clone(),
        target_label: target.target_label.clone(),
        source: TransportPublishTargetSource::Request,
        attempted: false,
        outcome_kind: TransportPublishOutcomeKind::Unsupported,
        message: Some("transport kind is not supported by radrootsd transport publish".to_owned()),
        latency_ms: None,
    }
}

fn relay_resolution_connection_failure(
    relay_url: impl Into<String>,
    source: TransportPublishTargetSource,
    metadata: &PublishTargetMetadata,
    message: impl Into<String>,
) -> TransportPublishTargetOutcome {
    TransportPublishTargetOutcome {
        transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
        endpoint_uri: relay_url.into(),
        target_scope: metadata.target_scope.clone(),
        target_label: metadata.target_label.clone(),
        source,
        attempted: false,
        outcome_kind: TransportPublishOutcomeKind::ConnectionFailed,
        message: Some(message.into()),
        latency_ms: None,
    }
}

fn target_snapshots_from_resolution(
    resolution: &PublishRelayResolution,
) -> Vec<TransportPublishTargetOutcome> {
    let mut snapshots = resolution.outcomes.clone();
    snapshots.extend(
        resolution
            .targets
            .iter()
            .map(|target| TransportPublishTargetOutcome {
                transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
                endpoint_uri: target.url.as_str().to_owned(),
                target_scope: target.target_scope.clone(),
                target_label: target.target_label.clone(),
                source: target.source,
                attempted: true,
                outcome_kind: TransportPublishOutcomeKind::ConnectionFailed,
                message: Some("publish_attempt_interrupted".to_owned()),
                latency_ms: None,
            }),
    );
    snapshots
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

fn relay_url_policy(config: &TransportPublishConfig) -> RadrootsRelayUrlPolicy {
    match config.nostr.relay_url_policy {
        crate::app::config::NostrRelayUrlPolicy::Public => RadrootsRelayUrlPolicy::Public,
        crate::app::config::NostrRelayUrlPolicy::Localhost => RadrootsRelayUrlPolicy::Localhost,
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

fn publish_outcomes_from_receipt(
    receipt: RadrootsRelayPublishRelayReceipt,
    targets: &[ResolvedPublishRelay],
    latency_ms: Option<u64>,
) -> Vec<TransportPublishTargetOutcome> {
    targets
        .iter()
        .filter(|target| target.url.as_str() == receipt.relay_url.as_str())
        .map(|target| TransportPublishTargetOutcome {
            transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
            endpoint_uri: receipt.relay_url.clone(),
            target_scope: target.target_scope.clone(),
            target_label: target.target_label.clone(),
            source: target.source,
            attempted: receipt.attempted,
            outcome_kind: publish_outcome_kind(receipt.outcome.kind),
            message: receipt.outcome.message.clone(),
            latency_ms,
        })
        .collect()
}

fn publish_outcome_kind(kind: RadrootsRelayOutcomeKind) -> TransportPublishOutcomeKind {
    match kind {
        RadrootsRelayOutcomeKind::Accepted => TransportPublishOutcomeKind::Accepted,
        RadrootsRelayOutcomeKind::DuplicateAccepted => {
            TransportPublishOutcomeKind::DuplicateAccepted
        }
        RadrootsRelayOutcomeKind::Blocked => TransportPublishOutcomeKind::Blocked,
        RadrootsRelayOutcomeKind::RateLimited => TransportPublishOutcomeKind::RateLimited,
        RadrootsRelayOutcomeKind::Invalid => TransportPublishOutcomeKind::Invalid,
        RadrootsRelayOutcomeKind::PowRequired => TransportPublishOutcomeKind::PowRequired,
        RadrootsRelayOutcomeKind::Restricted => TransportPublishOutcomeKind::Restricted,
        RadrootsRelayOutcomeKind::AuthRequired => TransportPublishOutcomeKind::AuthRequired,
        RadrootsRelayOutcomeKind::Muted => TransportPublishOutcomeKind::Muted,
        RadrootsRelayOutcomeKind::Unsupported => TransportPublishOutcomeKind::Unsupported,
        RadrootsRelayOutcomeKind::PaymentRequired => TransportPublishOutcomeKind::PaymentRequired,
        RadrootsRelayOutcomeKind::Error => TransportPublishOutcomeKind::Error,
        RadrootsRelayOutcomeKind::Timeout => TransportPublishOutcomeKind::Timeout,
        RadrootsRelayOutcomeKind::ConnectionFailed => TransportPublishOutcomeKind::ConnectionFailed,
        RadrootsRelayOutcomeKind::RelayUrlRejected => TransportPublishOutcomeKind::TargetRejected,
        RadrootsRelayOutcomeKind::SkippedAlreadyAccepted => {
            TransportPublishOutcomeKind::SkippedAlreadyAccepted
        }
        RadrootsRelayOutcomeKind::Unknown => TransportPublishOutcomeKind::Unknown,
    }
}

fn target_outcome_fingerprint(
    target: &TransportPublishTargetOutcome,
    index: usize,
) -> Result<RadrootsTransportTargetFingerprint, TransportPublishError> {
    let transport_kind = RadrootsTransportKind::parse_canonical(target.transport_kind.as_str())
        .map_err(|error| {
            TransportPublishError::InvalidPublishJobState(format!(
                "target outcome {index} has invalid transport kind: {error}"
            ))
        })?;
    let scope = target
        .target_scope
        .as_deref()
        .map(RadrootsTransportMeshScopeId::parse)
        .transpose()
        .map_err(|error| {
            TransportPublishError::InvalidPublishJobState(format!(
                "target outcome {index} has invalid target scope: {error}"
            ))
        })?;
    let label = target
        .target_label
        .as_deref()
        .map(RadrootsTransportTargetLabel::parse)
        .transpose()
        .map_err(|error| {
            TransportPublishError::InvalidPublishJobState(format!(
                "target outcome {index} has invalid target label: {error}"
            ))
        })?;
    let target = transport_target_from_outcome_parts(
        transport_kind,
        target.endpoint_uri.as_str(),
        scope,
        label,
    )
    .map_err(|error| {
        TransportPublishError::InvalidPublishJobState(format!(
            "target outcome {index} fingerprint failed: {error}"
        ))
    })?;
    Ok(target.fingerprint)
}

fn transport_target_from_outcome_parts(
    transport_kind: RadrootsTransportKind,
    endpoint_uri: &str,
    scope: Option<RadrootsTransportMeshScopeId>,
    label: Option<RadrootsTransportTargetLabel>,
) -> Result<RadrootsTransportTarget, radroots_transport::RadrootsTransportError> {
    match transport_kind {
        RadrootsTransportKind::Nostr => {
            RadrootsTransportTarget::nostr_relay_with_metadata(endpoint_uri, scope, label)
        }
        RadrootsTransportKind::Reticulum => {
            if endpoint_uri != RADROOTS_RETICULUM_ENDPOINT_URI {
                return Err(radroots_transport::RadrootsTransportError::InvalidTargetUri);
            }
            RadrootsTransportTarget::reticulum_with_metadata(endpoint_uri, scope, label)
        }
        RadrootsTransportKind::Local => {
            RadrootsTransportTarget::local_with_metadata(endpoint_uri, scope, label)
        }
    }
}

fn validate_delivery_policy_for_resolution(
    delivery_policy: &TransportPublishDeliveryPolicy,
    resolution: &PublishRelayResolution,
) -> Result<(), TransportPublishError> {
    if !matches!(
        delivery_policy,
        TransportPublishDeliveryPolicy::RequiredTargets { .. }
    ) {
        return Ok(());
    }
    let target_fingerprints = resolution.target_fingerprints()?;
    delivery_policy
        .validate_target_membership(target_fingerprints.as_slice())
        .map_err(|error| {
            TransportPublishError::InvalidSignedEvent(format!(
                "publish request delivery policy validation failed: {error}"
            ))
        })
}

fn required_outcomes_for_policy<'a>(
    required_targets: &[RadrootsTransportTargetFingerprint],
    outcomes: &'a [TransportPublishTargetOutcome],
) -> Vec<&'a TransportPublishTargetOutcome> {
    required_targets
        .iter()
        .filter_map(|required| {
            outcomes.iter().enumerate().find_map(|(index, outcome)| {
                target_outcome_fingerprint(outcome, index)
                    .ok()
                    .filter(|fingerprint| fingerprint == required)
                    .map(|_| outcome)
            })
        })
        .collect()
}

fn satisfaction_policy_from_delivery_policy(
    delivery_policy: &TransportPublishDeliveryPolicy,
    target_count: usize,
    nostr_targets: &[ResolvedPublishRelay],
) -> Result<RadrootsTransportSatisfactionPolicy, TransportPublishError> {
    match delivery_policy {
        TransportPublishDeliveryPolicy::Any => {
            Ok(RadrootsTransportSatisfactionPolicy::any_accepted())
        }
        TransportPublishDeliveryPolicy::All => {
            Ok(RadrootsTransportSatisfactionPolicy::all_accepted())
        }
        TransportPublishDeliveryPolicy::Quorum { quorum } => {
            let required = (*quorum).min(target_count).min(nostr_targets.len()).max(1);
            Ok(RadrootsTransportSatisfactionPolicy::quorum_accepted(
                u16::try_from(required).unwrap_or(u16::MAX),
            ))
        }
        TransportPublishDeliveryPolicy::RequiredTargets { targets } => {
            let nostr_required_targets = targets
                .iter()
                .filter_map(|required| {
                    nostr_targets.iter().find_map(|target| {
                        target
                            .fingerprint()
                            .ok()
                            .filter(|fingerprint| fingerprint == required)
                    })
                })
                .collect::<Vec<_>>();
            if nostr_required_targets.is_empty() {
                Ok(RadrootsTransportSatisfactionPolicy::no_wait())
            } else {
                Ok(RadrootsTransportSatisfactionPolicy::required_targets(
                    RadrootsTransportSatisfactionClass::Accepted,
                    nostr_required_targets,
                )
                .map_err(|error| TransportPublishError::Transport(error.to_string()))?)
            }
        }
    }
}

fn delivery_status(
    delivery_policy: &TransportPublishDeliveryPolicy,
    target_count: usize,
    outcomes: &[TransportPublishTargetOutcome],
) -> TransportPublishJobStatus {
    let (satisfied, status_outcomes) = match delivery_policy {
        TransportPublishDeliveryPolicy::RequiredTargets { targets } => {
            let required_outcomes = required_outcomes_for_policy(targets, outcomes);
            let satisfied = required_outcomes.len() == targets.len()
                && required_outcomes
                    .iter()
                    .all(|outcome| outcome.outcome_kind.counts_toward_accepted_delivery());
            (satisfied, required_outcomes)
        }
        TransportPublishDeliveryPolicy::Any
        | TransportPublishDeliveryPolicy::All
        | TransportPublishDeliveryPolicy::Quorum { .. } => {
            let required = delivery_policy.required_target_count(target_count);
            let acknowledged = outcomes
                .iter()
                .filter(|outcome| outcome.outcome_kind.counts_toward_accepted_delivery())
                .count();
            (
                acknowledged >= required,
                outcomes.iter().collect::<Vec<_>>(),
            )
        }
    };
    if satisfied {
        return TransportPublishJobStatus::DeliverySatisfied;
    }
    if status_outcomes
        .iter()
        .any(|outcome| outcome.outcome_kind.is_retryable())
    {
        TransportPublishJobStatus::DeliveryUnsatisfiedRetryable
    } else if status_outcomes.iter().any(|outcome| {
        outcome.outcome_kind == TransportPublishOutcomeKind::DeferredUntilImplemented
    }) && status_outcomes
        .iter()
        .all(|outcome| !outcome.outcome_kind.is_terminal_failure())
    {
        TransportPublishJobStatus::DeliveryDeferredUntilImplemented
    } else {
        TransportPublishJobStatus::DeliveryUnsatisfiedTerminal
    }
}

fn last_error_for_status(status: TransportPublishJobStatus) -> Option<&'static str> {
    match status {
        TransportPublishJobStatus::DeliverySatisfied => None,
        TransportPublishJobStatus::Rejected => Some("no_transport_publish_targets"),
        TransportPublishJobStatus::DeliveryDeferred => Some("delivery_deferred_until_implemented"),
        TransportPublishJobStatus::DeliveryDeferredUntilImplemented => {
            Some("delivery_deferred_until_implemented")
        }
        TransportPublishJobStatus::Accepted
        | TransportPublishJobStatus::Publishing
        | TransportPublishJobStatus::DeliveryUnsatisfiedRetryable
        | TransportPublishJobStatus::DeliveryUnsatisfiedTerminal => Some("delivery_unsatisfied"),
    }
}

fn timeout_receipts(targets: &[RadrootsRelayUrl]) -> Vec<RadrootsRelayPublishRelayReceipt> {
    targets
        .iter()
        .map(|target| {
            RadrootsRelayPublishRelayReceipt::attempted(
                target.as_str(),
                RadrootsRelayOutcome::timeout("timeout: publish attempt exceeded daemon bound"),
            )
        })
        .collect()
}

fn transport_error_receipts(
    targets: &[RadrootsRelayUrl],
    error: TransportPublishError,
) -> Vec<RadrootsRelayPublishRelayReceipt> {
    let message = format!("error: {error}");
    targets
        .iter()
        .map(|target| {
            RadrootsRelayPublishRelayReceipt::attempted(
                target.as_str(),
                RadrootsRelayOutcome::connection_failed(message.clone()),
            )
        })
        .collect()
}

pub fn write_token_file(path: &Path, token: &str) -> Result<(), TransportPublishError> {
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
) -> Result<(), TransportPublishError> {
    if value.len() == expected_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(TransportPublishError::InvalidScope(format!(
            "{field} must be {expected_len} lowercase hex characters"
        )))
    }
}

fn json_column<T: for<'de> Deserialize<'de>>(
    row: &SqliteRow,
    index: usize,
    field: &'static str,
) -> Result<T, TransportPublishError> {
    let value: String = row.try_get(index)?;
    serde_json::from_str(value.as_str()).map_err(|error| persisted_decode_error(field, error))
}

fn json_text<T: for<'de> Deserialize<'de>>(
    row: &SqliteRow,
    index: usize,
    field: &'static str,
) -> Result<T, TransportPublishError> {
    let value: String = row.try_get(index)?;
    serde_json::from_str(value.as_str()).map_err(|error| persisted_decode_error(field, error))
}

#[derive(Debug, Error)]
#[error("{field} integer value {value} is outside {target} range")]
struct TransportPublishStorageIntegerRangeError {
    field: &'static str,
    value: i64,
    target: &'static str,
}

fn checked_event_kind_column(row: &SqliteRow, index: usize) -> Result<u32, TransportPublishError> {
    let value = row.try_get::<i64, _>(index)?;
    if !(0..=i64::from(u32::MAX)).contains(&value) {
        return Err(persisted_decode_error(
            "event_kind",
            TransportPublishStorageIntegerRangeError {
                field: "event_kind",
                value,
                target: "u32",
            },
        ));
    }
    u32::try_from(value).map_err(|_| {
        persisted_decode_error(
            "event_kind",
            TransportPublishStorageIntegerRangeError {
                field: "event_kind",
                value,
                target: "u32",
            },
        )
    })
}

fn checked_usize_column(
    row: &SqliteRow,
    index: usize,
    field: &'static str,
) -> Result<usize, TransportPublishError> {
    let value = row.try_get::<i64, _>(index)?;
    usize::try_from(value).map_err(|_| {
        persisted_decode_error(
            field,
            TransportPublishStorageIntegerRangeError {
                field,
                value,
                target: "usize",
            },
        )
    })
}

fn storage_count_i64(value: usize, field: &'static str) -> Result<i64, TransportPublishError> {
    i64::try_from(value).map_err(|_| {
        TransportPublishError::InvalidPublishJobState(format!(
            "{field} value exceeds i64 storage range"
        ))
    })
}

fn checked_optional_u64_column(
    row: &SqliteRow,
    index: usize,
    field: &'static str,
) -> Result<Option<u64>, TransportPublishError> {
    row.try_get::<Option<i64>, _>(index)?
        .map(|value| {
            u64::try_from(value).map_err(|_| {
                persisted_decode_error(
                    field,
                    TransportPublishStorageIntegerRangeError {
                        field,
                        value,
                        target: "u64",
                    },
                )
            })
        })
        .transpose()
}

fn storage_target_scope(target_scope: Option<&str>) -> &str {
    target_scope.unwrap_or("")
}

fn storage_target_scope_to_protocol(target_scope: String) -> Option<String> {
    (!target_scope.is_empty()).then_some(target_scope)
}

fn persisted_decode_error<E>(field: &'static str, error: E) -> TransportPublishError
where
    E: std::error::Error,
{
    TransportPublishError::InvalidPublishJobState(format!(
        "{field} persisted value could not be decoded: {error}"
    ))
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
        PublishEventMetadata, PublishJobInsert, PublishJobVisibility, PublishPrincipal,
        PublishPrincipalInit, SCHEMA_VERSION, TRANSPORT_KIND_NOSTR, TRANSPORT_KIND_RETICULUM,
        TRANSPORT_PUBLISH_SCHEMA_SQL, TransportPublish, TransportPublishError,
        TransportPublishStore, generate_bearer_token, hash_bearer_token, parse_nostr_source_policy,
    };
    use crate::app::config::{
        NostrRelayUrlPolicy, TransportPublishConfig, TransportPublishNostrConfig,
    };
    use nostr::JsonUtil;
    use nostr::{EventBuilder, Kind, Tag};
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::RadrootsNostrTimestamp;
    use radroots_transport::{
        RADROOTS_RETICULUM_ENDPOINT_URI, RADROOTS_RETICULUM_UNAVAILABLE_MESSAGE,
        RadrootsTransportTarget,
    };
    use radroots_transport_nostr::{RadrootsMockRelayPublishAdapter, RadrootsRelayOutcome};
    use radroots_transport_publish_protocol::{
        NostrPublishTargetSourcePolicy, TransportPublishDeliveryPolicy,
        TransportPublishEventRequest, TransportPublishJobStatus, TransportPublishOutcomeKind,
        TransportPublishReticulumBehavior, TransportPublishTarget, TransportPublishTargetOutcome,
        TransportPublishTargetPolicy, TransportPublishTargetPolicyName,
        TransportPublishTargetSource,
    };
    use sqlx::Row;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
    use std::collections::BTreeMap;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    const RELAY_PRIMARY: &str = "wss://relay.example.com";
    const RELAY_SECONDARY: &str = "wss://relay-2.example.com";
    const RELAY_FORBIDDEN: &str = "wss://forbidden-relay.example.com";

    fn event_metadata(pubkey: &str, kind: u32) -> PublishEventMetadata {
        PublishEventMetadata {
            event_id: "0".repeat(64),
            pubkey: pubkey.to_owned(),
            kind,
        }
    }

    fn raw_event_json(pubkey: &str, kind: u32) -> String {
        format!(
            r#"{{"id":"{}","pubkey":"{}","created_at":1700000000,"kind":{},"tags":[["d","listing-1"]],"content":"{{}}","sig":"{}"}}"#,
            "0".repeat(64),
            pubkey,
            kind,
            "1".repeat(128)
        )
    }

    fn request(pubkey: &str, kind: u32) -> TransportPublishEventRequest {
        TransportPublishEventRequest {
            raw_event_json: raw_event_json(pubkey, kind),
            target_policy: TransportPublishTargetPolicy::nostr(
                NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                Vec::new(),
            ),
            delivery_policy: TransportPublishDeliveryPolicy::Any,
            idempotency_key: Some("idem-1".to_owned()),
            timeout_ms: None,
        }
    }

    fn schema_with_replacement(fragment: &str, replacement: &str) -> String {
        assert!(TRANSPORT_PUBLISH_SCHEMA_SQL.contains(fragment));
        TRANSPORT_PUBLISH_SCHEMA_SQL.replace(fragment, replacement)
    }

    fn create_existing_schema(
        database_path: &std::path::Path,
        schema_sql: &str,
        user_version: Option<i64>,
    ) {
        let mut connection = open_test_database(database_path);
        super::execute_raw_sql(&mut connection, schema_sql).expect("create schema");
        if let Some(version) = user_version {
            super::execute_sql(
                &mut connection,
                format!("PRAGMA user_version = {version}").as_str(),
            )
            .expect("set schema version");
        }
    }

    fn open_test_database(database_path: &std::path::Path) -> SqliteConnection {
        super::connect_sqlite(
            SqliteConnectOptions::new()
                .filename(database_path)
                .create_if_missing(true),
        )
        .expect("open schema database")
    }

    fn open_schema_error(database_path: &std::path::Path) -> TransportPublishError {
        match TransportPublishStore::open(database_path.to_path_buf()) {
            Ok(_) => panic!("malformed schema opened"),
            Err(error) => error,
        }
    }

    fn assert_schema_error(
        error: TransportPublishError,
        expected_table: &'static str,
        expected_detail: &str,
    ) {
        match error {
            TransportPublishError::Schema { table, detail } => {
                assert_eq!(table, expected_table);
                assert!(
                    detail.contains(expected_detail),
                    "schema detail `{detail}` did not contain `{expected_detail}`"
                );
            }
            error => panic!("unexpected error: {error}"),
        }
    }

    fn database_user_version(database_path: &std::path::Path) -> i64 {
        let mut connection = open_test_database(database_path);
        super::transport_publish_schema_version(&mut connection).expect("user version")
    }

    fn test_query_column_names(connection: &mut SqliteConnection, table: &str) -> Vec<String> {
        let sql = format!("PRAGMA table_info({table})");
        super::fetch_all_sql(connection, sql.as_str())
            .expect("query schema")
            .iter()
            .map(|row| row.try_get::<String, _>(1).expect("column name"))
            .collect()
    }

    fn signed_event(identity: &RadrootsIdentity, content: &str) -> String {
        // Transport tests require an already-signed wire fixture; they do not
        // exercise a Radroots product-authoring boundary.
        let event = EventBuilder::new(Kind::Custom(30_402), content)
            .tag(Tag::identifier("listing-1"))
            .custom_created_at(RadrootsNostrTimestamp::from_secs(1_700_000_000))
            .sign_with_keys(identity.keys())
            .expect("signed event");
        event.as_json()
    }

    fn raw_event_with_field(
        raw_event_json: String,
        field: &str,
        value: serde_json::Value,
    ) -> String {
        let mut event: serde_json::Value =
            serde_json::from_str(raw_event_json.as_str()).expect("raw event json");
        event[field] = value;
        serde_json::to_string(&event).expect("mutated raw event")
    }

    fn publish_request(
        raw_event_json: String,
        relays: Vec<String>,
        source_policy: NostrPublishTargetSourcePolicy,
        delivery_policy: TransportPublishDeliveryPolicy,
        idempotency_key: Option<&str>,
    ) -> TransportPublishEventRequest {
        TransportPublishEventRequest {
            raw_event_json,
            target_policy: TransportPublishTargetPolicy::nostr(source_policy, relays),
            delivery_policy,
            idempotency_key: idempotency_key.map(str::to_owned),
            timeout_ms: Some(5_000),
        }
    }

    fn reticulum_publish_request(
        raw_event_json: String,
        behavior: TransportPublishReticulumBehavior,
    ) -> TransportPublishEventRequest {
        TransportPublishEventRequest {
            raw_event_json,
            target_policy: TransportPublishTargetPolicy::explicit_targets(vec![
                TransportPublishTarget::reticulum(behavior),
            ]),
            delivery_policy: TransportPublishDeliveryPolicy::Any,
            idempotency_key: None,
            timeout_ms: Some(5_000),
        }
    }

    fn interrupted_target_snapshot(
        endpoint_uri: &str,
        source: TransportPublishTargetSource,
    ) -> TransportPublishTargetOutcome {
        TransportPublishTargetOutcome {
            transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
            endpoint_uri: endpoint_uri.to_owned(),
            target_scope: None,
            target_label: None,
            source,
            attempted: true,
            outcome_kind: TransportPublishOutcomeKind::ConnectionFailed,
            message: Some("publish_attempt_interrupted".to_owned()),
            latency_ms: None,
        }
    }

    fn accepted_target_outcome(
        endpoint_uri: &str,
        source: TransportPublishTargetSource,
    ) -> TransportPublishTargetOutcome {
        TransportPublishTargetOutcome {
            transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
            endpoint_uri: endpoint_uri.to_owned(),
            target_scope: None,
            target_label: None,
            source,
            attempted: true,
            outcome_kind: TransportPublishOutcomeKind::Accepted,
            message: None,
            latency_ms: Some(12),
        }
    }

    fn scoped_target_outcome(
        mut outcome: TransportPublishTargetOutcome,
        target_scope: &str,
        target_label: Option<&str>,
    ) -> TransportPublishTargetOutcome {
        outcome.target_scope = Some(target_scope.to_owned());
        outcome.target_label = target_label.map(str::to_owned);
        outcome
    }

    fn store_principal(store: &TransportPublishStore, pubkey: &str) -> PublishPrincipal {
        store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: hash_bearer_token(generate_bearer_token().as_str()),
                allowed_pubkeys: vec![pubkey.to_owned()],
                allowed_kinds: vec![30_402],
                allowed_target_policies: vec![TransportPublishTargetPolicyName::Nostr],
                allowed_explicit_transport_kinds: Vec::new(),
                allowed_nostr_source_policies: vec![
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                ],
                allow_request_targets: false,
                job_visibility: PublishJobVisibility::Own,
                expires_at_unix: None,
            })
            .expect("principal")
    }

    fn assert_invalid_job_state(error: TransportPublishError, expected: &str) {
        match error {
            TransportPublishError::InvalidPublishJobState(message) => {
                assert!(message.contains(expected), "{message}");
                assert!(!message.contains("rrd_tp_"));
                assert!(!message.contains("token"));
            }
            error => panic!("unexpected error: {error}"),
        }
    }

    fn assert_storage_integer_range_error(error: TransportPublishError, expected: &str) {
        match error {
            TransportPublishError::InvalidPublishJobState(message) => {
                assert!(message.contains(expected), "{message}");
                assert!(!message.contains("rrd_tp_"));
                assert!(!message.contains("token"));
            }
            error => panic!("unexpected error: {error}"),
        }
    }

    fn transport_publish(
        config: TransportPublishConfig,
    ) -> (TransportPublish, RadrootsMockRelayPublishAdapter) {
        transport_publish_with_resolver(config, Arc::new(StaticPublishRelayResolver::new()))
    }

    fn transport_publish_with_resolver(
        config: TransportPublishConfig,
        resolver: Arc<dyn super::PublishRelayResolver>,
    ) -> (TransportPublish, RadrootsMockRelayPublishAdapter) {
        let adapter = RadrootsMockRelayPublishAdapter::new();
        let proxy = TransportPublish::memory(config)
            .expect("proxy")
            .with_relay_resolver(resolver)
            .with_publisher(Arc::new(adapter.clone()));
        (proxy, adapter)
    }

    fn principal(
        proxy: &TransportPublish,
        pubkey: String,
        nostr_source_policies: Vec<NostrPublishTargetSourcePolicy>,
        allow_request_targets: bool,
        visibility: PublishJobVisibility,
    ) -> PublishPrincipal {
        proxy
            .store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: hash_bearer_token(generate_bearer_token().as_str()),
                allowed_pubkeys: vec![pubkey],
                allowed_kinds: vec![30_402],
                allowed_target_policies: vec![TransportPublishTargetPolicyName::Nostr],
                allowed_explicit_transport_kinds: Vec::new(),
                allowed_nostr_source_policies: nostr_source_policies,
                allow_request_targets,
                job_visibility: visibility,
                expires_at_unix: None,
            })
            .expect("principal")
    }

    fn explicit_target_principal(
        proxy: &TransportPublish,
        pubkey: String,
        visibility: PublishJobVisibility,
    ) -> PublishPrincipal {
        explicit_target_principal_with_kinds(
            proxy,
            pubkey,
            vec![
                TRANSPORT_KIND_NOSTR.to_owned(),
                TRANSPORT_KIND_RETICULUM.to_owned(),
            ],
            visibility,
        )
    }

    fn explicit_target_principal_with_kinds(
        proxy: &TransportPublish,
        pubkey: String,
        allowed_explicit_transport_kinds: Vec<String>,
        visibility: PublishJobVisibility,
    ) -> PublishPrincipal {
        proxy
            .store
            .create_principal(PublishPrincipalInit {
                label: "explicit-target-tester".to_owned(),
                token_hash: hash_bearer_token(generate_bearer_token().as_str()),
                allowed_pubkeys: vec![pubkey],
                allowed_kinds: vec![30_402],
                allowed_target_policies: vec![TransportPublishTargetPolicyName::ExplicitTargets],
                allowed_explicit_transport_kinds,
                allowed_nostr_source_policies: Vec::new(),
                allow_request_targets: true,
                job_visibility: visibility,
                expires_at_unix: None,
            })
            .expect("principal")
    }

    fn config_with_defaults(relays: Vec<&str>) -> TransportPublishConfig {
        TransportPublishConfig {
            nostr: TransportPublishNostrConfig {
                daemon_default_relays: relays.into_iter().map(str::to_owned).collect(),
                ..TransportPublishNostrConfig::default()
            },
            ..TransportPublishConfig::default()
        }
    }

    #[test]
    fn explicit_target_principals_require_canonical_allowed_transport_kinds() {
        let store = TransportPublishStore::memory().expect("store");
        let base = PublishPrincipalInit {
            label: "explicit-target-tester".to_owned(),
            token_hash: hash_bearer_token(generate_bearer_token().as_str()),
            allowed_pubkeys: vec!["a".repeat(64)],
            allowed_kinds: vec![30_402],
            allowed_target_policies: vec![TransportPublishTargetPolicyName::ExplicitTargets],
            allowed_explicit_transport_kinds: Vec::new(),
            allowed_nostr_source_policies: Vec::new(),
            allow_request_targets: true,
            job_visibility: PublishJobVisibility::Own,
            expires_at_unix: None,
        };

        assert!(matches!(
            store.create_principal(base.clone()),
            Err(TransportPublishError::InvalidScope(message))
                if message.contains("allowed explicit transport kind")
        ));

        let mut uppercase = base.clone();
        uppercase.allowed_explicit_transport_kinds = vec!["Nostr".to_owned()];
        assert!(matches!(
            store.create_principal(uppercase),
            Err(TransportPublishError::InvalidScope(message))
                if message.contains("explicit transport kind")
        ));

        let mut duplicate = base.clone();
        duplicate.allowed_explicit_transport_kinds = vec![
            TRANSPORT_KIND_NOSTR.to_owned(),
            TRANSPORT_KIND_NOSTR.to_owned(),
        ];
        assert!(matches!(
            store.create_principal(duplicate),
            Err(TransportPublishError::InvalidScope(message))
                if message.contains("canonical and unique")
        ));

        let mut removed_execution_kind = base.clone();
        removed_execution_kind.allowed_explicit_transport_kinds =
            vec![removed_proxy_transport_kind_string()];
        assert!(matches!(
            store.create_principal(removed_execution_kind),
            Err(TransportPublishError::InvalidScope(message))
                if message.contains("unknown explicit transport kind")
        ));

        let mut nostr_policy_with_explicit_kinds = base;
        nostr_policy_with_explicit_kinds.allowed_target_policies =
            vec![TransportPublishTargetPolicyName::Nostr];
        nostr_policy_with_explicit_kinds.allowed_explicit_transport_kinds =
            vec![TRANSPORT_KIND_NOSTR.to_owned()];
        nostr_policy_with_explicit_kinds.allowed_nostr_source_policies =
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly];
        assert!(matches!(
            store.create_principal(nostr_policy_with_explicit_kinds),
            Err(TransportPublishError::InvalidScope(message))
                if message.contains("without explicit target policy")
        ));
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
            url: &'a radroots_transport_nostr::RadrootsRelayUrl,
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
        assert!(token.starts_with("rrd_tp_"));
        let hash = hash_bearer_token(token.as_str());
        assert!(hash.starts_with("sha256:"));
        assert!(!hash.contains(token.as_str()));
    }

    #[test]
    fn nostr_source_policy_parser_accepts_contract_values() {
        assert_eq!(
            parse_nostr_source_policy("explicit_only").expect("policy"),
            NostrPublishTargetSourcePolicy::ExplicitOnly
        );
        assert!(parse_nostr_source_policy("unknown").is_err());
    }

    #[test]
    fn storage_authenticates_hashed_tokens_and_scopes_jobs() {
        let store = TransportPublishStore::memory().expect("store");
        let token = generate_bearer_token();
        let token_hash = hash_bearer_token(token.as_str());
        let accepted_identity = RadrootsIdentity::generate();
        let denied_identity = RadrootsIdentity::generate();
        let principal = store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: token_hash.clone(),
                allowed_pubkeys: vec![accepted_identity.public_key_hex()],
                allowed_kinds: vec![30_402],
                allowed_target_policies: vec![TransportPublishTargetPolicyName::Nostr],
                allowed_explicit_transport_kinds: Vec::new(),
                allowed_nostr_source_policies: vec![
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                ],
                allow_request_targets: false,
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
        let denied = publish_request(
            signed_event(&denied_identity, "{}"),
            Vec::new(),
            NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
            TransportPublishDeliveryPolicy::Any,
            None,
        );
        let denied_signed =
            super::signed_event_from_raw_json(denied.raw_event_json.as_str()).expect("denied raw");
        assert!(principal.allows_event(&denied_signed, &denied).is_err());

        let accepted = publish_request(
            signed_event(&accepted_identity, "{}"),
            Vec::new(),
            NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
            TransportPublishDeliveryPolicy::Any,
            None,
        );
        let accepted_signed = super::signed_event_from_raw_json(accepted.raw_event_json.as_str())
            .expect("accepted raw");
        principal
            .allows_event(&accepted_signed, &accepted)
            .expect("scope");
        let response = store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id.clone(),
                idempotency_key: Some("idem-1".to_owned()),
                event: PublishEventMetadata::from_signed_event(&accepted_signed),
                request: accepted.clone(),
                request_fingerprint: "fingerprint-1".to_owned(),
                effective_target_count: 1,
                target_snapshots: vec![interrupted_target_snapshot(
                    RELAY_PRIMARY,
                    TransportPublishTargetSource::DaemonDefault,
                )],
            })
            .expect("record job");
        assert!(!response.deduplicated);
        let duplicate = store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id.clone(),
                idempotency_key: Some("idem-1".to_owned()),
                event: PublishEventMetadata::from_signed_event(&accepted_signed),
                request: accepted,
                request_fingerprint: "fingerprint-1".to_owned(),
                effective_target_count: 1,
                target_snapshots: vec![interrupted_target_snapshot(
                    RELAY_PRIMARY,
                    TransportPublishTargetSource::DaemonDefault,
                )],
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
    fn store_egress_rejects_malformed_target_counts_for_get_list_and_dedupe() {
        let store = TransportPublishStore::memory().expect("store");
        let pubkey = "a".repeat(64);
        let principal = store_principal(&store, pubkey.as_str());
        let request = request(pubkey.as_str(), 30_402);
        let response = store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id.clone(),
                idempotency_key: Some("idem-invalid-target-count".to_owned()),
                event: event_metadata(pubkey.as_str(), 30_402),
                request: request.clone(),
                request_fingerprint: "fingerprint-invalid-target-count".to_owned(),
                effective_target_count: 1,
                target_snapshots: vec![accepted_target_outcome(
                    RELAY_PRIMARY,
                    TransportPublishTargetSource::DaemonDefault,
                )],
            })
            .expect("record job");
        store
            .complete_publish_job(
                response.job.job_id.as_str(),
                TransportPublishJobStatus::DeliverySatisfied,
                vec![accepted_target_outcome(
                    RELAY_PRIMARY,
                    TransportPublishTargetSource::DaemonDefault,
                )],
                None,
            )
            .expect("complete job");
        {
            let mut connection = store
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            super::block_on_sqlite(
                sqlx::query(
                    "UPDATE transport_publish_jobs SET effective_target_count = 2 WHERE job_id = ?1",
                )
                .bind(response.job.job_id.as_str())
                .execute(&mut *connection),
            )
            .expect("corrupt target count");
        }

        assert_invalid_job_state(
            store
                .job_by_id(response.job.job_id.as_str())
                .expect_err("invalid get"),
            "job target_count 2 does not match 1 target outcomes",
        );
        assert_invalid_job_state(
            store
                .list_jobs_for_principal(&principal, 50)
                .expect_err("invalid list"),
            "job target_count 2 does not match 1 target outcomes",
        );
        assert_invalid_job_state(
            store
                .record_publish_job(PublishJobInsert {
                    principal_id: principal.principal_id.clone(),
                    idempotency_key: Some("idem-invalid-target-count".to_owned()),
                    event: event_metadata(pubkey.as_str(), 30_402),
                    request,
                    request_fingerprint: "fingerprint-invalid-target-count".to_owned(),
                    effective_target_count: 1,
                    target_snapshots: vec![accepted_target_outcome(
                        RELAY_PRIMARY,
                        TransportPublishTargetSource::DaemonDefault,
                    )],
                })
                .expect_err("invalid dedupe"),
            "job target_count 2 does not match 1 target outcomes",
        );
    }

    #[test]
    fn store_egress_rejects_impossible_persisted_event_kind_values() {
        for invalid_kind in [-1_i64, i64::from(u32::MAX) + 1] {
            let store = TransportPublishStore::memory().expect("store");
            let pubkey = "a".repeat(64);
            let principal = store_principal(&store, pubkey.as_str());
            let request = request(pubkey.as_str(), 30_402);
            let response = store
                .record_publish_job(PublishJobInsert {
                    principal_id: principal.principal_id.clone(),
                    idempotency_key: Some(format!("idem-invalid-kind-{invalid_kind}")),
                    event: event_metadata(pubkey.as_str(), 30_402),
                    request: request.clone(),
                    request_fingerprint: format!("fingerprint-invalid-kind-{invalid_kind}"),
                    effective_target_count: 1,
                    target_snapshots: vec![accepted_target_outcome(
                        RELAY_PRIMARY,
                        TransportPublishTargetSource::DaemonDefault,
                    )],
                })
                .expect("record job");
            store
                .complete_publish_job(
                    response.job.job_id.as_str(),
                    TransportPublishJobStatus::DeliverySatisfied,
                    vec![accepted_target_outcome(
                        RELAY_PRIMARY,
                        TransportPublishTargetSource::DaemonDefault,
                    )],
                    None,
                )
                .expect("complete job");
            {
                let mut connection = store
                    .inner
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                super::block_on_sqlite(
                    sqlx::query(
                        "UPDATE transport_publish_jobs SET event_kind = ?2 WHERE job_id = ?1",
                    )
                    .bind(response.job.job_id.as_str())
                    .bind(invalid_kind)
                    .execute(&mut *connection),
                )
                .expect("corrupt event kind");
            }

            assert_storage_integer_range_error(
                store
                    .job_by_id(response.job.job_id.as_str())
                    .expect_err("invalid get"),
                "event_kind integer value",
            );
            assert_storage_integer_range_error(
                store
                    .list_jobs_for_principal(&principal, 50)
                    .expect_err("invalid list"),
                "event_kind integer value",
            );
            assert_storage_integer_range_error(
                store
                    .record_publish_job(PublishJobInsert {
                        principal_id: principal.principal_id.clone(),
                        idempotency_key: Some(format!("idem-invalid-kind-{invalid_kind}")),
                        event: event_metadata(pubkey.as_str(), 30_402),
                        request,
                        request_fingerprint: format!("fingerprint-invalid-kind-{invalid_kind}"),
                        effective_target_count: 1,
                        target_snapshots: vec![accepted_target_outcome(
                            RELAY_PRIMARY,
                            TransportPublishTargetSource::DaemonDefault,
                        )],
                    })
                    .expect_err("invalid dedupe"),
                "event_kind integer value",
            );
        }
    }

    #[test]
    fn store_egress_rejects_negative_persisted_effective_target_count() {
        let store = TransportPublishStore::memory().expect("store");
        let pubkey = "a".repeat(64);
        let principal = store_principal(&store, pubkey.as_str());
        let request = request(pubkey.as_str(), 30_402);
        let response = store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id.clone(),
                idempotency_key: Some("idem-negative-target-count".to_owned()),
                event: event_metadata(pubkey.as_str(), 30_402),
                request: request.clone(),
                request_fingerprint: "fingerprint-negative-target-count".to_owned(),
                effective_target_count: 1,
                target_snapshots: vec![accepted_target_outcome(
                    RELAY_PRIMARY,
                    TransportPublishTargetSource::DaemonDefault,
                )],
            })
            .expect("record job");
        store
            .complete_publish_job(
                response.job.job_id.as_str(),
                TransportPublishJobStatus::DeliverySatisfied,
                vec![accepted_target_outcome(
                    RELAY_PRIMARY,
                    TransportPublishTargetSource::DaemonDefault,
                )],
                None,
            )
            .expect("complete job");
        {
            let mut connection = store
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            super::block_on_sqlite(
                sqlx::query(
                    "UPDATE transport_publish_jobs SET effective_target_count = -1 WHERE job_id = ?1",
                )
                .bind(response.job.job_id.as_str())
                .execute(&mut *connection),
            )
            .expect("corrupt target count");
        }

        assert_storage_integer_range_error(
            store
                .job_by_id(response.job.job_id.as_str())
                .expect_err("invalid get"),
            "effective_target_count integer value -1 is outside usize range",
        );
        assert_storage_integer_range_error(
            store
                .list_jobs_for_principal(&principal, 50)
                .expect_err("invalid list"),
            "effective_target_count integer value -1 is outside usize range",
        );
        assert_storage_integer_range_error(
            store
                .record_publish_job(PublishJobInsert {
                    principal_id: principal.principal_id.clone(),
                    idempotency_key: Some("idem-negative-target-count".to_owned()),
                    event: event_metadata(pubkey.as_str(), 30_402),
                    request,
                    request_fingerprint: "fingerprint-negative-target-count".to_owned(),
                    effective_target_count: 1,
                    target_snapshots: vec![accepted_target_outcome(
                        RELAY_PRIMARY,
                        TransportPublishTargetSource::DaemonDefault,
                    )],
                })
                .expect_err("invalid dedupe"),
            "effective_target_count integer value -1 is outside usize range",
        );
    }

    #[test]
    fn store_egress_rejects_negative_persisted_target_latency() {
        let store = TransportPublishStore::memory().expect("store");
        let pubkey = "a".repeat(64);
        let principal = store_principal(&store, pubkey.as_str());
        let request = request(pubkey.as_str(), 30_402);
        let response = store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id.clone(),
                idempotency_key: Some("idem-negative-latency".to_owned()),
                event: event_metadata(pubkey.as_str(), 30_402),
                request: request.clone(),
                request_fingerprint: "fingerprint-negative-latency".to_owned(),
                effective_target_count: 1,
                target_snapshots: vec![accepted_target_outcome(
                    RELAY_PRIMARY,
                    TransportPublishTargetSource::DaemonDefault,
                )],
            })
            .expect("record job");
        store
            .complete_publish_job(
                response.job.job_id.as_str(),
                TransportPublishJobStatus::DeliverySatisfied,
                vec![accepted_target_outcome(
                    RELAY_PRIMARY,
                    TransportPublishTargetSource::DaemonDefault,
                )],
                None,
            )
            .expect("complete job");
        {
            let mut connection = store
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            super::block_on_sqlite(
                sqlx::query(
                    "UPDATE transport_publish_target_results SET latency_ms = -5 WHERE job_id = ?1",
                )
                .bind(response.job.job_id.as_str())
                .execute(&mut *connection),
            )
            .expect("corrupt latency");
        }

        assert_storage_integer_range_error(
            store
                .job_by_id(response.job.job_id.as_str())
                .expect_err("invalid get"),
            "latency_ms integer value -5 is outside u64 range",
        );
        assert_storage_integer_range_error(
            store
                .list_jobs_for_principal(&principal, 50)
                .expect_err("invalid list"),
            "latency_ms integer value -5 is outside u64 range",
        );
        assert_storage_integer_range_error(
            store
                .record_publish_job(PublishJobInsert {
                    principal_id: principal.principal_id.clone(),
                    idempotency_key: Some("idem-negative-latency".to_owned()),
                    event: event_metadata(pubkey.as_str(), 30_402),
                    request,
                    request_fingerprint: "fingerprint-negative-latency".to_owned(),
                    effective_target_count: 1,
                    target_snapshots: vec![accepted_target_outcome(
                        RELAY_PRIMARY,
                        TransportPublishTargetSource::DaemonDefault,
                    )],
                })
                .expect_err("invalid dedupe"),
            "latency_ms integer value -5 is outside u64 range",
        );
    }

    #[test]
    fn store_egress_rejects_explicit_target_outcome_drift() {
        let store = TransportPublishStore::memory().expect("store");
        let pubkey = "a".repeat(64);
        let principal = store_principal(&store, pubkey.as_str());
        let mut request = request(pubkey.as_str(), 30_402);
        request.target_policy =
            TransportPublishTargetPolicy::explicit_targets(vec![TransportPublishTarget::nostr(
                RELAY_PRIMARY,
            )]);
        let response = store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id.clone(),
                idempotency_key: Some("idem-explicit-drift".to_owned()),
                event: event_metadata(pubkey.as_str(), 30_402),
                request,
                request_fingerprint: "fingerprint-explicit-drift".to_owned(),
                effective_target_count: 1,
                target_snapshots: vec![accepted_target_outcome(
                    RELAY_PRIMARY,
                    TransportPublishTargetSource::Request,
                )],
            })
            .expect("record job");
        store
            .complete_publish_job(
                response.job.job_id.as_str(),
                TransportPublishJobStatus::DeliverySatisfied,
                vec![accepted_target_outcome(
                    RELAY_SECONDARY,
                    TransportPublishTargetSource::Request,
                )],
                None,
            )
            .expect("complete drifted job");

        assert_invalid_job_state(
            store
                .job_by_id(response.job.job_id.as_str())
                .expect_err("invalid explicit drift"),
            "transport target outcome 0 does not match explicit target policy",
        );
    }

    #[test]
    fn store_egress_rejects_explicit_target_scope_drift() {
        let store = TransportPublishStore::memory().expect("store");
        let pubkey = "a".repeat(64);
        let principal = store_principal(&store, pubkey.as_str());
        let mut request = request(pubkey.as_str(), 30_402);
        request.target_policy = TransportPublishTargetPolicy::explicit_targets(vec![
            TransportPublishTarget::nostr(RELAY_PRIMARY)
                .with_scope("farm.local")
                .with_label("Farm relay"),
        ]);
        let response = store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id.clone(),
                idempotency_key: Some("idem-explicit-scope-drift".to_owned()),
                event: event_metadata(pubkey.as_str(), 30_402),
                request,
                request_fingerprint: "fingerprint-explicit-scope-drift".to_owned(),
                effective_target_count: 1,
                target_snapshots: vec![scoped_target_outcome(
                    accepted_target_outcome(RELAY_PRIMARY, TransportPublishTargetSource::Request),
                    "farm.local",
                    Some("Farm relay"),
                )],
            })
            .expect("record job");
        store
            .complete_publish_job(
                response.job.job_id.as_str(),
                TransportPublishJobStatus::DeliverySatisfied,
                vec![scoped_target_outcome(
                    accepted_target_outcome(RELAY_PRIMARY, TransportPublishTargetSource::Request),
                    "farm.remote",
                    Some("Farm relay"),
                )],
                None,
            )
            .expect("complete drifted job");

        assert_invalid_job_state(
            store
                .job_by_id(response.job.job_id.as_str())
                .expect_err("invalid explicit scope drift"),
            "transport target outcome 0 does not match explicit target policy",
        );
    }

    #[test]
    fn store_open_recovers_interrupted_publishing_jobs() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory.path().join("publish-proxy.sqlite");
        let token_hash = hash_bearer_token(generate_bearer_token().as_str());
        let pubkey = "a".repeat(64);
        let request = request(pubkey.as_str(), 30_402);
        let (job_id, principal) = {
            let store = TransportPublishStore::open(database_path.clone()).expect("store");
            let principal = store
                .create_principal(PublishPrincipalInit {
                    label: "tester".to_owned(),
                    token_hash,
                    allowed_pubkeys: vec![pubkey.clone()],
                    allowed_kinds: vec![30_402],
                    allowed_target_policies: vec![TransportPublishTargetPolicyName::Nostr],
                    allowed_explicit_transport_kinds: Vec::new(),
                    allowed_nostr_source_policies: vec![
                        NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    ],
                    allow_request_targets: false,
                    job_visibility: PublishJobVisibility::Own,
                    expires_at_unix: None,
                })
                .expect("principal");
            let response = store
                .record_publish_job(PublishJobInsert {
                    principal_id: principal.principal_id.clone(),
                    idempotency_key: Some("idem-interrupted".to_owned()),
                    event: event_metadata(pubkey.as_str(), 30_402),
                    request,
                    request_fingerprint: "fingerprint-interrupted".to_owned(),
                    effective_target_count: 1,
                    target_snapshots: vec![interrupted_target_snapshot(
                        RELAY_PRIMARY,
                        TransportPublishTargetSource::DaemonDefault,
                    )],
                })
                .expect("record job");
            assert_eq!(response.job.status, TransportPublishJobStatus::Publishing);
            (response.job.job_id, principal)
        };

        let reopened = TransportPublishStore::open(database_path).expect("reopen store");
        let recovered = reopened.job_by_id(job_id.as_str()).expect("recovered job");
        assert_eq!(
            recovered.status,
            TransportPublishJobStatus::DeliveryUnsatisfiedRetryable
        );
        assert_eq!(
            recovered.last_error.as_deref(),
            Some("publish_attempt_interrupted")
        );
        assert!(recovered.completed_at_ms.is_some());
        assert_eq!(recovered.targets.len(), 1);
        assert_eq!(
            recovered.targets[0].outcome_kind,
            TransportPublishOutcomeKind::ConnectionFailed
        );
        recovered.validate().expect("valid recovered job");
        let listed = reopened
            .list_jobs_for_principal(&principal, 50)
            .expect("listed jobs");
        assert_eq!(listed.len(), 1);
        listed[0].validate().expect("valid listed recovered job");
    }

    #[test]
    fn store_open_recovers_interrupted_scoped_explicit_target_metadata() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory.path().join("publish-proxy-scoped.sqlite");
        let pubkey = "a".repeat(64);
        let (job_id, principal) = {
            let store = TransportPublishStore::open(database_path.clone()).expect("store");
            let principal = store_principal(&store, pubkey.as_str());
            let mut request = request(pubkey.as_str(), 30_402);
            request.target_policy = TransportPublishTargetPolicy::explicit_targets(vec![
                TransportPublishTarget::nostr(RELAY_PRIMARY)
                    .with_scope("farm.local")
                    .with_label("Farm relay"),
            ]);
            let response = store
                .record_publish_job(PublishJobInsert {
                    principal_id: principal.principal_id.clone(),
                    idempotency_key: Some("idem-interrupted-scoped".to_owned()),
                    event: event_metadata(pubkey.as_str(), 30_402),
                    request,
                    request_fingerprint: "fingerprint-interrupted-scoped".to_owned(),
                    effective_target_count: 1,
                    target_snapshots: vec![scoped_target_outcome(
                        interrupted_target_snapshot(
                            RELAY_PRIMARY,
                            TransportPublishTargetSource::Request,
                        ),
                        "farm.local",
                        Some("Farm relay"),
                    )],
                })
                .expect("record job");
            (response.job.job_id, principal)
        };

        let reopened = TransportPublishStore::open(database_path).expect("reopen store");
        let recovered = reopened.job_by_id(job_id.as_str()).expect("recovered job");
        assert_eq!(
            recovered.status,
            TransportPublishJobStatus::DeliveryUnsatisfiedRetryable
        );
        assert_eq!(recovered.targets.len(), 1);
        assert_eq!(
            recovered.targets[0].target_scope.as_deref(),
            Some("farm.local")
        );
        assert_eq!(
            recovered.targets[0].target_label.as_deref(),
            Some("Farm relay")
        );
        recovered.validate().expect("valid recovered job");
        let listed = reopened
            .list_jobs_for_principal(&principal, 50)
            .expect("listed jobs");
        assert_eq!(
            listed[0].targets[0].target_scope.as_deref(),
            Some("farm.local")
        );
        assert_eq!(
            listed[0].targets[0].target_label.as_deref(),
            Some("Farm relay")
        );
    }

    #[test]
    fn store_egress_rejects_recovered_explicit_target_snapshot_drift() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory.path().join("publish-proxy-drift.sqlite");
        let pubkey = "a".repeat(64);
        let (job_id, principal) = {
            let store = TransportPublishStore::open(database_path.clone()).expect("store");
            let principal = store_principal(&store, pubkey.as_str());
            let mut request = request(pubkey.as_str(), 30_402);
            request.target_policy = TransportPublishTargetPolicy::explicit_targets(vec![
                TransportPublishTarget::nostr(RELAY_PRIMARY),
            ]);
            let response = store
                .record_publish_job(PublishJobInsert {
                    principal_id: principal.principal_id.clone(),
                    idempotency_key: Some("idem-recovered-drift".to_owned()),
                    event: event_metadata(pubkey.as_str(), 30_402),
                    request,
                    request_fingerprint: "fingerprint-recovered-drift".to_owned(),
                    effective_target_count: 1,
                    target_snapshots: vec![accepted_target_outcome(
                        RELAY_SECONDARY,
                        TransportPublishTargetSource::Request,
                    )],
                })
                .expect("record job");
            (response.job.job_id, principal)
        };

        let reopened = TransportPublishStore::open(database_path).expect("reopen store");
        assert_invalid_job_state(
            reopened
                .job_by_id(job_id.as_str())
                .expect_err("invalid get"),
            "transport target outcome 0 does not match explicit target policy",
        );
        assert_invalid_job_state(
            reopened
                .list_jobs_for_principal(&principal, 50)
                .expect_err("invalid list"),
            "transport target outcome 0 does not match explicit target policy",
        );
    }

    #[test]
    fn store_open_rejects_interrupted_jobs_without_target_snapshots() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory
            .path()
            .join("publish-proxy-missing-snapshot.sqlite");
        let token_hash = hash_bearer_token(generate_bearer_token().as_str());
        let pubkey = "a".repeat(64);
        let request = request(pubkey.as_str(), 30_402);
        {
            let store = TransportPublishStore::open(database_path.clone()).expect("store");
            let principal = store
                .create_principal(PublishPrincipalInit {
                    label: "tester".to_owned(),
                    token_hash,
                    allowed_pubkeys: vec![pubkey.clone()],
                    allowed_kinds: vec![30_402],
                    allowed_target_policies: vec![TransportPublishTargetPolicyName::Nostr],
                    allowed_explicit_transport_kinds: Vec::new(),
                    allowed_nostr_source_policies: vec![
                        NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    ],
                    allow_request_targets: false,
                    job_visibility: PublishJobVisibility::Own,
                    expires_at_unix: None,
                })
                .expect("principal");
            let now = super::current_unix_millis();
            let mut connection = store
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            super::block_on_sqlite(
                sqlx::query(
                    r#"
                    INSERT INTO transport_publish_jobs (
                        job_id,
                        principal_id,
                        idempotency_key,
                        request_fingerprint,
                        status,
                        event_id,
                        event_pubkey,
                        event_kind,
                        target_policy_json,
                        delivery_policy_json,
                        requested_target_count,
                        effective_target_count,
                        request_json,
                        requested_at_ms,
                        updated_at_ms
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                    "#,
                )
                .bind("job-missing-snapshot")
                .bind(principal.principal_id.as_str())
                .bind("idem-missing-snapshot")
                .bind("fingerprint-missing-snapshot")
                .bind(
                    serde_json::to_string(&TransportPublishJobStatus::Publishing).expect("status"),
                )
                .bind("0".repeat(64))
                .bind(pubkey.as_str())
                .bind(30_402_i64)
                .bind(serde_json::to_string(&request.target_policy).expect("target policy"))
                .bind(serde_json::to_string(&request.delivery_policy).expect("delivery policy"))
                .bind(
                    super::storage_count_i64(
                        request.target_policy.request_target_count(),
                        "requested_target_count",
                    )
                    .expect("requested target count"),
                )
                .bind(1_i64)
                .bind(serde_json::to_string(&request).expect("request"))
                .bind(now)
                .bind(now)
                .execute(&mut *connection),
            )
            .expect("insert historical job");
        }

        let reopened = TransportPublishStore::open(database_path).expect("reopen store");
        let recovered = reopened
            .job_by_id("job-missing-snapshot")
            .expect("recovered job");
        assert_eq!(recovered.status, TransportPublishJobStatus::Rejected);
        assert_eq!(
            recovered.last_error.as_deref(),
            Some("publish_attempt_interrupted_missing_target_snapshot")
        );
        assert_eq!(recovered.target_count, 0);
        assert!(recovered.targets.is_empty());
        recovered.validate().expect("valid rejected recovered job");
    }

    #[test]
    fn transport_store_open_validates_current_principal_schema() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory.path().join("publish-proxy-current.sqlite");
        let store = TransportPublishStore::open(database_path).expect("open current schema");
        let mut connection = store
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let columns = test_query_column_names(&mut connection, "transport_publish_principals");
        assert!(
            columns
                .iter()
                .any(|column| column == "allowed_explicit_transport_kinds_json")
        );
        let version =
            super::transport_publish_schema_version(&mut connection).expect("user version");
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn transport_store_open_rejects_legacy_principal_schema_without_explicit_kind_allowlist() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory.path().join("publish-proxy-v1.sqlite");
        let token_hash = hash_bearer_token(generate_bearer_token().as_str());
        {
            let mut connection = open_test_database(database_path.as_path());
            super::execute_raw_sql(
                &mut connection,
                r#"
                    CREATE TABLE transport_publish_principals (
                        principal_id TEXT PRIMARY KEY NOT NULL,
                        label TEXT NOT NULL,
                        token_hash TEXT NOT NULL UNIQUE,
                        allowed_pubkeys_json TEXT NOT NULL,
                        allowed_kinds_json TEXT NOT NULL,
                        allowed_target_policies_json TEXT NOT NULL,
                        allowed_nostr_source_policies_json TEXT NOT NULL,
                        allow_request_targets INTEGER NOT NULL,
                        job_visibility TEXT NOT NULL,
                        expires_at_unix INTEGER,
                        revoked_at_unix INTEGER,
                        created_at_unix INTEGER NOT NULL
                    );
                    "#,
            )
            .expect("schema");
            super::execute_sql(
                &mut connection,
                format!("PRAGMA user_version = {SCHEMA_VERSION}").as_str(),
            )
            .expect("set schema version");
            super::block_on_sqlite(
                sqlx::query(
                    r#"
                    INSERT INTO transport_publish_principals (
                        principal_id,
                        label,
                        token_hash,
                        allowed_pubkeys_json,
                        allowed_kinds_json,
                        allowed_target_policies_json,
                        allowed_nostr_source_policies_json,
                        allow_request_targets,
                        job_visibility,
                        expires_at_unix,
                        revoked_at_unix,
                        created_at_unix
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, NULL, ?10)
                    "#,
                )
                .bind("principal-v1")
                .bind("v1")
                .bind(token_hash.as_str())
                .bind(serde_json::to_string(&vec!["a".repeat(64)]).expect("pubkeys"))
                .bind(serde_json::to_string(&vec![30_402]).expect("kinds"))
                .bind(
                    serde_json::to_string(&vec![TransportPublishTargetPolicyName::Nostr])
                        .expect("policies"),
                )
                .bind(
                    serde_json::to_string(&vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly])
                        .expect("source policies"),
                )
                .bind(false)
                .bind(PublishJobVisibility::Own.to_string())
                .bind(1_i64)
                .execute(&mut connection),
            )
            .expect("principal");
        }

        let error = match TransportPublishStore::open(database_path.clone()) {
            Ok(_) => panic!("legacy schema opened"),
            Err(error) => error,
        };
        match error {
            TransportPublishError::Schema { table, detail } => {
                assert_eq!(table, "transport_publish_principals");
                assert!(detail.contains("allowed_explicit_transport_kinds_json"));
            }
            error => panic!("unexpected error: {error}"),
        }

        let mut connection = open_test_database(database_path.as_path());
        let columns = test_query_column_names(&mut connection, "transport_publish_principals");
        assert!(
            !columns
                .iter()
                .any(|column| column == "allowed_explicit_transport_kinds_json")
        );
        assert_eq!(
            database_user_version(database_path.as_path()),
            SCHEMA_VERSION
        );
    }

    #[test]
    fn transport_store_open_rejects_legacy_schema_version() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory.path().join("publish-proxy-v3.sqlite");
        create_existing_schema(
            database_path.as_path(),
            TRANSPORT_PUBLISH_SCHEMA_SQL,
            Some(3),
        );

        assert_schema_error(
            open_schema_error(database_path.as_path()),
            "transport_publish_schema",
            "user_version",
        );
        assert_eq!(database_user_version(database_path.as_path()), 3);
    }

    #[test]
    fn transport_store_open_rejects_current_schema_missing_user_version() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory
            .path()
            .join("publish-proxy-missing-version.sqlite");
        create_existing_schema(database_path.as_path(), TRANSPORT_PUBLISH_SCHEMA_SQL, None);

        assert_schema_error(
            open_schema_error(database_path.as_path()),
            "transport_publish_schema",
            "user_version",
        );
        assert_eq!(database_user_version(database_path.as_path()), 0);
    }

    #[test]
    fn transport_store_open_rejects_current_schema_without_token_hash_unique_index() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory
            .path()
            .join("publish-proxy-missing-token-unique.sqlite");
        let schema = schema_with_replacement(
            "token_hash TEXT NOT NULL UNIQUE",
            "token_hash TEXT NOT NULL",
        );
        create_existing_schema(
            database_path.as_path(),
            schema.as_str(),
            Some(SCHEMA_VERSION),
        );

        assert_schema_error(
            open_schema_error(database_path.as_path()),
            "transport_publish_principals",
            "token_hash",
        );
        assert_eq!(
            database_user_version(database_path.as_path()),
            SCHEMA_VERSION
        );
    }

    #[test]
    fn transport_store_open_rejects_current_schema_without_idempotency_unique_index() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory
            .path()
            .join("publish-proxy-missing-idempotency-index.sqlite");
        let schema = schema_with_replacement(
            r#"CREATE UNIQUE INDEX IF NOT EXISTS transport_publish_jobs_principal_idempotency_idx
    ON transport_publish_jobs(principal_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;"#,
            "",
        );
        create_existing_schema(
            database_path.as_path(),
            schema.as_str(),
            Some(SCHEMA_VERSION),
        );

        assert_schema_error(
            open_schema_error(database_path.as_path()),
            "transport_publish_jobs",
            "principal_id, idempotency_key",
        );
        assert_eq!(
            database_user_version(database_path.as_path()),
            SCHEMA_VERSION
        );
    }

    #[test]
    fn transport_store_open_rejects_current_target_results_schema_without_scope_not_null() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory
            .path()
            .join("publish-proxy-target-scope-nullable.sqlite");
        let schema = schema_with_replacement("target_scope TEXT NOT NULL", "target_scope TEXT");
        create_existing_schema(
            database_path.as_path(),
            schema.as_str(),
            Some(SCHEMA_VERSION),
        );

        assert_schema_error(
            open_schema_error(database_path.as_path()),
            "transport_publish_target_results",
            "target_scope",
        );
        assert_eq!(
            database_user_version(database_path.as_path()),
            SCHEMA_VERSION
        );
    }

    #[test]
    fn transport_store_open_rejects_current_target_results_schema_without_scoped_primary_key() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory
            .path()
            .join("publish-proxy-target-results-unscoped-pk.sqlite");
        let schema = schema_with_replacement(
            "PRIMARY KEY(job_id, transport_kind, endpoint_uri, target_scope)",
            "PRIMARY KEY(job_id, transport_kind, endpoint_uri)",
        );
        create_existing_schema(
            database_path.as_path(),
            schema.as_str(),
            Some(SCHEMA_VERSION),
        );

        assert_schema_error(
            open_schema_error(database_path.as_path()),
            "transport_publish_target_results",
            "primary key",
        );
        assert_eq!(
            database_user_version(database_path.as_path()),
            SCHEMA_VERSION
        );
    }

    #[test]
    fn transport_store_open_rejects_current_schema_without_job_primary_key() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory.path().join("publish-proxy-missing-job-pk.sqlite");
        let schema =
            schema_with_replacement("job_id TEXT PRIMARY KEY NOT NULL", "job_id TEXT NOT NULL");
        create_existing_schema(
            database_path.as_path(),
            schema.as_str(),
            Some(SCHEMA_VERSION),
        );

        assert_schema_error(
            open_schema_error(database_path.as_path()),
            "transport_publish_jobs",
            "primary key",
        );
        assert_eq!(
            database_user_version(database_path.as_path()),
            SCHEMA_VERSION
        );
    }

    #[test]
    fn transport_store_open_rejects_current_schema_without_job_foreign_key() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_path = directory.path().join("publish-proxy-missing-job-fk.sqlite");
        let schema = schema_with_replacement(
            ",
    FOREIGN KEY(principal_id) REFERENCES transport_publish_principals(principal_id)",
            "",
        );
        create_existing_schema(
            database_path.as_path(),
            schema.as_str(),
            Some(SCHEMA_VERSION),
        );

        assert_schema_error(
            open_schema_error(database_path.as_path()),
            "transport_publish_jobs",
            "foreign key",
        );
        assert_eq!(
            database_user_version(database_path.as_path()),
            SCHEMA_VERSION
        );
    }

    #[tokio::test]
    async fn publish_event_verifies_and_records_daemon_default_outcome() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let event = signed_event(&identity, "{}");
        let raw_event = event.clone();
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    event,
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    TransportPublishDeliveryPolicy::Any,
                    Some("idem-valid"),
                ),
            )
            .await
            .expect("publish");

        assert!(!response.deduplicated);
        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliverySatisfied
        );
        assert_eq!(response.job.target_count, 1);
        assert_eq!(response.job.acknowledged_count, 1);
        assert_eq!(response.job.targets[0].endpoint_uri, RELAY_PRIMARY);
        assert_eq!(
            response.job.targets[0].source,
            TransportPublishTargetSource::DaemonDefault
        );
        assert_eq!(adapter.captured_raw_events(), vec![raw_event]);
    }

    #[tokio::test]
    async fn publish_event_rejects_tampered_content_before_publish() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let event = raw_event_with_field(
            signed_event(&identity, "trusted"),
            "content",
            serde_json::Value::String("tampered".to_owned()),
        );
        let error = proxy
            .publish_event(
                &principal,
                publish_request(
                    event,
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect_err("tampered event should fail");

        assert!(matches!(error, TransportPublishError::EventWire(_)));
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_rejects_wrong_signature_before_publish() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let event = signed_event(&identity, "{}");
        let parsed: serde_json::Value = serde_json::from_str(event.as_str()).expect("event json");
        let sig = parsed["sig"].as_str().expect("sig");
        let replacement = if sig.starts_with('0') { "1" } else { "0" };
        let mut tampered_sig = sig.to_owned();
        tampered_sig.replace_range(0..1, replacement);
        let event = raw_event_with_field(event, "sig", serde_json::Value::String(tampered_sig));
        let error = proxy
            .publish_event(
                &principal,
                publish_request(
                    event,
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect_err("wrong signature should fail");

        assert!(matches!(
            error,
            TransportPublishError::SignedEventSignature(_)
        ));
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_rejects_malformed_wire_fields() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let event = signed_event(&identity, "{}");
        let parsed: serde_json::Value = serde_json::from_str(event.as_str()).expect("event json");
        let event_id = parsed["id"].as_str().expect("event id").to_uppercase();
        let event = raw_event_with_field(event, "id", serde_json::Value::String(event_id));
        let error = proxy
            .publish_event(
                &principal,
                publish_request(
                    event,
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect_err("malformed field should fail");

        assert!(matches!(error, TransportPublishError::EventWire(_)));
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_uses_explicit_request_relays_when_allowed() {
        let identity = RadrootsIdentity::generate();
        let (proxy, _adapter) = transport_publish(config_with_defaults(vec![RELAY_SECONDARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::RequestThenAuthorWriteThenDaemonDefault],
            true,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    vec![RELAY_PRIMARY.to_owned()],
                    NostrPublishTargetSourcePolicy::RequestThenAuthorWriteThenDaemonDefault,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliverySatisfied
        );
        assert_eq!(response.job.targets[0].endpoint_uri, RELAY_PRIMARY);
        assert_eq!(
            response.job.targets[0].source,
            TransportPublishTargetSource::Request
        );
    }

    #[tokio::test]
    async fn publish_event_uses_cached_nip65_author_write_before_defaults() {
        let identity = RadrootsIdentity::generate();
        let (proxy, _adapter) = transport_publish(config_with_defaults(vec![RELAY_SECONDARY]));
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
            vec![NostrPublishTargetSourcePolicy::AuthorWriteThenDaemonDefault],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::AuthorWriteThenDaemonDefault,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(response.job.targets[0].endpoint_uri, RELAY_PRIMARY);
        assert_eq!(
            response.job.targets[0].source,
            TransportPublishTargetSource::NostrAuthorWrite
        );
    }

    #[tokio::test]
    async fn publish_event_records_invalid_cached_author_write_relay() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(config_with_defaults(vec![RELAY_SECONDARY]));
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
            vec![NostrPublishTargetSourcePolicy::AuthorWriteThenDaemonDefault],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::AuthorWriteThenDaemonDefault,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliverySatisfied
        );
        let accepted = response
            .job
            .targets
            .iter()
            .find(|relay| relay.endpoint_uri == RELAY_PRIMARY)
            .expect("accepted author relay");
        assert_eq!(
            accepted.source,
            TransportPublishTargetSource::NostrAuthorWrite
        );
        assert!(accepted.attempted);
        let rejected = response
            .job
            .targets
            .iter()
            .find(|relay| relay.endpoint_uri == "not a cached relay")
            .expect("rejected cached author relay");
        assert_eq!(
            rejected.source,
            TransportPublishTargetSource::NostrAuthorWrite
        );
        assert_eq!(
            rejected.outcome_kind,
            TransportPublishOutcomeKind::TargetRejected
        );
        assert!(!rejected.attempted);
        assert_eq!(adapter.captured_raw_events().len(), 1);
    }

    #[tokio::test]
    async fn publish_event_preserves_author_and_discovery_rejections_through_relay_selection() {
        let identity = RadrootsIdentity::generate();
        let mut config = config_with_defaults(vec![RELAY_SECONDARY]);
        config.nostr.author_relay_discovery_relays = vec!["not a discovery relay".to_owned()];
        let (proxy, adapter) = transport_publish(config);
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
            vec![NostrPublishTargetSourcePolicy::AuthorWriteThenDaemonDefault],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::AuthorWriteThenDaemonDefault,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliverySatisfied
        );
        let daemon_default = response
            .job
            .targets
            .iter()
            .find(|relay| relay.endpoint_uri == RELAY_SECONDARY)
            .expect("daemon default relay");
        assert_eq!(
            daemon_default.source,
            TransportPublishTargetSource::DaemonDefault
        );
        assert!(daemon_default.attempted);
        let cached = response
            .job
            .targets
            .iter()
            .find(|relay| relay.endpoint_uri == "not a cached relay")
            .expect("cached author rejection");
        assert_eq!(
            cached.source,
            TransportPublishTargetSource::NostrAuthorWrite
        );
        assert_eq!(
            cached.outcome_kind,
            TransportPublishOutcomeKind::TargetRejected
        );
        assert!(!cached.attempted);
        let discovery = response
            .job
            .targets
            .iter()
            .find(|relay| relay.endpoint_uri == "not a discovery relay")
            .expect("discovery relay rejection");
        assert_eq!(
            discovery.source,
            TransportPublishTargetSource::DaemonDefault
        );
        assert_eq!(
            discovery.outcome_kind,
            TransportPublishOutcomeKind::TargetRejected
        );
        assert!(!discovery.attempted);
        assert_eq!(adapter.captured_raw_events().len(), 1);
    }

    #[tokio::test]
    async fn publish_event_preserves_discovery_and_discovered_author_rejections() {
        let identity = RadrootsIdentity::generate();
        let mut config = config_with_defaults(vec![RELAY_PRIMARY]);
        config.nostr.author_relay_discovery_relays =
            vec![RELAY_PRIMARY.to_owned(), RELAY_FORBIDDEN.to_owned()];
        let resolver = StaticPublishRelayResolver::new().with_addresses(
            RELAY_FORBIDDEN,
            vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))],
        );
        let adapter = RadrootsMockRelayPublishAdapter::new();
        let proxy = TransportPublish::memory(config)
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
            vec![NostrPublishTargetSourcePolicy::AuthorWriteThenDaemonDefault],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::AuthorWriteThenDaemonDefault,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliverySatisfied
        );
        let accepted = response
            .job
            .targets
            .iter()
            .find(|relay| relay.endpoint_uri == RELAY_SECONDARY)
            .expect("discovered author relay");
        assert_eq!(
            accepted.source,
            TransportPublishTargetSource::NostrAuthorWrite
        );
        assert!(accepted.attempted);
        let discovered = response
            .job
            .targets
            .iter()
            .find(|relay| relay.endpoint_uri == "not a discovered author relay")
            .expect("discovered author rejection");
        assert_eq!(
            discovered.source,
            TransportPublishTargetSource::NostrAuthorWrite
        );
        assert_eq!(
            discovered.outcome_kind,
            TransportPublishOutcomeKind::TargetRejected
        );
        assert!(!discovered.attempted);
        let discovery = response
            .job
            .targets
            .iter()
            .find(|relay| relay.endpoint_uri == RELAY_FORBIDDEN)
            .expect("discovery relay rejection");
        assert_eq!(
            discovery.source,
            TransportPublishTargetSource::DaemonDefault
        );
        assert_eq!(
            discovery.outcome_kind,
            TransportPublishOutcomeKind::TargetRejected
        );
        assert!(!discovery.attempted);
        assert_eq!(adapter.captured_raw_events().len(), 1);
    }

    #[tokio::test]
    async fn publish_event_records_no_transport_publish_targets_failure() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(TransportPublishConfig::default());
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(response.job.status, TransportPublishJobStatus::Rejected);
        assert_eq!(
            response.job.last_error.as_deref(),
            Some("no_transport_publish_targets")
        );
        assert!(response.job.targets.is_empty());
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_records_reticulum_unavailable_as_deferred_until_implemented() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(TransportPublishConfig::default());
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let response = proxy
            .publish_event(
                &principal,
                reticulum_publish_request(
                    signed_event(&identity, "{}"),
                    TransportPublishReticulumBehavior::RejectDeliveryAttempts,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliveryDeferredUntilImplemented
        );
        assert!(response.job.terminal);
        assert!(!response.job.delivery_satisfied);
        assert_eq!(response.job.terminal_count, 0);
        assert!(response.job.completed_at_ms.is_some());
        assert_eq!(
            response.job.last_error.as_deref(),
            Some("delivery_deferred_until_implemented")
        );
        assert_eq!(response.job.targets.len(), 1);
        assert_eq!(
            response.job.targets[0].outcome_kind,
            TransportPublishOutcomeKind::DeferredUntilImplemented
        );
        assert_eq!(
            response.job.targets[0].message.as_deref(),
            Some(RADROOTS_RETICULUM_UNAVAILABLE_MESSAGE)
        );
        assert!(!response.job.targets[0].attempted);
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_records_explicit_nostr_target_when_kind_allowed() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let event = signed_event(&identity, "{}");
        let raw_event = event.clone();
        let request = TransportPublishEventRequest {
            raw_event_json: event,
            target_policy: TransportPublishTargetPolicy::explicit_targets(vec![
                TransportPublishTarget::nostr(RELAY_PRIMARY),
            ]),
            delivery_policy: TransportPublishDeliveryPolicy::Any,
            idempotency_key: None,
            timeout_ms: Some(5_000),
        };

        let response = proxy
            .publish_event(&principal, request)
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliverySatisfied
        );
        assert_eq!(response.job.targets.len(), 1);
        assert_eq!(
            response.job.targets[0].source,
            TransportPublishTargetSource::Request
        );
        assert_eq!(response.job.targets[0].endpoint_uri, RELAY_PRIMARY);
        assert_eq!(response.job.targets[0].target_scope, None);
        assert_eq!(response.job.targets[0].target_label, None);
        assert_eq!(adapter.captured_raw_events(), vec![raw_event]);
    }

    #[tokio::test]
    async fn publish_event_preserves_explicit_nostr_target_metadata_when_kind_allowed() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let event = signed_event(&identity, "{}");
        let raw_event = event.clone();
        let request = TransportPublishEventRequest {
            raw_event_json: event,
            target_policy: TransportPublishTargetPolicy::explicit_targets(vec![
                TransportPublishTarget::nostr(RELAY_PRIMARY)
                    .with_scope("farm.local")
                    .with_label("Farm relay"),
            ]),
            delivery_policy: TransportPublishDeliveryPolicy::Any,
            idempotency_key: None,
            timeout_ms: Some(5_000),
        };

        let response = proxy
            .publish_event(&principal, request)
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliverySatisfied
        );
        assert_eq!(response.job.targets.len(), 1);
        assert_eq!(response.job.targets[0].endpoint_uri, RELAY_PRIMARY);
        assert_eq!(
            response.job.targets[0].target_scope.as_deref(),
            Some("farm.local")
        );
        assert_eq!(
            response.job.targets[0].target_label.as_deref(),
            Some("Farm relay")
        );
        assert_eq!(
            response.job.targets[0].source,
            TransportPublishTargetSource::Request
        );
        assert_eq!(adapter.captured_raw_events(), vec![raw_event]);
        response.job.validate().expect("valid scoped job");
    }

    #[tokio::test]
    async fn publish_event_records_scoped_targets_with_shared_relay_url() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let event = signed_event(&identity, "{}");
        let raw_event = event.clone();
        let request = TransportPublishEventRequest {
            raw_event_json: event,
            target_policy: TransportPublishTargetPolicy::explicit_targets(vec![
                TransportPublishTarget::nostr(RELAY_PRIMARY)
                    .with_scope("farm.a")
                    .with_label("Farm A"),
                TransportPublishTarget::nostr(RELAY_PRIMARY)
                    .with_scope("farm.b")
                    .with_label("Farm B"),
            ]),
            delivery_policy: TransportPublishDeliveryPolicy::All,
            idempotency_key: None,
            timeout_ms: Some(5_000),
        };

        let response = proxy
            .publish_event(&principal, request)
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliverySatisfied
        );
        assert_eq!(response.job.targets.len(), 2);
        assert!(
            response
                .job
                .targets
                .iter()
                .all(|target| target.endpoint_uri == RELAY_PRIMARY)
        );
        assert_eq!(
            response
                .job
                .targets
                .iter()
                .map(|target| (
                    target.target_scope.as_deref(),
                    target.target_label.as_deref()
                ))
                .collect::<Vec<_>>(),
            vec![
                (Some("farm.a"), Some("Farm A")),
                (Some("farm.b"), Some("Farm B")),
            ]
        );
        assert_eq!(adapter.captured_raw_events(), vec![raw_event]);
        response
            .job
            .validate()
            .expect("valid scoped shared relay job");
    }

    #[tokio::test]
    async fn publish_event_required_targets_do_not_count_optional_success() {
        let identity = RadrootsIdentity::generate();
        let adapter = RadrootsMockRelayPublishAdapter::new()
            .with_outcome(
                RELAY_PRIMARY,
                RadrootsRelayOutcome::classify("restricted: required relay rejected"),
            )
            .with_outcome(RELAY_SECONDARY, RadrootsRelayOutcome::accepted());
        let (proxy, _) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let proxy = proxy.with_publisher(Arc::new(adapter.clone()));
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let required_target =
            RadrootsTransportTarget::nostr_relay(RELAY_PRIMARY).expect("required target");
        let request = TransportPublishEventRequest {
            raw_event_json: signed_event(&identity, "{}"),
            target_policy: TransportPublishTargetPolicy::explicit_targets(vec![
                TransportPublishTarget::nostr(RELAY_PRIMARY),
                TransportPublishTarget::nostr(RELAY_SECONDARY),
            ]),
            delivery_policy: TransportPublishDeliveryPolicy::required_targets(vec![
                required_target.fingerprint,
            ])
            .expect("required targets"),
            idempotency_key: None,
            timeout_ms: Some(5_000),
        };

        let response = proxy
            .publish_event(&principal, request)
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliveryUnsatisfiedTerminal
        );
        assert!(!response.job.delivery_satisfied);
        assert_eq!(response.job.acknowledged_count, 1);
        assert_eq!(adapter.captured_raw_events().len(), 1);
        response
            .job
            .validate()
            .expect("valid required target terminal job");
    }

    #[tokio::test]
    async fn publish_event_required_targets_ignore_optional_retryable_failures() {
        let identity = RadrootsIdentity::generate();
        let adapter = RadrootsMockRelayPublishAdapter::new()
            .with_outcome(RELAY_PRIMARY, RadrootsRelayOutcome::accepted())
            .with_outcome(
                RELAY_SECONDARY,
                RadrootsRelayOutcome::timeout("optional timeout"),
            );
        let (proxy, _) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let proxy = proxy.with_publisher(Arc::new(adapter.clone()));
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let required_target =
            RadrootsTransportTarget::nostr_relay(RELAY_PRIMARY).expect("required target");
        let request = TransportPublishEventRequest {
            raw_event_json: signed_event(&identity, "{}"),
            target_policy: TransportPublishTargetPolicy::explicit_targets(vec![
                TransportPublishTarget::nostr(RELAY_PRIMARY),
                TransportPublishTarget::nostr(RELAY_SECONDARY),
            ]),
            delivery_policy: TransportPublishDeliveryPolicy::required_targets(vec![
                required_target.fingerprint,
            ])
            .expect("required targets"),
            idempotency_key: None,
            timeout_ms: Some(5_000),
        };

        let response = proxy
            .publish_event(&principal, request)
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliverySatisfied
        );
        assert!(response.job.delivery_satisfied);
        assert_eq!(response.job.acknowledged_count, 1);
        assert_eq!(response.job.retryable_count, 1);
        assert_eq!(adapter.captured_raw_events().len(), 1);
        response
            .job
            .validate()
            .expect("valid required target satisfied job");
    }

    #[tokio::test]
    async fn publish_event_rejects_required_target_not_in_resolved_set() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let stale_target =
            RadrootsTransportTarget::nostr_relay(RELAY_SECONDARY).expect("stale target");
        let request = TransportPublishEventRequest {
            raw_event_json: signed_event(&identity, "{}"),
            target_policy: TransportPublishTargetPolicy::explicit_targets(vec![
                TransportPublishTarget::nostr(RELAY_PRIMARY),
            ]),
            delivery_policy: TransportPublishDeliveryPolicy::required_targets(vec![
                stale_target.fingerprint,
            ])
            .expect("required targets"),
            idempotency_key: None,
            timeout_ms: Some(5_000),
        };

        let err = proxy
            .publish_event(&principal, request)
            .await
            .expect_err("stale required target");

        assert!(matches!(
            err,
            TransportPublishError::InvalidSignedEvent(ref message)
                if message.contains("required target")
        ));
        assert!(adapter.captured_raw_events().is_empty());
        assert!(
            proxy
                .store
                .list_jobs_for_principal(&principal, 10)
                .expect("jobs")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn publish_event_rejects_duplicate_explicit_targets_before_recording_job() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let request = TransportPublishEventRequest {
            raw_event_json: signed_event(&identity, "{}"),
            target_policy: TransportPublishTargetPolicy::explicit_targets(vec![
                TransportPublishTarget::nostr(RELAY_PRIMARY),
                TransportPublishTarget::nostr(RELAY_PRIMARY),
            ]),
            delivery_policy: TransportPublishDeliveryPolicy::Any,
            idempotency_key: None,
            timeout_ms: Some(5_000),
        };

        let err = proxy
            .publish_event(&principal, request)
            .await
            .expect_err("duplicate explicit targets");

        assert!(matches!(
            err,
            TransportPublishError::InvalidSignedEvent(ref message)
                if message.contains("duplicates an earlier target")
        ));
        assert!(adapter.captured_raw_events().is_empty());
        assert!(
            proxy
                .store
                .list_jobs_for_principal(&principal, 10)
                .expect("jobs")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn publish_event_rejects_explicit_target_kind_not_allowed_before_recording_job() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(TransportPublishConfig::default());
        let principal = explicit_target_principal_with_kinds(
            &proxy,
            identity.public_key_hex(),
            vec![TRANSPORT_KIND_NOSTR.to_owned()],
            PublishJobVisibility::Own,
        );

        let err = proxy
            .publish_event(
                &principal,
                reticulum_publish_request(
                    signed_event(&identity, "{}"),
                    TransportPublishReticulumBehavior::RejectDeliveryAttempts,
                ),
            )
            .await
            .expect_err("disallowed explicit target kind");

        assert!(matches!(err, TransportPublishError::InvalidScope(_)));
        assert!(adapter.captured_raw_events().is_empty());
        assert!(
            proxy
                .store
                .list_jobs_for_principal(&principal, 10)
                .expect("jobs")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn publish_event_records_reticulum_deferred_as_terminal_nonfailure() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(TransportPublishConfig::default());
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let response = proxy
            .publish_event(
                &principal,
                reticulum_publish_request(
                    signed_event(&identity, "{}"),
                    TransportPublishReticulumBehavior::DeferDeliveryPlans,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliveryDeferredUntilImplemented
        );
        assert!(response.job.terminal);
        assert!(!response.job.delivery_satisfied);
        assert_eq!(response.job.terminal_count, 0);
        assert!(response.job.completed_at_ms.is_some());
        assert_eq!(
            response.job.last_error.as_deref(),
            Some("delivery_deferred_until_implemented")
        );
        assert_eq!(response.job.targets.len(), 1);
        assert_eq!(
            response.job.targets[0].outcome_kind,
            TransportPublishOutcomeKind::DeferredUntilImplemented
        );
        assert!(!response.job.targets[0].attempted);
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_rejects_noncanonical_reticulum_endpoint_before_recording_job() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(TransportPublishConfig::default());
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let mut request = reticulum_publish_request(
            signed_event(&identity, "{}"),
            TransportPublishReticulumBehavior::RejectDeliveryAttempts,
        );
        request.target_policy =
            TransportPublishTargetPolicy::explicit_targets(vec![TransportPublishTarget {
                transport_kind: "reticulum".to_owned(),
                endpoint_uri: "reticulum:unavailable-alt".to_owned(),
                target_scope: None,
                target_label: None,
                reticulum_behavior: Some(TransportPublishReticulumBehavior::RejectDeliveryAttempts),
            }]);

        let err = proxy
            .publish_event(&principal, request)
            .await
            .expect_err("noncanonical Reticulum endpoint");

        assert!(matches!(err, TransportPublishError::InvalidSignedEvent(_)));
        assert!(adapter.captured_raw_events().is_empty());
        assert!(
            proxy
                .store
                .list_jobs_for_principal(&principal, 10)
                .expect("jobs")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn publish_event_rejects_reticulum_behavior_on_non_reticulum_before_recording_job() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(TransportPublishConfig::default());
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let mut request = publish_request(
            signed_event(&identity, "{}"),
            vec![RELAY_PRIMARY.to_owned()],
            NostrPublishTargetSourcePolicy::ExplicitOnly,
            TransportPublishDeliveryPolicy::Any,
            None,
        );
        request.target_policy =
            TransportPublishTargetPolicy::explicit_targets(vec![TransportPublishTarget {
                transport_kind: "nostr".to_owned(),
                endpoint_uri: RELAY_PRIMARY.to_owned(),
                target_scope: None,
                target_label: None,
                reticulum_behavior: Some(TransportPublishReticulumBehavior::RejectDeliveryAttempts),
            }]);

        let err = proxy
            .publish_event(&principal, request)
            .await
            .expect_err("non-Reticulum reticulum behavior");

        assert!(matches!(err, TransportPublishError::InvalidSignedEvent(_)));
        assert!(adapter.captured_raw_events().is_empty());
        assert!(
            proxy
                .store
                .list_jobs_for_principal(&principal, 10)
                .expect("jobs")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn publish_event_rejects_noncanonical_reticulum_kind_before_recording_job() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(TransportPublishConfig::default());
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let mut request = reticulum_publish_request(
            signed_event(&identity, "{}"),
            TransportPublishReticulumBehavior::RejectDeliveryAttempts,
        );
        request.target_policy =
            TransportPublishTargetPolicy::explicit_targets(vec![TransportPublishTarget {
                transport_kind: "Reticulum".to_owned(),
                endpoint_uri: RADROOTS_RETICULUM_ENDPOINT_URI.to_owned(),
                target_scope: None,
                target_label: None,
                reticulum_behavior: Some(TransportPublishReticulumBehavior::RejectDeliveryAttempts),
            }]);

        let err = proxy
            .publish_event(&principal, request)
            .await
            .expect_err("noncanonical Reticulum kind");

        assert!(matches!(err, TransportPublishError::InvalidSignedEvent(_)));
        assert!(adapter.captured_raw_events().is_empty());
        assert!(
            proxy
                .store
                .list_jobs_for_principal(&principal, 10)
                .expect("jobs")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn publish_event_rejects_removed_execution_kind_before_recording_job() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(TransportPublishConfig::default());
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let mut request = reticulum_publish_request(
            signed_event(&identity, "{}"),
            TransportPublishReticulumBehavior::RejectDeliveryAttempts,
        );
        request.target_policy =
            TransportPublishTargetPolicy::explicit_targets(vec![TransportPublishTarget {
                transport_kind: removed_execution_kind_string(),
                endpoint_uri: removed_execution_endpoint_uri(),
                target_scope: None,
                target_label: None,
                reticulum_behavior: None,
            }]);

        let err = proxy
            .publish_event(&principal, request)
            .await
            .expect_err("removed execution kind");

        assert!(matches!(err, TransportPublishError::InvalidSignedEvent(_)));
        assert!(adapter.captured_raw_events().is_empty());
        assert!(
            proxy
                .store
                .list_jobs_for_principal(&principal, 10)
                .expect("jobs")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn publish_event_rejects_removed_execution_target_before_recording_job() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(TransportPublishConfig::default());
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let mut request = reticulum_publish_request(
            signed_event(&identity, "{}"),
            TransportPublishReticulumBehavior::RejectDeliveryAttempts,
        );
        request.target_policy =
            TransportPublishTargetPolicy::explicit_targets(vec![TransportPublishTarget {
                transport_kind: removed_proxy_transport_kind_string(),
                endpoint_uri: removed_execution_endpoint_uri(),
                target_scope: None,
                target_label: None,
                reticulum_behavior: None,
            }]);

        let err = proxy
            .publish_event(&principal, request)
            .await
            .expect_err("removed execution target");

        assert!(matches!(err, TransportPublishError::InvalidSignedEvent(_)));
        assert!(adapter.captured_raw_events().is_empty());
        assert!(
            proxy
                .store
                .list_jobs_for_principal(&principal, 10)
                .expect("jobs")
                .is_empty()
        );
    }

    fn removed_execution_kind_string() -> String {
        ["radrootsd", "_proxy"].concat()
    }

    fn removed_proxy_transport_kind_string() -> String {
        ["pro", "xy"].concat()
    }

    fn removed_execution_endpoint_uri() -> String {
        ["radrootsd-", "pro", "xy:publish"].concat()
    }

    #[tokio::test]
    async fn publish_event_records_unsafe_request_relay_rejection() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(TransportPublishConfig::default());
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::ExplicitOnly],
            true,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    vec!["wss://127.0.0.1:7777".to_owned()],
                    NostrPublishTargetSourcePolicy::ExplicitOnly,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliveryUnsatisfiedTerminal
        );
        assert_eq!(response.job.targets.len(), 1);
        assert_eq!(
            response.job.targets[0].outcome_kind,
            TransportPublishOutcomeKind::TargetRejected
        );
        assert!(!response.job.targets[0].attempted);
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_rejects_forbidden_public_dns_destination_before_publish() {
        let identity = RadrootsIdentity::generate();
        let resolver = StaticPublishRelayResolver::new()
            .with_addresses(RELAY_PRIMARY, vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))]);
        let (proxy, adapter) = transport_publish_with_resolver(
            config_with_defaults(vec![RELAY_PRIMARY]),
            Arc::new(resolver),
        );
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliveryUnsatisfiedTerminal
        );
        assert_eq!(response.job.targets.len(), 1);
        assert_eq!(
            response.job.targets[0].outcome_kind,
            TransportPublishOutcomeKind::TargetRejected
        );
        assert!(!response.job.targets[0].attempted);
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_records_dns_failure_as_unattempted_retryable_outcome() {
        let identity = RadrootsIdentity::generate();
        let resolver = StaticPublishRelayResolver::new().with_failure(RELAY_PRIMARY, "no records");
        let (proxy, adapter) = transport_publish_with_resolver(
            config_with_defaults(vec![RELAY_PRIMARY]),
            Arc::new(resolver),
        );
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliveryUnsatisfiedRetryable
        );
        assert_eq!(
            response.job.last_error.as_deref(),
            Some("delivery_unsatisfied")
        );
        assert_eq!(response.job.targets.len(), 1);
        assert_eq!(
            response.job.targets[0].outcome_kind,
            TransportPublishOutcomeKind::ConnectionFailed
        );
        assert!(!response.job.targets[0].attempted);
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_localhost_policy_skips_public_dns_guard() {
        let identity = RadrootsIdentity::generate();
        let mut config = config_with_defaults(vec!["ws://localhost:7777"]);
        config.nostr.relay_url_policy = NostrRelayUrlPolicy::Localhost;
        let resolver = StaticPublishRelayResolver::new()
            .with_failure("ws://localhost:7777", "localhost resolution should not run");
        let (proxy, adapter) = transport_publish_with_resolver(config, Arc::new(resolver));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliverySatisfied
        );
        assert_eq!(response.job.targets[0].endpoint_uri, "ws://localhost:7777");
        assert!(!adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_deduplicates_same_intent_and_conflicts_different_intent() {
        let identity = RadrootsIdentity::generate();
        let (proxy, _adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let request = publish_request(
            signed_event(&identity, "{}"),
            Vec::new(),
            NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
            TransportPublishDeliveryPolicy::Any,
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
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    TransportPublishDeliveryPolicy::Any,
                    Some("idem-conflict"),
                ),
            )
            .await
            .expect_err("conflict");
        assert!(matches!(
            conflict,
            TransportPublishError::IdempotencyConflict(_)
        ));
    }

    #[tokio::test]
    async fn publish_event_rejects_zero_and_excessive_timeout_before_job_creation() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let mut zero = publish_request(
            signed_event(&identity, "{}"),
            Vec::new(),
            NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
            TransportPublishDeliveryPolicy::Any,
            Some("idem-zero-timeout"),
        );
        zero.timeout_ms = Some(0);
        let zero_error = proxy
            .publish_event(&principal, zero)
            .await
            .expect_err("zero timeout should fail");
        assert!(matches!(
            zero_error,
            TransportPublishError::InvalidSignedEvent(_)
        ));

        let mut excessive = publish_request(
            signed_event(&identity, "changed"),
            Vec::new(),
            NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
            TransportPublishDeliveryPolicy::Any,
            Some("idem-excessive-timeout"),
        );
        excessive.timeout_ms = Some(10_001);
        let excessive_error = proxy
            .publish_event(&principal, excessive)
            .await
            .expect_err("excessive timeout should fail");
        assert!(matches!(
            excessive_error,
            TransportPublishError::InvalidSignedEvent(_)
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
        let (proxy, _adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let event = signed_event(&identity, "{}");
        let mut default_timeout = publish_request(
            event.clone(),
            Vec::new(),
            NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
            TransportPublishDeliveryPolicy::Any,
            Some("idem-default-timeout"),
        );
        default_timeout.timeout_ms = None;
        let mut explicit_default = publish_request(
            event,
            Vec::new(),
            NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
            TransportPublishDeliveryPolicy::Any,
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
        let (proxy, _adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let event = signed_event(&identity, "{}");
        let first = publish_request(
            event.clone(),
            Vec::new(),
            NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
            TransportPublishDeliveryPolicy::Any,
            Some("idem-timeout-conflict"),
        );
        let mut conflict = publish_request(
            event,
            Vec::new(),
            NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
            TransportPublishDeliveryPolicy::Any,
            Some("idem-timeout-conflict"),
        );
        conflict.timeout_ms = Some(6_000);

        proxy.publish_event(&principal, first).await.expect("first");
        let error = proxy
            .publish_event(&principal, conflict)
            .await
            .expect_err("timeout conflict");
        assert!(matches!(
            error,
            TransportPublishError::IdempotencyConflict(_)
        ));
    }

    #[tokio::test]
    async fn publish_event_concurrency_limit_rejects_without_job_creation() {
        let identity = RadrootsIdentity::generate();
        let mut config = config_with_defaults(vec![RELAY_PRIMARY]);
        config.max_concurrent_publish_jobs = 1;
        let (proxy, adapter) = transport_publish(config);
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
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
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    TransportPublishDeliveryPolicy::Any,
                    Some("idem-concurrency"),
                ),
            )
            .await
            .expect_err("concurrency limit");
        assert!(matches!(error, TransportPublishError::ConcurrencyLimit));
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
        let (proxy, _adapter) = transport_publish(config_with_defaults(vec![RELAY_PRIMARY]));
        let owner = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let other = principal(
            &proxy,
            other_identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let admin = principal(
            &proxy,
            other_identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Admin,
        );
        let response = proxy
            .publish_event(
                &owner,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    TransportPublishDeliveryPolicy::Any,
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
        let proxy = TransportPublish::memory(config_with_defaults(vec![RELAY_PRIMARY]))
            .expect("proxy")
            .with_publisher(Arc::new(adapter));
        let principal = principal(
            &proxy,
            identity.public_key_hex(),
            vec![NostrPublishTargetSourcePolicy::DaemonDefaultOnly],
            false,
            PublishJobVisibility::Own,
        );
        let response = proxy
            .publish_event(
                &principal,
                publish_request(
                    signed_event(&identity, "{}"),
                    Vec::new(),
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                    TransportPublishDeliveryPolicy::Any,
                    None,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliveryUnsatisfiedRetryable
        );
        assert_eq!(response.job.retryable_count, 1);
    }
}
