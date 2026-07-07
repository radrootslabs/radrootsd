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
use radroots_relay_transport::{
    RadrootsNostrClientPublishAdapter, RadrootsRelayOutcome, RadrootsRelayOutcomeKind,
    RadrootsRelayPublishAdapter, RadrootsRelayPublishRelayReceipt, RadrootsRelayPublishRequest,
    RadrootsRelayTargetSet, RadrootsRelayTransportError, RadrootsRelayUrl, RadrootsRelayUrlPolicy,
};
use radroots_transport::RadrootsTransportSatisfactionPolicy;
use radroots_transport_publish_protocol::{
    NostrPublishTargetSourcePolicy, SignedNostrEventWire, TransportPublishDeliveryPolicy,
    TransportPublishEventRequest, TransportPublishEventResponse, TransportPublishJobStatus,
    TransportPublishJobView, TransportPublishOutcomeKind, TransportPublishPreviewBehavior,
    TransportPublishTarget, TransportPublishTargetOutcome, TransportPublishTargetPolicy,
    TransportPublishTargetPolicyName, TransportPublishTargetSource,
};
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::app::config::TransportPublishConfig;

const TOKEN_PREFIX: &str = "rrd_tp_";
const TOKEN_HASH_PREFIX: &str = "sha256:";
const SCHEMA_VERSION: i64 = 1;
const TRANSPORT_KIND_NOSTR: &str = "nostr";
const TRANSPORT_KIND_RETICULUM: &str = "reticulum";

#[derive(Debug, Error)]
pub enum TransportPublishError {
    #[error("transport publish storage error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("transport publish json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("transport publish io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid transport publish scope: {0}")]
    InvalidScope(String),
    #[error("invalid signed Nostr event: {0}")]
    InvalidSignedEvent(String),
    #[error("signed Nostr event verification failed: {0:?}")]
    SignedEventVerification(RadrootsNostrEventVerification),
    #[error("signed Nostr event conversion error: {0}")]
    Draft(#[from] RadrootsDraftError),
    #[error("transport publish relay error: {0}")]
    Relay(#[from] RadrootsRelayTransportError),
    #[error("transport publish transport error: {0}")]
    Transport(String),
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
        principal.allows_event(&request)?;
        let signed_event = signed_event_from_wire(&request.event)?;
        if signed_event.raw_json.len() > self.config.max_event_bytes {
            return Err(TransportPublishError::InvalidSignedEvent(
                "signed event exceeds transport_publish max_event_bytes".to_owned(),
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
            .resolve_targets_for_request(signed_event.pubkey.as_str(), &request)
            .await?;
        let response = self.store.record_publish_job(PublishJobInsert {
            principal_id: principal.principal_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            request: request.clone(),
            request_fingerprint,
            effective_target_count: resolution.target_count(),
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
        for target in targets {
            match target.transport_kind.as_str() {
                TRANSPORT_KIND_NOSTR => {
                    self.resolve_request_target(&mut resolved, &mut outcomes, target)
                        .await;
                }
                TRANSPORT_KIND_RETICULUM => {
                    outcomes.push(reticulum_preview_outcome(target));
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
                )
                .await;
            }
            Err(error) => outcomes.push(TransportPublishTargetOutcome {
                transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
                endpoint_uri: target.endpoint_uri.trim().to_owned(),
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
                    )
                    .await;
                }
                Err(error) => outcomes.push(TransportPublishTargetOutcome {
                    transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
                    endpoint_uri: relay.trim().to_owned(),
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
                    )
                    .await;
                }
                Err(error) => outcomes.push(TransportPublishTargetOutcome {
                    transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
                    endpoint_uri: relay.trim().to_owned(),
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
                    self.push_checked_relay_target(&mut targets, &mut outcomes, url, source)
                        .await;
                }
                Err(error) => outcomes.push(TransportPublishTargetOutcome {
                    transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
                    endpoint_uri: relay.trim().to_owned(),
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
                Err(error) => outcomes.push(TransportPublishTargetOutcome {
                    transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
                    endpoint_uri: url.as_str().to_owned(),
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
                format!("dns lookup failed: {error}"),
            )),
        }
    }

    async fn complete_job_execution(
        &self,
        job_id: &str,
        signed_event: RadrootsSignedNostrEvent,
        delivery_policy: TransportPublishDeliveryPolicy,
        timeout_ms: u64,
        resolution: PublishRelayResolution,
    ) -> Result<TransportPublishJobView, TransportPublishError> {
        let target_count = resolution.target_count();
        if resolution.targets.is_empty() {
            let status = no_publishable_target_status(&resolution.outcomes);
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
        let source_by_relay = resolution.source_by_relay();
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
            resolution.targets.len(),
        );
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
                Ok(Err(error)) => transport_error_receipts(&resolution.targets, error),
                Err(_) => timeout_receipts(&resolution.targets),
            };
        let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let mut outcomes = resolution.outcomes;
        outcomes.extend(receipts.into_iter().map(|receipt| {
            publish_outcome_from_receipt(receipt, &source_by_relay, Some(latency_ms))
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
    inner: Arc<Mutex<Connection>>,
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
    pub allowed_nostr_source_policies: Vec<NostrPublishTargetSourcePolicy>,
    pub allow_request_targets: bool,
    pub job_visibility: PublishJobVisibility,
    pub expires_at_unix: Option<i64>,
}

impl PublishPrincipal {
    pub fn allows_event(
        &self,
        request: &TransportPublishEventRequest,
    ) -> Result<(), TransportPublishError> {
        ensure_lower_hex("pubkey", request.event.pubkey.as_str(), 64)?;
        if !self
            .allowed_pubkeys
            .iter()
            .any(|pubkey| pubkey == &request.event.pubkey)
        {
            return Err(TransportPublishError::InvalidScope(
                "principal is not allowed to publish for event pubkey".to_owned(),
            ));
        }
        if !self.allowed_kinds.contains(&request.event.kind) {
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
    pub request: TransportPublishEventRequest,
    pub request_fingerprint: String,
    pub effective_target_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPublishRelay {
    pub url: RadrootsRelayUrl,
    pub source: TransportPublishTargetSource,
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

    fn source_by_relay(&self) -> BTreeMap<String, TransportPublishTargetSource> {
        self.targets
            .iter()
            .map(|target| (target.url.as_str().to_owned(), target.source))
            .collect()
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
        let connection = Connection::open(path)?;
        Self::from_connection(connection)
    }

    pub fn memory() -> Result<Self, TransportPublishError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, TransportPublishError> {
        connection.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS transport_publish_principals (
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
                source TEXT NOT NULL,
                attempted INTEGER NOT NULL,
                outcome_kind TEXT NOT NULL,
                message TEXT,
                latency_ms INTEGER,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(job_id, transport_kind, endpoint_uri),
                FOREIGN KEY(job_id) REFERENCES transport_publish_jobs(job_id)
            );
            CREATE TABLE IF NOT EXISTS transport_publish_nostr_author_cache (
                pubkey TEXT PRIMARY KEY NOT NULL,
                relays_json TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            "#,
        )?;
        recover_interrupted_publish_jobs(&connection)?;
        connection.pragma_update(None, "user_version", SCHEMA_VERSION)?;
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
        connection.execute(
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
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, ?11)
            "#,
            params![
                principal_id,
                input.label.trim(),
                input.token_hash,
                serde_json::to_string(&input.allowed_pubkeys)?,
                serde_json::to_string(&input.allowed_kinds)?,
                serde_json::to_string(&input.allowed_target_policies)?,
                serde_json::to_string(&input.allowed_nostr_source_policies)?,
                input.allow_request_targets,
                input.job_visibility.to_string(),
                input.expires_at_unix,
                now,
            ],
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
                    allowed_target_policies_json,
                    allowed_nostr_source_policies_json,
                    allow_request_targets,
                    job_visibility,
                    expires_at_unix
                FROM transport_publish_principals
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
    ) -> Result<Option<PublishPrincipal>, TransportPublishError> {
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
                    allowed_target_policies_json,
                    allowed_nostr_source_policies_json,
                    allow_request_targets,
                    job_visibility,
                    expires_at_unix
                FROM transport_publish_principals
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
    ) -> Result<TransportPublishEventResponse, TransportPublishError> {
        if let Some(idempotency_key) = insert.idempotency_key.as_deref() {
            if let Some(existing) =
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
            params![
                job_id,
                insert.principal_id,
                insert.idempotency_key,
                insert.request_fingerprint,
                serde_json::to_string(&TransportPublishJobStatus::Publishing)?,
                insert.request.event.id,
                insert.request.event.pubkey,
                insert.request.event.kind,
                serde_json::to_string(&insert.request.target_policy)?,
                serde_json::to_string(&insert.request.delivery_policy)?,
                insert.request.target_policy.request_target_count(),
                insert.effective_target_count,
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
                return Err(TransportPublishError::IdempotencyConflict(
                    "idempotency key conflicts with an existing publish job".to_owned(),
                ));
            }
            Err(error) => return Err(error.into()),
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
        job.view.targets = self.target_outcomes(job.view.job_id.as_str())?;
        finalize_job_view(&mut job.view);
        Ok(Some(job.view))
    }

    pub fn list_jobs_for_principal(
        &self,
        principal: &PublishPrincipal,
        limit: usize,
    ) -> Result<Vec<TransportPublishJobView>, TransportPublishError> {
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
                row.view.targets = self.target_outcomes(row.view.job_id.as_str())?;
                finalize_job_view(&mut row.view);
                Ok(row.view)
            })
            .collect()
    }

    fn job_for_principal_id_and_key(
        &self,
        principal_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<PublishJobRow>, TransportPublishError> {
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
        job.view.targets = self.target_outcomes(job.view.job_id.as_str())?;
        finalize_job_view(&mut job.view);
        Ok(Some(job))
    }

    pub fn job_by_id(
        &self,
        job_id: &str,
    ) -> Result<TransportPublishJobView, TransportPublishError> {
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
            return Err(TransportPublishError::InvalidScope(
                "unknown publish job".to_owned(),
            ));
        };
        job.view.targets = self.target_outcomes(job.view.job_id.as_str())?;
        finalize_job_view(&mut job.view);
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
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        connection.execute(
            r#"
            UPDATE transport_publish_jobs
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
            "DELETE FROM transport_publish_target_results WHERE job_id = ?1",
            params![job_id],
        )?;
        for outcome in outcomes {
            connection.execute(
                r#"
                INSERT OR REPLACE INTO transport_publish_target_results (
                    job_id,
                    transport_kind,
                    endpoint_uri,
                    source,
                    attempted,
                    outcome_kind,
                    message,
                    latency_ms,
                    updated_at_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                "#,
                params![
                    job_id,
                    outcome.transport_kind,
                    outcome.endpoint_uri,
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
    ) -> Result<Vec<String>, TransportPublishError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let relays_json = connection
            .query_row(
                "SELECT relays_json FROM transport_publish_nostr_author_cache WHERE pubkey = ?1",
                params![pubkey],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
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
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        connection.execute(
            r#"
            INSERT INTO transport_publish_nostr_author_cache (pubkey, relays_json, updated_at_ms)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(pubkey) DO UPDATE SET
                relays_json = excluded.relays_json,
                updated_at_ms = excluded.updated_at_ms
            "#,
            params![pubkey, serde_json::to_string(relays)?, now],
        )?;
        Ok(())
    }

    fn target_outcomes(
        &self,
        job_id: &str,
    ) -> Result<Vec<TransportPublishTargetOutcome>, TransportPublishError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut stmt = connection.prepare(
            r#"
            SELECT transport_kind, endpoint_uri, source, attempted, outcome_kind, message, latency_ms
            FROM transport_publish_target_results
            WHERE job_id = ?1
            ORDER BY transport_kind, endpoint_uri
            "#,
        )?;
        let outcomes = stmt
            .query_map(params![job_id], target_outcome_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(outcomes)
    }
}

struct PublishJobRow {
    principal_id: String,
    request_fingerprint: String,
    view: TransportPublishJobView,
}

fn recover_interrupted_publish_jobs(connection: &Connection) -> Result<(), TransportPublishError> {
    let now = current_unix_millis();
    connection.execute(
        r#"
        UPDATE transport_publish_jobs
        SET status = ?1,
            updated_at_ms = ?2,
            completed_at_ms = ?3,
            last_error = ?4
        WHERE status = ?5
        "#,
        params![
            serde_json::to_string(&TransportPublishJobStatus::DeliveryUnsatisfiedRetryable)?,
            now,
            now,
            "publish_attempt_interrupted",
            serde_json::to_string(&TransportPublishJobStatus::Publishing)?,
        ],
    )?;
    Ok(())
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

fn principal_from_row(row: &Row<'_>) -> Result<PublishPrincipal, rusqlite::Error> {
    let visibility: String = row.get(7)?;
    Ok(PublishPrincipal {
        principal_id: row.get(0)?,
        label: row.get(1)?,
        allowed_pubkeys: json_column(row, 2)?,
        allowed_kinds: json_column(row, 3)?,
        allowed_target_policies: json_column(row, 4)?,
        allowed_nostr_source_policies: json_column(row, 5)?,
        allow_request_targets: row.get(6)?,
        job_visibility: PublishJobVisibility::from_str(visibility.as_str())
            .map_err(|error| conversion_error(7, error))?,
        expires_at_unix: row.get(8)?,
    })
}

fn job_from_row(row: &Row<'_>) -> Result<PublishJobRow, rusqlite::Error> {
    let status: TransportPublishJobStatus = json_text(row, 3)?;
    let target_policy: TransportPublishTargetPolicy = json_text(row, 7)?;
    let delivery_policy: TransportPublishDeliveryPolicy = json_text(row, 8)?;
    let target_count: i64 = row.get(9)?;
    Ok(PublishJobRow {
        principal_id: row.get(1)?,
        request_fingerprint: row.get(2)?,
        view: TransportPublishJobView {
            job_id: row.get(0)?,
            status,
            terminal: false,
            delivery_satisfied: false,
            event_id: row.get(4)?,
            pubkey: row.get(5)?,
            event_kind: row.get::<_, i64>(6)? as u32,
            target_policy,
            delivery_policy,
            target_count: usize::try_from(target_count).unwrap_or(0),
            acknowledged_count: 0,
            retryable_count: 0,
            terminal_count: 0,
            requested_at_ms: row.get(10)?,
            completed_at_ms: row.get(11)?,
            last_error: row.get(12)?,
            targets: Vec::new(),
        },
    })
}

fn target_outcome_from_row(
    row: &Row<'_>,
) -> Result<TransportPublishTargetOutcome, rusqlite::Error> {
    let source: TransportPublishTargetSource = json_text(row, 2)?;
    let outcome_kind: TransportPublishOutcomeKind = json_text(row, 4)?;
    Ok(TransportPublishTargetOutcome {
        transport_kind: row.get(0)?,
        endpoint_uri: row.get(1)?,
        source,
        attempted: row.get(3)?,
        outcome_kind,
        message: row.get(5)?,
        latency_ms: row
            .get::<_, Option<i64>>(6)?
            .map(|latency| u64::try_from(latency).unwrap_or(0)),
    })
}

fn finalize_job_view(view: &mut TransportPublishJobView) {
    view.acknowledged_count = view
        .targets
        .iter()
        .filter(|relay| relay.outcome_kind.counts_toward_satisfaction())
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
    if input
        .allowed_kinds
        .iter()
        .any(|kind| *kind > u16::MAX as u32)
    {
        return Err(TransportPublishError::InvalidScope(
            "allowed kind exceeds transport publish range".to_owned(),
        ));
    }
    if input.allowed_target_policies.is_empty() {
        return Err(TransportPublishError::InvalidScope(
            "principal must include at least one allowed target policy".to_owned(),
        ));
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

fn signed_event_from_wire(
    event: &SignedNostrEventWire,
) -> Result<RadrootsSignedNostrEvent, TransportPublishError> {
    event
        .validate()
        .map_err(|error| TransportPublishError::InvalidSignedEvent(error.to_string()))?;
    let created_at = u32::try_from(event.created_at).map_err(|_| {
        TransportPublishError::InvalidSignedEvent(
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
        verification => return Err(TransportPublishError::SignedEventVerification(verification)),
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
    .map_err(TransportPublishError::from)
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
) {
    if !targets.iter().any(|target| target.url == url) {
        targets.push(ResolvedPublishRelay { url, source });
    }
}

fn reticulum_preview_outcome(target: &TransportPublishTarget) -> TransportPublishTargetOutcome {
    let outcome_kind = match target.preview_behavior.unwrap_or_default() {
        TransportPublishPreviewBehavior::RejectDeliveryAttempts => {
            TransportPublishOutcomeKind::PreviewUnavailable
        }
        TransportPublishPreviewBehavior::DeferDeliveryPlans => {
            TransportPublishOutcomeKind::DeferredUntilImplemented
        }
    };
    TransportPublishTargetOutcome {
        transport_kind: TRANSPORT_KIND_RETICULUM.to_owned(),
        endpoint_uri: target.endpoint_uri.trim().to_owned(),
        source: TransportPublishTargetSource::ReticulumPreview,
        attempted: false,
        outcome_kind,
        message: Some(
            "reticulum transport is registered for preview but not routable by radrootsd"
                .to_owned(),
        ),
        latency_ms: None,
    }
}

fn unsupported_transport_outcome(target: &TransportPublishTarget) -> TransportPublishTargetOutcome {
    TransportPublishTargetOutcome {
        transport_kind: target.transport_kind.trim().to_owned(),
        endpoint_uri: target.endpoint_uri.trim().to_owned(),
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
    message: impl Into<String>,
) -> TransportPublishTargetOutcome {
    TransportPublishTargetOutcome {
        transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
        endpoint_uri: relay_url.into(),
        source,
        attempted: false,
        outcome_kind: TransportPublishOutcomeKind::ConnectionFailed,
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

fn publish_outcome_from_receipt(
    receipt: RadrootsRelayPublishRelayReceipt,
    source_by_relay: &BTreeMap<String, TransportPublishTargetSource>,
    latency_ms: Option<u64>,
) -> TransportPublishTargetOutcome {
    let source = source_by_relay
        .get(receipt.relay_url.as_str())
        .copied()
        .unwrap_or(TransportPublishTargetSource::DaemonDefault);
    TransportPublishTargetOutcome {
        transport_kind: TRANSPORT_KIND_NOSTR.to_owned(),
        endpoint_uri: receipt.relay_url,
        source,
        attempted: receipt.attempted,
        outcome_kind: publish_outcome_kind(receipt.outcome.kind),
        message: receipt.outcome.message,
        latency_ms,
    }
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

fn satisfaction_policy_from_delivery_policy(
    delivery_policy: &TransportPublishDeliveryPolicy,
    target_count: usize,
    nostr_target_count: usize,
) -> RadrootsTransportSatisfactionPolicy {
    match delivery_policy {
        TransportPublishDeliveryPolicy::Any => RadrootsTransportSatisfactionPolicy::any_accepted(),
        TransportPublishDeliveryPolicy::All => RadrootsTransportSatisfactionPolicy::all_accepted(),
        TransportPublishDeliveryPolicy::Quorum { quorum } => {
            let required = (*quorum).min(target_count).min(nostr_target_count).max(1);
            RadrootsTransportSatisfactionPolicy::quorum_accepted(
                u16::try_from(required).unwrap_or(u16::MAX),
            )
        }
    }
}

fn delivery_status(
    delivery_policy: &TransportPublishDeliveryPolicy,
    target_count: usize,
    outcomes: &[TransportPublishTargetOutcome],
) -> TransportPublishJobStatus {
    let required = delivery_policy.required_target_count(target_count);
    let acknowledged = outcomes
        .iter()
        .filter(|outcome| outcome.outcome_kind.counts_toward_satisfaction())
        .count();
    if acknowledged >= required {
        return TransportPublishJobStatus::DeliverySatisfied;
    }
    if outcomes
        .iter()
        .any(|outcome| outcome.outcome_kind.is_retryable())
    {
        TransportPublishJobStatus::DeliveryUnsatisfiedRetryable
    } else if outcomes.iter().any(|outcome| {
        outcome.outcome_kind == TransportPublishOutcomeKind::DeferredUntilImplemented
    }) && outcomes
        .iter()
        .all(|outcome| !outcome.outcome_kind.is_terminal_failure())
    {
        TransportPublishJobStatus::DeliveryDeferred
    } else if outcomes
        .iter()
        .any(|outcome| outcome.outcome_kind == TransportPublishOutcomeKind::PreviewUnavailable)
        && outcomes
            .iter()
            .all(|outcome| !outcome.outcome_kind.is_terminal_failure())
    {
        TransportPublishJobStatus::DeliveryPreviewUnavailable
    } else {
        TransportPublishJobStatus::DeliveryUnsatisfiedTerminal
    }
}

fn no_publishable_target_status(
    outcomes: &[TransportPublishTargetOutcome],
) -> TransportPublishJobStatus {
    if outcomes.is_empty() {
        return TransportPublishJobStatus::Rejected;
    }
    delivery_status(&TransportPublishDeliveryPolicy::Any, 1, outcomes)
}

fn last_error_for_status(status: TransportPublishJobStatus) -> Option<&'static str> {
    match status {
        TransportPublishJobStatus::DeliverySatisfied => None,
        TransportPublishJobStatus::Rejected => Some("no_transport_publish_targets"),
        TransportPublishJobStatus::DeliveryDeferred => Some("delivery_deferred_until_implemented"),
        TransportPublishJobStatus::DeliveryPreviewUnavailable => {
            Some("delivery_preview_unavailable")
        }
        TransportPublishJobStatus::Accepted
        | TransportPublishJobStatus::Publishing
        | TransportPublishJobStatus::DeliveryUnsatisfiedRetryable
        | TransportPublishJobStatus::DeliveryUnsatisfiedTerminal => Some("delivery_unsatisfied"),
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
    error: TransportPublishError,
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
        TransportPublish, TransportPublishError, TransportPublishStore, generate_bearer_token,
        hash_bearer_token, parse_nostr_source_policy,
    };
    use crate::app::config::{
        NostrRelayUrlPolicy, TransportPublishConfig, TransportPublishNostrConfig,
    };
    use nostr::JsonUtil;
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::{
        RadrootsNostrEventVerification, RadrootsNostrTimestamp, radroots_nostr_build_event,
    };
    use radroots_relay_transport::{RadrootsMockRelayPublishAdapter, RadrootsRelayOutcome};
    use radroots_transport_publish_protocol::{
        NostrPublishTargetSourcePolicy, SignedNostrEventWire, TransportPublishDeliveryPolicy,
        TransportPublishEventRequest, TransportPublishJobStatus, TransportPublishOutcomeKind,
        TransportPublishPreviewBehavior, TransportPublishTarget, TransportPublishTargetPolicy,
        TransportPublishTargetPolicyName, TransportPublishTargetSource,
    };
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

    fn request(pubkey: &str, kind: u32) -> TransportPublishEventRequest {
        TransportPublishEventRequest {
            event: event(pubkey, kind),
            target_policy: TransportPublishTargetPolicy::nostr(
                NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                Vec::new(),
            ),
            delivery_policy: TransportPublishDeliveryPolicy::Any,
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
        source_policy: NostrPublishTargetSourcePolicy,
        delivery_policy: TransportPublishDeliveryPolicy,
        idempotency_key: Option<&str>,
    ) -> TransportPublishEventRequest {
        TransportPublishEventRequest {
            event,
            target_policy: TransportPublishTargetPolicy::nostr(source_policy, relays),
            delivery_policy,
            idempotency_key: idempotency_key.map(str::to_owned),
            timeout_ms: Some(5_000),
        }
    }

    fn reticulum_publish_request(
        event: SignedNostrEventWire,
        behavior: TransportPublishPreviewBehavior,
    ) -> TransportPublishEventRequest {
        TransportPublishEventRequest {
            event,
            target_policy: TransportPublishTargetPolicy::explicit_targets(vec![
                TransportPublishTarget::reticulum_preview(
                    "reticulum:preview-unavailable",
                    behavior,
                ),
            ]),
            delivery_policy: TransportPublishDeliveryPolicy::Any,
            idempotency_key: None,
            timeout_ms: Some(5_000),
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
        proxy
            .store
            .create_principal(PublishPrincipalInit {
                label: "explicit-target-tester".to_owned(),
                token_hash: hash_bearer_token(generate_bearer_token().as_str()),
                allowed_pubkeys: vec![pubkey],
                allowed_kinds: vec![30_402],
                allowed_target_policies: vec![TransportPublishTargetPolicyName::ExplicitTargets],
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
        let principal = store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: token_hash.clone(),
                allowed_pubkeys: vec!["a".repeat(64)],
                allowed_kinds: vec![30_402],
                allowed_target_policies: vec![TransportPublishTargetPolicyName::Nostr],
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
                effective_target_count: 1,
            })
            .expect("record job");
        assert!(!response.deduplicated);
        let duplicate = store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id.clone(),
                idempotency_key: Some("idem-1".to_owned()),
                request: accepted,
                request_fingerprint: "fingerprint-1".to_owned(),
                effective_target_count: 1,
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
            let store = TransportPublishStore::open(database_path.clone()).expect("store");
            let principal = store
                .create_principal(PublishPrincipalInit {
                    label: "tester".to_owned(),
                    token_hash,
                    allowed_pubkeys: vec![pubkey],
                    allowed_kinds: vec![30_402],
                    allowed_target_policies: vec![TransportPublishTargetPolicyName::Nostr],
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
                    principal_id: principal.principal_id,
                    idempotency_key: Some("idem-interrupted".to_owned()),
                    request,
                    request_fingerprint: "fingerprint-interrupted".to_owned(),
                    effective_target_count: 1,
                })
                .expect("record job");
            assert_eq!(response.job.status, TransportPublishJobStatus::Publishing);
            response.job.job_id
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
        assert!(recovered.targets.is_empty());
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
        let raw_event = serde_json::to_string(&event).expect("raw event");
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
        let mut event = signed_event(&identity, "trusted");
        event.content = "tampered".to_owned();
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

        assert!(matches!(
            error,
            TransportPublishError::SignedEventVerification(
                RadrootsNostrEventVerification::IdMismatch
            )
        ));
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
        let mut event = signed_event(&identity, "{}");
        let replacement = if event.sig.starts_with('0') { "1" } else { "0" };
        event.sig.replace_range(0..1, replacement);
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
            TransportPublishError::SignedEventVerification(
                RadrootsNostrEventVerification::SignatureInvalid
            )
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
        let mut event = signed_event(&identity, "{}");
        event.id = event.id.to_uppercase();
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

        assert!(matches!(
            error,
            TransportPublishError::InvalidSignedEvent(_)
        ));
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
    async fn publish_event_records_reticulum_preview_unavailable_without_terminal_failure() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(TransportPublishConfig::default());
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let response = proxy
            .publish_event(
                &principal,
                reticulum_publish_request(
                    signed_event(&identity, "{}"),
                    TransportPublishPreviewBehavior::RejectDeliveryAttempts,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliveryPreviewUnavailable
        );
        assert!(!response.job.terminal);
        assert!(!response.job.delivery_satisfied);
        assert_eq!(response.job.terminal_count, 0);
        assert_eq!(
            response.job.last_error.as_deref(),
            Some("delivery_preview_unavailable")
        );
        assert_eq!(response.job.targets.len(), 1);
        assert_eq!(
            response.job.targets[0].outcome_kind,
            TransportPublishOutcomeKind::PreviewUnavailable
        );
        assert!(!response.job.targets[0].attempted);
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_event_records_reticulum_deferred_without_terminal_failure() {
        let identity = RadrootsIdentity::generate();
        let (proxy, adapter) = transport_publish(TransportPublishConfig::default());
        let principal =
            explicit_target_principal(&proxy, identity.public_key_hex(), PublishJobVisibility::Own);
        let response = proxy
            .publish_event(
                &principal,
                reticulum_publish_request(
                    signed_event(&identity, "{}"),
                    TransportPublishPreviewBehavior::DeferDeliveryPlans,
                ),
            )
            .await
            .expect("publish");

        assert_eq!(
            response.job.status,
            TransportPublishJobStatus::DeliveryDeferred
        );
        assert!(!response.job.terminal);
        assert!(!response.job.delivery_satisfied);
        assert_eq!(response.job.terminal_count, 0);
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
