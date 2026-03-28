use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::app::config::BridgeDeliveryPolicy;
use crate::core::bridge::publish::{BridgePublishExecution, BridgeRelayPublishResult};

const BRIDGE_JOB_STORE_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeJobStatus {
    Accepted,
    Published,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeJobRecord {
    pub job_id: String,
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub status: BridgeJobStatus,
    pub requested_at_unix: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_unix: Option<u64>,
    pub signer_mode: String,
    pub event_kind: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_addr: Option<String>,
    pub delivery_policy: BridgeDeliveryPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_quorum: Option<usize>,
    pub relay_count: usize,
    pub acknowledged_relay_count: usize,
    pub required_acknowledged_relay_count: usize,
    pub attempt_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempt_summaries: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relay_results: Vec<BridgeRelayPublishResult>,
    pub relay_outcome_summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct BridgeJobStoreSnapshot {
    pub retained_jobs: usize,
    pub retained_idempotency_keys: usize,
    pub capacity: usize,
}

#[derive(Clone)]
pub struct BridgeJobStore {
    inner: Arc<RwLock<BridgeJobStoreInner>>,
    persistence: Option<Arc<BridgeJobStorePersistence>>,
}

#[derive(Debug)]
struct BridgeJobStoreInner {
    jobs: HashMap<String, BridgeJobRecord>,
    idempotency: HashMap<String, BridgeIdempotencyRecord>,
    order: VecDeque<String>,
    capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BridgeIdempotencyRecord {
    job_id: String,
    request_fingerprint: String,
}

#[derive(Debug, Clone)]
struct BridgeJobStorePersistence {
    path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedBridgeJobStore {
    version: u32,
    jobs: HashMap<String, BridgeJobRecord>,
    idempotency: HashMap<String, BridgeIdempotencyRecord>,
    order: VecDeque<String>,
}

#[derive(Debug, Error)]
pub enum BridgeJobStoreError {
    #[error("invalid bridge job store path: {0}")]
    InvalidStatePath(PathBuf),
    #[error("unsupported bridge job store version: {0}")]
    UnsupportedStateVersion(u32),
    #[error("bridge job store io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("bridge job store json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("idempotency_key `{key}` conflicts with existing bridge job `{existing_job_id}`")]
    IdempotencyConflict {
        key: String,
        existing_job_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeJobReservation {
    Accepted(BridgeJobRecord),
    Duplicate(BridgeJobRecord),
}

impl BridgeJobStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(BridgeJobStoreInner {
                jobs: HashMap::new(),
                idempotency: HashMap::new(),
                order: VecDeque::new(),
                capacity,
            })),
            persistence: None,
        }
    }

    pub fn load(path: PathBuf, capacity: usize) -> Result<Self, BridgeJobStoreError> {
        let persistence = Arc::new(BridgeJobStorePersistence::new(path));
        let inner = persistence.load(capacity)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            persistence: Some(persistence),
        })
    }

    pub fn reserve(
        &self,
        mut record: BridgeJobRecord,
        request_fingerprint: String,
    ) -> Result<BridgeJobReservation, BridgeJobStoreError> {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if let Some(idempotency_key) = record.idempotency_key.as_ref() {
            if let Some(existing_idempotency) = inner.idempotency.get(idempotency_key) {
                if existing_idempotency.request_fingerprint != request_fingerprint {
                    return Err(BridgeJobStoreError::IdempotencyConflict {
                        key: idempotency_key.clone(),
                        existing_job_id: existing_idempotency.job_id.clone(),
                    });
                }
                if let Some(existing) = inner.jobs.get(&existing_idempotency.job_id) {
                    return Ok(BridgeJobReservation::Duplicate(existing.clone()));
                }
            }
        }

        record.status = BridgeJobStatus::Accepted;
        inner.order.push_back(record.job_id.clone());
        if let Some(idempotency_key) = record.idempotency_key.as_ref() {
            inner.idempotency.insert(
                idempotency_key.clone(),
                BridgeIdempotencyRecord {
                    job_id: record.job_id.clone(),
                    request_fingerprint,
                },
            );
        }
        inner.jobs.insert(record.job_id.clone(), record.clone());
        inner.prune();
        let persisted = persisted_store_from_inner(&inner);
        drop(inner);
        self.persist_snapshot(&persisted)?;
        Ok(BridgeJobReservation::Accepted(record))
    }

    pub fn complete(
        &self,
        job_id: &str,
        event_id: Option<String>,
        execution: BridgePublishExecution,
    ) -> Result<Option<BridgeJobRecord>, BridgeJobStoreError> {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let Some(record) = inner.jobs.get_mut(job_id) else {
            return Ok(None);
        };
        if let Some(event_id) = event_id {
            record.event_id = Some(event_id);
        }
        record.status = if execution.published {
            BridgeJobStatus::Published
        } else {
            BridgeJobStatus::Failed
        };
        record.completed_at_unix = Some(unix_timestamp_now());
        record.relay_count = execution.relay_count;
        record.acknowledged_relay_count = execution.acknowledged_relay_count;
        record.required_acknowledged_relay_count = execution.required_acknowledged_relay_count;
        record.attempt_count = execution.attempt_count;
        record.attempt_summaries = execution.attempt_summaries;
        record.relay_results = execution.relay_results;
        record.relay_outcome_summary = execution.relay_outcome_summary;
        let completed = record.clone();
        let persisted = persisted_store_from_inner(&inner);
        drop(inner);
        self.persist_snapshot(&persisted)?;
        Ok(Some(completed))
    }

    pub fn get(&self, job_id: &str) -> Option<BridgeJobRecord> {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .jobs
            .get(job_id)
            .cloned()
    }

    pub fn snapshot(&self) -> BridgeJobStoreSnapshot {
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        BridgeJobStoreSnapshot {
            retained_jobs: inner.jobs.len(),
            retained_idempotency_keys: inner.idempotency.len(),
            capacity: inner.capacity,
        }
    }

    fn persist_snapshot(
        &self,
        snapshot: &PersistedBridgeJobStore,
    ) -> Result<(), BridgeJobStoreError> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };
        persistence.persist(snapshot)
    }
}

impl BridgeJobStoreInner {
    fn prune(&mut self) {
        while self.jobs.len() > self.capacity {
            let Some(job_id) = self.order.pop_front() else {
                break;
            };
            let Some(removed) = self.jobs.remove(&job_id) else {
                continue;
            };
            if let Some(idempotency_key) = removed.idempotency_key {
                if self
                    .idempotency
                    .get(&idempotency_key)
                    .map(|record| record.job_id.as_str())
                    == Some(job_id.as_str())
                {
                    self.idempotency.remove(&idempotency_key);
                }
            }
        }
    }
}

impl BridgeJobStorePersistence {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn load(&self, capacity: usize) -> Result<BridgeJobStoreInner, BridgeJobStoreError> {
        if !self.path.exists() {
            return Ok(BridgeJobStoreInner {
                jobs: HashMap::new(),
                idempotency: HashMap::new(),
                order: VecDeque::new(),
                capacity,
            });
        }

        let payload = std::fs::read_to_string(&self.path)?;
        let snapshot: PersistedBridgeJobStore = serde_json::from_str(&payload)?;
        if snapshot.version != BRIDGE_JOB_STORE_VERSION {
            return Err(BridgeJobStoreError::UnsupportedStateVersion(
                snapshot.version,
            ));
        }
        let mut inner = BridgeJobStoreInner {
            jobs: snapshot.jobs,
            idempotency: snapshot.idempotency,
            order: snapshot.order,
            capacity,
        };
        inner.prune();
        Ok(inner)
    }

    fn persist(&self, snapshot: &PersistedBridgeJobStore) -> Result<(), BridgeJobStoreError> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let payload = serde_json::to_vec_pretty(snapshot)?;
        let temp_path = temp_store_path(&self.path)?;
        std::fs::write(&temp_path, payload)?;
        std::fs::rename(&temp_path, &self.path)?;
        Ok(())
    }
}

fn persisted_store_from_inner(inner: &BridgeJobStoreInner) -> PersistedBridgeJobStore {
    PersistedBridgeJobStore {
        version: BRIDGE_JOB_STORE_VERSION,
        jobs: inner.jobs.clone(),
        idempotency: inner.idempotency.clone(),
        order: inner.order.clone(),
    }
}

fn temp_store_path(path: &Path) -> Result<PathBuf, BridgeJobStoreError> {
    let file_name = path
        .file_name()
        .ok_or_else(|| BridgeJobStoreError::InvalidStatePath(path.to_path_buf()))?;
    Ok(path.with_file_name(format!("{}.tmp", file_name.to_string_lossy())))
}

pub fn new_publish_job(
    command: &str,
    job_id: String,
    idempotency_key: Option<String>,
    signer_mode: String,
    event_kind: u32,
    event_id: Option<String>,
    event_addr: Option<String>,
    delivery_policy: BridgeDeliveryPolicy,
    delivery_quorum: Option<usize>,
) -> BridgeJobRecord {
    BridgeJobRecord {
        job_id,
        command: command.to_string(),
        idempotency_key,
        status: BridgeJobStatus::Accepted,
        requested_at_unix: unix_timestamp_now(),
        completed_at_unix: None,
        signer_mode,
        event_kind,
        event_id,
        event_addr,
        delivery_policy,
        delivery_quorum,
        relay_count: 0,
        acknowledged_relay_count: 0,
        required_acknowledged_relay_count: 0,
        attempt_count: 0,
        attempt_summaries: Vec::new(),
        relay_results: Vec::new(),
        relay_outcome_summary: "accepted".to_string(),
    }
}

pub fn new_listing_publish_job(
    job_id: String,
    idempotency_key: Option<String>,
    signer_mode: String,
    event_kind: u32,
    event_id: Option<String>,
    event_addr: String,
    delivery_policy: BridgeDeliveryPolicy,
    delivery_quorum: Option<usize>,
) -> BridgeJobRecord {
    new_publish_job(
        "bridge.listing.publish",
        job_id,
        idempotency_key,
        signer_mode,
        event_kind,
        event_id,
        Some(event_addr),
        delivery_policy,
        delivery_quorum,
    )
}

pub fn new_order_request_job(
    job_id: String,
    idempotency_key: Option<String>,
    signer_mode: String,
    event_kind: u32,
    event_id: Option<String>,
    listing_addr: String,
    delivery_policy: BridgeDeliveryPolicy,
    delivery_quorum: Option<usize>,
) -> BridgeJobRecord {
    new_publish_job(
        "bridge.order.request",
        job_id,
        idempotency_key,
        signer_mode,
        event_kind,
        event_id,
        Some(listing_addr),
        delivery_policy,
        delivery_quorum,
    )
}

fn unix_timestamp_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_secs()
}

#[cfg(test)]
mod tests {
    use crate::app::config::BridgeDeliveryPolicy;
    use crate::core::bridge::publish::BridgePublishExecution;

    use super::{
        BridgeJobReservation, BridgeJobStatus, BridgeJobStore, PersistedBridgeJobStore,
        new_listing_publish_job, new_order_request_job,
    };

    #[test]
    fn reserve_returns_existing_job_for_same_idempotency_key() {
        let store = BridgeJobStore::new(8);
        let first = new_listing_publish_job(
            "job-1".to_string(),
            Some("same".to_string()),
            "embedded_service_identity".to_string(),
            30402,
            Some("event-1".to_string()),
            "30402:author:listing".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );
        let second = new_listing_publish_job(
            "job-2".to_string(),
            Some("same".to_string()),
            "embedded_service_identity".to_string(),
            30402,
            Some("event-2".to_string()),
            "30402:author:listing".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );

        assert!(matches!(
            store
                .reserve(first.clone(), "fingerprint-1".to_string())
                .expect("reserve"),
            BridgeJobReservation::Accepted(_)
        ));
        let existing = match store
            .reserve(second, "fingerprint-1".to_string())
            .expect("same idempotency key")
        {
            BridgeJobReservation::Duplicate(existing) => existing,
            BridgeJobReservation::Accepted(_) => panic!("expected duplicate reservation"),
        };
        assert_eq!(existing.job_id, first.job_id);
        assert_eq!(existing.status, BridgeJobStatus::Accepted);
    }

    #[test]
    fn reserve_rejects_conflicting_idempotency_key_reuse() {
        let store = BridgeJobStore::new(8);
        let first = new_listing_publish_job(
            "job-1".to_string(),
            Some("same".to_string()),
            "embedded_service_identity".to_string(),
            30402,
            Some("event-1".to_string()),
            "30402:author:listing".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );
        let second = new_listing_publish_job(
            "job-2".to_string(),
            Some("same".to_string()),
            "embedded_service_identity".to_string(),
            30402,
            Some("event-2".to_string()),
            "30402:author:listing".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );

        store
            .reserve(first, "fingerprint-1".to_string())
            .expect("reserve first");
        let err = store
            .reserve(second, "fingerprint-2".to_string())
            .expect_err("conflicting idempotency");
        assert!(err.to_string().contains("conflicts"));
    }

    #[test]
    fn complete_updates_job_record() {
        let store = BridgeJobStore::new(8);
        let job = new_listing_publish_job(
            "job-1".to_string(),
            None,
            "embedded_service_identity".to_string(),
            30402,
            Some("event-1".to_string()),
            "30402:author:listing".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );
        assert!(matches!(
            store
                .reserve(job, "fingerprint-1".to_string())
                .expect("reserve job"),
            BridgeJobReservation::Accepted(_)
        ));

        let completed = store
            .complete(
                "job-1",
                Some("event-1".to_string()),
                BridgePublishExecution {
                    published: true,
                    relay_count: 2,
                    acknowledged_relay_count: 1,
                    required_acknowledged_relay_count: 1,
                    delivery_policy: BridgeDeliveryPolicy::Any,
                    attempt_count: 1,
                    relay_outcome_summary: "1/2 relays acknowledged publish".to_string(),
                    relay_results: Vec::new(),
                    attempt_summaries: vec!["attempt 1".to_string()],
                },
            )
            .expect("complete job")
            .expect("record");

        assert_eq!(completed.status, BridgeJobStatus::Published);
        assert_eq!(completed.attempt_count, 1);
        assert_eq!(completed.acknowledged_relay_count, 1);
        assert!(completed.completed_at_unix.is_some());
    }

    #[test]
    fn reserve_prunes_oldest_jobs_when_capacity_is_exceeded() {
        let store = BridgeJobStore::new(1);
        let first = new_listing_publish_job(
            "job-1".to_string(),
            Some("first".to_string()),
            "embedded_service_identity".to_string(),
            30402,
            Some("event-1".to_string()),
            "30402:author:listing-1".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );
        let second = new_listing_publish_job(
            "job-2".to_string(),
            Some("second".to_string()),
            "embedded_service_identity".to_string(),
            30402,
            Some("event-2".to_string()),
            "30402:author:listing-2".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );

        assert!(matches!(
            store
                .reserve(first, "fingerprint-1".to_string())
                .expect("first"),
            BridgeJobReservation::Accepted(_)
        ));
        assert!(matches!(
            store
                .reserve(second, "fingerprint-2".to_string())
                .expect("second"),
            BridgeJobReservation::Accepted(_)
        ));

        assert!(store.get("job-1").is_none());
        assert!(store.get("job-2").is_some());
        assert_eq!(store.snapshot().retained_jobs, 1);
    }

    #[test]
    fn order_request_job_uses_order_command_name() {
        let job = new_order_request_job(
            "job-1".to_string(),
            Some("same".to_string()),
            "nip46_session:session-1".to_string(),
            5322,
            Some("event-1".to_string()),
            "30402:author:listing".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );

        assert_eq!(job.command, "bridge.order.request");
        assert_eq!(job.event_addr.as_deref(), Some("30402:author:listing"));
        assert_eq!(job.signer_mode, "nip46_session:session-1");
    }

    #[test]
    fn load_recovers_persisted_jobs_and_idempotency() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("radrootsd-bridge-jobs-{nanos}.json"));
        let store = BridgeJobStore::load(path.clone(), 8).expect("load empty store");
        let first = new_listing_publish_job(
            "job-1".to_string(),
            Some("same".to_string()),
            "embedded_service_identity".to_string(),
            30402,
            Some("event-1".to_string()),
            "30402:author:listing".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );
        assert!(matches!(
            store
                .reserve(first, "fingerprint-1".to_string())
                .expect("reserve first"),
            BridgeJobReservation::Accepted(_)
        ));

        let loaded = BridgeJobStore::load(path.clone(), 8).expect("reload store");
        let duplicate = new_listing_publish_job(
            "job-2".to_string(),
            Some("same".to_string()),
            "embedded_service_identity".to_string(),
            30402,
            Some("event-2".to_string()),
            "30402:author:listing".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );
        let existing = match loaded
            .reserve(duplicate, "fingerprint-1".to_string())
            .expect("dedupe after reload")
        {
            BridgeJobReservation::Duplicate(existing) => existing,
            BridgeJobReservation::Accepted(_) => panic!("expected duplicate reservation"),
        };
        assert_eq!(existing.job_id, "job-1");

        let payload = std::fs::read_to_string(&path).expect("persisted payload");
        let persisted: PersistedBridgeJobStore =
            serde_json::from_str(&payload).expect("persisted store");
        assert_eq!(persisted.version, 2);

        let _ = std::fs::remove_file(path);
    }
}
