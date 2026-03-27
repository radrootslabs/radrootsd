use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use serde::Serialize;

use crate::app::config::BridgeDeliveryPolicy;
use crate::core::bridge::publish::{BridgePublishExecution, BridgeRelayPublishResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeJobStatus {
    Accepted,
    Published,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
}

#[derive(Debug)]
struct BridgeJobStoreInner {
    jobs: HashMap<String, BridgeJobRecord>,
    idempotency: HashMap<String, String>,
    order: VecDeque<String>,
    capacity: usize,
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
        }
    }

    pub fn reserve(&self, mut record: BridgeJobRecord) -> Result<BridgeJobRecord, BridgeJobRecord> {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if let Some(idempotency_key) = record.idempotency_key.as_ref() {
            if let Some(job_id) = inner.idempotency.get(idempotency_key) {
                if let Some(existing) = inner.jobs.get(job_id) {
                    return Err(existing.clone());
                }
            }
        }

        record.status = BridgeJobStatus::Accepted;
        inner.order.push_back(record.job_id.clone());
        if let Some(idempotency_key) = record.idempotency_key.as_ref() {
            inner
                .idempotency
                .insert(idempotency_key.clone(), record.job_id.clone());
        }
        inner.jobs.insert(record.job_id.clone(), record.clone());
        inner.prune();
        Ok(record)
    }

    pub fn complete(
        &self,
        job_id: &str,
        execution: BridgePublishExecution,
    ) -> Option<BridgeJobRecord> {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let record = inner.jobs.get_mut(job_id)?;
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
        Some(record.clone())
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
                if self.idempotency.get(&idempotency_key) == Some(&job_id) {
                    self.idempotency.remove(&idempotency_key);
                }
            }
        }
    }
}

pub fn new_listing_publish_job(
    job_id: String,
    idempotency_key: Option<String>,
    event_kind: u32,
    event_id: String,
    event_addr: String,
    delivery_policy: BridgeDeliveryPolicy,
    delivery_quorum: Option<usize>,
) -> BridgeJobRecord {
    BridgeJobRecord {
        job_id,
        command: "bridge.listing.publish".to_string(),
        idempotency_key,
        status: BridgeJobStatus::Accepted,
        requested_at_unix: unix_timestamp_now(),
        completed_at_unix: None,
        signer_mode: "embedded_service_identity".to_string(),
        event_kind,
        event_id: Some(event_id),
        event_addr: Some(event_addr),
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

    use super::{BridgeJobStatus, BridgeJobStore, new_listing_publish_job};

    #[test]
    fn reserve_returns_existing_job_for_same_idempotency_key() {
        let store = BridgeJobStore::new(8);
        let first = new_listing_publish_job(
            "job-1".to_string(),
            Some("same".to_string()),
            30402,
            "event-1".to_string(),
            "30402:author:listing".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );
        let second = new_listing_publish_job(
            "job-2".to_string(),
            Some("same".to_string()),
            30402,
            "event-2".to_string(),
            "30402:author:listing".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );

        assert!(store.reserve(first.clone()).is_ok());
        let existing = store.reserve(second).expect_err("same idempotency key");
        assert_eq!(existing.job_id, first.job_id);
        assert_eq!(existing.status, BridgeJobStatus::Accepted);
    }

    #[test]
    fn complete_updates_job_record() {
        let store = BridgeJobStore::new(8);
        let job = new_listing_publish_job(
            "job-1".to_string(),
            None,
            30402,
            "event-1".to_string(),
            "30402:author:listing".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );
        store.reserve(job).expect("reserve job");

        let completed = store
            .complete(
                "job-1",
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
            .expect("complete job");

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
            30402,
            "event-1".to_string(),
            "30402:author:listing-1".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );
        let second = new_listing_publish_job(
            "job-2".to_string(),
            Some("second".to_string()),
            30402,
            "event-2".to_string(),
            "30402:author:listing-2".to_string(),
            BridgeDeliveryPolicy::Any,
            None,
        );

        store.reserve(first).expect("first");
        store.reserve(second).expect("second");

        assert!(store.get("job-1").is_none());
        assert!(store.get("job-2").is_some());
        assert_eq!(store.snapshot().retained_jobs, 1);
    }
}
