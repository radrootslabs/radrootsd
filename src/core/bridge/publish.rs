use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use radroots_nostr::prelude::{RadrootsNostrClient, RadrootsNostrOutput, RadrootsNostrRelayUrl};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::app::config::{BridgeConfig, BridgeDeliveryPolicy};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeRelayPublishResult {
    pub relay_url: String,
    pub acknowledged: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BridgePublishExecution {
    pub published: bool,
    pub relay_count: usize,
    pub acknowledged_relay_count: usize,
    pub required_acknowledged_relay_count: usize,
    pub delivery_policy: BridgeDeliveryPolicy,
    pub attempt_count: usize,
    pub relay_outcome_summary: String,
    pub relay_results: Vec<BridgeRelayPublishResult>,
    pub attempt_summaries: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgePublishSettings {
    pub connect_timeout_secs: u64,
    pub delivery_policy: BridgeDeliveryPolicy,
    pub delivery_quorum: Option<usize>,
    pub publish_max_attempts: usize,
    pub publish_initial_backoff_millis: u64,
    pub publish_max_backoff_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BridgePublishAttemptResult {
    attempt_number: usize,
    acknowledged_relay_count: usize,
    relay_outcome_summary: String,
    relay_results: Vec<BridgeRelayPublishResult>,
}

impl BridgePublishSettings {
    pub fn from_config(config: &BridgeConfig) -> Self {
        Self {
            connect_timeout_secs: config.connect_timeout_secs,
            delivery_policy: config.delivery_policy,
            delivery_quorum: config.delivery_quorum,
            publish_max_attempts: config.publish_max_attempts,
            publish_initial_backoff_millis: config.publish_initial_backoff_millis,
            publish_max_backoff_millis: config.publish_max_backoff_millis,
        }
    }

    fn required_acknowledged_relay_count(&self, relay_count: usize) -> Result<usize, String> {
        if relay_count == 0 {
            return Err("cannot publish without at least one relay".to_string());
        }
        if self.connect_timeout_secs == 0 {
            return Err("bridge.connect_timeout_secs must be greater than zero".to_string());
        }
        if self.publish_max_attempts == 0 {
            return Err("bridge.publish_max_attempts must be greater than zero".to_string());
        }
        if self.publish_initial_backoff_millis == 0 {
            return Err(
                "bridge.publish_initial_backoff_millis must be greater than zero".to_string(),
            );
        }
        if self.publish_max_backoff_millis == 0 {
            return Err("bridge.publish_max_backoff_millis must be greater than zero".to_string());
        }
        if self.publish_initial_backoff_millis > self.publish_max_backoff_millis {
            return Err(
                "bridge.publish_max_backoff_millis must be greater than or equal to bridge.publish_initial_backoff_millis"
                    .to_string(),
            );
        }

        match self.delivery_policy {
            BridgeDeliveryPolicy::Any => Ok(1),
            BridgeDeliveryPolicy::All => Ok(relay_count),
            BridgeDeliveryPolicy::Quorum => {
                let delivery_quorum = self.delivery_quorum.ok_or_else(|| {
                    "bridge.delivery_quorum must be set when bridge.delivery_policy is `quorum`"
                        .to_string()
                })?;
                if delivery_quorum == 0 {
                    return Err("bridge.delivery_quorum must be greater than zero".to_string());
                }
                if delivery_quorum > relay_count {
                    return Err(format!(
                        "bridge.delivery_quorum `{delivery_quorum}` cannot be satisfied by `{relay_count}` target relays"
                    ));
                }
                Ok(delivery_quorum)
            }
        }
    }

    fn backoff_for_attempt(&self, completed_attempt_number: usize) -> u64 {
        let exponent = completed_attempt_number.saturating_sub(1) as u32;
        let scaled = self
            .publish_initial_backoff_millis
            .saturating_mul(2_u64.saturating_pow(exponent));
        scaled.min(self.publish_max_backoff_millis)
    }
}

pub async fn connect_and_publish_event(
    client: &RadrootsNostrClient,
    settings: &BridgePublishSettings,
    event: &radroots_nostr::prelude::RadrootsNostrEvent,
) -> BridgePublishExecution {
    let relays = client
        .relays()
        .await
        .keys()
        .cloned()
        .collect::<Vec<RadrootsNostrRelayUrl>>();
    publish_with_policy(&relays, settings, || async {
        client.connect().await;
        client
            .wait_for_connection(Duration::from_secs(settings.connect_timeout_secs))
            .await;
        client
            .send_event(event)
            .await
            .map_err(|error| error.to_string())
    })
    .await
}

pub async fn publish_with_policy<T, F, Fut>(
    relays: &[RadrootsNostrRelayUrl],
    settings: &BridgePublishSettings,
    mut send_attempt: F,
) -> BridgePublishExecution
where
    T: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<RadrootsNostrOutput<T>, String>>,
{
    let relay_count = relays.len();
    let required_acknowledged_relay_count =
        match settings.required_acknowledged_relay_count(relay_count) {
            Ok(required) => required,
            Err(error) => {
                let relay_results = relays
                    .iter()
                    .map(|relay| BridgeRelayPublishResult {
                        relay_url: relay.to_string(),
                        acknowledged: false,
                        detail: Some(error.clone()),
                    })
                    .collect::<Vec<_>>();
                return BridgePublishExecution {
                    published: false,
                    relay_count,
                    acknowledged_relay_count: 0,
                    required_acknowledged_relay_count: 0,
                    delivery_policy: settings.delivery_policy,
                    attempt_count: 0,
                    relay_outcome_summary: error.clone(),
                    relay_results,
                    attempt_summaries: vec![error],
                };
            }
        };
    let mut attempt_results = Vec::new();

    for attempt_number in 1..=settings.publish_max_attempts {
        let attempt = match send_attempt().await {
            Ok(output) => build_publish_attempt_result(relays, attempt_number, &output),
            Err(error) => build_failed_publish_attempt_result(relays, attempt_number, error),
        };
        let threshold_reached =
            attempt.acknowledged_relay_count >= required_acknowledged_relay_count;
        attempt_results.push(attempt);

        if threshold_reached {
            let final_attempt = attempt_results
                .last()
                .expect("publish attempt results contain the successful attempt");
            return BridgePublishExecution {
                published: true,
                relay_count,
                acknowledged_relay_count: final_attempt.acknowledged_relay_count,
                required_acknowledged_relay_count,
                delivery_policy: settings.delivery_policy,
                attempt_count: attempt_results.len(),
                relay_outcome_summary: summarize_delivery_policy_result(
                    settings.delivery_policy,
                    required_acknowledged_relay_count,
                    &attempt_results,
                ),
                relay_results: final_attempt.relay_results.clone(),
                attempt_summaries: attempt_results
                    .iter()
                    .map(|attempt| attempt.relay_outcome_summary.clone())
                    .collect(),
            };
        }

        if attempt_number < settings.publish_max_attempts {
            sleep(Duration::from_millis(
                settings.backoff_for_attempt(attempt_number),
            ))
            .await;
        }
    }

    let final_attempt = attempt_results
        .last()
        .expect("publish attempt results contain at least one attempt");
    BridgePublishExecution {
        published: false,
        relay_count,
        acknowledged_relay_count: final_attempt.acknowledged_relay_count,
        required_acknowledged_relay_count,
        delivery_policy: settings.delivery_policy,
        attempt_count: attempt_results.len(),
        relay_outcome_summary: summarize_delivery_policy_result(
            settings.delivery_policy,
            required_acknowledged_relay_count,
            &attempt_results,
        ),
        relay_results: final_attempt.relay_results.clone(),
        attempt_summaries: attempt_results
            .iter()
            .map(|attempt| attempt.relay_outcome_summary.clone())
            .collect(),
    }
}

fn build_publish_relay_results<T>(
    relays: &[RadrootsNostrRelayUrl],
    output: &RadrootsNostrOutput<T>,
) -> Vec<BridgeRelayPublishResult>
where
    T: std::fmt::Debug,
{
    let acknowledged_relays = output
        .success
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let failed_relays = output
        .failed
        .iter()
        .map(|(relay, error)| (relay.to_string(), error.to_string()))
        .collect::<BTreeMap<_, _>>();

    relays
        .iter()
        .map(|relay| {
            let relay_url = relay.to_string();
            if acknowledged_relays.contains(&relay_url) {
                BridgeRelayPublishResult {
                    relay_url,
                    acknowledged: true,
                    detail: None,
                }
            } else {
                BridgeRelayPublishResult {
                    relay_url: relay_url.clone(),
                    acknowledged: false,
                    detail: Some(
                        failed_relays
                            .get(&relay_url)
                            .cloned()
                            .unwrap_or_else(|| "no relay acknowledgement reported".to_owned()),
                    ),
                }
            }
        })
        .collect()
}

fn build_publish_attempt_result<T>(
    relays: &[RadrootsNostrRelayUrl],
    attempt_number: usize,
    output: &RadrootsNostrOutput<T>,
) -> BridgePublishAttemptResult
where
    T: std::fmt::Debug,
{
    let relay_results = build_publish_relay_results(relays, output);
    let acknowledged_relay_count = relay_results
        .iter()
        .filter(|result| result.acknowledged)
        .count();
    BridgePublishAttemptResult {
        attempt_number,
        acknowledged_relay_count,
        relay_outcome_summary: summarize_publish_results(&relay_results),
        relay_results,
    }
}

fn build_failed_publish_attempt_result(
    relays: &[RadrootsNostrRelayUrl],
    attempt_number: usize,
    error: String,
) -> BridgePublishAttemptResult {
    let relay_results = relays
        .iter()
        .map(|relay| BridgeRelayPublishResult {
            relay_url: relay.to_string(),
            acknowledged: false,
            detail: Some(error.clone()),
        })
        .collect::<Vec<_>>();
    BridgePublishAttemptResult {
        attempt_number,
        acknowledged_relay_count: 0,
        relay_outcome_summary: summarize_publish_results(&relay_results),
        relay_results,
    }
}

fn summarize_publish_results(relay_results: &[BridgeRelayPublishResult]) -> String {
    let relay_count = relay_results.len();
    let acknowledged_relay_count = relay_results
        .iter()
        .filter(|result| result.acknowledged)
        .count();
    if relay_count == 0 {
        return "no relay acknowledged the publish".to_owned();
    }

    let mut summary =
        format!("{acknowledged_relay_count}/{relay_count} relays acknowledged publish");
    let acknowledged = relay_results
        .iter()
        .filter(|result| result.acknowledged)
        .map(|result| result.relay_url.clone())
        .collect::<Vec<_>>();
    if !acknowledged.is_empty() {
        summary.push_str("; acknowledged: ");
        summary.push_str(&acknowledged.join(", "));
    }
    let failures = relay_results
        .iter()
        .filter(|result| !result.acknowledged)
        .map(|result| match result.detail.as_deref() {
            Some(detail) => format!("{}: {detail}", result.relay_url),
            None => result.relay_url.clone(),
        })
        .collect::<Vec<_>>();
    if !failures.is_empty() {
        summary.push_str("; failures: ");
        summary.push_str(&failures.join("; "));
    }
    summary
}

fn summarize_delivery_policy_result(
    delivery_policy: BridgeDeliveryPolicy,
    required_acknowledged_relay_count: usize,
    attempt_results: &[BridgePublishAttemptResult],
) -> String {
    let attempt_count = attempt_results.len();
    let final_attempt = attempt_results
        .last()
        .expect("delivery policy summary requires at least one attempt");
    let mut summary = format!(
        "delivery policy {} required {required_acknowledged_relay_count} acknowledgements across {attempt_count} attempt(s); final attempt {}: {}",
        delivery_policy.as_str(),
        final_attempt.attempt_number,
        final_attempt.relay_outcome_summary,
    );
    if attempt_results.len() > 1 {
        let attempt_summaries = attempt_results
            .iter()
            .map(|attempt| {
                format!(
                    "attempt {}: {}",
                    attempt.attempt_number, attempt.relay_outcome_summary
                )
            })
            .collect::<Vec<_>>();
        summary.push_str("; ");
        summary.push_str(&attempt_summaries.join(" | "));
    }
    summary
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use radroots_nostr::prelude::{
        RadrootsNostrEventId, RadrootsNostrOutput, RadrootsNostrRelayUrl,
    };
    use tokio::time::Instant;

    use crate::app::config::{BridgeConfig, BridgeDeliveryPolicy};

    use super::{BridgePublishSettings, publish_with_policy};

    #[test]
    fn publish_settings_from_config_copies_values() {
        let config = BridgeConfig {
            enabled: true,
            bearer_token: Some("secret".to_string()),
            connect_timeout_secs: 15,
            delivery_policy: BridgeDeliveryPolicy::Quorum,
            delivery_quorum: Some(2),
            publish_max_attempts: 3,
            publish_initial_backoff_millis: 125,
            publish_max_backoff_millis: 500,
            job_status_retention: 64,
            ..BridgeConfig::default()
        };

        assert_eq!(
            BridgePublishSettings::from_config(&config),
            BridgePublishSettings {
                connect_timeout_secs: 15,
                delivery_policy: BridgeDeliveryPolicy::Quorum,
                delivery_quorum: Some(2),
                publish_max_attempts: 3,
                publish_initial_backoff_millis: 125,
                publish_max_backoff_millis: 500,
            }
        );
    }

    #[tokio::test]
    async fn publish_with_policy_retries_until_threshold_is_met() {
        let relays = vec![
            RadrootsNostrRelayUrl::parse("wss://relay-a.example.com").expect("relay-a"),
            RadrootsNostrRelayUrl::parse("wss://relay-b.example.com").expect("relay-b"),
        ];
        let settings = BridgePublishSettings {
            connect_timeout_secs: 10,
            delivery_policy: BridgeDeliveryPolicy::All,
            delivery_quorum: None,
            publish_max_attempts: 2,
            publish_initial_backoff_millis: 10,
            publish_max_backoff_millis: 10,
        };
        let attempts = Arc::new(Mutex::new(vec![
            publish_output(
                "1111111111111111111111111111111111111111111111111111111111111111",
                &["wss://relay-a.example.com"],
                &[("wss://relay-b.example.com", "blocked")],
            ),
            publish_output(
                "2222222222222222222222222222222222222222222222222222222222222222",
                &["wss://relay-a.example.com", "wss://relay-b.example.com"],
                &[],
            ),
        ]));

        let start = Instant::now();
        let outcome = publish_with_policy(&relays, &settings, || {
            let attempts = Arc::clone(&attempts);
            async move {
                let output = attempts.lock().expect("attempts lock").remove(0);
                Ok(output)
            }
        })
        .await;

        assert!(outcome.published);
        assert_eq!(outcome.delivery_policy, BridgeDeliveryPolicy::All);
        assert_eq!(outcome.required_acknowledged_relay_count, 2);
        assert_eq!(outcome.attempt_count, 2);
        assert_eq!(outcome.acknowledged_relay_count, 2);
        assert_eq!(outcome.relay_results.len(), 2);
        assert_eq!(outcome.attempt_summaries.len(), 2);
        assert!(
            outcome
                .relay_outcome_summary
                .contains("delivery policy all")
        );
        assert!(outcome.relay_outcome_summary.contains("attempt 1"));
        assert!(start.elapsed() >= std::time::Duration::from_millis(10));
    }

    #[tokio::test]
    async fn publish_with_policy_reports_threshold_failure() {
        let relays = vec![
            RadrootsNostrRelayUrl::parse("wss://relay-a.example.com").expect("relay-a"),
            RadrootsNostrRelayUrl::parse("wss://relay-b.example.com").expect("relay-b"),
        ];
        let settings = BridgePublishSettings {
            connect_timeout_secs: 10,
            delivery_policy: BridgeDeliveryPolicy::Quorum,
            delivery_quorum: Some(2),
            publish_max_attempts: 2,
            publish_initial_backoff_millis: 1,
            publish_max_backoff_millis: 1,
        };

        let outcome =
            publish_with_policy::<RadrootsNostrEventId, _, _>(&relays, &settings, || async {
                Ok(publish_output(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    &["wss://relay-a.example.com"],
                    &[("wss://relay-b.example.com", "blocked")],
                ))
            })
            .await;

        assert!(!outcome.published);
        assert_eq!(outcome.delivery_policy, BridgeDeliveryPolicy::Quorum);
        assert_eq!(outcome.required_acknowledged_relay_count, 2);
        assert_eq!(outcome.attempt_count, 2);
        assert!(
            outcome
                .relay_outcome_summary
                .contains("delivery policy quorum")
        );
    }

    #[tokio::test]
    async fn publish_with_policy_reports_configuration_failure_without_attempts() {
        let settings = BridgePublishSettings {
            connect_timeout_secs: 0,
            delivery_policy: BridgeDeliveryPolicy::Any,
            delivery_quorum: None,
            publish_max_attempts: 1,
            publish_initial_backoff_millis: 10,
            publish_max_backoff_millis: 10,
        };

        let outcome = publish_with_policy::<RadrootsNostrEventId, _, _>(&[], &settings, || async {
            unreachable!("configuration failure should short-circuit")
        })
        .await;

        assert!(!outcome.published);
        assert_eq!(outcome.attempt_count, 0);
        assert!(outcome.relay_outcome_summary.contains("cannot publish"));
    }

    fn publish_output(
        event_id_hex: &str,
        succeeded_relays: &[&str],
        failed_relays: &[(&str, &str)],
    ) -> RadrootsNostrOutput<RadrootsNostrEventId> {
        let success = succeeded_relays
            .iter()
            .map(|relay| RadrootsNostrRelayUrl::parse(*relay).expect("success relay"))
            .collect::<HashSet<_>>();
        let failed = failed_relays
            .iter()
            .map(|(relay, error)| {
                (
                    RadrootsNostrRelayUrl::parse(*relay).expect("failed relay"),
                    (*error).to_owned(),
                )
            })
            .collect::<HashMap<_, _>>();

        RadrootsNostrOutput {
            val: RadrootsNostrEventId::parse(event_id_hex).expect("event id"),
            success,
            failed,
        }
    }
}
