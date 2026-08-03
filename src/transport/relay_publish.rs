//! Daemon-owned adaptation from the V5 publish RPC to bounded Nostr attempts.

use core::future::Future;
use core::pin::Pin;
use core::time::Duration;
#[cfg(test)]
use std::collections::BTreeMap;
use std::net::IpAddr;
#[cfg(test)]
use std::sync::{Arc, Mutex};

use nostr::JsonUtil;
use radroots_event::SignedEvent;
use radroots_transport::RadrootsTransportSatisfactionPolicy;

use crate::host_nostr::DaemonNostrClient;

pub(crate) type PublishFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<RelayPublishReceipt>, RelayPublishError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum RelayUrlPolicy {
    Public,
    Localhost,
}

impl RelayUrlPolicy {
    const fn native(self) -> radroots_transport_nostr::RelayUrlPolicy {
        match self {
            Self::Public => radroots_transport_nostr::RelayUrlPolicy::Public,
            Self::Localhost => radroots_transport_nostr::RelayUrlPolicy::Local,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct RelayUrl {
    inner: radroots_transport_nostr::RelayUrl,
    policy: RelayUrlPolicy,
}

impl RelayUrl {
    pub(crate) fn parse(
        value: impl AsRef<str>,
        policy: RelayUrlPolicy,
    ) -> Result<Self, RelayPublishError> {
        let inner = radroots_transport_nostr::RelayUrl::parse(value, policy.native())
            .map_err(|error| RelayPublishError(error.to_string()))?;
        Ok(Self { inner, policy })
    }

    pub(crate) fn as_str(&self) -> &str {
        self.inner.as_str()
    }

    pub(crate) fn validate_public_resolved_ip_addrs(
        &self,
        addresses: impl IntoIterator<Item = IpAddr>,
    ) -> Result<(), RelayPublishError> {
        self.inner
            .validate_resolved_addresses(self.policy.native(), addresses)
            .map_err(|error| RelayPublishError(error.to_string()))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RelayTargetSet {
    relays: Vec<RelayUrl>,
}

impl RelayTargetSet {
    pub(crate) fn from_urls(mut relays: Vec<RelayUrl>) -> Result<Self, RelayPublishError> {
        if relays.is_empty() {
            return Err(RelayPublishError(
                "relay target set must not be empty".to_owned(),
            ));
        }
        relays.sort();
        relays.dedup();
        Ok(Self { relays })
    }

    pub(crate) fn relays(&self) -> &[RelayUrl] {
        self.relays.as_slice()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RelayPublishRequest {
    signed_event: SignedEvent,
    targets: RelayTargetSet,
    satisfaction_policy: RadrootsTransportSatisfactionPolicy,
    now_ms: i64,
}

impl RelayPublishRequest {
    pub(crate) fn new(signed_event: SignedEvent, targets: RelayTargetSet, now_ms: i64) -> Self {
        Self {
            signed_event,
            targets,
            satisfaction_policy: RadrootsTransportSatisfactionPolicy::all_accepted(),
            now_ms,
        }
    }

    pub(crate) fn with_satisfaction_policy(
        mut self,
        satisfaction_policy: RadrootsTransportSatisfactionPolicy,
    ) -> Self {
        self.satisfaction_policy = satisfaction_policy;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RelayOutcomeKind {
    Accepted,
    DuplicateAccepted,
    Blocked,
    RateLimited,
    Invalid,
    PowRequired,
    Restricted,
    AuthRequired,
    Muted,
    Unsupported,
    PaymentRequired,
    Error,
    Timeout,
    ConnectionFailed,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RelayOutcome {
    pub(crate) kind: RelayOutcomeKind,
    pub(crate) message: Option<String>,
}

impl RelayOutcome {
    pub(crate) const fn accepted() -> Self {
        Self {
            kind: RelayOutcomeKind::Accepted,
            message: None,
        }
    }

    pub(crate) fn classify(message: impl AsRef<str>) -> Self {
        let message = message.as_ref().trim().to_owned();
        let lower = message.to_ascii_lowercase();
        let kind = if lower.starts_with("duplicate:") {
            RelayOutcomeKind::DuplicateAccepted
        } else if lower.starts_with("blocked:") {
            RelayOutcomeKind::Blocked
        } else if lower.starts_with("rate-limited:") || lower.contains("rate limit") {
            RelayOutcomeKind::RateLimited
        } else if lower.starts_with("invalid:") {
            RelayOutcomeKind::Invalid
        } else if lower.starts_with("pow:") {
            RelayOutcomeKind::PowRequired
        } else if lower.starts_with("restricted:") {
            RelayOutcomeKind::Restricted
        } else if lower.starts_with("auth-required:") || lower.contains("auth required") {
            RelayOutcomeKind::AuthRequired
        } else if lower.starts_with("mute:") {
            RelayOutcomeKind::Muted
        } else if lower.starts_with("unsupported:") {
            RelayOutcomeKind::Unsupported
        } else if lower.starts_with("payment-required:") {
            RelayOutcomeKind::PaymentRequired
        } else if lower.starts_with("timeout:") || lower.contains("timeout") {
            RelayOutcomeKind::Timeout
        } else if lower.starts_with("error:") {
            RelayOutcomeKind::Error
        } else {
            RelayOutcomeKind::Unknown
        };
        Self {
            kind,
            message: Some(message),
        }
    }

    pub(crate) fn timeout(message: impl Into<String>) -> Self {
        Self {
            kind: RelayOutcomeKind::Timeout,
            message: Some(message.into()),
        }
    }

    pub(crate) fn connection_failed(message: impl Into<String>) -> Self {
        Self {
            kind: RelayOutcomeKind::ConnectionFailed,
            message: Some(message.into()),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RelayPublishReceipt {
    pub(crate) relay_url: String,
    pub(crate) outcome: RelayOutcome,
    pub(crate) attempted: bool,
}

impl RelayPublishReceipt {
    pub(crate) fn attempted(relay_url: impl Into<String>, outcome: RelayOutcome) -> Self {
        Self {
            relay_url: relay_url.into(),
            outcome,
            attempted: true,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct RelayPublishError(pub(crate) String);

pub(crate) trait RelayPublishAdapter: Send + Sync {
    fn publish(&self, request: RelayPublishRequest) -> PublishFuture<'_>;
}

#[derive(Clone)]
pub(crate) struct LiveRelayPublishAdapter {
    client: DaemonNostrClient,
}

impl LiveRelayPublishAdapter {
    pub(crate) const fn new(client: DaemonNostrClient) -> Self {
        Self { client }
    }
}

impl RelayPublishAdapter for LiveRelayPublishAdapter {
    fn publish(&self, request: RelayPublishRequest) -> PublishFuture<'_> {
        Box::pin(async move {
            if request.now_ms < 0 {
                return Err(RelayPublishError(
                    "publish timestamp must be nonnegative".to_owned(),
                ));
            }
            let event = nostr::Event::from_json(request.signed_event.raw_json())
                .map_err(|error| RelayPublishError(error.to_string()))?;
            let client = self.client.clone().into_inner();
            let mut receipts = Vec::with_capacity(request.targets.relays().len());
            for relay in request.targets.relays() {
                let relay_url = relay.as_str().to_owned();
                let attempt = async {
                    client.add_relay(relay_url.as_str()).await?;
                    client
                        .try_connect_relay(relay_url.as_str(), Duration::from_secs(10))
                        .await?;
                    client.send_event_to([relay_url.as_str()], &event).await
                };
                let outcome = match attempt.await {
                    Ok(output)
                        if output.success.iter().any(|url| {
                            url.to_string().trim_end_matches('/') == relay_url.trim_end_matches('/')
                        }) =>
                    {
                        RelayOutcome::accepted()
                    }
                    Ok(output) => output
                        .failed
                        .values()
                        .next()
                        .map(RelayOutcome::classify)
                        .unwrap_or_else(|| RelayOutcome::connection_failed("relay omitted result")),
                    Err(error) => RelayOutcome::connection_failed(error.to_string()),
                };
                receipts.push(RelayPublishReceipt::attempted(relay_url, outcome));
            }
            Ok(receipts)
        })
    }
}

#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct MockRelayPublishAdapter {
    outcomes: BTreeMap<String, RelayOutcome>,
    captured_raw_events: Arc<Mutex<Vec<String>>>,
}

#[cfg(test)]
impl MockRelayPublishAdapter {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_outcome(
        mut self,
        relay_url: impl Into<String>,
        outcome: RelayOutcome,
    ) -> Self {
        self.outcomes.insert(relay_url.into(), outcome);
        self
    }

    pub(crate) fn captured_raw_events(&self) -> Vec<String> {
        self.captured_raw_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[cfg(test)]
impl RelayPublishAdapter for MockRelayPublishAdapter {
    fn publish(&self, request: RelayPublishRequest) -> PublishFuture<'_> {
        Box::pin(async move {
            self.captured_raw_events
                .lock()
                .map_err(|_| RelayPublishError("captured event lock poisoned".to_owned()))?
                .push(request.signed_event.raw_json().to_owned());
            Ok(request
                .targets
                .relays()
                .iter()
                .map(|relay| {
                    RelayPublishReceipt::attempted(
                        relay.as_str(),
                        self.outcomes
                            .get(relay.as_str())
                            .cloned()
                            .unwrap_or_else(RelayOutcome::accepted),
                    )
                })
                .collect())
        })
    }
}
