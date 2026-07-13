use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_transport_publish_protocol::{
    METHOD_CAPABILITIES, METHOD_EVENT, METHOD_JOB_GET, METHOD_JOB_LIST,
    TransportPublishCapabilities, TransportPublishEventRequest,
};
use serde::Deserialize;

use crate::core::transport_publish::TransportPublishError;
use crate::transport::jsonrpc::auth::require_publish_principal;
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct JobGetParams {
    job_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobListParams {
    limit: Option<usize>,
}

pub fn module(ctx: RpcContext, registry: MethodRegistry) -> Result<RpcModule<RpcContext>> {
    let mut module = RpcModule::new(ctx);
    register_capabilities(&mut module, &registry)?;
    register_event(&mut module, &registry)?;
    register_job_get(&mut module, &registry)?;
    register_job_list(&mut module, &registry)?;
    Ok(module)
}

fn register_capabilities(
    module: &mut RpcModule<RpcContext>,
    registry: &MethodRegistry,
) -> Result<()> {
    registry.track(METHOD_CAPABILITIES);
    module.register_async_method(METHOD_CAPABILITIES, |_params, ctx, extensions| async move {
        require_publish_principal(&extensions)?;
        Ok::<TransportPublishCapabilities, RpcError>(TransportPublishCapabilities::v4(
            ctx.state.transport_publish.config.max_event_bytes,
            ctx.state.transport_publish.config.max_targets_per_request,
        ))
    })?;
    Ok(())
}

fn register_event(module: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track(METHOD_EVENT);
    module.register_async_method(METHOD_EVENT, |params, ctx, extensions| async move {
        let principal = require_publish_principal(&extensions)?;
        let request: TransportPublishEventRequest = params
            .parse()
            .map_err(|error| RpcError::InvalidParams(error.to_string()))?;
        ctx.state
            .transport_publish
            .publish_event(&principal, request)
            .await
            .map_err(rpc_error_from_transport_publish)
    })?;
    Ok(())
}

fn register_job_get(module: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track(METHOD_JOB_GET);
    module.register_async_method(METHOD_JOB_GET, |params, ctx, extensions| async move {
        let principal = require_publish_principal(&extensions)?;
        let params: JobGetParams = params
            .parse()
            .map_err(|error| RpcError::InvalidParams(error.to_string()))?;
        let job_id = params.job_id.trim();
        if job_id.is_empty() {
            return Err(RpcError::InvalidParams("missing job_id".to_owned()));
        }
        ctx.state
            .transport_publish
            .store
            .job_by_id_for_principal(job_id, &principal)
            .map_err(|error| RpcError::Other(error.to_string()))?
            .ok_or_else(|| RpcError::Other(format!("unknown publish job: {job_id}")))
    })?;
    Ok(())
}

fn register_job_list(module: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track(METHOD_JOB_LIST);
    module.register_async_method(METHOD_JOB_LIST, |params, ctx, extensions| async move {
        let principal = require_publish_principal(&extensions)?;
        let params = if params.len_bytes() == 0 || params.as_str() == Some("[]") {
            JobListParams { limit: None }
        } else {
            params
                .parse::<JobListParams>()
                .map_err(|error| RpcError::InvalidParams(error.to_string()))?
        };
        if params.limit == Some(0) {
            return Err(RpcError::InvalidParams(
                "limit must be greater than zero".to_owned(),
            ));
        }
        let configured_limit = ctx.state.transport_publish.config.job_list_limit;
        let limit = params
            .limit
            .unwrap_or(configured_limit)
            .min(configured_limit);
        ctx.state
            .transport_publish
            .store
            .list_jobs_for_principal(&principal, limit)
            .map_err(|error| RpcError::Other(error.to_string()))
    })?;
    Ok(())
}

fn rpc_error_from_transport_publish(error: TransportPublishError) -> RpcError {
    match error {
        TransportPublishError::InvalidScope(message) => RpcError::Unauthorized(message),
        TransportPublishError::InvalidSignedEvent(message) => RpcError::InvalidParams(message),
        TransportPublishError::SignedEventVerification(_)
        | TransportPublishError::Draft(_)
        | TransportPublishError::Relay(_) => RpcError::InvalidParams(error.to_string()),
        TransportPublishError::IdempotencyConflict(_) => RpcError::Other(error.to_string()),
        other => RpcError::Other(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::module;
    use std::sync::Arc;

    use crate::app::config::{Nip46Config, TransportPublishConfig, TransportPublishNostrConfig};
    use crate::core::Radrootsd;
    use crate::core::transport_publish::{
        PublishJobVisibility, PublishPrincipalInit, generate_bearer_token, hash_bearer_token,
    };
    use crate::transport::jsonrpc::auth::{
        TransportPublishAuthorization, authorize_transport_publish_request,
    };
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};
    use jsonrpsee::server::RpcModule;
    use nostr::JsonUtil;
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::{
        RadrootsNostrMetadata, RadrootsNostrTimestamp, radroots_nostr_build_event,
    };
    use radroots_transport_nostr::RadrootsMockRelayPublishAdapter;
    use radroots_transport_publish_protocol::{
        NostrPublishTargetSourcePolicy, SignedEventWire, TransportPublishTargetPolicyName,
    };

    fn signed_event(identity: &RadrootsIdentity) -> SignedEventWire {
        let event = radroots_nostr_build_event(
            30_402,
            "{}",
            vec![vec!["d".to_owned(), "listing-1".to_owned()]],
        )
        .expect("event builder")
        .custom_created_at(RadrootsNostrTimestamp::from_secs(1_700_000_000))
        .sign_with_keys(identity.keys())
        .expect("signed event");
        serde_json::from_str(event.as_json().as_str()).expect("event wire")
    }

    fn module_with_principal_and_config(
        admin: bool,
        transport_publish_config: TransportPublishConfig,
    ) -> (RpcModule<RpcContext>, RpcContext, String, SignedEventWire) {
        let identity = RadrootsIdentity::generate();
        let signed_event = signed_event(&identity);
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let state = Radrootsd::new(
            identity.clone(),
            metadata,
            transport_publish_config,
            Nip46Config::default(),
        )
        .expect("state");
        let mut state = state;
        state.transport_publish = state
            .transport_publish
            .clone()
            .with_publisher(Arc::new(RadrootsMockRelayPublishAdapter::new()));
        let token = generate_bearer_token();
        let principal = state
            .transport_publish
            .store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: hash_bearer_token(token.as_str()),
                allowed_pubkeys: vec![identity.public_key_hex()],
                allowed_kinds: vec![30_402],
                allowed_target_policies: vec![TransportPublishTargetPolicyName::Nostr],
                allowed_explicit_transport_kinds: Vec::new(),
                allowed_nostr_source_policies: vec![
                    NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                ],
                allow_request_targets: false,
                job_visibility: if admin {
                    PublishJobVisibility::Admin
                } else {
                    PublishJobVisibility::Own
                },
                expires_at_unix: None,
            })
            .expect("principal");
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state, registry.clone());
        let mut module = module(ctx.clone(), registry).expect("module");
        module
            .extensions_mut()
            .insert(TransportPublishAuthorization::Authorized(principal));
        (module, ctx, token, signed_event)
    }

    fn module_with_principal(
        admin: bool,
    ) -> (RpcModule<RpcContext>, RpcContext, String, SignedEventWire) {
        module_with_principal_and_config(
            admin,
            TransportPublishConfig {
                nostr: TransportPublishNostrConfig {
                    daemon_default_relays: vec!["wss://relay.example.com".to_owned()],
                    ..TransportPublishNostrConfig::default()
                },
                ..TransportPublishConfig::default()
            },
        )
    }

    #[tokio::test]
    async fn publish_event_records_job_and_deduplicates_idempotency() {
        let (module, _ctx, _token, event) = module_with_principal(false);
        let request = format!(
            r#"{{
                "jsonrpc":"2.0",
                "method":"transport.publish.event",
                "params":{{
                    "event":{},
                    "target_policy":{{"kind":"nostr","source_policy":"daemon_default_only","relay_urls":[]}},
                    "delivery_policy":{{"mode":"any"}},
                    "idempotency_key":"idem-1"
                }},
                "id":1
            }}"#,
            serde_json::to_string(&event).expect("event json")
        );
        let (response, _stream) = module
            .raw_json_request(request.as_str(), 1)
            .await
            .expect("request");
        assert!(response.get().contains("\"deduplicated\":false"));
        let (response, _stream) = module
            .raw_json_request(request.as_str(), 1)
            .await
            .expect("request");
        assert!(response.get().contains("\"deduplicated\":true"));
    }

    #[tokio::test]
    async fn publish_event_rejects_principal_scope_gap() {
        let (module, _ctx, _token, _pubkey) = module_with_principal(false);
        let other_identity = RadrootsIdentity::generate();
        let event = signed_event(&other_identity);
        let request = format!(
            r#"{{
                "jsonrpc":"2.0",
                "method":"transport.publish.event",
                "params":{{
                    "event":{},
                    "target_policy":{{"kind":"nostr","source_policy":"daemon_default_only","relay_urls":[]}},
                    "delivery_policy":{{"mode":"any"}}
                }},
                "id":1
            }}"#,
            serde_json::to_string(&event).expect("event json")
        );
        let (response, _stream) = module
            .raw_json_request(request.as_str(), 1)
            .await
            .expect("request");
        assert!(response.get().contains("unauthorized"));
    }

    #[tokio::test]
    async fn publish_job_list_rejects_malformed_and_zero_limits() {
        let (module, _ctx, _token, _event) = module_with_principal(false);
        let malformed = r#"{
            "jsonrpc":"2.0",
            "method":"transport.publish.job.list",
            "params":"bad",
            "id":1
        }"#;
        let (response, _stream) = module
            .raw_json_request(malformed, 1)
            .await
            .expect("malformed request");
        assert!(response.get().contains("\"code\":-32602"));

        let zero = r#"{
            "jsonrpc":"2.0",
            "method":"transport.publish.job.list",
            "params":{"limit":0},
            "id":1
        }"#;
        let (response, _stream) = module
            .raw_json_request(zero, 1)
            .await
            .expect("zero request");
        assert!(response.get().contains("\"code\":-32602"));
        assert!(response.get().contains("limit must be greater than zero"));
    }

    #[tokio::test]
    async fn publish_job_list_rejects_unknown_fields() {
        let (module, _ctx, _token, _event) = module_with_principal(false);
        for params in [
            r#"{"cursor":"next"}"#,
            r#"{"status":"publishing"}"#,
            r#"{"limit":1,"extra":true}"#,
        ] {
            let request = format!(
                r#"{{
                    "jsonrpc":"2.0",
                    "method":"transport.publish.job.list",
                    "params":{params},
                    "id":1
                }}"#
            );
            let (response, _stream) = module
                .raw_json_request(request.as_str(), 1)
                .await
                .expect("unknown field request");
            assert!(
                response.get().contains("\"code\":-32602"),
                "{}",
                response.get()
            );
        }
    }

    #[tokio::test]
    async fn publish_job_list_uses_configured_limit_when_omitted_and_caps_positive_limits() {
        let mut config = TransportPublishConfig {
            nostr: TransportPublishNostrConfig {
                daemon_default_relays: vec!["wss://relay.example.com".to_owned()],
                ..TransportPublishNostrConfig::default()
            },
            ..TransportPublishConfig::default()
        };
        config.job_list_limit = 1;
        let (module, _ctx, _token, event) = module_with_principal_and_config(false, config);
        for idempotency_key in ["idem-list-1", "idem-list-2"] {
            let request = format!(
                r#"{{
                    "jsonrpc":"2.0",
                    "method":"transport.publish.event",
                    "params":{{
                        "event":{},
                    "target_policy":{{"kind":"nostr","source_policy":"daemon_default_only","relay_urls":[]}},
                        "delivery_policy":{{"mode":"any"}},
                        "idempotency_key":"{idempotency_key}"
                    }},
                    "id":1
                }}"#,
                serde_json::to_string(&event).expect("event json")
            );
            let (response, _stream) = module
                .raw_json_request(request.as_str(), 1)
                .await
                .expect("publish request");
            assert!(response.get().contains("\"deduplicated\":false"));
        }

        let omitted = r#"{
            "jsonrpc":"2.0",
            "method":"transport.publish.job.list",
            "id":1
        }"#;
        let (response, _stream) = module
            .raw_json_request(omitted, 1)
            .await
            .expect("omitted request");
        let value: serde_json::Value =
            serde_json::from_str(response.get()).expect("omitted response json");
        assert_eq!(value["result"].as_array().expect("jobs").len(), 1);

        let empty_array = r#"{
            "jsonrpc":"2.0",
            "method":"transport.publish.job.list",
            "params":[],
            "id":1
        }"#;
        let (response, _stream) = module
            .raw_json_request(empty_array, 1)
            .await
            .expect("empty array request");
        let value: serde_json::Value =
            serde_json::from_str(response.get()).expect("empty array response json");
        assert_eq!(value["result"].as_array().expect("jobs").len(), 1);

        let over_limit = r#"{
            "jsonrpc":"2.0",
            "method":"transport.publish.job.list",
            "params":{"limit":50},
            "id":1
        }"#;
        let (response, _stream) = module
            .raw_json_request(over_limit, 1)
            .await
            .expect("over limit request");
        let value: serde_json::Value =
            serde_json::from_str(response.get()).expect("over limit response json");
        assert_eq!(value["result"].as_array().expect("jobs").len(), 1);
    }

    #[test]
    fn http_auth_finds_principal_from_hashed_token() {
        let (_module, ctx, token, _pubkey) = module_with_principal(false);
        let header = format!("Bearer {token}");
        let auth = authorize_transport_publish_request(
            Some(header.as_str()),
            &ctx.state.transport_publish.store,
        );
        assert!(matches!(auth, TransportPublishAuthorization::Authorized(_)));
    }
}
