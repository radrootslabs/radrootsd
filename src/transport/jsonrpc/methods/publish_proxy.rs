use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_publish_proxy_protocol::{
    METHOD_CAPABILITIES, METHOD_EVENT, METHOD_JOB_GET, METHOD_JOB_LIST, METHOD_RELAYS_RESOLVE,
    PublishCapabilities, PublishDeliveryPolicy, PublishEventRequest, PublishRelayOutcome,
    PublishRelaySource,
};
use serde::{Deserialize, Serialize};

use crate::core::publish_proxy::PublishProxyError;
use crate::transport::jsonrpc::auth::require_publish_principal;
use crate::transport::jsonrpc::{MethodRegistry, RpcContext, RpcError};

#[derive(Debug, Deserialize)]
struct JobGetParams {
    job_id: String,
}

#[derive(Debug, Deserialize)]
struct JobListParams {
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RelaysResolveParams {
    event: radroots_publish_proxy_protocol::SignedNostrEventWire,
    relay_policy: radroots_publish_proxy_protocol::PublishRelayPolicy,
    #[serde(default)]
    relays: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct RelaysResolveResponse {
    relays: Vec<ResolvedRelayResponseItem>,
    rejected_relays: Vec<PublishRelayOutcome>,
}

#[derive(Clone, Debug, Serialize)]
struct ResolvedRelayResponseItem {
    relay_url: String,
    source: PublishRelaySource,
}

pub fn module(ctx: RpcContext, registry: MethodRegistry) -> Result<RpcModule<RpcContext>> {
    let mut module = RpcModule::new(ctx);
    register_capabilities(&mut module, &registry)?;
    register_event(&mut module, &registry)?;
    register_job_get(&mut module, &registry)?;
    register_job_list(&mut module, &registry)?;
    register_relays_resolve(&mut module, &registry)?;
    Ok(module)
}

fn register_capabilities(
    module: &mut RpcModule<RpcContext>,
    registry: &MethodRegistry,
) -> Result<()> {
    registry.track(METHOD_CAPABILITIES);
    module.register_async_method(METHOD_CAPABILITIES, |_params, ctx, extensions| async move {
        require_publish_principal(&extensions)?;
        Ok::<PublishCapabilities, RpcError>(PublishCapabilities::v1(
            ctx.state.publish_proxy.config.max_event_bytes,
            ctx.state.publish_proxy.config.max_relays_per_request,
        ))
    })?;
    Ok(())
}

fn register_event(module: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track(METHOD_EVENT);
    module.register_async_method(METHOD_EVENT, |params, ctx, extensions| async move {
        let principal = require_publish_principal(&extensions)?;
        let request: PublishEventRequest = params
            .parse()
            .map_err(|error| RpcError::InvalidParams(error.to_string()))?;
        ctx.state
            .publish_proxy
            .publish_event(&principal, request)
            .await
            .map_err(rpc_error_from_publish_proxy)
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
            .publish_proxy
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
        let params = params
            .parse::<JobListParams>()
            .unwrap_or(JobListParams { limit: None });
        let configured_limit = ctx.state.publish_proxy.config.job_list_limit;
        let limit = params
            .limit
            .unwrap_or(configured_limit)
            .min(configured_limit);
        ctx.state
            .publish_proxy
            .store
            .list_jobs_for_principal(&principal, limit)
            .map_err(|error| RpcError::Other(error.to_string()))
    })?;
    Ok(())
}

fn register_relays_resolve(
    module: &mut RpcModule<RpcContext>,
    registry: &MethodRegistry,
) -> Result<()> {
    registry.track(METHOD_RELAYS_RESOLVE);
    module.register_async_method(
        METHOD_RELAYS_RESOLVE,
        |params, ctx, extensions| async move {
            let principal = require_publish_principal(&extensions)?;
            let params: RelaysResolveParams = params
                .parse()
                .map_err(|error| RpcError::InvalidParams(error.to_string()))?;
            params
                .event
                .validate()
                .map_err(|error| RpcError::InvalidParams(error.to_string()))?;
            let request = PublishEventRequest {
                event: params.event,
                relays: params.relays,
                relay_policy: params.relay_policy,
                delivery_policy: PublishDeliveryPolicy::Any,
                idempotency_key: None,
                timeout_ms: None,
            };
            principal
                .allows_event(&request)
                .map_err(|error| RpcError::Unauthorized(error.to_string()))?;
            let resolution = ctx
                .state
                .publish_proxy
                .resolve_relays_for_request(request.event.pubkey.as_str(), &request)
                .await
                .map_err(rpc_error_from_publish_proxy)?;
            Ok::<RelaysResolveResponse, RpcError>(RelaysResolveResponse {
                relays: resolution
                    .targets
                    .into_iter()
                    .map(|target| ResolvedRelayResponseItem {
                        relay_url: target.url.into_string(),
                        source: target.source,
                    })
                    .collect(),
                rejected_relays: resolution.outcomes,
            })
        },
    )?;
    Ok(())
}

fn rpc_error_from_publish_proxy(error: PublishProxyError) -> RpcError {
    match error {
        PublishProxyError::InvalidScope(message) => RpcError::Unauthorized(message),
        PublishProxyError::InvalidSignedEvent(message) => RpcError::InvalidParams(message),
        PublishProxyError::SignedEventVerification(_)
        | PublishProxyError::Draft(_)
        | PublishProxyError::Relay(_) => RpcError::InvalidParams(error.to_string()),
        PublishProxyError::IdempotencyConflict(_) => RpcError::Other(error.to_string()),
        other => RpcError::Other(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::module;
    use std::sync::Arc;

    use crate::app::config::{Nip46Config, PublishProxyConfig};
    use crate::core::Radrootsd;
    use crate::core::publish_proxy::{
        PublishJobVisibility, PublishPrincipalInit, generate_bearer_token, hash_bearer_token,
    };
    use crate::transport::jsonrpc::auth::{
        PublishProxyAuthorization, authorize_publish_proxy_request,
    };
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};
    use jsonrpsee::server::RpcModule;
    use nostr::JsonUtil;
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::{
        RadrootsNostrMetadata, RadrootsNostrTimestamp, radroots_nostr_build_event,
    };
    use radroots_publish_proxy_protocol::{PublishRelayPolicy, SignedNostrEventWire};
    use radroots_relay_transport::RadrootsMockRelayPublishAdapter;

    fn signed_event(identity: &RadrootsIdentity) -> SignedNostrEventWire {
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

    fn module_with_principal(
        admin: bool,
    ) -> (
        RpcModule<RpcContext>,
        RpcContext,
        String,
        SignedNostrEventWire,
    ) {
        let identity = RadrootsIdentity::generate();
        let signed_event = signed_event(&identity);
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let publish_proxy_config = PublishProxyConfig {
            daemon_default_publish_relays: vec!["wss://relay.example.com".to_owned()],
            ..PublishProxyConfig::default()
        };
        let state = Radrootsd::new(
            identity.clone(),
            metadata,
            publish_proxy_config,
            Nip46Config::default(),
        )
        .expect("state");
        let mut state = state;
        state.publish_proxy = state
            .publish_proxy
            .clone()
            .with_publisher(Arc::new(RadrootsMockRelayPublishAdapter::new()));
        let token = generate_bearer_token();
        let principal = state
            .publish_proxy
            .store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: hash_bearer_token(token.as_str()),
                allowed_pubkeys: vec![identity.public_key_hex()],
                allowed_kinds: vec![30_402],
                allowed_relay_policies: vec![PublishRelayPolicy::DaemonDefaultOnly],
                allow_request_relays: false,
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
            .insert(PublishProxyAuthorization::Authorized(principal));
        (module, ctx, token, signed_event)
    }

    #[tokio::test]
    async fn publish_event_records_job_and_deduplicates_idempotency() {
        let (module, _ctx, _token, event) = module_with_principal(false);
        let request = format!(
            r#"{{
                "jsonrpc":"2.0",
                "method":"publish.event",
                "params":{{
                    "event":{},
                    "relays":[],
                    "relay_policy":"daemon_default_only",
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
                "method":"publish.event",
                "params":{{
                    "event":{},
                    "relays":[],
                    "relay_policy":"daemon_default_only",
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
    async fn publish_relays_resolve_returns_daemon_default_targets() {
        let (module, _ctx, _token, event) = module_with_principal(false);
        let request = format!(
            r#"{{
                "jsonrpc":"2.0",
                "method":"publish.relays.resolve",
                "params":{{
                    "event":{},
                    "relay_policy":"daemon_default_only",
                    "relays":[]
                }},
                "id":1
            }}"#,
            serde_json::to_string(&event).expect("event json")
        );
        let (response, _stream) = module
            .raw_json_request(request.as_str(), 1)
            .await
            .expect("request");
        assert!(
            response
                .get()
                .contains("\"relay_url\":\"wss://relay.example.com\"")
        );
        assert!(response.get().contains("\"source\":\"daemon_default\""));
    }

    #[test]
    fn http_auth_finds_principal_from_hashed_token() {
        let (_module, ctx, token, _pubkey) = module_with_principal(false);
        let header = format!("Bearer {token}");
        let auth =
            authorize_publish_proxy_request(Some(header.as_str()), &ctx.state.publish_proxy.store);
        assert!(matches!(auth, PublishProxyAuthorization::Authorized(_)));
    }
}
