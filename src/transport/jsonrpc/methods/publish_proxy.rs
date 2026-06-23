use anyhow::Result;
use jsonrpsee::server::RpcModule;
use radroots_publish_proxy_protocol::{
    METHOD_CAPABILITIES, METHOD_EVENT, METHOD_JOB_GET, METHOD_JOB_LIST, METHOD_RELAYS_RESOLVE,
    PublishCapabilities, PublishEventRequest, PublishRelayOutcome,
};
use serde::{Deserialize, Serialize};

use crate::core::publish_proxy::PublishJobInsert;
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
    relays: Vec<PublishRelayOutcome>,
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
        request
            .validate(ctx.state.publish_proxy.config.max_relays_per_request)
            .map_err(|error| RpcError::InvalidParams(error.to_string()))?;
        let event_size = request.event.content.len()
            + request.event.id.len()
            + request.event.pubkey.len()
            + request.event.sig.len()
            + request
                .event
                .tags
                .iter()
                .flatten()
                .map(String::len)
                .sum::<usize>();
        if event_size > ctx.state.publish_proxy.config.max_event_bytes {
            return Err(RpcError::InvalidParams(
                "signed event exceeds publish_proxy max_event_bytes".to_owned(),
            ));
        }
        principal
            .allows_event(&request)
            .map_err(|error| RpcError::Unauthorized(error.to_string()))?;
        let idempotency_key = request.idempotency_key.clone();
        ctx.state
            .publish_proxy
            .store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id,
                idempotency_key,
                request,
            })
            .map_err(|error| RpcError::Other(error.to_string()))
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
        |params, _ctx, extensions| async move {
            require_publish_principal(&extensions)?;
            let params: RelaysResolveParams = params
                .parse()
                .map_err(|error| RpcError::InvalidParams(error.to_string()))?;
            params
                .event
                .validate()
                .map_err(|error| RpcError::InvalidParams(error.to_string()))?;
            let _ = params.relay_policy;
            let _ = params.relays;
            Ok::<RelaysResolveResponse, RpcError>(RelaysResolveResponse { relays: Vec::new() })
        },
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::module;
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
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::RadrootsNostrMetadata;
    use radroots_publish_proxy_protocol::PublishRelayPolicy;

    fn event_json(pubkey: &str) -> String {
        serde_json::json!({
            "id": "0".repeat(64),
            "pubkey": pubkey,
            "created_at": 1_700_000_000u64,
            "kind": 30402u32,
            "tags": [["d", "listing-1"]],
            "content": "{}",
            "sig": "1".repeat(128)
        })
        .to_string()
    }

    fn module_with_principal(admin: bool) -> (RpcModule<RpcContext>, RpcContext, String, String) {
        let identity = RadrootsIdentity::generate();
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let state = Radrootsd::new(
            identity,
            metadata,
            PublishProxyConfig::default(),
            Nip46Config::default(),
        )
        .expect("state");
        let token = generate_bearer_token();
        let principal = state
            .publish_proxy
            .store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: hash_bearer_token(token.as_str()),
                allowed_pubkeys: vec!["a".repeat(64)],
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
        (module, ctx, token, "a".repeat(64))
    }

    #[tokio::test]
    async fn publish_event_records_job_and_deduplicates_idempotency() {
        let (module, _ctx, _token, pubkey) = module_with_principal(false);
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
            event_json(pubkey.as_str())
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
            event_json("b".repeat(64).as_str())
        );
        let (response, _stream) = module
            .raw_json_request(request.as_str(), 1)
            .await
            .expect("request");
        assert!(response.get().contains("unauthorized"));
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
