#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

pub mod nip46;
pub mod publish_proxy;

pub fn register_all(
    root: &mut RpcModule<RpcContext>,
    ctx: RpcContext,
    registry: MethodRegistry,
) -> Result<()> {
    if ctx.state.publish_proxy.config.enabled {
        root.merge(publish_proxy::module(ctx.clone(), registry.clone())?)?;
    }
    if ctx.state.nip46_config.public_jsonrpc_enabled {
        root.merge(nip46::module(ctx, registry)?)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use jsonrpsee::server::RpcModule;
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::RadrootsNostrMetadata;

    use super::register_all;
    use crate::app::config::{Nip46Config, PublishProxyConfig};
    use crate::core::Radrootsd;
    use crate::transport::jsonrpc::auth::PublishProxyAuthorization;
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

    mod removed_surface_fixtures {
        pub const BRIDGE_STATUS_METHOD: &str = "bridge.status";
    }

    fn state(publish_proxy_enabled: bool, nip46_public_jsonrpc_enabled: bool) -> Radrootsd {
        let identity = RadrootsIdentity::generate();
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let publish_proxy = PublishProxyConfig {
            enabled: publish_proxy_enabled,
            ..PublishProxyConfig::default()
        };
        let nip46 = Nip46Config {
            public_jsonrpc_enabled: nip46_public_jsonrpc_enabled,
            ..Nip46Config::default()
        };
        Radrootsd::new(identity, metadata, publish_proxy, nip46).expect("state")
    }

    #[test]
    fn register_all_exposes_publish_proxy_methods_by_default() {
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state(true, false), registry.clone());
        let mut root = RpcModule::new(ctx.clone());
        register_all(&mut root, ctx, registry).expect("register");

        assert!(root.method("publish.capabilities").is_some());
        assert!(root.method("publish.event").is_some());
        assert!(root.method("publish.job.get").is_some());
        assert!(root.method("publish.job.list").is_some());
        assert!(root.method("publish.relays.resolve").is_some());
        assert!(
            root.method(removed_surface_fixtures::BRIDGE_STATUS_METHOD)
                .is_none()
        );
        assert!(root.method("nip46.connect").is_none());
    }

    #[test]
    fn register_all_exposes_nip46_when_public_jsonrpc_is_enabled() {
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state(true, true), registry.clone());
        let mut root = RpcModule::new(ctx.clone());
        register_all(&mut root, ctx, registry).expect("register");

        assert!(root.method("publish.capabilities").is_some());
        assert!(root.method("nip46.connect").is_some());
    }

    #[tokio::test]
    async fn publish_capabilities_rejects_unauthenticated_requests() {
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state(true, false), registry.clone());
        let mut root = RpcModule::new(ctx.clone());
        register_all(&mut root, ctx, registry).expect("register");

        let (response, _stream) = root
            .raw_json_request(
                r#"{"jsonrpc":"2.0","method":"publish.capabilities","id":1}"#,
                1,
            )
            .await
            .expect("request");
        assert!(response.get().contains("unauthorized"));
    }

    #[tokio::test]
    async fn publish_capabilities_accepts_authenticated_requests() {
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state(true, false), registry.clone());
        let principal = ctx
            .state
            .publish_proxy
            .store
            .create_principal(crate::core::publish_proxy::PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: crate::core::publish_proxy::hash_bearer_token("secret"),
                allowed_pubkeys: vec!["a".repeat(64)],
                allowed_kinds: vec![30_402],
                allowed_relay_policies: vec![
                    radroots_publish_proxy_protocol::PublishRelayPolicy::DaemonDefaultOnly,
                ],
                allow_request_relays: false,
                job_visibility: crate::core::publish_proxy::PublishJobVisibility::Own,
                expires_at_unix: None,
            })
            .expect("principal");
        let mut root = RpcModule::new(ctx.clone());
        root.extensions_mut()
            .insert(PublishProxyAuthorization::Authorized(principal));
        register_all(&mut root, ctx, registry).expect("register");

        let (response, _stream) = root
            .raw_json_request(
                r#"{"jsonrpc":"2.0","method":"publish.capabilities","id":1}"#,
                1,
            )
            .await
            .expect("request");
        assert!(response.get().contains("\"scoped_bearer_token\""));
        assert!(response.get().contains("\"signed_event_ingress\":true"));
    }
}
