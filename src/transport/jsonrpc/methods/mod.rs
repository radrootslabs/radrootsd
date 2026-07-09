#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

pub mod nip46;
pub mod transport_publish;

pub fn register_all(
    root: &mut RpcModule<RpcContext>,
    ctx: RpcContext,
    registry: MethodRegistry,
) -> Result<()> {
    if ctx.state.transport_publish.config.enabled {
        root.merge(transport_publish::module(ctx.clone(), registry.clone())?)?;
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
    use radroots_transport::RADROOTS_RETICULUM_UNAVAILABLE_MESSAGE;

    use super::register_all;
    use crate::app::config::{Nip46Config, TransportPublishConfig};
    use crate::core::Radrootsd;
    use crate::transport::jsonrpc::auth::TransportPublishAuthorization;
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

    mod removed_surface_fixtures {
        pub const BRIDGE_STATUS_METHOD: &str = "bridge.status";
    }

    fn state(transport_publish_enabled: bool, nip46_public_jsonrpc_enabled: bool) -> Radrootsd {
        let identity = RadrootsIdentity::generate();
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let transport_publish = TransportPublishConfig {
            enabled: transport_publish_enabled,
            ..TransportPublishConfig::default()
        };
        let nip46 = Nip46Config {
            public_jsonrpc_enabled: nip46_public_jsonrpc_enabled,
            ..Nip46Config::default()
        };
        Radrootsd::new(identity, metadata, transport_publish, nip46).expect("state")
    }

    #[test]
    fn register_all_exposes_transport_publish_methods_by_default() {
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state(true, false), registry.clone());
        let mut root = RpcModule::new(ctx.clone());
        register_all(&mut root, ctx, registry).expect("register");

        assert!(root.method("transport.publish.capabilities").is_some());
        assert!(root.method("transport.publish.event").is_some());
        assert!(root.method("transport.publish.job.get").is_some());
        assert!(root.method("transport.publish.job.list").is_some());
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

        assert!(root.method("transport.publish.capabilities").is_some());
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
                r#"{"jsonrpc":"2.0","method":"transport.publish.capabilities","id":1}"#,
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
            .transport_publish
            .store
            .create_principal(crate::core::transport_publish::PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: crate::core::transport_publish::hash_bearer_token("secret"),
                allowed_pubkeys: vec!["a".repeat(64)],
                allowed_kinds: vec![30_402],
                allowed_target_policies: vec![
                    radroots_transport_publish_protocol::TransportPublishTargetPolicyName::Nostr,
                ],
                allowed_explicit_transport_kinds: Vec::new(),
                allowed_nostr_source_policies: vec![
                    radroots_transport_publish_protocol::NostrPublishTargetSourcePolicy::DaemonDefaultOnly,
                ],
                allow_request_targets: false,
                job_visibility: crate::core::transport_publish::PublishJobVisibility::Own,
                expires_at_unix: None,
            })
            .expect("principal");
        let mut root = RpcModule::new(ctx.clone());
        root.extensions_mut()
            .insert(TransportPublishAuthorization::Authorized(principal));
        register_all(&mut root, ctx, registry).expect("register");

        let (response, _stream) = root
            .raw_json_request(
                r#"{"jsonrpc":"2.0","method":"transport.publish.capabilities","id":1}"#,
                1,
            )
            .await
            .expect("request");
        assert!(response.get().contains("\"scoped_bearer_token\""));
        assert!(response.get().contains("\"signed_event_ingress\":true"));
        assert!(response.get().contains("\"transports\":["));
        assert!(
            response
                .get()
                .contains("\"api_version\":\"radrootsd.transport_publish.v3\"")
        );
        assert!(response.get().contains("\"transport\":\"reticulum\""));
        assert!(response.get().contains("\"configured\":true"));
        assert!(
            response
                .get()
                .contains("\"implementation\":\"preview_unavailable\"")
        );
        assert!(response.get().contains("\"usable_for_delivery\":false"));
        assert!(
            response
                .get()
                .contains(RADROOTS_RETICULUM_UNAVAILABLE_MESSAGE)
        );
    }
}
