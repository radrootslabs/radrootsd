#![forbid(unsafe_code)]

use anyhow::Result;
use jsonrpsee::server::RpcModule;

use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

pub mod bridge;
pub mod nip46;

pub fn register_all(
    root: &mut RpcModule<RpcContext>,
    ctx: RpcContext,
    registry: MethodRegistry,
) -> Result<()> {
    if ctx.state.bridge_config.enabled {
        root.merge(bridge::module(ctx.clone(), registry.clone())?)?;
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
    use crate::app::config::{BridgeConfig, Nip46Config};
    use crate::core::Radrootsd;
    use crate::transport::jsonrpc::auth::BridgeAuthorization;
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};

    fn state(bridge_enabled: bool, nip46_public_jsonrpc_enabled: bool) -> Radrootsd {
        let identity = RadrootsIdentity::generate();
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let bridge = BridgeConfig {
            enabled: bridge_enabled,
            bearer_token: Some("secret".to_string()),
            ..BridgeConfig::default()
        };
        let nip46 = Nip46Config {
            public_jsonrpc_enabled: nip46_public_jsonrpc_enabled,
            ..Nip46Config::default()
        };
        Radrootsd::new(identity, metadata, bridge, nip46).expect("state")
    }

    #[test]
    fn register_all_exposes_bridge_methods_by_default() {
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state(true, false), registry.clone());
        let mut root = RpcModule::new(ctx.clone());
        register_all(&mut root, ctx, registry).expect("register");

        assert!(root.method("bridge.status").is_some());
        assert!(root.method("bridge.job.list").is_some());
        assert!(root.method("bridge.job.status").is_some());
        assert!(root.method("bridge.profile.publish").is_some());
        assert!(root.method("bridge.farm.publish").is_some());
        assert!(root.method("bridge.listing.publish").is_some());
        assert!(root.method("bridge.order.request").is_some());
        assert!(root.method("bridge.order.response").is_some());
        assert!(root.method("bridge.order.revision").is_some());
        assert!(root.method("bridge.order.revision.accept").is_some());
        assert!(root.method("bridge.order.revision.decline").is_some());
        assert!(root.method("bridge.order.question").is_some());
        assert!(root.method("bridge.order.answer").is_some());
        assert!(root.method("bridge.order.discount.request").is_some());
        assert!(root.method("bridge.order.discount.offer").is_some());
        assert!(root.method("bridge.order.discount.accept").is_some());
        assert!(root.method("bridge.order.discount.decline").is_some());
        assert!(root.method("bridge.order.cancel").is_some());
        assert!(root.method("bridge.order.fulfillment.update").is_some());
        assert!(root.method("bridge.order.receipt").is_some());
        assert!(root.method("nip46.connect").is_none());
    }

    #[test]
    fn register_all_exposes_nip46_when_public_jsonrpc_is_enabled() {
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state(true, true), registry.clone());
        let mut root = RpcModule::new(ctx.clone());
        register_all(&mut root, ctx, registry).expect("register");

        assert!(root.method("bridge.status").is_some());
        assert!(root.method("nip46.connect").is_some());
    }

    #[tokio::test]
    async fn bridge_status_rejects_unauthenticated_requests() {
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state(true, false), registry.clone());
        let mut root = RpcModule::new(ctx.clone());
        register_all(&mut root, ctx, registry).expect("register");

        let (response, _stream) = root
            .raw_json_request(r#"{"jsonrpc":"2.0","method":"bridge.status","id":1}"#, 1)
            .await
            .expect("request");
        assert!(response.get().contains("unauthorized"));
    }

    #[tokio::test]
    async fn bridge_status_accepts_authenticated_requests() {
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state(true, false), registry.clone());
        let mut root = RpcModule::new(ctx.clone());
        root.extensions_mut()
            .insert(BridgeAuthorization::Authorized);
        register_all(&mut root, ctx, registry).expect("register");

        let (response, _stream) = root
            .raw_json_request(r#"{"jsonrpc":"2.0","method":"bridge.status","id":1}"#, 1)
            .await
            .expect("request");
        assert!(response.get().contains("\"auth_mode\":\"bearer_token\""));
        assert!(
            response
                .get()
                .contains("\"signer_mode\":\"selectable_per_request\"")
        );
        assert!(
            response
                .get()
                .contains("\"default_signer_mode\":\"embedded_service_identity\"")
        );
        assert!(response.get().contains(
            "\"supported_signer_modes\":[\"embedded_service_identity\",\"nip46_session\"]"
        ));
        assert!(
            response
                .get()
                .contains("\"available_nip46_signer_sessions\":0")
        );
        assert!(response.get().contains("\"accepted_jobs\":0"));
        assert!(response.get().contains("\"published_jobs\":0"));
        assert!(response.get().contains("\"failed_jobs\":0"));
        assert!(response.get().contains("\"recovered_failed_jobs\":0"));
    }

    #[tokio::test]
    async fn bridge_job_list_accepts_authenticated_requests() {
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state(true, false), registry.clone());
        let mut root = RpcModule::new(ctx.clone());
        root.extensions_mut()
            .insert(BridgeAuthorization::Authorized);
        register_all(&mut root, ctx, registry).expect("register");

        let (response, _stream) = root
            .raw_json_request(r#"{"jsonrpc":"2.0","method":"bridge.job.list","id":1}"#, 1)
            .await
            .expect("request");
        assert!(response.get().contains("\"result\":[]"));
    }
}
