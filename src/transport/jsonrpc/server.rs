#![forbid(unsafe_code)]

use std::net::SocketAddr;

use anyhow::Result;
use jsonrpsee::server::middleware::rpc::{
    Batch, MethodResponse, Notification, Request, RpcServiceBuilder, RpcServiceT,
};
use jsonrpsee::server::{
    BatchRequestConfig, HttpBody, HttpRequest, RpcModule, ServerBuilder, ServerConfigBuilder,
    ServerHandle,
};
use jsonrpsee::types::{ErrorObject, Id};

use crate::app::config::RpcConfig;
use crate::core::publish_proxy::PublishProxyStore;
use crate::transport::jsonrpc::RpcContext;
use crate::transport::jsonrpc::auth;

#[derive(Clone)]
struct RejectPublishNotifications<S> {
    service: S,
}

impl<S> RpcServiceT for RejectPublishNotifications<S>
where
    S: RpcServiceT<
            MethodResponse = MethodResponse,
            NotificationResponse = MethodResponse,
            BatchResponse = MethodResponse,
        > + Clone
        + Send
        + Sync
        + 'static,
{
    type MethodResponse = MethodResponse;
    type NotificationResponse = MethodResponse;
    type BatchResponse = MethodResponse;

    fn call<'a>(
        &self,
        request: Request<'a>,
    ) -> impl Future<Output = Self::MethodResponse> + Send + 'a {
        self.service.call(request)
    }

    fn batch<'a>(
        &self,
        requests: Batch<'a>,
    ) -> impl Future<Output = Self::BatchResponse> + Send + 'a {
        self.service.batch(requests)
    }

    fn notification<'a>(
        &self,
        notification: Notification<'a>,
    ) -> impl Future<Output = Self::NotificationResponse> + Send + 'a {
        let service = self.service.clone();
        async move {
            if notification.method_name().starts_with("publish.") {
                MethodResponse::error(
                    Id::Null,
                    ErrorObject::owned(
                        -32600,
                        "publish notifications are not accepted",
                        None::<()>,
                    ),
                )
            } else {
                service.notification(notification).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::start_server;
    use crate::app::config::{Nip46Config, PublishProxyConfig, RpcConfig};
    use crate::core::Radrootsd;
    use crate::core::publish_proxy::{
        PublishJobVisibility, PublishPrincipalInit, generate_bearer_token, hash_bearer_token,
    };
    use crate::transport::jsonrpc::methods;
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};
    use jsonrpsee::server::RpcModule;
    use nostr::JsonUtil;
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::{
        RadrootsNostrMetadata, RadrootsNostrTimestamp, radroots_nostr_build_event,
    };
    use radroots_publish_proxy_protocol::PublishRelayPolicy;
    use radroots_relay_transport::RadrootsMockRelayPublishAdapter;
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const RELAY_PRIMARY: &str = "wss://relay.example.com";

    fn unused_addr() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local addr");
        listener.local_addr().expect("local addr")
    }

    fn signed_event_json(identity: &RadrootsIdentity) -> String {
        radroots_nostr_build_event(
            30_402,
            "{}",
            vec![vec!["d".to_owned(), "listing-1".to_owned()]],
        )
        .expect("event builder")
        .custom_created_at(RadrootsNostrTimestamp::from_secs(1_700_000_000))
        .sign_with_keys(identity.keys())
        .expect("signed event")
        .as_json()
    }

    async fn post_json(addr: SocketAddr, body: &str, token: Option<&str>) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        let auth_header = token
            .map(|token| format!("Authorization: Bearer {token}\r\n"))
            .unwrap_or_default();
        let request = format!(
            "POST / HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{auth_header}\r\n{body}",
            body.len()
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.expect("read response");
        String::from_utf8(bytes).expect("response utf8")
    }

    fn publish_server_state() -> (Radrootsd, String, RadrootsIdentity) {
        let identity = RadrootsIdentity::generate();
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let publish_proxy_config = PublishProxyConfig {
            daemon_default_publish_relays: vec![RELAY_PRIMARY.to_owned()],
            ..PublishProxyConfig::default()
        };
        let mut state = Radrootsd::new(
            identity.clone(),
            metadata,
            publish_proxy_config,
            Nip46Config::default(),
        )
        .expect("state");
        state.publish_proxy = state
            .publish_proxy
            .clone()
            .with_publisher(Arc::new(RadrootsMockRelayPublishAdapter::new()));
        let token = generate_bearer_token();
        state
            .publish_proxy
            .store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: hash_bearer_token(token.as_str()),
                allowed_pubkeys: vec![identity.public_key_hex()],
                allowed_kinds: vec![30_402],
                allowed_relay_policies: vec![PublishRelayPolicy::DaemonDefaultOnly],
                allow_request_relays: false,
                job_visibility: PublishJobVisibility::Own,
                expires_at_unix: None,
            })
            .expect("principal");
        (state, token, identity)
    }

    async fn start_publish_server(
        state: Radrootsd,
        rpc_cfg: RpcConfig,
    ) -> (SocketAddr, jsonrpsee::server::ServerHandle) {
        let addr = unused_addr();
        let store = state.publish_proxy.store.clone();
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state, registry.clone());
        let mut root = RpcModule::new(ctx.clone());
        methods::register_all(&mut root, ctx, registry).expect("register methods");
        let handle = start_server(addr, &rpc_cfg, store, root)
            .await
            .expect("start server");
        (addr, handle)
    }

    #[tokio::test]
    async fn publish_notifications_do_not_create_jobs() {
        let (state, token, identity) = publish_server_state();
        let store = state.publish_proxy.store.clone();
        let (addr, handle) = start_publish_server(state, RpcConfig::default()).await;
        let notification = format!(
            r#"{{
                "jsonrpc":"2.0",
                "method":"publish.event",
                "params":{{
                    "event":{},
                    "relays":[],
                    "relay_policy":"daemon_default_only",
                    "delivery_policy":{{"mode":"any"}}
                }}
            }}"#,
            signed_event_json(&identity)
        );
        let response = post_json(addr, notification.as_str(), Some(token.as_str())).await;
        handle.stop().expect("stop server");

        assert!(
            response.contains("publish notifications are not accepted")
                || response.ends_with("\r\n\r\n")
        );
        let principal = store
            .principal_for_token_hash(hash_bearer_token(token.as_str()).as_str())
            .expect("principal lookup")
            .expect("principal");
        assert!(
            store
                .list_jobs_for_principal(&principal, 10)
                .expect("jobs")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn batch_requests_are_disabled_by_default() {
        let (state, token, identity) = publish_server_state();
        let store = state.publish_proxy.store.clone();
        let (addr, handle) = start_publish_server(state, RpcConfig::default()).await;
        let batch = format!(
            r#"[{{
                "jsonrpc":"2.0",
                "method":"publish.event",
                "params":{{
                    "event":{},
                    "relays":[],
                    "relay_policy":"daemon_default_only",
                    "delivery_policy":{{"mode":"any"}}
                }},
                "id":1
            }}]"#,
            signed_event_json(&identity)
        );
        let response = post_json(addr, batch.as_str(), Some(token.as_str())).await;
        handle.stop().expect("stop server");

        assert!(
            response.contains("Batched requests are not supported by this server"),
            "{response}"
        );
        let principal = store
            .principal_for_token_hash(hash_bearer_token(token.as_str()).as_str())
            .expect("principal lookup")
            .expect("principal");
        assert!(
            store
                .list_jobs_for_principal(&principal, 10)
                .expect("jobs")
                .is_empty()
        );
    }
}

pub async fn start_server(
    addr: SocketAddr,
    rpc_cfg: &RpcConfig,
    publish_proxy_store: PublishProxyStore,
    root: RpcModule<RpcContext>,
) -> Result<ServerHandle> {
    let mut builder = ServerConfigBuilder::new()
        .max_request_body_size(rpc_cfg.max_request_body_size)
        .max_response_body_size(rpc_cfg.max_response_body_size)
        .max_connections(rpc_cfg.max_connections)
        .max_subscriptions_per_connection(rpc_cfg.max_subscriptions_per_connection)
        .set_message_buffer_capacity(rpc_cfg.message_buffer_capacity);

    if let Some(limit) = rpc_cfg.batch_request_limit {
        let cfg = if limit == 0 {
            BatchRequestConfig::Disabled
        } else {
            BatchRequestConfig::Limit(limit)
        };
        builder = builder.set_batch_request_config(cfg);
    }

    let server_cfg = builder.build();
    let rpc_middleware =
        RpcServiceBuilder::new().layer_fn(|service| RejectPublishNotifications { service });
    let server = ServerBuilder::with_config(server_cfg)
        .set_rpc_middleware(rpc_middleware)
        .set_http_middleware(tower::ServiceBuilder::new().map_request(
            move |mut request: HttpRequest<HttpBody>| {
                let publish_proxy_auth = auth::authorize_publish_proxy_request(
                    request
                        .headers()
                        .get("authorization")
                        .and_then(|value| value.to_str().ok()),
                    &publish_proxy_store,
                );
                request.extensions_mut().insert(publish_proxy_auth);
                request
            },
        ))
        .build(addr)
        .await?;
    Ok(server.start(root))
}
