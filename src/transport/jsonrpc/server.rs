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
use crate::core::transport_publish::TransportPublishStore;
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
            if notification.method_name().starts_with("transport.publish.") {
                MethodResponse::error(
                    Id::Null,
                    ErrorObject::owned(
                        -32600,
                        "transport publish notifications are not accepted",
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
    use crate::app::config::{
        Nip46Config, NostrRelayUrlPolicy, RpcConfig, TransportPublishConfig,
        TransportPublishNostrConfig,
    };
    use crate::core::Radrootsd;
    use crate::core::transport_publish::{
        PublishJobVisibility, PublishPrincipalInit, PublishRelayResolveFuture,
        PublishRelayResolver, generate_bearer_token, hash_bearer_token,
    };
    use crate::transport::jsonrpc::methods;
    use crate::transport::jsonrpc::{MethodRegistry, RpcContext};
    use jsonrpsee::server::RpcModule;
    use nostr::JsonUtil;
    use radroots_identity::RadrootsIdentity;
    use radroots_nostr::prelude::{
        RadrootsNostrMetadata, RadrootsNostrTimestamp, radroots_nostr_build_event,
    };
    use radroots_transport_nostr::RadrootsMockRelayPublishAdapter;
    use radroots_transport_publish_protocol::{
        NostrPublishTargetSourcePolicy, TransportPublishTargetPolicyName,
    };
    use serde_json::Value;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const RELAY_PRIMARY: &str = "ws://localhost:7777";
    const RELAY_PUBLIC: &str = "wss://relay.example.com";

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

    fn publish_server_state_with_config(
        transport_publish_config: TransportPublishConfig,
        resolver: Option<Arc<dyn PublishRelayResolver>>,
    ) -> (
        Radrootsd,
        String,
        RadrootsIdentity,
        RadrootsMockRelayPublishAdapter,
    ) {
        let identity = RadrootsIdentity::generate();
        let metadata: RadrootsNostrMetadata =
            serde_json::from_str(r#"{"name":"radrootsd-test"}"#).expect("metadata");
        let mut state = Radrootsd::new(
            identity.clone(),
            metadata,
            transport_publish_config,
            Nip46Config::default(),
        )
        .expect("state");
        let adapter = RadrootsMockRelayPublishAdapter::new();
        let mut transport_publish = state.transport_publish.clone();
        if let Some(resolver) = resolver {
            transport_publish = transport_publish.with_relay_resolver(resolver);
        }
        state.transport_publish = transport_publish.with_publisher(Arc::new(adapter.clone()));
        let token = generate_bearer_token();
        state
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
                job_visibility: PublishJobVisibility::Own,
                expires_at_unix: None,
            })
            .expect("principal");
        (state, token, identity, adapter)
    }

    fn publish_server_state() -> (
        Radrootsd,
        String,
        RadrootsIdentity,
        RadrootsMockRelayPublishAdapter,
    ) {
        publish_server_state_with_config(
            TransportPublishConfig {
                nostr: TransportPublishNostrConfig {
                    daemon_default_relays: vec![RELAY_PRIMARY.to_owned()],
                    relay_url_policy: NostrRelayUrlPolicy::Localhost,
                    ..TransportPublishNostrConfig::default()
                },
                ..TransportPublishConfig::default()
            },
            None,
        )
    }

    struct StaticPublishRelayResolver {
        addresses: Vec<IpAddr>,
    }

    impl StaticPublishRelayResolver {
        fn forbidden_localhost() -> Self {
            Self {
                addresses: vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))],
            }
        }
    }

    impl PublishRelayResolver for StaticPublishRelayResolver {
        fn resolve<'a>(
            &'a self,
            _url: &'a radroots_transport_nostr::RadrootsRelayUrl,
        ) -> PublishRelayResolveFuture<'a> {
            Box::pin(async move { Ok(self.addresses.clone()) })
        }
    }

    async fn start_publish_server(
        state: Radrootsd,
        rpc_cfg: RpcConfig,
    ) -> (SocketAddr, jsonrpsee::server::ServerHandle) {
        let addr = unused_addr();
        let store = state.transport_publish.store.clone();
        let registry = MethodRegistry::default();
        let ctx = RpcContext::new(state, registry.clone());
        let mut root = RpcModule::new(ctx.clone());
        methods::register_all(&mut root, ctx, registry).expect("register methods");
        let handle = start_server(addr, &rpc_cfg, store, root)
            .await
            .expect("start server");
        (addr, handle)
    }

    fn json_response_body(response: &str) -> Value {
        let (_headers, body) = response.split_once("\r\n\r\n").expect("http body");
        serde_json::from_str(body).expect("json response body")
    }

    #[tokio::test]
    async fn raw_http_publish_event_get_and_list_preserve_signed_event() {
        let (state, token, identity, adapter) = publish_server_state();
        let event_json = signed_event_json(&identity);
        let (addr, handle) = start_publish_server(state, RpcConfig::default()).await;
        let publish = format!(
            r#"{{
                "jsonrpc":"2.0",
                "method":"transport.publish.event",
                "params":{{
                    "raw_event_json":{},
                    "target_policy":{{"kind":"nostr","source_policy":"daemon_default_only","relay_urls":[]}},
                    "delivery_policy":{{"mode":"any"}},
                    "idempotency_key":"raw-http-idem"
                }},
                "id":1
            }}"#,
            serde_json::to_string(&event_json).expect("raw event param")
        );
        let publish_response = post_json(addr, publish.as_str(), Some(token.as_str())).await;
        let publish_value = json_response_body(publish_response.as_str());
        let job_id = publish_value["result"]["job"]["job_id"]
            .as_str()
            .expect("job id")
            .to_owned();
        assert_eq!(publish_value["result"]["deduplicated"], false);
        assert_eq!(
            publish_value["result"]["job"]["status"],
            "delivery_satisfied"
        );

        let get = format!(
            r#"{{
                "jsonrpc":"2.0",
                "method":"transport.publish.job.get",
                "params":{{"job_id":"{job_id}"}},
                "id":2
            }}"#
        );
        let get_response = post_json(addr, get.as_str(), Some(token.as_str())).await;
        let get_value = json_response_body(get_response.as_str());
        assert_eq!(get_value["result"]["job_id"], job_id);
        assert_eq!(get_value["result"]["status"], "delivery_satisfied");

        let list = r#"{
            "jsonrpc":"2.0",
            "method":"transport.publish.job.list",
            "params":{"limit":10},
            "id":3
        }"#;
        let list_response = post_json(addr, list, Some(token.as_str())).await;
        let list_value = json_response_body(list_response.as_str());
        let jobs = list_value["result"].as_array().expect("jobs");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["job_id"], job_id);
        handle.stop().expect("stop server");

        assert_eq!(adapter.captured_raw_events(), vec![event_json]);
    }

    #[tokio::test]
    async fn raw_http_publish_event_rejects_public_relay_forbidden_dns_destination() {
        let (state, token, identity, adapter) = publish_server_state_with_config(
            TransportPublishConfig {
                nostr: TransportPublishNostrConfig {
                    daemon_default_relays: vec![RELAY_PUBLIC.to_owned()],
                    relay_url_policy: NostrRelayUrlPolicy::Public,
                    ..TransportPublishNostrConfig::default()
                },
                ..TransportPublishConfig::default()
            },
            Some(Arc::new(StaticPublishRelayResolver::forbidden_localhost())),
        );
        let event_json = signed_event_json(&identity);
        let (addr, handle) = start_publish_server(state, RpcConfig::default()).await;
        let publish = format!(
            r#"{{
                "jsonrpc":"2.0",
                "method":"transport.publish.event",
                "params":{{
                    "raw_event_json":{},
                    "target_policy":{{"kind":"nostr","source_policy":"daemon_default_only","relay_urls":[]}},
                    "delivery_policy":{{"mode":"any"}},
                    "idempotency_key":"raw-http-public-dns-reject"
                }},
                "id":1
            }}"#,
            serde_json::to_string(&event_json).expect("raw event param")
        );
        let publish_response = post_json(addr, publish.as_str(), Some(token.as_str())).await;
        handle.stop().expect("stop server");

        let publish_value = json_response_body(publish_response.as_str());
        let job = &publish_value["result"]["job"];
        assert_eq!(publish_value["result"]["deduplicated"], false);
        assert_eq!(job["status"], "delivery_unsatisfied_terminal");
        assert_eq!(job["last_error"], "delivery_unsatisfied");
        let targets = job["targets"].as_array().expect("target outcomes");
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0]["endpoint_uri"], RELAY_PUBLIC);
        assert_eq!(targets[0]["source"], "daemon_default");
        assert_eq!(targets[0]["outcome_kind"], "target_rejected");
        assert_eq!(targets[0]["attempted"], false);
        assert!(adapter.captured_raw_events().is_empty());
    }

    #[tokio::test]
    async fn publish_notifications_do_not_create_jobs() {
        let (state, token, identity, _adapter) = publish_server_state();
        let store = state.transport_publish.store.clone();
        let (addr, handle) = start_publish_server(state, RpcConfig::default()).await;
        let notification = format!(
            r#"{{
                "jsonrpc":"2.0",
                "method":"transport.publish.event",
                "params":{{
                    "raw_event_json":{},
                    "target_policy":{{"kind":"nostr","source_policy":"daemon_default_only","relay_urls":[]}},
                    "delivery_policy":{{"mode":"any"}}
                }}
            }}"#,
            serde_json::to_string(&signed_event_json(&identity)).expect("raw event param")
        );
        let response = post_json(addr, notification.as_str(), Some(token.as_str())).await;
        handle.stop().expect("stop server");

        assert!(
            response.contains("transport publish notifications are not accepted")
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
        let (state, token, identity, _adapter) = publish_server_state();
        let store = state.transport_publish.store.clone();
        let (addr, handle) = start_publish_server(state, RpcConfig::default()).await;
        let batch = format!(
            r#"[{{
                "jsonrpc":"2.0",
                "method":"transport.publish.event",
                "params":{{
                    "raw_event_json":{},
                    "target_policy":{{"kind":"nostr","source_policy":"daemon_default_only","relay_urls":[]}},
                    "delivery_policy":{{"mode":"any"}}
                }},
                "id":1
            }}]"#,
            serde_json::to_string(&signed_event_json(&identity)).expect("raw event param")
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
    transport_publish_store: TransportPublishStore,
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
                let transport_publish_auth = auth::authorize_transport_publish_request(
                    request
                        .headers()
                        .get("authorization")
                        .and_then(|value| value.to_str().ok()),
                    &transport_publish_store,
                );
                request.extensions_mut().insert(transport_publish_auth);
                request
            },
        ))
        .build(addr)
        .await?;
    Ok(server.start(root))
}
