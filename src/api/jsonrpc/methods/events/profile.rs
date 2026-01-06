use anyhow::Result;
use jsonrpsee::server::RpcModule;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::api::jsonrpc::nostr::{publish_response, PublishResponse};
use crate::api::jsonrpc::{MethodRegistry, RpcContext, RpcError};
use crate::nip46::client as nip46_client;
use radroots_events::profile::{RadrootsProfile, RadrootsProfileType};
use radroots_events_codec::profile::encode::to_wire_parts_with_profile_type;
use radroots_nostr::prelude::{radroots_nostr_build_event, radroots_nostr_send_event};

#[derive(Debug, Deserialize)]
struct PublishProfileParams {
    profile: RadrootsProfile,
    profile_type: RadrootsProfileType,
    session_id: Option<String>,
}

pub fn register(m: &mut RpcModule<RpcContext>, registry: &MethodRegistry) -> Result<()> {
    registry.track("events.profile.publish");
    m.register_async_method("events.profile.publish", |params, ctx, _| async move {
        let PublishProfileParams {
            profile,
            profile_type,
            session_id,
        } = params
            .parse()
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;

        let parts = to_wire_parts_with_profile_type(&profile, Some(profile_type))
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?;
        let content = canonicalize_json_string(&parts.content)?;
        let builder = radroots_nostr_build_event(parts.kind, content, parts.tags)
            .map_err(|e| RpcError::Other(format!("failed to build profile event: {e}")))?;

        let response = match session_id {
            Some(session_id) => publish_with_session(ctx.as_ref().clone(), session_id, builder).await?,
            None => publish_with_runtime(ctx.as_ref().clone(), builder).await?,
        };

        Ok::<PublishResponse, RpcError>(response)
    })?;

    Ok(())
}

async fn publish_with_runtime(
    ctx: RpcContext,
    builder: radroots_nostr::prelude::RadrootsNostrEventBuilder,
) -> Result<PublishResponse, RpcError> {
    let relays = ctx.state.client.relays().await;
    if relays.is_empty() {
        return Err(RpcError::NoRelays);
    }

    let output = radroots_nostr_send_event(&ctx.state.client, builder)
        .await
        .map_err(|e| RpcError::Other(format!("failed to publish metadata: {e}")))?;

    Ok(publish_response(output))
}

async fn publish_with_session(
    ctx: RpcContext,
    session_id: String,
    builder: radroots_nostr::prelude::RadrootsNostrEventBuilder,
) -> Result<PublishResponse, RpcError> {
    let session = ctx
        .state
        .nip46_sessions
        .get(&session_id)
        .await
        .ok_or_else(|| RpcError::InvalidParams("unknown session".to_string()))?;
    if session.relays.is_empty() {
        return Err(RpcError::NoRelays);
    }
    let user_pubkey = session.user_pubkey.clone().ok_or_else(|| {
        RpcError::InvalidParams("missing user pubkey; call nip46.get_public_key".to_string())
    })?;
    let unsigned = builder.build(user_pubkey);
    let signed = nip46_client::sign_event(&session, unsigned, "profile.publish").await?;
    let output = session
        .client
        .send_event(&signed)
        .await
        .map_err(|e| RpcError::Other(format!("failed to publish metadata: {e}")))?;

    Ok(publish_response(output))
}

fn canonicalize_json_string(content: &str) -> Result<String, RpcError> {
    let value: Value = serde_json::from_str(content)
        .map_err(|e| RpcError::InvalidParams(format!("invalid metadata json: {e}")))?;
    let canonical = canonicalize_value(value);
    serde_json::to_string(&canonical)
        .map_err(|e| RpcError::Other(format!("canonical json failed: {e}")))
}

fn canonicalize_value(value: Value) -> Value {
    match value {
        Value::Object(map) => canonicalize_object(map),
        Value::Array(values) => {
            let values = values
                .into_iter()
                .map(canonicalize_value)
                .collect::<Vec<_>>();
            Value::Array(values)
        }
        other => other,
    }
}

fn canonicalize_object(map: Map<String, Value>) -> Value {
    let mut entries = map.into_iter().collect::<Vec<_>>();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut ordered = Map::new();
    for (key, value) in entries {
        ordered.insert(key, canonicalize_value(value));
    }
    Value::Object(ordered)
}

#[cfg(test)]
mod tests {
    use super::canonicalize_json_string;

    #[test]
    fn canonicalize_json_string_orders_keys() {
        let input = r#"{"b":1,"a":{"d":2,"c":3}}"#;
        let canonical = canonicalize_json_string(input).expect("canonical");
        assert_eq!(canonical, r#"{"a":{"c":3,"d":2},"b":1}"#);
    }
}
