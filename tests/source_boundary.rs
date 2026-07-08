use std::{
    fs,
    path::{Path, PathBuf},
};

struct ForbiddenConcept {
    pattern: &'static str,
    reason: &'static str,
}

const FORBIDDEN_DAEMON_TRANSPORT_CONCEPTS: &[ForbiddenConcept] = &[
    ForbiddenConcept {
        pattern: "\"radrootsd_proxy\"",
        reason: "proxy targets must use first-class shared proxy transport modeling",
    },
    ForbiddenConcept {
        pattern: "radrootsd.publish_proxy.v1",
        reason: "daemon publish proxy v1 is removed",
    },
    ForbiddenConcept {
        pattern: "publish.relays.resolve",
        reason: "relay-resolution RPC is replaced by transport publish target policy",
    },
    ForbiddenConcept {
        pattern: "\"publish.event\"",
        reason: "publish.event is replaced by transport.publish.event",
    },
    ForbiddenConcept {
        pattern: "\"transport_kinds\"",
        reason: "capabilities must expose per-transport readiness instead of kind-only lists",
    },
    ForbiddenConcept {
        pattern: "allowed_relay_policy",
        reason: "relay policy is Nostr-specific and must not be a generic transport API",
    },
    ForbiddenConcept {
        pattern: "relay_policy",
        reason: "relay policy is Nostr-specific and must not be a generic transport API",
    },
    ForbiddenConcept {
        pattern: "PublishRelayPolicy",
        reason: "old relay-shaped publish policy names must not return",
    },
    ForbiddenConcept {
        pattern: "PublishRelayOutcome",
        reason: "old relay-shaped publish outcome names must not return",
    },
    ForbiddenConcept {
        pattern: "PublishRelaySource",
        reason: "old relay-shaped publish source names must not return",
    },
];

#[test]
fn transport_publish_sources_reject_removed_protocol_identifiers() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let src_dir = manifest_dir.join("src");
    let mut findings = Vec::new();

    for path in rust_source_files(src_dir.as_path()) {
        let source = read_source(path.as_path());
        let relative_path = relative_path(manifest_dir, path.as_path());

        for concept in FORBIDDEN_DAEMON_TRANSPORT_CONCEPTS {
            if contains_forbidden_concept(source.as_str(), concept.pattern) {
                findings.push(format!(
                    "{} contains removed daemon transport concept `{}`: {}",
                    relative_path, concept.pattern, concept.reason
                ));
            }
        }

        for line in removed_reticulum_preview_endpoint_lines(source.as_str()) {
            findings.push(format!(
                "{relative_path}:{line} contains removed Reticulum preview endpoint `reticulum:preview`"
            ));
        }
    }

    assert!(
        findings.is_empty(),
        "daemon transport source-boundary violations:\n{}",
        findings.join("\n")
    );
}

#[test]
fn transport_publish_sources_reject_proxy_explicit_targets() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let protocol_source = read_source(
        manifest_dir
            .join("../lib/crates/transport_publish_protocol/src/lib.rs")
            .as_path(),
    );
    for required in [
        "ExplicitProxyTarget",
        "transport_kind == RadrootsTransportKind::Proxy",
        "cannot be used as a daemon explicit target",
    ] {
        assert!(
            protocol_source.contains(required),
            "transport publish protocol must retain proxy explicit-target rejection witness `{required}`"
        );
    }

    let daemon_source = read_source(manifest_dir.join("src/core/transport_publish.rs").as_path());
    assert!(
        daemon_source.contains("publish_event_rejects_proxy_target_before_recording_job"),
        "daemon transport publish tests must prove proxy explicit targets are rejected before job recording"
    );
}

#[test]
fn transport_publish_sources_require_principal_explicit_kind_scope() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let daemon_source = read_source(manifest_dir.join("src/core/transport_publish.rs").as_path());

    for required in [
        "allowed_explicit_transport_kinds_json",
        "pub allowed_explicit_transport_kinds: Vec<String>,",
        "parse_explicit_transport_kind",
        "principal must include at least one allowed explicit transport kind",
        "principal is not allowed to use explicit transport target kind",
        "publish_event_records_explicit_nostr_target_when_kind_allowed",
        "publish_event_rejects_explicit_target_kind_not_allowed_before_recording_job",
    ] {
        assert!(
            daemon_source.contains(required),
            "daemon transport publish sources must retain explicit target kind-scope witness `{required}`"
        );
    }
}

#[test]
fn transport_publish_sources_reject_runtime_principal_schema_repair() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let daemon_source = read_source(manifest_dir.join("src/core/transport_publish.rs").as_path());

    for required in [
        "validate_transport_publish_schema",
        "TransportPublishError::Schema",
        "transport_store_open_validates_current_principal_schema",
        "transport_store_open_rejects_legacy_principal_schema_without_explicit_kind_allowlist",
    ] {
        assert!(
            daemon_source.contains(required),
            "daemon transport publish source must retain strict schema validation witness `{required}`"
        );
    }

    for forbidden in [
        concat!("ensure_transport_publish", "_schema"),
        concat!("ALTER TABLE ", "transport_publish_principals"),
    ] {
        assert!(
            !daemon_source.contains(forbidden),
            "daemon transport publish source must not contain runtime schema repair `{forbidden}`"
        );
    }
}

#[test]
fn transport_publish_store_egress_requires_protocol_validation() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let daemon_source = read_source(manifest_dir.join("src/core/transport_publish.rs").as_path());

    for required in [
        "fn finalize_job_row_for_egress",
        "job.view.targets = self.target_outcomes(job.view.job_id.as_str())?;",
        "finalize_job_view(&mut job.view);",
        "job.view\n            .validate()",
        "TransportPublishError::InvalidPublishJobState",
        "store_egress_rejects_malformed_target_counts_for_get_list_and_dedupe",
        "store_egress_rejects_explicit_target_outcome_drift",
        "store_egress_rejects_recovered_explicit_target_snapshot_drift",
    ] {
        assert!(
            daemon_source.contains(required),
            "daemon transport publish store must retain validated public egress witness `{required}`"
        );
    }
}

#[test]
fn transport_publish_capabilities_expose_per_transport_readiness() {
    let methods_source = read_source(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/transport/jsonrpc/methods/mod.rs")
            .as_path(),
    );
    for required in [
        "transport.publish.capabilities",
        r#"\"transport_kind\":\"reticulum\""#,
        r#"\"implementation_state\":\"preview_unavailable\""#,
        r#"\"usable_for_delivery\":false"#,
        "RADROOTS_RETICULUM_UNAVAILABLE_MESSAGE",
    ] {
        assert!(
            methods_source.contains(required),
            "daemon capabilities tests must retain transport readiness witness `{required}`"
        );
    }

    let protocol_source = read_source(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../lib/crates/transport_publish_protocol/src/lib.rs")
            .as_path(),
    );
    for required in [
        "pub implementation_state: TransportPublishImplementationState,",
        "pub usable_for_delivery: bool,",
        "RADROOTS_RETICULUM_UNAVAILABLE_MESSAGE",
    ] {
        assert!(
            protocol_source.contains(required),
            "transport publish protocol must retain capability field `{required}`"
        );
    }
}

fn rust_source_files(root: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    collect_rust_source_files(root, &mut paths);
    paths.sort();
    paths
}

fn collect_rust_source_files(root: &Path, paths: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", root.display()))
    {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rust_source_files(path.as_path(), paths);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            paths.push(path);
        }
    }
}

fn read_source(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read source {}: {error}", path.display()))
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .expect("source path is under repo root")
        .to_string_lossy()
        .replace('\\', "/")
}

fn contains_forbidden_concept(source: &str, pattern: &str) -> bool {
    source.match_indices(pattern).any(|(index, _)| {
        let before = source[..index].chars().next_back();
        let after = source[index + pattern.len()..].chars().next();
        before.is_none_or(|character| !is_rust_identifier_character(character))
            && after.is_none_or(|character| !is_rust_identifier_character(character))
    })
}

fn removed_reticulum_preview_endpoint_lines(source: &str) -> Vec<usize> {
    source
        .match_indices("reticulum:preview")
        .filter_map(|(index, _)| {
            let after = source[index + "reticulum:preview".len()..].chars().next();
            (after != Some('-')).then(|| line_number(source, index))
        })
        .collect()
}

fn is_rust_identifier_character(character: char) -> bool {
    character == '_' || character.is_ascii_alphanumeric()
}

fn line_number(source: &str, index: usize) -> usize {
    source[..index]
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count()
        + 1
}
