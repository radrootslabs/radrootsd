#![forbid(unsafe_code)]

use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::process::Command;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceLock {
    schema: String,
    repository: String,
    revision: String,
    architecture: String,
    workspace_catalog_sha256: String,
    version: String,
    source_archive_sha256: String,
    lockfile: String,
    lockfile_sha256: String,
}

fn validate(lock: &str, cargo_lock: &[u8], metadata: &Value) -> Result<(), &'static str> {
    let lock: SourceLock = toml::from_str(lock).map_err(|_| "invalid source lock")?;
    if lock.schema != "radroots.lib.source-lock.v1"
        || lock.repository != "https://github.com/radrootslabs/lib"
        || lock.architecture != "radroots.crates.release.v2"
        || lock.lockfile != "Cargo.lock"
    {
        return Err("invalid source identity");
    }
    for (value, length) in [
        (&lock.revision, 40),
        (&lock.workspace_catalog_sha256, 64),
        (&lock.source_archive_sha256, 64),
        (&lock.lockfile_sha256, 64),
    ] {
        if value.len() != length
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("invalid source digest");
        }
    }
    if lock.lockfile_sha256 != format!("{:x}", Sha256::digest(cargo_lock)) {
        return Err("stale Cargo.lock digest");
    }
    let source = format!(
        "git+https://github.com/radrootslabs/lib.git?rev={}",
        lock.revision
    );
    let resolved = format!("{source}#{}", lock.revision);
    let packages = metadata["packages"].as_array().ok_or("invalid metadata")?;
    let daemon = packages
        .iter()
        .find(|p| p["name"] == "radrootsd")
        .ok_or("missing daemon")?;
    let dependencies = daemon["dependencies"]
        .as_array()
        .ok_or("missing dependencies")?;
    let mut direct = 0;
    for dependency in dependencies {
        let name = dependency["name"]
            .as_str()
            .ok_or("missing dependency name")?;
        if name.starts_with("radroots_") {
            if dependency["source"] != source
                || dependency["req"] != format!("={}", lock.version)
                || !dependency["rename"].is_null()
            {
                return Err("unlocked direct foundation dependency");
            }
            direct += 1;
        }
    }
    let mut shared = 0;
    for package in packages {
        let name = package["name"].as_str().ok_or("missing package name")?;
        if name == "radroots" || name.starts_with("radroots_") {
            if package["source"] != resolved || package["version"] != lock.version {
                return Err("unlocked resolved foundation package");
            }
            shared += 1;
        }
    }
    if direct == 0 || shared == 0 {
        return Err("missing foundation dependencies");
    }
    Ok(())
}

#[test]
fn committed_source_lock_matches_actual_lock_and_offline_resolved_graph() {
    let metadata = Command::new("cargo")
        .args(["metadata", "--locked", "--offline", "--format-version", "1"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("offline locked metadata");
    assert!(metadata.status.success(), "offline locked metadata failed");
    let metadata = serde_json::from_slice(&metadata.stdout).expect("metadata JSON");
    validate(
        include_str!("../radroots.lib.source-lock.v1.toml"),
        include_bytes!("../Cargo.lock"),
        &metadata,
    )
    .expect("exact source-lock agreement");
}

fn fixture() -> (String, Value) {
    let text = include_str!("../radroots.lib.source-lock.v1.toml");
    let lock: SourceLock = toml::from_str(text).expect("source lock");
    let source = format!(
        "git+https://github.com/radrootslabs/lib.git?rev={}",
        lock.revision
    );
    let metadata = json!({"packages": [
        {"name": "radrootsd", "dependencies": [{"name": "radroots_protocol", "source": source, "req": "=0.1.0-alpha", "rename": null}]},
        {"name": "radroots_protocol", "source": format!("{source}#{}", lock.revision), "version": "0.1.0-alpha"}
    ]});
    (
        text.replace(
            &lock.lockfile_sha256,
            &format!("{:x}", Sha256::digest(b"fixture lock")),
        ),
        metadata,
    )
}

#[test]
fn changed_lock_bytes_and_malformed_source_identity_are_rejected() {
    let (lock, metadata) = fixture();
    assert!(validate(&lock, b"fixture lock", &metadata).is_ok());
    assert_eq!(
        validate(&lock, b"different lock", &metadata),
        Err("stale Cargo.lock digest")
    );
    for changed in [
        format!("{lock}unknown = true\n"),
        format!("{lock}schema = \"duplicate\"\n"),
        lock.replace("lockfile = \"Cargo.lock\"", "lockfile = \"../Cargo.lock\""),
    ] {
        assert!(validate(&changed, b"fixture lock", &metadata).is_err());
    }
}

#[test]
fn path_or_unlocked_dependency_overrides_are_rejected() {
    let (lock, metadata) = fixture();
    for (field, value) in [
        ("source", Value::Null),
        (
            "source",
            json!("git+https://github.com/radrootslabs/lib.git?branch=master"),
        ),
        ("req", json!("^0.1")),
        ("rename", json!("hidden_dependency")),
    ] {
        let mut changed = metadata.clone();
        changed["packages"][0]["dependencies"][0][field] = value;
        assert!(validate(&lock, b"fixture lock", &changed).is_err());
    }
    for (field, value) in [("source", Value::Null), ("version", json!("0.2.0"))] {
        let mut changed = metadata.clone();
        changed["packages"][1][field] = value;
        assert!(validate(&lock, b"fixture lock", &changed).is_err());
    }
}
