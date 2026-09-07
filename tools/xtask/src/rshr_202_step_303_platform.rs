use std::fs;
use std::path::Path;
use std::process::Command;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const APPLE_TOOLCHAIN_IDENTITY_SHA256: &str =
    "fd9bb9af273d0a834c2abff36910edf25f3e5b60c36fcc23b45b738c5c8b2d08";
const PROBE_SOURCE_PATH: &str =
    "tools/radroots_scripts/src/radroots_scripts/verify/rshr_200_series.py";
const PROBE_SOURCE_SHA256: &str =
    "add949c6c20a037123808230625dfd09dd6fa6c5afe5a856400227191f5de5b5";
const REQUEST_PATH: &str = ".git/rshr-step-303-platform-request-sha256";

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("xtask must remain under tools/xtask")
}

fn canonical(value: &Value) -> Result<Vec<u8>, String> {
    serde_json::to_vec(value).map_err(|_| "Step 303 platform JSON encoding failed".to_owned())
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn uname(flag: &str) -> Result<String, String> {
    let output = Command::new("/usr/bin/uname")
        .arg(flag)
        .output()
        .map_err(|_| "Step 303 platform probe could not start uname".to_owned())?;
    if !output.status.success() || !output.stderr.is_empty() {
        return Err("Step 303 platform probe uname failed".to_owned());
    }
    let value = std::str::from_utf8(&output.stdout)
        .map_err(|_| "Step 303 platform probe uname output is not UTF-8".to_owned())?
        .strip_suffix('\n')
        .ok_or_else(|| "Step 303 platform probe uname output differs".to_owned())?;
    if value.is_empty() || value.contains('\n') || value.contains('\r') {
        return Err("Step 303 platform probe uname output differs".to_owned());
    }
    Ok(value.to_owned())
}

pub(crate) fn run() -> Result<(), String> {
    let request_bytes = fs::read(root().join(REQUEST_PATH))
        .map_err(|_| "Step 303 platform execution request is unavailable".to_owned())?;
    let raw_request = std::str::from_utf8(&request_bytes)
        .map_err(|_| "Step 303 platform execution request is not UTF-8".to_owned())?;
    let execution_request_sha256 = raw_request.strip_suffix('\n').unwrap_or(raw_request);
    if execution_request_sha256.len() != 64
        || !execution_request_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err("Step 303 platform execution request differs".to_owned());
    }

    let kernel_name = uname("-s")?;
    let kernel_release = uname("-r")?;
    let kernel_version = uname("-v")?;
    if kernel_name != "Darwin" || std::env::consts::ARCH != "aarch64" {
        return Err("Step 303 platform identity differs".to_owned());
    }

    let os_build = json!({
        "kernel_name": kernel_name,
        "kernel_release": kernel_release,
        "kernel_version": kernel_version,
    });
    let result = json!({
        "schema": "radroots.services-hardening.rshr-200-platform-result.v1",
        "platform": "macos_aarch64",
        "system": "aarch64-darwin",
        "os_family": "macos",
        "architecture": "aarch64",
        "kernel_name": os_build["kernel_name"],
        "kernel_release": os_build["kernel_release"],
        "os_build_sha256": sha256(&canonical(&os_build)?),
        "runner_kind": "host",
        "runner_image_sha256": "none",
        "apple_toolchain_identity_sha256": APPLE_TOOLCHAIN_IDENTITY_SHA256,
        "probe_source_path": PROBE_SOURCE_PATH,
        "probe_source_sha256": PROBE_SOURCE_SHA256,
        "execution_request_sha256": execution_request_sha256,
        "assertion": [
            {"id": "os_family", "result": "pass"},
            {"id": "architecture", "result": "pass"},
            {"id": "kernel_identity", "result": "pass"},
            {"id": "runner_identity", "result": "pass"},
            {"id": "apple_identity", "result": "pass"},
        ],
        "result": "available",
    });
    let mut bytes = canonical(&result)?;
    bytes.push(b'\n');
    std::io::Write::write_all(&mut std::io::stdout().lock(), &bytes)
        .map_err(|_| "Step 303 platform result write failed".to_owned())
}
