use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const STEP: u16 = 303;
const GATE_DIGEST: &str = "79dc2cfbe14c07aeefba9779d61823291c7101d93fece3935909fc13f02261d0";
const LIB_REVISION: &str = "055096853fca95e15d0f813d33a14aca13be3881";
const NIX_SHA256: &str = "a59ab70f97f6d571642d13c7506aafec0a4275520d53daee2d8451be7c495cd1";
const NIX_VERSION_SHA256: &str = "6db806391ffaea4cdb08ade0031feac399c0cd08474b3bfde8cb33f88a36c8e1";
const MAX_OUTPUT_BYTES: usize = 32 * 1024 * 1024;

const EXACT_SOURCES: &[(&str, &str)] = &[
    (
        "Cargo.lock",
        "b9278c33ad53b93e02b68017e5f8b62616feac9c6779747f1d446fe69c642052",
    ),
    (
        "Cargo.toml",
        "a29ad1fa4fd60ac96b0b52698f3cd9fadb1324a890aa4f8fd1bc9a86935a8e8f",
    ),
    (
        "flake.lock",
        "5d5b11622c341292f429a5f93e55f38a3506431b139720ff62f983b35bf7d55f",
    ),
    (
        "flake.nix",
        "96d777e49c572087c4411b27e47f1b15e32983317cb24b9f9058485b4d090bab",
    ),
];

pub(crate) struct Arguments {
    pub(crate) step: u16,
    pub(crate) check_id: String,
    pub(crate) source_revision: String,
    pub(crate) source_tree: String,
    pub(crate) candidate_digest: String,
    pub(crate) platform: String,
    pub(crate) execution_request_sha256: String,
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("xtask must remain under tools/xtask")
        .to_path_buf()
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn canonical(value: &Value) -> Result<Vec<u8>, String> {
    serde_json::to_vec(value).map_err(|_| "Step 303 JSON encoding failed".to_owned())
}

fn execute(command: &mut Command, label: &str) -> Result<Output, String> {
    let output = command
        .current_dir(root())
        .env("CARGO_NET_OFFLINE", "true")
        .env("CARGO_TERM_COLOR", "never")
        .output()
        .map_err(|_| format!("{label} could not start"))?;
    if output.stdout.len() > MAX_OUTPUT_BYTES || output.stderr.len() > MAX_OUTPUT_BYTES {
        return Err(format!("{label} exceeded its output bound"));
    }
    Ok(output)
}

fn bounded(command: &mut Command, label: &str) -> Result<Output, String> {
    let output = execute(command, label)?;
    if !output.status.success() {
        return Err(format!("{label} failed"));
    }
    Ok(output)
}

fn rejected(command: &mut Command, label: &str) -> Result<(), String> {
    if execute(command, label)?.status.success() {
        return Err(format!("{label} unexpectedly succeeded"));
    }
    Ok(())
}

fn resolve_nix() -> Result<PathBuf, String> {
    if let Some(explicit) = env::var_os("RSHR_NIX_EXECUTABLE") {
        return fs::canonicalize(explicit)
            .map_err(|_| "Step 303 Nix client is unavailable".to_owned());
    }
    let path = env::var_os("PATH").ok_or_else(|| "Step 303 PATH is absent".to_owned())?;
    env::split_paths(&path)
        .map(|directory| directory.join("nix"))
        .find(|candidate| candidate.is_file())
        .and_then(|candidate| fs::canonicalize(candidate).ok())
        .ok_or_else(|| "Step 303 Nix client is unavailable".to_owned())
}

fn object_keys(value: &Value, label: &str) -> Result<Vec<String>, String> {
    let mut keys = value
        .as_object()
        .ok_or_else(|| format!("Step 303 {label} is not an object"))?
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    keys.sort_unstable();
    Ok(keys)
}

fn require_lock() -> Result<(), String> {
    let lock: Value = serde_json::from_slice(
        &fs::read(root().join("flake.lock"))
            .map_err(|_| "Step 303 flake lock is unreadable".to_owned())?,
    )
    .map_err(|_| "Step 303 flake lock is invalid".to_owned())?;
    if lock.pointer("/nodes/root/inputs/lib") != Some(&json!("lib"))
        || lock.pointer("/nodes/lib/locked/rev") != Some(&json!(LIB_REVISION))
        || lock.pointer("/nodes/lib/original/rev") != Some(&json!(LIB_REVISION))
    {
        return Err("Step 303 exact Nix tooling input differs".to_owned());
    }
    Ok(())
}

fn require_outputs(nix: &Path) -> Result<(), String> {
    let show = bounded(
        Command::new(nix).args([
            "--offline",
            "flake",
            "show",
            "--json",
            "--all-systems",
            "--no-write-lock-file",
        ]),
        "Step 303 Nix output inventory",
    )?;
    let inventory: Value = serde_json::from_slice(&show.stdout)
        .map_err(|_| "Step 303 Nix output inventory is invalid".to_owned())?;
    if object_keys(&inventory, "root output inventory")? != ["apps", "checks", "packages"] {
        return Err("Step 303 root output inventory differs".to_owned());
    }
    let systems = ["aarch64-darwin", "x86_64-linux"];
    for family in ["apps", "checks", "packages"] {
        if object_keys(&inventory[family], family)? != systems {
            return Err(format!("Step 303 {family} systems differ"));
        }
    }
    for system in systems {
        if object_keys(&inventory["apps"][system], "apps")? != ["default"]
            || object_keys(&inventory["checks"][system], "checks")? != ["default"]
            || object_keys(&inventory["packages"][system], "packages")? != ["default"]
            || inventory["apps"][system]["default"]["description"]
                != "Run the built radrootsd daemon"
            || inventory["checks"][system]["default"]["name"] != "radrootsd-check-1"
            || inventory["packages"][system]["default"]["name"] != "radrootsd-0.1.0"
        {
            return Err("Step 303 radrootsd output inventory differs".to_owned());
        }
    }
    for system in ["x86_64-darwin", "aarch64-linux", "x86_64-windows"] {
        rejected(
            Command::new(nix).args([
                "--offline",
                "eval",
                "--raw",
                &format!(".#packages.{system}.default.name"),
            ]),
            "Step 303 excluded-system evaluation",
        )?;
    }
    for attribute in [
        ".#devShells.aarch64-darwin.default.name",
        ".#nixosModules.default",
        ".#packages.x86_64-linux.oci.name",
    ] {
        rejected(
            Command::new(nix).args(["--offline", "eval", "--raw", attribute]),
            "Step 303 unowned output evaluation",
        )?;
    }
    Ok(())
}

fn require_nix() -> Result<(), String> {
    let executable = resolve_nix()?;
    if sha256(&fs::read(&executable).map_err(|_| "Step 303 Nix client is unreadable")?)
        != NIX_SHA256
    {
        return Err("Step 303 Nix client identity differs".to_owned());
    }
    let version = bounded(
        Command::new(&executable).arg("--version"),
        "Step 303 Nix version",
    )?;
    if sha256(&version.stdout) != NIX_VERSION_SHA256 {
        return Err("Step 303 Nix version differs".to_owned());
    }
    bounded(
        Command::new(&executable).args([
            "--offline",
            "flake",
            "check",
            "--all-systems",
            "--no-build",
            "--no-write-lock-file",
        ]),
        "Step 303 Nix evaluation",
    )?;
    require_outputs(&executable)
}

fn expected_contract(verifier_sha256: &str) -> Value {
    json!({
        "argv_template": [
            "cargo", "extbuild", "run", "--", "cargo", "run", "--offline", "--locked",
            "-q", "-p", "radrootsd_xtask", "--", "rshr-step-303-gate", "--step={step}",
            "--check-id={check_id}", "--source-revision={source_revision}",
            "--source-tree={source_tree}", "--candidate-digest={candidate_digest}",
            "--platform=macos_aarch64",
            "--execution-request-sha256={execution_request_sha256}"
        ],
        "assertion_id": [format!("step_303_gate_01_{GATE_DIGEST}")],
        "check_id": format!("gate-01-{GATE_DIGEST}"),
        "environment_authority": {
            "cache_policy_id": "rshr-200-step-287-cache-policy.v1",
            "cache_policy_sha256": "3e81d178bce97b6c349dfbb00c68fd6f620ac00b1a1c8d37b12e9998f3c9eaaa",
            "cadence_policy_id": "rshr-200-step-287-cadence-policy.v1",
            "cadence_policy_sha256": "d24903df8659ee3772297c84994911efe7d21cb8b988320ddc6ddce0431892a1",
            "isolation": "extbuild_host_constrained",
            "network": "disabled",
            "network_policy_id": "none",
            "network_policy_sha256": "none",
            "resource_policy_id": "rshr-200-step-287-resource-policy.v1",
            "resource_policy_sha256": "05d3c7a89185d3c55678d97955193fce2ed92b1eee5af99083d77ea64c98d14e"
        },
        "environment_names": [
            "EXT_BUILD_CONFIG", "EXT_BUILD_MACHINE_CONFIG", "EXT_BUILD_ROOT", "HOME", "PATH",
            "RUSTUP_TOOLCHAIN", "TMPDIR"
        ],
        "gate_definition_sha256": GATE_DIGEST,
        "required_platforms": ["macos_aarch64"],
        "required_tools": ["rustc"],
        "result_schema": "radroots.services-hardening.rshr-200-step-check-result.v1",
        "schema": "radroots.services-hardening.rshr-200-step-check-command.v1",
        "step": STEP,
        "verifier_path": "tools/xtask/src/rshr_202_step_303_gate.rs",
        "verifier_sha256": verifier_sha256
    })
}

pub(crate) fn run(arguments: Arguments) -> Result<(), String> {
    let check_id = format!("gate-01-{GATE_DIGEST}");
    if arguments.step != STEP
        || arguments.check_id != check_id
        || arguments.candidate_digest != "none"
        || arguments.platform != "macos_aarch64"
        || arguments.source_revision.len() != 40
        || arguments.source_tree.len() != 40
        || arguments.execution_request_sha256.len() != 64
        || !arguments
            .source_revision
            .bytes()
            .chain(arguments.source_tree.bytes())
            .chain(arguments.execution_request_sha256.bytes())
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err("Step 303 gate arguments differ".to_owned());
    }
    let root = root();
    if root.join(".github").exists() || root.join(".act").exists() || root.join("docs").exists() {
        return Err("Step 303 forbidden repository root is present".to_owned());
    }
    for (relative, expected) in EXACT_SOURCES {
        let bytes = fs::read(root.join(relative))
            .map_err(|_| "Step 303 governed source is unreadable".to_owned())?;
        if sha256(&bytes) != *expected {
            return Err("Step 303 governed source bytes differ".to_owned());
        }
    }
    let flake_source = fs::read_to_string(root.join("flake.nix"))
        .map_err(|_| "Step 303 flake source is unreadable".to_owned())?;
    for forbidden in [
        "writeShellApplication",
        "git rev-parse",
        "repo_root",
        "devShells",
        "nixosModules",
        "aarch64-linux",
        "x86_64-darwin",
    ] {
        if flake_source.contains(forbidden) {
            return Err("Step 303 checkout wrapper or unowned output is present".to_owned());
        }
    }

    let verifier_path = root.join("tools/xtask/src/rshr_202_step_303_gate.rs");
    let verifier_sha256 =
        sha256(&fs::read(verifier_path).map_err(|_| "Step 303 verifier is unreadable")?);
    let authority_path = root.join("contracts/rshr-202-step-303-gates.v1.json");
    let authority_bytes =
        fs::read(authority_path).map_err(|_| "Step 303 gate authority is unreadable")?;
    let authority: Value = serde_json::from_slice(&authority_bytes)
        .map_err(|_| "Step 303 gate authority is invalid".to_owned())?;
    let mut canonical_authority = canonical(&authority)?;
    canonical_authority.push(b'\n');
    let contracts = authority
        .get("gate_command_contract")
        .and_then(Value::as_array)
        .ok_or_else(|| "Step 303 gate contract is absent".to_owned())?;
    if authority_bytes != canonical_authority
        || authority.get("schema")
            != Some(&Value::String(
                "radroots.radrootsd.rshr-202-step-303-gates.v1".to_owned(),
            ))
        || authority.get("step") != Some(&json!([STEP]))
        || contracts.as_slice() != [expected_contract(&verifier_sha256)]
    {
        return Err("Step 303 gate authority differs".to_owned());
    }

    require_lock()?;
    bounded(
        Command::new("cargo").args(["+1.97.1", "fmt", "--all", "--", "--check"]),
        "Step 303 formatting",
    )?;
    bounded(
        Command::new("cargo").args([
            "+1.97.1",
            "check",
            "--offline",
            "--locked",
            "--workspace",
            "--all-targets",
        ]),
        "Step 303 Cargo check",
    )?;
    require_nix()?;

    let contract = &contracts[0];
    let assertion = json!([{
        "id": format!("step_303_gate_01_{GATE_DIGEST}"),
        "result": "pass"
    }]);
    let result = json!({
        "schema": "radroots.services-hardening.rshr-200-step-check-result.v1",
        "step": STEP,
        "check_id": check_id,
        "gate_definition_sha256": GATE_DIGEST,
        "source_revision": arguments.source_revision,
        "source_tree": arguments.source_tree,
        "candidate_generation": 0,
        "candidate_digest": "none",
        "command_contract_sha256": sha256(&canonical(contract)?),
        "verifier_sha256": verifier_sha256,
        "execution_request": [{
            "platform": arguments.platform,
            "sha256": arguments.execution_request_sha256
        }],
        "assertion_inventory_sha256": sha256(&canonical(&assertion)?),
        "assertion": assertion,
        "result": "pass"
    });
    let mut bytes = canonical(&result)?;
    bytes.push(b'\n');
    std::io::Write::write_all(&mut std::io::stdout().lock(), &bytes)
        .map_err(|_| "Step 303 result write failed".to_owned())
}
