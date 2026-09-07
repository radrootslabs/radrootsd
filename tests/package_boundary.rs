#![forbid(unsafe_code)]

const ROOT: &str = include_str!("../src/lib.rs");
const PUBLIC_API: &str = include_str!("../contracts/api_baselines/radrootsd.txt");
const FLAKE: &str = include_str!("../flake.nix");

#[test]
fn implementation_modules_are_private_and_api_is_owned() {
    for module in ["app", "core", "host_nostr", "transport"] {
        assert!(ROOT.contains(&format!("mod {module};")));
        assert!(!ROOT.contains(&format!("pub mod {module};")));
        assert!(!PUBLIC_API.contains(&format!("radrootsd::{module}::")));
    }
    assert!(PUBLIC_API.contains("pub struct radrootsd::RadrootsdRunError"));
    assert!(PUBLIC_API.contains("pub async fn radrootsd::run_daemon()"));
    for dependency in ["anyhow::", "jsonrpsee::", "nostr::", "sqlx::"] {
        assert!(!PUBLIC_API.contains(dependency), "leaked {dependency}");
    }
}

#[test]
fn public_error_is_redacted_and_source_free() {
    let source = std::fs::read_to_string("src/lib.rs").expect("root source");
    for required in [
        "formatter.write_str(\"RadrootsdRunError\")",
        "formatter.write_str(\"daemon execution failed\")",
        "impl Error for RadrootsdRunError {}",
    ] {
        assert!(source.contains(required), "missing {required}");
    }
    assert!(!PUBLIC_API.contains("std::io::Error"));
}

#[test]
fn nix_outputs_are_real_owned_and_exactly_bounded() {
    for required in [
        "github:radrootslabs/lib/3563f3b5a4331eb2cb3f925cafc9de524d844228",
        "\"aarch64-darwin\"",
        "\"x86_64-linux\"",
        "craneLib.buildPackage",
        "craneLib.mkCargoDerivation",
        "program = \"${package}/bin/radrootsd\"",
        "default = (daemonOutputs system).package",
        "default = (daemonOutputs system).check",
        "default = (daemonOutputs system).app",
    ] {
        assert!(
            FLAKE.contains(required),
            "missing governed Nix source: {required}"
        );
    }
    for forbidden in [
        "writeShellApplication",
        "git rev-parse",
        "repo_root",
        "devShells",
        "nixosModules",
        "aarch64-linux",
        "x86_64-darwin",
    ] {
        assert!(
            !FLAKE.contains(forbidden),
            "forbidden Nix surface is present: {forbidden}"
        );
    }
}
