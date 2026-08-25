#![forbid(unsafe_code)]

const ROOT: &str = include_str!("../src/lib.rs");
const PUBLIC_API: &str = include_str!("../contracts/api_baselines/radrootsd.txt");

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
