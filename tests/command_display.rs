use std::process::Command;

#[test]
fn help_and_version_succeed_before_configuration_or_identity_state() {
    for arguments in [
        vec!["--help"],
        vec!["--version"],
        vec!["transport-publish", "--help"],
        vec!["transport-publish", "principal", "init", "--help"],
    ] {
        let root = tempfile::tempdir().unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_radrootsd"))
            .current_dir(root.path())
            .arg("--config")
            .arg(root.path().join("absent-config.toml"))
            .arg("--identity")
            .arg(root.path().join("absent-identity.enc"))
            .args(&arguments)
            .output()
            .unwrap();
        assert!(output.status.success(), "{arguments:?}");
        assert!(!output.stdout.is_empty());
        assert!(output.stderr.is_empty());
        assert!(std::fs::read_dir(root.path()).unwrap().next().is_none());
    }
}

#[test]
fn malformed_arguments_keep_redacted_failure_without_creating_state() {
    let root = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_radrootsd"))
        .current_dir(root.path())
        .arg("--config")
        .arg(root.path().join("absent-config.toml"))
        .arg("--unknown-daemon-option")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(output.stderr, b"Fatal error: daemon execution failed\n");
    assert!(std::fs::read_dir(root.path()).unwrap().next().is_none());
}
