use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{fs, io::Write, path::Path, process::Command};

const LOCK: &str = "radroots.lib.source-lock.v1.toml";

#[derive(Debug, PartialEq)]
pub(super) enum Error {
    Input,
    Graph,
    Stale,
    Io,
}

#[derive(Deserialize, Serialize)]
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

pub(super) fn run(arguments: &[String]) -> Result<(), Error> {
    let [action] = arguments else {
        return Err(Error::Input);
    };
    if !matches!(action.as_str(), "check" | "refresh") {
        return Err(Error::Input);
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or(Error::Input)?;
    let source = read_bounded(&root.join(LOCK), 16 * 1024)?;
    let cargo_lock = read_bounded(&root.join("Cargo.lock"), 32 * 1024 * 1024)?;
    let output = Command::new("cargo")
        .args(["metadata", "--locked", "--offline", "--format-version", "1"])
        .current_dir(root)
        .output()
        .map_err(|_| Error::Io)?;
    if !output.status.success() || output.stdout.len() > 16 * 1024 * 1024 {
        return Err(Error::Graph);
    }
    let metadata = serde_json::from_slice(&output.stdout).map_err(|_| Error::Graph)?;
    let refreshed = render(&source, &cargo_lock, &metadata)?;
    if action == "check" {
        let original: SourceLock =
            toml::from_str(std::str::from_utf8(&source).map_err(|_| Error::Input)?)
                .map_err(|_| Error::Input)?;
        if original.lockfile_sha256 != digest(&cargo_lock) {
            return Err(Error::Stale);
        }
        println!("source lock current");
        return Ok(());
    }
    let mut temporary = tempfile::NamedTempFile::new_in(root).map_err(|_| Error::Io)?;
    temporary
        .write_all(refreshed.as_bytes())
        .map_err(|_| Error::Io)?;
    temporary.as_file().sync_all().map_err(|_| Error::Io)?;
    if read_bounded(&root.join(LOCK), 16 * 1024)? != source
        || read_bounded(&root.join("Cargo.lock"), 32 * 1024 * 1024)? != cargo_lock
    {
        return Err(Error::Stale);
    }
    let permissions = fs::metadata(root.join(LOCK))
        .map_err(|_| Error::Io)?
        .permissions();
    temporary
        .as_file()
        .set_permissions(permissions)
        .map_err(|_| Error::Io)?;
    temporary.persist(root.join(LOCK)).map_err(|_| Error::Io)?;
    println!("source lock refreshed");
    Ok(())
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>, Error> {
    let metadata = fs::symlink_metadata(path).map_err(|_| Error::Io)?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err(Error::Input);
    }
    let bytes = fs::read(path).map_err(|_| Error::Io)?;
    if bytes.len() as u64 > limit {
        return Err(Error::Input);
    }
    Ok(bytes)
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn render(source: &[u8], cargo_lock: &[u8], metadata: &Value) -> Result<String, Error> {
    let mut lock: SourceLock =
        toml::from_str(std::str::from_utf8(source).map_err(|_| Error::Input)?)
            .map_err(|_| Error::Input)?;
    if lock.schema != "radroots.lib.source-lock.v1"
        || lock.repository != "https://github.com/radrootslabs/lib"
        || lock.architecture != "radroots.crates.release.v2"
        || lock.lockfile != "Cargo.lock"
        || lock.version != "0.1.0-alpha"
    {
        return Err(Error::Input);
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
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Error::Input);
        }
    }
    let source = format!(
        "git+https://github.com/radrootslabs/lib.git?rev={}",
        lock.revision
    );
    let resolved = format!("{source}#{}", lock.revision);
    let packages = metadata["packages"].as_array().ok_or(Error::Graph)?;
    let daemon = packages
        .iter()
        .find(|package| package["name"] == "radrootsd")
        .ok_or(Error::Graph)?;
    let dependencies = daemon["dependencies"].as_array().ok_or(Error::Graph)?;
    let mut direct = 0;
    for dependency in dependencies {
        let name = dependency["name"].as_str().ok_or(Error::Graph)?;
        if name.starts_with("radroots_") {
            if dependency["source"] != source
                || dependency["req"] != format!("={}", lock.version)
                || !dependency["rename"].is_null()
            {
                return Err(Error::Graph);
            }
            direct += 1;
        }
    }
    let mut shared = 0;
    for package in packages {
        let name = package["name"].as_str().ok_or(Error::Graph)?;
        if name == "radroots" || name.starts_with("radroots_") {
            if package["source"] != resolved || package["version"] != lock.version {
                return Err(Error::Graph);
            }
            shared += 1;
        }
    }
    if direct == 0 || shared == 0 {
        return Err(Error::Graph);
    }
    lock.lockfile_sha256 = digest(cargo_lock);
    toml::to_string(&lock).map_err(|_| Error::Input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture() -> (Vec<u8>, Value) {
        let source = include_bytes!("../../../radroots.lib.source-lock.v1.toml").to_vec();
        let lock: SourceLock =
            toml::from_str(std::str::from_utf8(&source).expect("UTF8")).expect("source lock");
        let git_source = format!(
            "git+https://github.com/radrootslabs/lib.git?rev={}",
            lock.revision
        );
        let metadata = json!({"packages": [
            {"name": "radrootsd", "dependencies": [{"name": "radroots_protocol", "source": git_source,
                "req": "=0.1.0-alpha", "rename": null}]},
            {"name": "radroots_protocol", "source": format!("{git_source}#{}", lock.revision),
                "version": "0.1.0-alpha"}
        ]});
        (source, metadata)
    }

    #[test]
    fn refresh_changes_only_actual_lock_digest_and_is_idempotent() {
        let (source, metadata) = fixture();
        let before: toml::Value =
            toml::from_str(std::str::from_utf8(&source).expect("UTF8")).expect("before");
        let refreshed = render(&source, b"new locked graph", &metadata).expect("refresh");
        let mut after: toml::Value = toml::from_str(&refreshed).expect("after");
        assert_eq!(
            after["lockfile_sha256"].as_str(),
            Some(digest(b"new locked graph").as_str())
        );
        after["lockfile_sha256"] = before["lockfile_sha256"].clone();
        assert_eq!(before, after);
        assert_eq!(
            render(refreshed.as_bytes(), b"new locked graph", &metadata).expect("replay"),
            refreshed
        );
    }

    #[test]
    fn duplicate_unknown_and_malformed_authority_is_rejected() {
        let (source, metadata) = fixture();
        for suffix in [
            "\nrevision = \"duplicate\"\n",
            "\nunknown = true\n",
            "\ninvalid = [",
        ] {
            let mut changed = source.clone();
            changed.extend_from_slice(suffix.as_bytes());
            assert_eq!(render(&changed, b"graph", &metadata), Err(Error::Input));
        }
        let changed = String::from_utf8(source)
            .expect("UTF8")
            .replace("lockfile = \"Cargo.lock\"", "lockfile = \"../Cargo.lock\"");
        assert_eq!(
            render(changed.as_bytes(), b"graph", &metadata),
            Err(Error::Input)
        );
    }

    #[test]
    fn direct_and_resolved_source_pin_version_or_alias_drift_is_rejected() {
        let (source, metadata) = fixture();
        for (field, value) in [
            ("source", Value::Null),
            ("req", json!("^0.1")),
            ("rename", json!("alias")),
        ] {
            let mut changed = metadata.clone();
            changed["packages"][0]["dependencies"][0][field] = value;
            assert_eq!(render(&source, b"graph", &changed), Err(Error::Graph));
        }
        for (field, value) in [("source", Value::Null), ("version", json!("0.2.0"))] {
            let mut changed = metadata.clone();
            changed["packages"][1][field] = value;
            assert_eq!(render(&source, b"graph", &changed), Err(Error::Graph));
        }
        assert_eq!(
            render(&source, b"graph", &json!({"packages": []})),
            Err(Error::Graph)
        );
    }

    #[test]
    fn oversized_or_non_regular_inputs_are_rejected() {
        let directory = tempfile::tempdir().expect("directory");
        let file = directory.path().join("input");
        fs::write(&file, b"12345").expect("input");
        assert_eq!(read_bounded(&file, 5).expect("bounded file"), b"12345");
        assert_eq!(read_bounded(&file, 4), Err(Error::Input));
        assert_eq!(read_bounded(directory.path(), 1024), Err(Error::Input));
        #[cfg(unix)]
        {
            let link = directory.path().join("link");
            std::os::unix::fs::symlink(&file, &link).expect("symlink");
            assert_eq!(read_bounded(&link, 5), Err(Error::Input));
        }
    }

    #[test]
    fn unsupported_commands_fail_before_graph_or_file_access() {
        for arguments in [
            vec![],
            vec!["write".to_owned()],
            vec!["refresh".to_owned(), "extra".to_owned()],
        ] {
            assert_eq!(run(&arguments), Err(Error::Input));
        }
    }
}
