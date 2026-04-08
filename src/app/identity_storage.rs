use std::path::{Path, PathBuf};

use anyhow::Result;
use radroots_identity::{IdentityError, RadrootsIdentity, RadrootsIdentityFile};

const RADROOTSD_IDENTITY_KEY_SLOT: &str = "radrootsd_identity";

#[cfg(test)]
pub fn encrypted_identity_key_path(path: impl AsRef<Path>) -> PathBuf {
    radroots_runtime::local_wrapping_key_path(path)
}

pub fn load_service_identity(
    path: Option<&Path>,
    allow_generate: bool,
) -> Result<RadrootsIdentity> {
    let path = resolved_identity_path(path);
    if path.exists() {
        return load_encrypted_identity(&path);
    }
    if !allow_generate {
        return Err(IdentityError::GenerationNotAllowed(path).into());
    }

    let identity = RadrootsIdentity::generate();
    store_encrypted_identity(&path, &identity)?;
    Ok(identity)
}

pub fn store_encrypted_identity(path: impl AsRef<Path>, identity: &RadrootsIdentity) -> Result<()> {
    let payload = serde_json::to_vec(&identity.to_file())?;
    radroots_runtime::seal_local_secret_file(path, RADROOTSD_IDENTITY_KEY_SLOT, &payload)?;
    Ok(())
}

pub fn load_encrypted_identity(path: impl AsRef<Path>) -> Result<RadrootsIdentity> {
    let payload = radroots_runtime::open_local_secret_file(path, RADROOTSD_IDENTITY_KEY_SLOT)?;
    let file: RadrootsIdentityFile = serde_json::from_slice(&payload)?;
    Ok(RadrootsIdentity::try_from(file)?)
}

fn resolved_identity_path(path: Option<&Path>) -> PathBuf {
    path.map(Path::to_path_buf).unwrap_or_else(|| {
        crate::app::config::default_identity_path_for_process()
            .expect("resolve canonical radrootsd identity path")
    })
}

#[cfg(test)]
mod tests {
    use super::{encrypted_identity_key_path, load_service_identity};

    #[test]
    fn load_service_identity_generates_encrypted_identity_artifacts() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("radrootsd-identity.secret.json");

        let generated =
            load_service_identity(Some(&path), true).expect("generate encrypted identity");
        let loaded = load_service_identity(Some(&path), false).expect("load encrypted identity");

        assert_eq!(generated.id(), loaded.id());
        assert!(path.is_file());
        assert!(encrypted_identity_key_path(&path).is_file());
    }

    #[test]
    fn load_service_identity_fails_when_wrapping_key_is_missing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("radrootsd-identity.secret.json");
        let _ = load_service_identity(Some(&path), true).expect("generate encrypted identity");
        std::fs::remove_file(encrypted_identity_key_path(&path)).expect("remove wrapping key");

        let err = load_service_identity(Some(&path), false)
            .expect_err("missing wrapping key should fail");
        assert!(err.to_string().contains("identity"));
    }
}
