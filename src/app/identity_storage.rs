use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use nostr::{Keys, SecretKey};
use serde::{Deserialize, Serialize};

const RADROOTSD_IDENTITY_KEY_SLOT: &str = "radrootsd_identity";

/// Host-private service signing identity.
///
/// Public identity values cross package boundaries through
/// `radroots_identity`; secret-key generation and custody remain daemon-owned.
#[derive(Clone)]
pub(crate) struct DaemonIdentity {
    keys: Keys,
}

impl core::fmt::Debug for DaemonIdentity {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("DaemonIdentity")
            .field("public_key", &self.public_key_hex())
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DaemonIdentityFile {
    secret_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    public_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<nostr::Event>,
    #[serde(skip_serializing_if = "Option::is_none")]
    application_handler: Option<nostr::Event>,
}

impl DaemonIdentity {
    pub(crate) fn generate() -> Self {
        Self {
            keys: Keys::generate(),
        }
    }

    pub(crate) const fn keys(&self) -> &Keys {
        &self.keys
    }

    pub(crate) fn public_key(&self) -> nostr::PublicKey {
        self.keys.public_key()
    }

    pub(crate) fn public_key_hex(&self) -> String {
        self.public_key().to_hex()
    }

    #[cfg(test)]
    pub(crate) fn id(&self) -> String {
        self.public_key_hex()
    }

    fn to_file(&self) -> DaemonIdentityFile {
        DaemonIdentityFile {
            secret_key: self.keys.secret_key().to_secret_hex(),
            public_key: Some(self.public_key_hex()),
            identifier: None,
            metadata: None,
            application_handler: None,
        }
    }

    fn from_file(file: DaemonIdentityFile) -> Result<Self> {
        let secret_key = SecretKey::parse(file.secret_key.as_str())
            .map_err(|_| anyhow::anyhow!("invalid daemon identity secret"))?;
        let identity = Self {
            keys: Keys::new(secret_key),
        };
        if file
            .public_key
            .as_deref()
            .is_some_and(|expected| expected != identity.public_key_hex())
        {
            bail!("daemon identity public key does not match encrypted secret");
        }
        Ok(identity)
    }
}

#[cfg(test)]
pub fn encrypted_identity_key_path(path: impl AsRef<Path>) -> PathBuf {
    radroots_runtime::local_wrapping_key_path(path)
}

pub fn load_service_identity(path: Option<&Path>, allow_generate: bool) -> Result<DaemonIdentity> {
    let path = resolved_identity_path(path);
    if path.exists() {
        return load_encrypted_identity(&path);
    }
    if !allow_generate {
        bail!(
            "daemon identity generation is not allowed at {}",
            path.display()
        );
    }

    let identity = DaemonIdentity::generate();
    store_encrypted_identity(&path, &identity)?;
    Ok(identity)
}

pub fn store_encrypted_identity(path: impl AsRef<Path>, identity: &DaemonIdentity) -> Result<()> {
    let payload = serde_json::to_vec(&identity.to_file())?;
    radroots_runtime::seal_local_secret_file(path, RADROOTSD_IDENTITY_KEY_SLOT, &payload)?;
    Ok(())
}

pub fn load_encrypted_identity(path: impl AsRef<Path>) -> Result<DaemonIdentity> {
    let payload = radroots_runtime::open_local_secret_file(path, RADROOTSD_IDENTITY_KEY_SLOT)?;
    let file: DaemonIdentityFile = serde_json::from_slice(&payload)?;
    DaemonIdentity::from_file(file)
}

fn resolved_identity_path(path: Option<&Path>) -> PathBuf {
    path.map(Path::to_path_buf).unwrap_or_else(|| {
        crate::app::paths::default_identity_path_for_process()
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
