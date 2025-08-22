use radroots_runtime::{JsonFile, RuntimeJsonError};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

pub const DEFAULT_IDENTITY_PATH: &str = "identity.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub key: String,
}

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error(transparent)]
    Store(#[from] RuntimeJsonError),

    #[error("invalid secret key: {0}")]
    InvalidSecretKey(String),

    #[error(
        "identity file missing at {0} and generation is not permitted (pass --allow-generate-identity)"
    )]
    GenerationNotAllowed(PathBuf),
}

impl Identity {
    pub fn load_or_generate<P: AsRef<Path>>(
        path: Option<P>,
        allow_generate: bool,
    ) -> Result<JsonFile<Self>, IdentityError> {
        let p = path
            .map(|p| p.as_ref().to_path_buf())
            .unwrap_or_else(|| PathBuf::from(DEFAULT_IDENTITY_PATH));

        if p.exists() {
            let store = JsonFile::load(&p)?;
            return Ok(store);
        }

        if !allow_generate {
            return Err(IdentityError::GenerationNotAllowed(p));
        }

        let store = JsonFile::load_or_create_with(&p, || {
            let keys = nostr::Keys::generate();
            let secret_hex = keys.secret_key().to_secret_hex();
            let tag = Uuid::new_v4();
            warn!(
                "No identity file found at {:?}; generated new secret (tag={tag})",
                p
            );
            Identity { key: secret_hex }
        })?;

        Ok(store)
    }

    pub fn to_keys(&self) -> Result<nostr::Keys, IdentityError> {
        nostr::Keys::from_str(&self.key)
            .map_err(|_| IdentityError::InvalidSecretKey(self.key.clone()))
    }
}
