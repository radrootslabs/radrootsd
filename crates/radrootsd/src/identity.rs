use radroots_identity::IdentitySpec;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub key: String,
}

impl IdentitySpec for Identity {
    type Keys = nostr::Keys;
    type ParseError = nostr::key::Error;

    fn generate_new() -> Self {
        let keys = nostr::Keys::generate();
        Self {
            key: keys.secret_key().to_secret_hex(),
        }
    }

    fn to_keys(&self) -> Result<Self::Keys, Self::ParseError> {
        nostr::Keys::from_str(&self.key)
    }
}
