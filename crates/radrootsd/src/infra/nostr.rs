use nostr::{key::PublicKey, nips::nip19::FromBech32};

pub fn parse_pubkey(s: &str) -> Option<PublicKey> {
    PublicKey::from_bech32(s)
        .or_else(|_| PublicKey::from_hex(s))
        .ok()
}
