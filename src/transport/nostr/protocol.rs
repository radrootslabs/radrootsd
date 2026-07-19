#![forbid(unsafe_code)]

use nostr::nips::nip46::NostrConnectMessage;
use nostr::{Event, EventBuilder};
use radroots_nostr::prelude::{RadrootsNostrKeys, RadrootsNostrPublicKey};

/// Encrypts and locally signs a NIP-46 transport message.
///
/// The completed event is returned for transport-only publication. NIP-46
/// kind 24133 is protocol traffic, not a generic Radroots product-authoring
/// surface.
pub(crate) fn sign_nip46_message(
    sender_keys: &RadrootsNostrKeys,
    receiver_pubkey: RadrootsNostrPublicKey,
    message: NostrConnectMessage,
) -> Result<Event, nostr::event::builder::Error> {
    EventBuilder::nostr_connect(sender_keys, receiver_pubkey, message)?.sign_with_keys(sender_keys)
}

#[cfg(test)]
mod tests {
    use nostr::JsonUtil;
    use nostr::nips::{nip44, nip46::NostrConnectRequest};
    use radroots_nostr::prelude::{RadrootsNostrKeys, RadrootsNostrKind};

    use super::sign_nip46_message;

    #[test]
    fn signed_nip46_message_is_bound_to_sender_and_receiver() {
        let sender = RadrootsNostrKeys::generate();
        let receiver = RadrootsNostrKeys::generate();
        let message = nostr::nips::nip46::NostrConnectMessage::request(&NostrConnectRequest::Ping);
        let request_id = message.id().to_owned();

        let event = sign_nip46_message(&sender, receiver.public_key(), message)
            .expect("signed NIP-46 message");

        assert_eq!(event.kind, RadrootsNostrKind::NostrConnect);
        assert_eq!(event.pubkey, sender.public_key());
        assert!(
            event
                .tags
                .iter()
                .any(|tag| { tag.as_slice() == ["p".to_owned(), receiver.public_key().to_hex()] })
        );
        assert!(event.verify().is_ok());

        let plaintext = nip44::decrypt(receiver.secret_key(), &sender.public_key(), &event.content)
            .expect("decrypt NIP-46 message");
        let decrypted = nostr::nips::nip46::NostrConnectMessage::from_json(plaintext)
            .expect("parse NIP-46 message");
        assert!(decrypted.is_request());
        assert_eq!(decrypted.id(), request_id);
    }
}
