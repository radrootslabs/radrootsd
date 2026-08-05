use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use nostr::{Keys, SecretKey};
use radroots_secrets::context::{
    EnvelopeContext, EnvelopePurpose, EnvelopeSubject, PayloadSchemaId,
};
use radroots_secrets::envelope::{
    ENVELOPE_VERSION, LEGACY_ENVELOPE_VERSION, LegacyV1ResealAuthority, Nonce, SealMaterial,
    SealRequest,
};
use radroots_secrets::error::Operation;
use radroots_secrets::id::{BackendKind, KeyVersion};
use radroots_secrets::wrapping::{
    BoxFuture, LegacyV1UnwrapRequest, SecretMaterial, UnwrapRequest, WrapRequest, WrappedSecret,
};
use radroots_secrets::{EncryptedEnvelope, KeyWrapping, SecretId, SecretRef};
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

const RADROOTSD_IDENTITY_KEY_SLOT: &str = "radrootsd_identity";
const WRAPPING_KEY_BYTES: usize = 32;
const WRAPPING_NONCE_BYTES: usize = 24;
const LEGACY_WRAPPED_KEY_VERSION: u8 = 1;
const WRAPPED_KEY_VERSION: u8 = 2;
const WRAPPING_AAD_DOMAIN: &[u8] = b"radrootsd.wrapped_data_key.v2";

struct DaemonFileKeyWrapping {
    key_path: PathBuf,
}

impl DaemonFileKeyWrapping {
    fn new(identity_path: &Path) -> Self {
        Self {
            key_path: encrypted_identity_key_path(identity_path),
        }
    }

    fn load_or_create_key(&self) -> Result<[u8; WRAPPING_KEY_BYTES], radroots_secrets::Error> {
        if let Ok(raw) = fs::read(&self.key_path) {
            return key_from_bytes(raw.as_slice());
        }
        if let Some(parent) = self
            .key_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|_| radroots_secrets::Error::BackendFailure {
                backend: BackendKind::External,
                operation: radroots_secrets::error::Operation::Provision,
            })?;
        }
        let key: [u8; WRAPPING_KEY_BYTES] = rand::random();
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.key_path)
        {
            Ok(mut file) => {
                file.write_all(&key)
                    .map_err(|_| secret_backend_failure(Operation::Write))?;
                file.sync_all()
                    .map_err(|_| secret_backend_failure(Operation::Write))?;
                set_secret_permissions(&self.key_path)
                    .map_err(|_| secret_backend_failure(Operation::Write))?;
                Ok(key)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let raw = fs::read(&self.key_path)
                    .map_err(|_| secret_backend_failure(Operation::Read))?;
                key_from_bytes(raw.as_slice())
            }
            Err(_) => Err(secret_backend_failure(Operation::Provision)),
        }
    }

    fn load_key(&self) -> Result<[u8; WRAPPING_KEY_BYTES], radroots_secrets::Error> {
        let raw = fs::read(&self.key_path).map_err(|_| secret_backend_failure(Operation::Read))?;
        key_from_bytes(raw.as_slice())
    }
}

impl KeyWrapping for DaemonFileKeyWrapping {
    fn wrap<'a>(
        &'a self,
        request: WrapRequest<'a>,
    ) -> BoxFuture<'a, Result<WrappedSecret, radroots_secrets::Error>> {
        Box::pin(async move {
            validate_daemon_reference(request.reference(), Operation::Wrap)?;
            let mut key = self.load_or_create_key()?;
            let nonce: [u8; WRAPPING_NONCE_BYTES] = rand::random();
            let aad = wrapping_aad(request.reference(), request.context());
            let ciphertext = request.plaintext().expose_secret(|plaintext| {
                XChaCha20Poly1305::new(Key::from_slice(&key)).encrypt(
                    XNonce::from_slice(&nonce),
                    Payload {
                        msg: plaintext,
                        aad: aad.as_slice(),
                    },
                )
            });
            key.zeroize();
            let ciphertext = ciphertext.map_err(|_| secret_backend_failure(Operation::Wrap))?;
            let mut wrapped = Vec::with_capacity(1 + nonce.len() + ciphertext.len());
            wrapped.push(WRAPPED_KEY_VERSION);
            wrapped.extend_from_slice(&nonce);
            wrapped.extend_from_slice(ciphertext.as_slice());
            WrappedSecret::from_bytes(wrapped)
        })
    }

    fn unwrap<'a>(
        &'a self,
        request: UnwrapRequest<'a>,
    ) -> BoxFuture<'a, Result<SecretMaterial, radroots_secrets::Error>> {
        Box::pin(async move {
            validate_daemon_reference(request.reference(), Operation::Unwrap)?;
            let aad = wrapping_aad(request.reference(), request.context());
            self.unwrap_with_aad(request.wrapped(), WRAPPED_KEY_VERSION, aad.as_slice())
        })
    }

    fn unwrap_legacy_v1<'a>(
        &'a self,
        request: LegacyV1UnwrapRequest<'a>,
    ) -> BoxFuture<'a, Result<SecretMaterial, radroots_secrets::Error>> {
        Box::pin(async move {
            validate_daemon_reference(request.reference(), Operation::Unwrap)?;
            self.unwrap_with_aad(
                request.wrapped(),
                LEGACY_WRAPPED_KEY_VERSION,
                request.reference().id().as_str().as_bytes(),
            )
        })
    }
}

impl DaemonFileKeyWrapping {
    fn unwrap_with_aad(
        &self,
        wrapped: &WrappedSecret,
        expected_version: u8,
        aad: &[u8],
    ) -> Result<SecretMaterial, radroots_secrets::Error> {
        let wrapped = wrapped.as_bytes();
        if wrapped.len() <= 1 + WRAPPING_NONCE_BYTES || wrapped[0] != expected_version {
            return Err(secret_backend_failure(Operation::Unwrap));
        }
        let mut key = self.load_key()?;
        let plaintext = XChaCha20Poly1305::new(Key::from_slice(&key)).decrypt(
            XNonce::from_slice(&wrapped[1..1 + WRAPPING_NONCE_BYTES]),
            Payload {
                msg: &wrapped[1 + WRAPPING_NONCE_BYTES..],
                aad,
            },
        );
        key.zeroize();
        SecretMaterial::from_slice(
            &plaintext.map_err(|_| secret_backend_failure(Operation::Unwrap))?,
        )
    }
}

fn wrapping_aad(reference: &SecretRef, context: &EnvelopeContext) -> Vec<u8> {
    let id = reference.id().as_str().as_bytes();
    let mut aad = Vec::with_capacity(WRAPPING_AAD_DOMAIN.len() + 2 + id.len() + 4 + 32);
    aad.extend_from_slice(WRAPPING_AAD_DOMAIN);
    aad.extend_from_slice(
        &u16::try_from(id.len())
            .expect("validated secret identifier length fits u16")
            .to_be_bytes(),
    );
    aad.extend_from_slice(id);
    aad.extend_from_slice(&reference.key_version().get().to_be_bytes());
    aad.extend_from_slice(&context.authentication_digest());
    aad
}

fn validate_daemon_reference(
    reference: &SecretRef,
    operation: Operation,
) -> Result<(), radroots_secrets::Error> {
    if reference.backend() != BackendKind::External
        || reference.key_version().get() != 1
        || reference.id().as_str() != RADROOTSD_IDENTITY_KEY_SLOT
    {
        return Err(secret_backend_failure(operation));
    }
    Ok(())
}

fn identity_secret_ref() -> Result<SecretRef, radroots_secrets::Error> {
    Ok(SecretRef::new(
        SecretId::parse(RADROOTSD_IDENTITY_KEY_SLOT)?,
        BackendKind::External,
        KeyVersion::new(1)?,
    ))
}

fn identity_envelope_context() -> Result<EnvelopeContext, radroots_secrets::Error> {
    Ok(EnvelopeContext::new(
        EnvelopePurpose::parse("radroots.service_identity")?,
        EnvelopeSubject::parse("service", "radrootsd")?,
        PayloadSchemaId::parse("radroots.daemon_identity.v1")?,
    ))
}

fn secret_backend_failure(operation: Operation) -> radroots_secrets::Error {
    radroots_secrets::Error::BackendFailure {
        backend: BackendKind::External,
        operation,
    }
}

fn key_from_bytes(raw: &[u8]) -> Result<[u8; WRAPPING_KEY_BYTES], radroots_secrets::Error> {
    raw.try_into()
        .map_err(|_| secret_backend_failure(Operation::Read))
}

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

pub fn encrypted_identity_key_path(path: impl AsRef<Path>) -> PathBuf {
    let mut value = OsString::from(path.as_ref().as_os_str());
    value.push(".key");
    PathBuf::from(value)
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
    let path = path.as_ref();
    if let Some(parent) = path.parent().filter(|path| !path.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_vec(&identity.to_file())?;
    let plaintext = SecretMaterial::from_slice(payload.as_slice())?;
    let data_key = SecretMaterial::from_slice(&rand::random::<[u8; 32]>())?;
    let nonce = Nonce::new(rand::random());
    let wrapping = DaemonFileKeyWrapping::new(path);
    let context = identity_envelope_context()?;
    let envelope = futures_executor::block_on(EncryptedEnvelope::seal(
        &wrapping,
        SealRequest::new(
            identity_secret_ref()?,
            context,
            &plaintext,
            SealMaterial::new(data_key, nonce),
        ),
    ))?;
    persist_envelope(path, envelope.encode()?.as_slice())
}

fn persist_envelope(path: &Path, encoded: &[u8]) -> Result<()> {
    let mut temporary = tempfile::NamedTempFile::new_in(
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?;
    temporary.write_all(encoded)?;
    temporary.as_file().sync_all()?;
    set_file_permissions(temporary.as_file())?;
    temporary.persist(path)?;
    sync_parent(path)?;
    Ok(())
}

pub fn load_encrypted_identity(path: impl AsRef<Path>) -> Result<DaemonIdentity> {
    let path = path.as_ref();
    let encoded = fs::read(path)?;
    let envelope = EncryptedEnvelope::decode(encoded.as_slice())
        .context("decode encrypted daemon identity")?;
    let wrapping = DaemonFileKeyWrapping::new(path);
    let context = identity_envelope_context()?;
    if envelope.version() == LEGACY_ENVELOPE_VERSION {
        return migrate_legacy_identity(path, envelope, &wrapping, context);
    }
    if envelope.version() != ENVELOPE_VERSION {
        bail!("unsupported encrypted daemon identity version");
    }
    open_identity(&envelope, &wrapping, &context)
}

fn open_identity(
    envelope: &EncryptedEnvelope,
    wrapping: &DaemonFileKeyWrapping,
    context: &EnvelopeContext,
) -> Result<DaemonIdentity> {
    let payload = futures_executor::block_on(envelope.open(wrapping, context))
        .context("open encrypted daemon identity")?;
    let file: DaemonIdentityFile = payload.expose_secret(|bytes| serde_json::from_slice(bytes))?;
    DaemonIdentity::from_file(file)
}

fn migrate_legacy_identity(
    path: &Path,
    envelope: EncryptedEnvelope,
    wrapping: &DaemonFileKeyWrapping,
    context: EnvelopeContext,
) -> Result<DaemonIdentity> {
    let expected_reference = identity_secret_ref()?;
    let resealed = futures_executor::block_on(envelope.reseal_legacy_v1(
        wrapping,
        &LegacyV1ResealAuthority::new(),
        &expected_reference,
        identity_secret_ref()?,
        context.clone(),
        &valid_identity_payload,
        SealMaterial::new(
            SecretMaterial::from_slice(&rand::random::<[u8; 32]>())?,
            Nonce::new(rand::random()),
        ),
    ))
    .context("migrate legacy encrypted daemon identity")?;
    let envelope = resealed.into_envelope();
    let identity = open_identity(&envelope, wrapping, &context)?;
    persist_envelope(path, envelope.encode()?.as_slice())?;
    Ok(identity)
}

fn valid_identity_payload(bytes: &[u8]) -> bool {
    serde_json::from_slice::<DaemonIdentityFile>(bytes)
        .ok()
        .and_then(|file| DaemonIdentity::from_file(file).ok())
        .is_some()
}

#[cfg(unix)]
fn set_secret_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_secret_permissions(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

fn set_file_permissions(file: &fs::File) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        Ok(())
    }
}

fn sync_parent(path: &Path) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::File::open(parent)?.sync_all()
}

fn resolved_identity_path(path: Option<&Path>) -> PathBuf {
    path.map(Path::to_path_buf).unwrap_or_else(|| {
        crate::app::paths::default_identity_path_for_process()
            .expect("resolve canonical radrootsd identity path")
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use chacha20poly1305::aead::{Aead, KeyInit, Payload};
    use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
    use radroots_secrets::EncryptedEnvelope;
    use radroots_secrets::envelope::{ENVELOPE_VERSION, LEGACY_ENVELOPE_VERSION};

    use super::{
        DaemonIdentity, RADROOTSD_IDENTITY_KEY_SLOT, encrypted_identity_key_path,
        identity_envelope_context, load_service_identity, set_secret_permissions,
    };

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
        let envelope = EncryptedEnvelope::decode(&fs::read(&path).expect("read envelope"))
            .expect("decode envelope");
        assert_eq!(envelope.version(), ENVELOPE_VERSION);
        assert_eq!(
            envelope.context(),
            Some(&identity_envelope_context().expect("identity context"))
        );
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

    #[test]
    fn load_service_identity_atomically_migrates_legacy_v1() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("radrootsd-identity.secret.json");
        let expected = DaemonIdentity::generate();
        store_legacy_identity(&path, &expected);

        let loaded = load_service_identity(Some(&path), false).expect("migrate legacy identity");
        assert_eq!(loaded.id(), expected.id());

        let encoded = fs::read(&path).expect("read migrated identity");
        let migrated = EncryptedEnvelope::decode(&encoded).expect("decode migrated identity");
        assert_eq!(migrated.version(), ENVELOPE_VERSION);
        assert_eq!(
            migrated.context(),
            Some(&identity_envelope_context().expect("identity context"))
        );
        let reloaded = load_service_identity(Some(&path), false).expect("reload migrated identity");
        assert_eq!(reloaded.id(), expected.id());
    }

    fn store_legacy_identity(path: &std::path::Path, identity: &DaemonIdentity) {
        const NONCE_BYTES: usize = 24;
        const TAG_BYTES: usize = 16;

        let wrapping_key = [0x11; 32];
        let data_key = [0x22; 32];
        let wrapping_nonce = [0x33; NONCE_BYTES];
        let envelope_nonce = [0x44; NONCE_BYTES];
        let payload = serde_json::to_vec(&identity.to_file()).expect("encode identity payload");

        let wrapped_ciphertext = XChaCha20Poly1305::new(Key::from_slice(&wrapping_key))
            .encrypt(
                XNonce::from_slice(&wrapping_nonce),
                Payload {
                    msg: &data_key,
                    aad: RADROOTSD_IDENTITY_KEY_SLOT.as_bytes(),
                },
            )
            .expect("wrap legacy data key");
        let mut wrapped = Vec::with_capacity(1 + NONCE_BYTES + wrapped_ciphertext.len());
        wrapped.push(1);
        wrapped.extend_from_slice(&wrapping_nonce);
        wrapped.extend_from_slice(&wrapped_ciphertext);

        let id = RADROOTSD_IDENTITY_KEY_SLOT.as_bytes();
        let ciphertext_len = u32::try_from(payload.len() + TAG_BYTES).expect("ciphertext length");
        let mut encoded = Vec::new();
        encoded.extend_from_slice(b"RRS1");
        encoded.extend_from_slice(&LEGACY_ENVELOPE_VERSION.to_be_bytes());
        encoded.push(1); // XChaCha20-Poly1305
        encoded.push(1); // provider-wrapped key source
        encoded.push(4); // external backend
        encoded.extend_from_slice(&1_u32.to_be_bytes());
        encoded.extend_from_slice(&u16::try_from(id.len()).expect("id length").to_be_bytes());
        encoded.extend_from_slice(id);
        encoded.extend_from_slice(&envelope_nonce);
        encoded.extend_from_slice(
            &u32::try_from(wrapped.len())
                .expect("wrapped length")
                .to_be_bytes(),
        );
        encoded.extend_from_slice(&wrapped);
        encoded.extend_from_slice(&ciphertext_len.to_be_bytes());
        let ciphertext = XChaCha20Poly1305::new(Key::from_slice(&data_key))
            .encrypt(
                XNonce::from_slice(&envelope_nonce),
                Payload {
                    msg: &payload,
                    aad: &encoded,
                },
            )
            .expect("encrypt legacy payload");
        encoded.extend_from_slice(&ciphertext);

        fs::write(path, encoded).expect("write legacy envelope");
        let key_path = encrypted_identity_key_path(path);
        fs::write(&key_path, wrapping_key).expect("write wrapping key");
        set_secret_permissions(&key_path).expect("secure wrapping key");
    }
}
