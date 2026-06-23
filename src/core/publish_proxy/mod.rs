use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use radroots_publish_proxy_protocol::{
    PublishDeliveryPolicy, PublishEventRequest, PublishEventResponse, PublishJobStatus,
    PublishJobView, PublishRelayOutcome, PublishRelayOutcomeKind, PublishRelayPolicy,
    PublishRelaySource,
};
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::app::config::PublishProxyConfig;

const TOKEN_PREFIX: &str = "rrd_pp_";
const TOKEN_HASH_PREFIX: &str = "sha256:";
const SCHEMA_VERSION: i64 = 1;

#[derive(Debug, Error)]
pub enum PublishProxyError {
    #[error("publish proxy storage error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("publish proxy json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("publish proxy io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid publish proxy scope: {0}")]
    InvalidScope(String),
    #[error("publish proxy idempotency conflict for key `{0}`")]
    IdempotencyConflict(String),
}

#[derive(Clone)]
pub struct PublishProxy {
    pub config: PublishProxyConfig,
    pub store: PublishProxyStore,
}

impl PublishProxy {
    pub fn open(config: PublishProxyConfig) -> Result<Self, PublishProxyError> {
        let store = PublishProxyStore::open(config.database_path.clone())?;
        Ok(Self { config, store })
    }

    pub fn memory(config: PublishProxyConfig) -> Result<Self, PublishProxyError> {
        let store = PublishProxyStore::memory()?;
        Ok(Self { config, store })
    }
}

#[derive(Clone)]
pub struct PublishProxyStore {
    inner: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishJobVisibility {
    Own,
    Admin,
}

impl FromStr for PublishJobVisibility {
    type Err = PublishProxyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "own" => Ok(Self::Own),
            "admin" => Ok(Self::Admin),
            other => Err(PublishProxyError::InvalidScope(format!(
                "unknown job visibility `{other}`"
            ))),
        }
    }
}

impl fmt::Display for PublishJobVisibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Own => f.write_str("own"),
            Self::Admin => f.write_str("admin"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishPrincipalInit {
    pub label: String,
    pub token_hash: String,
    pub allowed_pubkeys: Vec<String>,
    pub allowed_kinds: Vec<u32>,
    pub allowed_relay_policies: Vec<PublishRelayPolicy>,
    pub allow_request_relays: bool,
    pub job_visibility: PublishJobVisibility,
    pub expires_at_unix: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishPrincipal {
    pub principal_id: String,
    pub label: String,
    pub allowed_pubkeys: Vec<String>,
    pub allowed_kinds: Vec<u32>,
    pub allowed_relay_policies: Vec<PublishRelayPolicy>,
    pub allow_request_relays: bool,
    pub job_visibility: PublishJobVisibility,
    pub expires_at_unix: Option<i64>,
}

impl PublishPrincipal {
    pub fn allows_event(&self, request: &PublishEventRequest) -> Result<(), PublishProxyError> {
        ensure_lower_hex("pubkey", request.event.pubkey.as_str(), 64)?;
        if !self
            .allowed_pubkeys
            .iter()
            .any(|pubkey| pubkey == &request.event.pubkey)
        {
            return Err(PublishProxyError::InvalidScope(
                "principal is not allowed to publish for event pubkey".to_owned(),
            ));
        }
        if !self.allowed_kinds.contains(&request.event.kind) {
            return Err(PublishProxyError::InvalidScope(
                "principal is not allowed to publish event kind".to_owned(),
            ));
        }
        if !self.allowed_relay_policies.contains(&request.relay_policy) {
            return Err(PublishProxyError::InvalidScope(
                "principal is not allowed to use requested relay policy".to_owned(),
            ));
        }
        if !self.allow_request_relays && !request.relays.is_empty() {
            return Err(PublishProxyError::InvalidScope(
                "principal is not allowed to provide request relays".to_owned(),
            ));
        }
        Ok(())
    }

    fn can_read_job(&self, principal_id: &str) -> bool {
        self.job_visibility == PublishJobVisibility::Admin || self.principal_id == principal_id
    }
}

#[derive(Debug, Clone)]
pub struct PublishJobInsert {
    pub principal_id: String,
    pub idempotency_key: Option<String>,
    pub request: PublishEventRequest,
}

impl PublishProxyStore {
    pub fn open(path: PathBuf) -> Result<Self, PublishProxyError> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        Self::from_connection(connection)
    }

    pub fn memory() -> Result<Self, PublishProxyError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, PublishProxyError> {
        connection.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS publish_proxy_principals (
                principal_id TEXT PRIMARY KEY NOT NULL,
                label TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                allowed_pubkeys_json TEXT NOT NULL,
                allowed_kinds_json TEXT NOT NULL,
                allowed_relay_policies_json TEXT NOT NULL,
                allow_request_relays INTEGER NOT NULL,
                job_visibility TEXT NOT NULL,
                expires_at_unix INTEGER,
                revoked_at_unix INTEGER,
                created_at_unix INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS publish_proxy_jobs (
                job_id TEXT PRIMARY KEY NOT NULL,
                principal_id TEXT NOT NULL,
                idempotency_key TEXT,
                status TEXT NOT NULL,
                event_id TEXT NOT NULL,
                event_pubkey TEXT NOT NULL,
                event_kind INTEGER NOT NULL,
                relay_policy_json TEXT NOT NULL,
                delivery_policy_json TEXT NOT NULL,
                requested_relay_count INTEGER NOT NULL,
                request_json TEXT NOT NULL,
                requested_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                completed_at_ms INTEGER,
                last_error TEXT,
                FOREIGN KEY(principal_id) REFERENCES publish_proxy_principals(principal_id)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS publish_proxy_jobs_principal_idempotency_idx
                ON publish_proxy_jobs(principal_id, idempotency_key)
                WHERE idempotency_key IS NOT NULL;
            CREATE TABLE IF NOT EXISTS publish_proxy_relay_results (
                job_id TEXT NOT NULL,
                relay_url TEXT NOT NULL,
                source TEXT NOT NULL,
                attempted INTEGER NOT NULL,
                outcome_kind TEXT NOT NULL,
                message TEXT,
                latency_ms INTEGER,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(job_id, relay_url),
                FOREIGN KEY(job_id) REFERENCES publish_proxy_jobs(job_id)
            );
            CREATE TABLE IF NOT EXISTS publish_proxy_relay_list_cache (
                pubkey TEXT PRIMARY KEY NOT NULL,
                relays_json TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            "#,
        )?;
        connection.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn create_principal(
        &self,
        input: PublishPrincipalInit,
    ) -> Result<PublishPrincipal, PublishProxyError> {
        validate_principal_init(&input)?;
        let principal_id = Uuid::new_v4().to_string();
        let now = current_unix_secs();
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        connection.execute(
            r#"
            INSERT INTO publish_proxy_principals (
                principal_id,
                label,
                token_hash,
                allowed_pubkeys_json,
                allowed_kinds_json,
                allowed_relay_policies_json,
                allow_request_relays,
                job_visibility,
                expires_at_unix,
                revoked_at_unix,
                created_at_unix
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10)
            "#,
            params![
                principal_id,
                input.label.trim(),
                input.token_hash,
                serde_json::to_string(&input.allowed_pubkeys)?,
                serde_json::to_string(&input.allowed_kinds)?,
                serde_json::to_string(&input.allowed_relay_policies)?,
                input.allow_request_relays,
                input.job_visibility.to_string(),
                input.expires_at_unix,
                now,
            ],
        )?;
        drop(connection);
        self.principal_by_id(principal_id.as_str())?
            .ok_or_else(|| PublishProxyError::InvalidScope("created principal missing".to_owned()))
    }

    pub fn principal_for_token_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<PublishPrincipal>, PublishProxyError> {
        let now = current_unix_secs();
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let principal = connection
            .query_row(
                r#"
                SELECT
                    principal_id,
                    label,
                    allowed_pubkeys_json,
                    allowed_kinds_json,
                    allowed_relay_policies_json,
                    allow_request_relays,
                    job_visibility,
                    expires_at_unix
                FROM publish_proxy_principals
                WHERE token_hash = ?1
                  AND revoked_at_unix IS NULL
                  AND (expires_at_unix IS NULL OR expires_at_unix > ?2)
                "#,
                params![token_hash, now],
                principal_from_row,
            )
            .optional()?;
        Ok(principal)
    }

    pub fn principal_by_id(
        &self,
        principal_id: &str,
    ) -> Result<Option<PublishPrincipal>, PublishProxyError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let principal = connection
            .query_row(
                r#"
                SELECT
                    principal_id,
                    label,
                    allowed_pubkeys_json,
                    allowed_kinds_json,
                    allowed_relay_policies_json,
                    allow_request_relays,
                    job_visibility,
                    expires_at_unix
                FROM publish_proxy_principals
                WHERE principal_id = ?1
                "#,
                params![principal_id],
                principal_from_row,
            )
            .optional()?;
        Ok(principal)
    }

    pub fn record_publish_job(
        &self,
        insert: PublishJobInsert,
    ) -> Result<PublishEventResponse, PublishProxyError> {
        if let Some(idempotency_key) = insert.idempotency_key.as_deref() {
            if let Some(existing) =
                self.job_for_principal_id_and_key(insert.principal_id.as_str(), idempotency_key)?
            {
                return Ok(PublishEventResponse {
                    deduplicated: true,
                    job: existing,
                });
            }
        }

        let job_id = Uuid::new_v4().to_string();
        let now = current_unix_millis();
        let request_json = serde_json::to_string(&insert.request)?;
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let insert_result = connection.execute(
            r#"
            INSERT INTO publish_proxy_jobs (
                job_id,
                principal_id,
                idempotency_key,
                status,
                event_id,
                event_pubkey,
                event_kind,
                relay_policy_json,
                delivery_policy_json,
                requested_relay_count,
                request_json,
                requested_at_ms,
                updated_at_ms
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            "#,
            params![
                job_id,
                insert.principal_id,
                insert.idempotency_key,
                serde_json::to_string(&PublishJobStatus::Accepted)?,
                insert.request.event.id,
                insert.request.event.pubkey,
                insert.request.event.kind,
                serde_json::to_string(&insert.request.relay_policy)?,
                serde_json::to_string(&insert.request.delivery_policy)?,
                insert.request.relays.len(),
                request_json,
                now,
                now,
            ],
        );
        match insert_result {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                return Err(PublishProxyError::IdempotencyConflict(
                    "idempotency key conflicts with an existing publish job".to_owned(),
                ));
            }
            Err(error) => return Err(error.into()),
        }
        drop(connection);
        let job = self
            .job_by_id_for_principal_id(job_id.as_str(), insert.principal_id.as_str())?
            .ok_or_else(|| PublishProxyError::InvalidScope("created job missing".to_owned()))?;
        Ok(PublishEventResponse {
            deduplicated: false,
            job,
        })
    }

    pub fn job_by_id_for_principal(
        &self,
        job_id: &str,
        principal: &PublishPrincipal,
    ) -> Result<Option<PublishJobView>, PublishProxyError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sql = job_select_sql("WHERE job_id = ?1");
        let row = connection
            .query_row(sql.as_str(), params![job_id], job_from_row)
            .optional()?;
        drop(connection);
        let Some(mut job) = row else {
            return Ok(None);
        };
        if !principal.can_read_job(job.principal_id.as_str()) {
            return Ok(None);
        }
        job.view.relays = self.relay_outcomes(job.view.job_id.as_str())?;
        finalize_job_view(&mut job.view);
        Ok(Some(job.view))
    }

    pub fn list_jobs_for_principal(
        &self,
        principal: &PublishPrincipal,
        limit: usize,
    ) -> Result<Vec<PublishJobView>, PublishProxyError> {
        let limit = i64::try_from(limit.clamp(1, 200)).unwrap_or(200);
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sql = if principal.job_visibility == PublishJobVisibility::Admin {
            job_select_sql("ORDER BY requested_at_ms DESC, job_id DESC LIMIT ?1")
        } else {
            job_select_sql(
                "WHERE principal_id = ?1 ORDER BY requested_at_ms DESC, job_id DESC LIMIT ?2",
            )
        };
        let mut stmt = connection.prepare(sql.as_str())?;
        let rows = if principal.job_visibility == PublishJobVisibility::Admin {
            stmt.query_map(params![limit], job_from_row)?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(params![principal.principal_id, limit], job_from_row)?
                .collect::<Result<Vec<_>, _>>()?
        };
        drop(stmt);
        drop(connection);

        rows.into_iter()
            .map(|mut row| {
                row.view.relays = self.relay_outcomes(row.view.job_id.as_str())?;
                finalize_job_view(&mut row.view);
                Ok(row.view)
            })
            .collect()
    }

    fn job_for_principal_id_and_key(
        &self,
        principal_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<PublishJobView>, PublishProxyError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sql = job_select_sql("WHERE principal_id = ?1 AND idempotency_key = ?2");
        let row = connection
            .query_row(
                sql.as_str(),
                params![principal_id, idempotency_key],
                job_from_row,
            )
            .optional()?;
        drop(connection);
        let Some(mut job) = row else {
            return Ok(None);
        };
        job.view.relays = self.relay_outcomes(job.view.job_id.as_str())?;
        finalize_job_view(&mut job.view);
        Ok(Some(job.view))
    }

    fn job_by_id_for_principal_id(
        &self,
        job_id: &str,
        principal_id: &str,
    ) -> Result<Option<PublishJobView>, PublishProxyError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sql = job_select_sql("WHERE job_id = ?1 AND principal_id = ?2");
        let row = connection
            .query_row(sql.as_str(), params![job_id, principal_id], job_from_row)
            .optional()?;
        drop(connection);
        let Some(mut job) = row else {
            return Ok(None);
        };
        job.view.relays = self.relay_outcomes(job.view.job_id.as_str())?;
        finalize_job_view(&mut job.view);
        Ok(Some(job.view))
    }

    fn relay_outcomes(&self, job_id: &str) -> Result<Vec<PublishRelayOutcome>, PublishProxyError> {
        let connection = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut stmt = connection.prepare(
            r#"
            SELECT relay_url, source, attempted, outcome_kind, message, latency_ms
            FROM publish_proxy_relay_results
            WHERE job_id = ?1
            ORDER BY relay_url
            "#,
        )?;
        let outcomes = stmt
            .query_map(params![job_id], relay_outcome_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(outcomes)
    }
}

struct PublishJobRow {
    principal_id: String,
    view: PublishJobView,
}

fn job_select_sql(tail: &str) -> String {
    format!(
        r#"
        SELECT
            job_id,
            principal_id,
            status,
            event_id,
            event_pubkey,
            event_kind,
            relay_policy_json,
            delivery_policy_json,
            requested_relay_count,
            requested_at_ms,
            completed_at_ms,
            last_error
        FROM publish_proxy_jobs
        {tail}
        "#
    )
}

fn principal_from_row(row: &Row<'_>) -> Result<PublishPrincipal, rusqlite::Error> {
    let visibility: String = row.get(6)?;
    Ok(PublishPrincipal {
        principal_id: row.get(0)?,
        label: row.get(1)?,
        allowed_pubkeys: json_column(row, 2)?,
        allowed_kinds: json_column(row, 3)?,
        allowed_relay_policies: json_column(row, 4)?,
        allow_request_relays: row.get(5)?,
        job_visibility: PublishJobVisibility::from_str(visibility.as_str())
            .map_err(|error| conversion_error(6, error))?,
        expires_at_unix: row.get(7)?,
    })
}

fn job_from_row(row: &Row<'_>) -> Result<PublishJobRow, rusqlite::Error> {
    let status: PublishJobStatus = json_text(row, 2)?;
    let relay_policy: PublishRelayPolicy = json_text(row, 6)?;
    let delivery_policy: PublishDeliveryPolicy = json_text(row, 7)?;
    let relay_count: i64 = row.get(8)?;
    Ok(PublishJobRow {
        principal_id: row.get(1)?,
        view: PublishJobView {
            job_id: row.get(0)?,
            status,
            terminal: false,
            delivery_satisfied: false,
            event_id: row.get(3)?,
            pubkey: row.get(4)?,
            event_kind: row.get::<_, i64>(5)? as u32,
            relay_policy,
            delivery_policy,
            relay_count: usize::try_from(relay_count).unwrap_or(0),
            acknowledged_count: 0,
            retryable_count: 0,
            terminal_count: 0,
            requested_at_ms: row.get(9)?,
            completed_at_ms: row.get(10)?,
            last_error: row.get(11)?,
            relays: Vec::new(),
        },
    })
}

fn relay_outcome_from_row(row: &Row<'_>) -> Result<PublishRelayOutcome, rusqlite::Error> {
    let source: PublishRelaySource = json_text(row, 1)?;
    let outcome_kind: PublishRelayOutcomeKind = json_text(row, 3)?;
    Ok(PublishRelayOutcome {
        relay_url: row.get(0)?,
        source,
        attempted: row.get(2)?,
        outcome_kind,
        message: row.get(4)?,
        latency_ms: row
            .get::<_, Option<i64>>(5)?
            .map(|latency| u64::try_from(latency).unwrap_or(0)),
    })
}

fn finalize_job_view(view: &mut PublishJobView) {
    view.acknowledged_count = view
        .relays
        .iter()
        .filter(|relay| relay.outcome_kind.counts_toward_quorum())
        .count();
    view.retryable_count = view
        .relays
        .iter()
        .filter(|relay| relay.outcome_kind.is_retryable())
        .count();
    view.terminal_count = view
        .relays
        .iter()
        .filter(|relay| relay.outcome_kind.is_terminal_failure())
        .count();
    view.terminal = matches!(
        view.status,
        PublishJobStatus::DeliverySatisfied
            | PublishJobStatus::DeliveryUnsatisfiedTerminal
            | PublishJobStatus::Rejected
    );
    view.delivery_satisfied = view.status == PublishJobStatus::DeliverySatisfied;
}

fn validate_principal_init(input: &PublishPrincipalInit) -> Result<(), PublishProxyError> {
    if input.label.trim().is_empty() {
        return Err(PublishProxyError::InvalidScope(
            "principal label must not be empty".to_owned(),
        ));
    }
    if !input.token_hash.starts_with(TOKEN_HASH_PREFIX) {
        return Err(PublishProxyError::InvalidScope(
            "principal token hash must use sha256 prefix".to_owned(),
        ));
    }
    if input.allowed_pubkeys.is_empty() {
        return Err(PublishProxyError::InvalidScope(
            "principal must include at least one allowed pubkey".to_owned(),
        ));
    }
    for pubkey in &input.allowed_pubkeys {
        ensure_lower_hex("allowed_pubkey", pubkey, 64)?;
    }
    if input.allowed_kinds.is_empty() {
        return Err(PublishProxyError::InvalidScope(
            "principal must include at least one allowed kind".to_owned(),
        ));
    }
    if input
        .allowed_kinds
        .iter()
        .any(|kind| *kind > u16::MAX as u32)
    {
        return Err(PublishProxyError::InvalidScope(
            "allowed kind exceeds publish proxy range".to_owned(),
        ));
    }
    if input.allowed_relay_policies.is_empty() {
        return Err(PublishProxyError::InvalidScope(
            "principal must include at least one allowed relay policy".to_owned(),
        ));
    }
    Ok(())
}

pub fn generate_bearer_token() -> String {
    let bytes: [u8; 32] = rand::random();
    format!("{TOKEN_PREFIX}{}", hex_lower(&bytes))
}

pub fn hash_bearer_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    format!("{TOKEN_HASH_PREFIX}{}", hex_lower(&hasher.finalize()))
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

pub fn parse_relay_policy(value: &str) -> Result<PublishRelayPolicy, PublishProxyError> {
    match value {
        "explicit_only" => Ok(PublishRelayPolicy::ExplicitOnly),
        "request_then_author_write_then_daemon_default" => {
            Ok(PublishRelayPolicy::RequestThenAuthorWriteThenDaemonDefault)
        }
        "author_write_then_daemon_default" => Ok(PublishRelayPolicy::AuthorWriteThenDaemonDefault),
        "daemon_default_only" => Ok(PublishRelayPolicy::DaemonDefaultOnly),
        other => Err(PublishProxyError::InvalidScope(format!(
            "unknown relay policy `{other}`"
        ))),
    }
}

pub fn write_token_file(path: &Path, token: &str) -> Result<(), PublishProxyError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(path)?;
    file.write_all(token.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

fn ensure_lower_hex(
    field: &str,
    value: &str,
    expected_len: usize,
) -> Result<(), PublishProxyError> {
    if value.len() == expected_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(PublishProxyError::InvalidScope(format!(
            "{field} must be {expected_len} lowercase hex characters"
        )))
    }
}

fn json_column<T: for<'de> Deserialize<'de>>(
    row: &Row<'_>,
    index: usize,
) -> Result<T, rusqlite::Error> {
    let value: String = row.get(index)?;
    serde_json::from_str(value.as_str()).map_err(|error| conversion_error(index, error))
}

fn json_text<T: for<'de> Deserialize<'de>>(
    row: &Row<'_>,
    index: usize,
) -> Result<T, rusqlite::Error> {
    let value: String = row.get(index)?;
    serde_json::from_str(value.as_str()).map_err(|error| conversion_error(index, error))
}

fn conversion_error<E>(index: usize, error: E) -> rusqlite::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
}

fn current_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

fn current_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{
        PublishJobInsert, PublishJobVisibility, PublishPrincipalInit, PublishProxyStore,
        generate_bearer_token, hash_bearer_token, parse_relay_policy,
    };
    use radroots_publish_proxy_protocol::{
        PublishDeliveryPolicy, PublishEventRequest, PublishRelayPolicy, SignedNostrEventWire,
    };

    fn event(pubkey: &str, kind: u32) -> SignedNostrEventWire {
        SignedNostrEventWire {
            id: "0".repeat(64),
            pubkey: pubkey.to_owned(),
            created_at: 1_700_000_000,
            kind,
            tags: vec![vec!["d".to_owned(), "listing-1".to_owned()]],
            content: "{}".to_owned(),
            sig: "1".repeat(128),
        }
    }

    fn request(pubkey: &str, kind: u32) -> PublishEventRequest {
        PublishEventRequest {
            event: event(pubkey, kind),
            relays: Vec::new(),
            relay_policy: PublishRelayPolicy::DaemonDefaultOnly,
            delivery_policy: PublishDeliveryPolicy::Any,
            idempotency_key: Some("idem-1".to_owned()),
            timeout_ms: None,
        }
    }

    #[test]
    fn token_generation_and_hashing_do_not_store_plaintext() {
        let token = generate_bearer_token();
        assert!(token.starts_with("rrd_pp_"));
        let hash = hash_bearer_token(token.as_str());
        assert!(hash.starts_with("sha256:"));
        assert!(!hash.contains(token.as_str()));
    }

    #[test]
    fn relay_policy_parser_accepts_contract_values() {
        assert_eq!(
            parse_relay_policy("explicit_only").expect("policy"),
            PublishRelayPolicy::ExplicitOnly
        );
        assert!(parse_relay_policy("unknown").is_err());
    }

    #[test]
    fn storage_authenticates_hashed_tokens_and_scopes_jobs() {
        let store = PublishProxyStore::memory().expect("store");
        let token = generate_bearer_token();
        let token_hash = hash_bearer_token(token.as_str());
        let principal = store
            .create_principal(PublishPrincipalInit {
                label: "tester".to_owned(),
                token_hash: token_hash.clone(),
                allowed_pubkeys: vec!["a".repeat(64)],
                allowed_kinds: vec![30_402],
                allowed_relay_policies: vec![PublishRelayPolicy::DaemonDefaultOnly],
                allow_request_relays: false,
                job_visibility: PublishJobVisibility::Own,
                expires_at_unix: None,
            })
            .expect("principal");
        assert_eq!(
            store
                .principal_for_token_hash(token_hash.as_str())
                .expect("lookup")
                .expect("principal")
                .principal_id,
            principal.principal_id
        );
        let denied = request("b".repeat(64).as_str(), 30_402);
        assert!(principal.allows_event(&denied).is_err());

        let accepted = request("a".repeat(64).as_str(), 30_402);
        principal.allows_event(&accepted).expect("scope");
        let response = store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id.clone(),
                idempotency_key: Some("idem-1".to_owned()),
                request: accepted.clone(),
            })
            .expect("record job");
        assert!(!response.deduplicated);
        let duplicate = store
            .record_publish_job(PublishJobInsert {
                principal_id: principal.principal_id.clone(),
                idempotency_key: Some("idem-1".to_owned()),
                request: accepted,
            })
            .expect("dedupe");
        assert!(duplicate.deduplicated);
        assert_eq!(duplicate.job.job_id, response.job.job_id);
        assert_eq!(
            store
                .list_jobs_for_principal(&principal, 50)
                .expect("jobs")
                .len(),
            1
        );
    }
}
