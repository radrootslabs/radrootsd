use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use radroots_runtime_paths::{
    DEFAULT_CONFIG_FILE_NAME, DEFAULT_SERVICE_IDENTITY_FILE_NAME, RadrootsPathOverrides,
    RadrootsPathProfile, RadrootsPathResolver, RadrootsRuntimeNamespace,
};
use serde::Serialize;

const RADROOTSD_RUNTIME_ID: &str = "radrootsd";
const BRIDGE_STATE_DIR_NAME: &str = "bridge";
const BRIDGE_STATE_FILE_NAME: &str = "bridge-jobs.json";
const RADROOTSD_PATHS_PROFILE_ENV: &str = "RADROOTSD_PATHS_PROFILE";
const RADROOTSD_PATHS_REPO_LOCAL_ROOT_ENV: &str = "RADROOTSD_PATHS_REPO_LOCAL_ROOT";
const RADROOTSD_DEFAULT_SHARED_SECRET_BACKEND: &str = "encrypted_file";
const RADROOTSD_ALLOWED_PROFILES: [&str; 3] = ["interactive_user", "service_host", "repo_local"];
const RADROOTSD_ALLOWED_SHARED_SECRET_BACKENDS: [&str; 1] = ["encrypted_file"];
const SUBORDINATE_PATH_OVERRIDE_SOURCE: &str = "config_artifact";
const SUBORDINATE_PATH_OVERRIDE_KEYS: [&str; 2] =
    ["config.service.logs_dir", "config.bridge.state_path"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RadrootsdRuntimePaths {
    pub(crate) config_path: PathBuf,
    pub(crate) logs_dir: PathBuf,
    pub(crate) identity_path: PathBuf,
    pub(crate) bridge_state_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RadrootsdRuntimeContractOutput {
    pub active_profile: String,
    pub allowed_profiles: Vec<String>,
    pub path_overrides: RadrootsdRuntimePathOverrideContractOutput,
    pub default_shared_secret_backend: String,
    pub allowed_shared_secret_backends: Vec<String>,
    pub canonical_config_path: PathBuf,
    pub canonical_logs_dir: PathBuf,
    pub canonical_identity_path: PathBuf,
    pub canonical_bridge_state_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RadrootsdRuntimePathOverrideContractOutput {
    pub profile_source: String,
    pub root_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo_local_root: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo_local_root_source: Option<String>,
    pub subordinate_path_override_source: String,
    pub subordinate_path_override_keys: Vec<String>,
}

struct RadrootsdRuntimePathSelection {
    profile: RadrootsPathProfile,
    profile_source: String,
    repo_local_root: Option<PathBuf>,
    repo_local_root_source: Option<String>,
}

fn parse_path_profile(value: &str) -> Result<RadrootsPathProfile> {
    match value {
        "interactive_user" => Ok(RadrootsPathProfile::InteractiveUser),
        "service_host" => Ok(RadrootsPathProfile::ServiceHost),
        "repo_local" => Ok(RadrootsPathProfile::RepoLocal),
        _ => bail!(
            "{RADROOTSD_PATHS_PROFILE_ENV} must be `interactive_user`, `service_host`, or `repo_local`"
        ),
    }
}

pub(crate) fn process_path_selection() -> Result<(RadrootsPathProfile, Option<PathBuf>)> {
    let selection = process_path_selection_with_sources()?;
    Ok((selection.profile, selection.repo_local_root))
}

fn process_path_selection_with_sources() -> Result<RadrootsdRuntimePathSelection> {
    let profile = match std::env::var(RADROOTSD_PATHS_PROFILE_ENV) {
        Ok(value) => (
            parse_path_profile(&value)?,
            format!("process_env:{RADROOTSD_PATHS_PROFILE_ENV}"),
        ),
        Err(std::env::VarError::NotPresent) => {
            (RadrootsPathProfile::InteractiveUser, "default".to_owned())
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("{RADROOTSD_PATHS_PROFILE_ENV} must be valid utf-8 when set")
        }
    };
    let repo_local_root_raw = std::env::var_os(RADROOTSD_PATHS_REPO_LOCAL_ROOT_ENV);
    let repo_local_root = repo_local_root_raw.as_ref().map(PathBuf::from);
    Ok(RadrootsdRuntimePathSelection {
        profile: profile.0,
        profile_source: profile.1,
        repo_local_root,
        repo_local_root_source: repo_local_root_raw
            .as_ref()
            .map(|_| format!("process_env:{RADROOTSD_PATHS_REPO_LOCAL_ROOT_ENV}")),
    })
}

fn path_overrides_for(
    profile: RadrootsPathProfile,
    repo_local_root: Option<&Path>,
) -> Result<RadrootsPathOverrides> {
    match profile {
        RadrootsPathProfile::RepoLocal => {
            let repo_local_root = repo_local_root.context(format!(
                "{RADROOTSD_PATHS_REPO_LOCAL_ROOT_ENV} must be set when {RADROOTSD_PATHS_PROFILE_ENV}=repo_local"
            ))?;
            Ok(RadrootsPathOverrides::repo_local(repo_local_root))
        }
        _ => Ok(RadrootsPathOverrides::default()),
    }
}

pub(crate) fn resolve_runtime_paths_with_resolver(
    resolver: &RadrootsPathResolver,
    profile: RadrootsPathProfile,
    repo_local_root: Option<&Path>,
) -> Result<RadrootsdRuntimePaths> {
    let namespace = RadrootsRuntimeNamespace::service(RADROOTSD_RUNTIME_ID)
        .map_err(|error| anyhow::anyhow!("resolve radrootsd namespace: {error}"))?;
    let overrides = path_overrides_for(profile, repo_local_root)?;
    let namespaced = resolver
        .resolve(profile, &overrides)
        .map_err(|error| anyhow::anyhow!("resolve radrootsd runtime paths: {error}"))?
        .namespaced(&namespace);
    Ok(RadrootsdRuntimePaths {
        config_path: namespaced.config.join(DEFAULT_CONFIG_FILE_NAME),
        logs_dir: namespaced.logs,
        identity_path: namespaced.secrets.join(DEFAULT_SERVICE_IDENTITY_FILE_NAME),
        bridge_state_path: namespaced
            .data
            .join(BRIDGE_STATE_DIR_NAME)
            .join(BRIDGE_STATE_FILE_NAME),
    })
}

pub(crate) fn default_runtime_paths_for_process() -> Result<RadrootsdRuntimePaths> {
    let (profile, repo_local_root) = process_path_selection()?;
    resolve_runtime_paths_with_resolver(
        &RadrootsPathResolver::current(),
        profile,
        repo_local_root.as_deref(),
    )
}

pub(crate) fn default_bridge_state_path() -> PathBuf {
    default_runtime_paths_for_process()
        .expect("resolve canonical radrootsd runtime paths")
        .bridge_state_path
}

#[cfg_attr(test, allow(dead_code))]
pub fn default_config_path_for_process() -> Result<PathBuf> {
    Ok(default_runtime_paths_for_process()?.config_path)
}

pub fn default_identity_path_for_process() -> Result<PathBuf> {
    Ok(default_runtime_paths_for_process()?.identity_path)
}

#[cfg_attr(test, allow(dead_code))]
pub fn runtime_contract_for_process() -> Result<RadrootsdRuntimeContractOutput> {
    let selection = process_path_selection_with_sources()?;
    runtime_contract_with_selection(&RadrootsPathResolver::current(), &selection)
}

pub(crate) fn runtime_contract_with_resolver(
    resolver: &RadrootsPathResolver,
    profile: RadrootsPathProfile,
    repo_local_root: Option<&Path>,
) -> Result<RadrootsdRuntimeContractOutput> {
    runtime_contract_with_selection(
        resolver,
        &RadrootsdRuntimePathSelection {
            profile,
            profile_source: "caller".to_owned(),
            repo_local_root: repo_local_root.map(Path::to_path_buf),
            repo_local_root_source: repo_local_root.map(|_| "caller".to_owned()),
        },
    )
}

fn runtime_contract_with_selection(
    resolver: &RadrootsPathResolver,
    selection: &RadrootsdRuntimePathSelection,
) -> Result<RadrootsdRuntimeContractOutput> {
    let profile = selection.profile;
    let repo_local_root = selection.repo_local_root.as_deref();
    let paths = resolve_runtime_paths_with_resolver(resolver, profile, repo_local_root)?;
    Ok(RadrootsdRuntimeContractOutput {
        active_profile: profile.to_string(),
        allowed_profiles: RADROOTSD_ALLOWED_PROFILES
            .into_iter()
            .map(str::to_owned)
            .collect(),
        path_overrides: RadrootsdRuntimePathOverrideContractOutput {
            profile_source: selection.profile_source.clone(),
            root_source: root_source_for_profile(profile).to_owned(),
            repo_local_root: selection.repo_local_root.clone(),
            repo_local_root_source: selection.repo_local_root_source.clone(),
            subordinate_path_override_source: SUBORDINATE_PATH_OVERRIDE_SOURCE.to_owned(),
            subordinate_path_override_keys: SUBORDINATE_PATH_OVERRIDE_KEYS
                .into_iter()
                .map(str::to_owned)
                .collect(),
        },
        default_shared_secret_backend: RADROOTSD_DEFAULT_SHARED_SECRET_BACKEND.to_owned(),
        allowed_shared_secret_backends: RADROOTSD_ALLOWED_SHARED_SECRET_BACKENDS
            .into_iter()
            .map(str::to_owned)
            .collect(),
        canonical_config_path: paths.config_path,
        canonical_logs_dir: paths.logs_dir,
        canonical_identity_path: paths.identity_path,
        canonical_bridge_state_path: paths.bridge_state_path,
    })
}

fn root_source_for_profile(profile: RadrootsPathProfile) -> &'static str {
    match profile {
        RadrootsPathProfile::InteractiveUser => "host_defaults",
        RadrootsPathProfile::ServiceHost => "service_host_defaults",
        RadrootsPathProfile::RepoLocal => "repo_local_root",
        RadrootsPathProfile::MobileNative => "mobile_native_defaults",
    }
}
