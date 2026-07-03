use std::path::{Path, PathBuf};

use anyhow::Result;
use radroots_runtime_paths::{
    DEFAULT_CONFIG_FILE_NAME, DEFAULT_SERVICE_IDENTITY_FILE_NAME, RadrootsPathProfile,
    RadrootsPathResolver, RadrootsRuntimePathSelection, RadrootsRuntimeSelectionContract,
    RadrootsRuntimeSelectionOverrideContract,
};
use serde::Serialize;

const RADROOTSD_RUNTIME_ID: &str = "radrootsd";
const PUBLISH_PROXY_DATABASE_FILE_NAME: &str = "publish_proxy.sqlite";
const RADROOTSD_PATHS_PROFILE_ENV: &str = "RADROOTSD_PATHS_PROFILE";
const RADROOTSD_PATHS_REPO_LOCAL_ROOT_ENV: &str = "RADROOTSD_PATHS_REPO_LOCAL_ROOT";
const RADROOTSD_DEFAULT_SHARED_SECRET_BACKEND: &str = "encrypted_file";
const RADROOTSD_ALLOWED_PROFILES: [&str; 3] = ["interactive_user", "service_host", "repo_local"];
const RADROOTSD_ALLOWED_SHARED_SECRET_BACKENDS: [&str; 1] = ["encrypted_file"];
const SUBORDINATE_PATH_OVERRIDE_SOURCE: &str = "config_artifact";
const SUBORDINATE_PATH_OVERRIDE_KEYS: [&str; 2] = [
    "config.service.logs_dir",
    "config.publish_proxy.database_path",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RadrootsdRuntimePaths {
    pub(crate) config_path: PathBuf,
    pub(crate) logs_dir: PathBuf,
    pub(crate) identity_path: PathBuf,
    pub(crate) publish_proxy_database_path: PathBuf,
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
    pub canonical_publish_proxy_database_path: PathBuf,
}

pub type RadrootsdRuntimePathOverrideContractOutput = RadrootsRuntimeSelectionOverrideContract;

pub(crate) fn process_path_selection() -> Result<(RadrootsPathProfile, Option<PathBuf>)> {
    let selection = process_path_selection_with_sources()?;
    Ok((selection.profile, selection.repo_local_root))
}

fn process_path_selection_with_sources() -> Result<RadrootsRuntimePathSelection> {
    RadrootsRuntimePathSelection::from_env(
        RADROOTSD_PATHS_PROFILE_ENV,
        RADROOTSD_PATHS_REPO_LOCAL_ROOT_ENV,
        RadrootsPathProfile::InteractiveUser,
    )
    .map_err(|error| anyhow::anyhow!(error.to_string()))
}

pub(crate) fn resolve_runtime_paths_with_resolver(
    resolver: &RadrootsPathResolver,
    profile: RadrootsPathProfile,
    repo_local_root: Option<&Path>,
) -> Result<RadrootsdRuntimePaths> {
    let selection =
        RadrootsRuntimePathSelection::caller(profile, repo_local_root.map(Path::to_path_buf));
    let namespaced = selection
        .resolve_service_roots(
            resolver,
            RADROOTSD_RUNTIME_ID,
            RADROOTSD_PATHS_PROFILE_ENV,
            RADROOTSD_PATHS_REPO_LOCAL_ROOT_ENV,
        )
        .map_err(|error| anyhow::anyhow!("resolve radrootsd runtime paths: {error}"))?;
    Ok(RadrootsdRuntimePaths {
        config_path: namespaced.config.join(DEFAULT_CONFIG_FILE_NAME),
        logs_dir: namespaced.logs,
        identity_path: namespaced.secrets.join(DEFAULT_SERVICE_IDENTITY_FILE_NAME),
        publish_proxy_database_path: namespaced.data.join(PUBLISH_PROXY_DATABASE_FILE_NAME),
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

pub(crate) fn default_publish_proxy_database_path() -> PathBuf {
    default_runtime_paths_for_process()
        .expect("resolve canonical radrootsd runtime paths")
        .publish_proxy_database_path
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

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn runtime_contract_with_resolver(
    resolver: &RadrootsPathResolver,
    profile: RadrootsPathProfile,
    repo_local_root: Option<&Path>,
) -> Result<RadrootsdRuntimeContractOutput> {
    runtime_contract_with_selection(
        resolver,
        &RadrootsRuntimePathSelection::caller(profile, repo_local_root.map(Path::to_path_buf)),
    )
}

fn runtime_contract_with_selection(
    resolver: &RadrootsPathResolver,
    selection: &RadrootsRuntimePathSelection,
) -> Result<RadrootsdRuntimeContractOutput> {
    let profile = selection.profile;
    let repo_local_root = selection.repo_local_root.as_deref();
    let paths = resolve_runtime_paths_with_resolver(resolver, profile, repo_local_root)?;
    let base_contract: RadrootsRuntimeSelectionContract = selection.contract(
        &RADROOTSD_ALLOWED_PROFILES,
        SUBORDINATE_PATH_OVERRIDE_SOURCE,
        &SUBORDINATE_PATH_OVERRIDE_KEYS,
    );
    Ok(RadrootsdRuntimeContractOutput {
        active_profile: base_contract.active_profile,
        allowed_profiles: base_contract.allowed_profiles,
        path_overrides: base_contract.path_overrides,
        default_shared_secret_backend: RADROOTSD_DEFAULT_SHARED_SECRET_BACKEND.to_owned(),
        allowed_shared_secret_backends: RADROOTSD_ALLOWED_SHARED_SECRET_BACKENDS
            .into_iter()
            .map(str::to_owned)
            .collect(),
        canonical_config_path: paths.config_path,
        canonical_logs_dir: paths.logs_dir,
        canonical_identity_path: paths.identity_path,
        canonical_publish_proxy_database_path: paths.publish_proxy_database_path,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use radroots_runtime_paths::{
        RadrootsHostEnvironment, RadrootsPathProfile, RadrootsPathResolver, RadrootsPlatform,
    };

    use super::runtime_contract_with_resolver;

    fn linux_resolver() -> RadrootsPathResolver {
        RadrootsPathResolver::new(
            RadrootsPlatform::Linux,
            RadrootsHostEnvironment {
                home_dir: Some(PathBuf::from("/home/treesap")),
                ..RadrootsHostEnvironment::default()
            },
        )
    }

    #[test]
    fn runtime_contract_output_contains_canonical_runtime_paths() {
        let contract = runtime_contract_with_resolver(
            &linux_resolver(),
            RadrootsPathProfile::InteractiveUser,
            None,
        )
        .expect("contract");

        assert_eq!(contract.active_profile, "interactive_user");
        assert_eq!(
            contract.allowed_profiles,
            ["interactive_user", "service_host", "repo_local"]
        );
        assert_eq!(contract.path_overrides.root_source, "host_defaults");
        assert_eq!(
            contract.canonical_config_path,
            PathBuf::from("/home/treesap/.radroots/config/services/radrootsd/config.toml")
        );
        assert_eq!(
            contract.canonical_logs_dir,
            PathBuf::from("/home/treesap/.radroots/logs/services/radrootsd")
        );
        assert_eq!(
            contract.canonical_identity_path,
            PathBuf::from(
                "/home/treesap/.radroots/secrets/services/radrootsd/identity.secret.json"
            )
        );
        assert_eq!(
            contract.canonical_publish_proxy_database_path,
            PathBuf::from("/home/treesap/.radroots/data/services/radrootsd/publish_proxy.sqlite")
        );
    }
}
