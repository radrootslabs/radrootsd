use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use serde::Serialize;

const RADROOTSD_RUNTIME_ID: &str = "radrootsd";
const DEFAULT_CONFIG_FILE_NAME: &str = "config.toml";
const DEFAULT_SERVICE_IDENTITY_FILE_NAME: &str = "identity.secret.json";
const TRANSPORT_PUBLISH_DATABASE_FILE_NAME: &str = "transport_publish.sqlite";
const RADROOTSD_PATHS_PROFILE_ENV: &str = "RADROOTSD_PATHS_PROFILE";
const RADROOTSD_PATHS_REPO_LOCAL_ROOT_ENV: &str = "RADROOTSD_PATHS_REPO_LOCAL_ROOT";
const RADROOTSD_DEFAULT_SHARED_SECRET_BACKEND: &str = "encrypted_file";
const RADROOTSD_ALLOWED_PROFILES: [&str; 3] = ["interactive_user", "service_host", "repo_local"];
const RADROOTSD_ALLOWED_SHARED_SECRET_BACKENDS: [&str; 1] = ["encrypted_file"];
const SUBORDINATE_PATH_OVERRIDE_SOURCE: &str = "config_artifact";
const SUBORDINATE_PATH_OVERRIDE_KEYS: [&str; 2] = [
    "config.service.logs_dir",
    "config.transport_publish.database_path",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum Platform {
    Linux,
    Macos,
    Windows,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HostEnvironment {
    pub(crate) home_dir: Option<PathBuf>,
    pub(crate) appdata_dir: Option<PathBuf>,
    pub(crate) localappdata_dir: Option<PathBuf>,
    pub(crate) programdata_dir: Option<PathBuf>,
}

impl HostEnvironment {
    fn current() -> Self {
        Self {
            home_dir: std::env::var_os("HOME").map(PathBuf::from),
            appdata_dir: std::env::var_os("APPDATA").map(PathBuf::from),
            localappdata_dir: std::env::var_os("LOCALAPPDATA").map(PathBuf::from),
            programdata_dir: std::env::var_os("PROGRAMDATA").map(PathBuf::from),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathProfile {
    InteractiveUser,
    ServiceHost,
    RepoLocal,
}

impl PathProfile {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "interactive_user" => Ok(Self::InteractiveUser),
            "service_host" => Ok(Self::ServiceHost),
            "repo_local" => Ok(Self::RepoLocal),
            _ => bail!("unknown radrootsd path profile `{value}`"),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::InteractiveUser => "interactive_user",
            Self::ServiceHost => "service_host",
            Self::RepoLocal => "repo_local",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PathResolver {
    platform: Platform,
    environment: HostEnvironment,
}

impl PathResolver {
    pub(crate) const fn new(platform: Platform, environment: HostEnvironment) -> Self {
        Self {
            platform,
            environment,
        }
    }

    pub(crate) fn current() -> Self {
        #[cfg(target_os = "windows")]
        let platform = Platform::Windows;
        #[cfg(target_os = "macos")]
        let platform = Platform::Macos;
        #[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
        let platform = Platform::Linux;
        Self::new(platform, HostEnvironment::current())
    }

    fn roots(&self, profile: PathProfile, repo_local_root: Option<&Path>) -> Result<RuntimeRoots> {
        match profile {
            PathProfile::RepoLocal => repo_local_root
                .map(RuntimeRoots::from_base)
                .ok_or_else(|| anyhow::anyhow!("repo_local requires an explicit root")),
            PathProfile::ServiceHost => match self.platform {
                Platform::Linux | Platform::Macos => Ok(RuntimeRoots {
                    config: PathBuf::from("/etc/radroots"),
                    data: PathBuf::from("/var/lib/radroots"),
                    logs: PathBuf::from("/var/log/radroots"),
                    secrets: PathBuf::from("/etc/radroots/secrets"),
                }),
                Platform::Windows => {
                    let base = self
                        .environment
                        .programdata_dir
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("PROGRAMDATA is required"))?
                        .join("Radroots");
                    Ok(RuntimeRoots {
                        config: base.join("config"),
                        data: base.join("data"),
                        logs: base.join("logs"),
                        secrets: base.join("secrets"),
                    })
                }
            },
            PathProfile::InteractiveUser => match self.platform {
                Platform::Linux | Platform::Macos => {
                    let base = self
                        .environment
                        .home_dir
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("HOME is required"))?
                        .join(".radroots");
                    Ok(RuntimeRoots::from_base(base.as_path()))
                }
                Platform::Windows => {
                    let roaming = self
                        .environment
                        .appdata_dir
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("APPDATA is required"))?
                        .join("Radroots");
                    let local = self
                        .environment
                        .localappdata_dir
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("LOCALAPPDATA is required"))?
                        .join("Radroots");
                    Ok(RuntimeRoots {
                        config: roaming.join("config"),
                        data: local.join("data"),
                        logs: local.join("logs"),
                        secrets: roaming.join("secrets"),
                    })
                }
            },
        }
    }
}

#[derive(Debug, Clone)]
struct RuntimeRoots {
    config: PathBuf,
    data: PathBuf,
    logs: PathBuf,
    secrets: PathBuf,
}

impl RuntimeRoots {
    fn from_base(base: &Path) -> Self {
        Self {
            config: base.join("config"),
            data: base.join("data"),
            logs: base.join("logs"),
            secrets: base.join("secrets"),
        }
    }

    fn service(self) -> Self {
        Self {
            config: self.config.join("services").join(RADROOTSD_RUNTIME_ID),
            data: self.data.join("services").join(RADROOTSD_RUNTIME_ID),
            logs: self.logs.join("services").join(RADROOTSD_RUNTIME_ID),
            secrets: self.secrets.join("services").join(RADROOTSD_RUNTIME_ID),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimePathSelection {
    pub(crate) profile: PathProfile,
    pub(crate) repo_local_root: Option<PathBuf>,
    profile_source: String,
    repo_local_root_source: Option<String>,
}

impl RuntimePathSelection {
    #[cfg(test)]
    pub(crate) fn caller(profile: PathProfile, repo_local_root: Option<PathBuf>) -> Self {
        Self {
            profile,
            repo_local_root,
            profile_source: "caller".to_owned(),
            repo_local_root_source: None,
        }
    }

    fn from_env() -> Result<Self> {
        let profile_value = std::env::var(RADROOTSD_PATHS_PROFILE_ENV).ok();
        let profile = profile_value
            .as_deref()
            .map(PathProfile::parse)
            .transpose()?
            .unwrap_or(PathProfile::InteractiveUser);
        let repo_local_root = std::env::var_os(RADROOTSD_PATHS_REPO_LOCAL_ROOT_ENV)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        if profile == PathProfile::RepoLocal && repo_local_root.is_none() {
            bail!("{RADROOTSD_PATHS_REPO_LOCAL_ROOT_ENV} is required for repo_local");
        }
        Ok(Self {
            profile,
            repo_local_root,
            profile_source: if profile_value.is_some() {
                RADROOTSD_PATHS_PROFILE_ENV.to_owned()
            } else {
                "default".to_owned()
            },
            repo_local_root_source: std::env::var_os(RADROOTSD_PATHS_REPO_LOCAL_ROOT_ENV)
                .map(|_| RADROOTSD_PATHS_REPO_LOCAL_ROOT_ENV.to_owned()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RadrootsdRuntimePathOverrideContractOutput {
    pub profile_source: String,
    pub root_source: String,
    pub repo_local_root: Option<PathBuf>,
    pub repo_local_root_source: Option<String>,
    pub subordinate_path_override_source: String,
    pub subordinate_path_override_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RadrootsdRuntimePaths {
    pub(crate) config_path: PathBuf,
    pub(crate) logs_dir: PathBuf,
    pub(crate) identity_path: PathBuf,
    pub(crate) transport_publish_database_path: PathBuf,
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
    pub canonical_transport_publish_database_path: PathBuf,
}

pub(crate) fn process_path_selection() -> Result<(PathProfile, Option<PathBuf>)> {
    let selection = RuntimePathSelection::from_env()?;
    Ok((selection.profile, selection.repo_local_root))
}

pub(crate) fn resolve_runtime_paths_with_resolver(
    resolver: &PathResolver,
    profile: PathProfile,
    repo_local_root: Option<&Path>,
) -> Result<RadrootsdRuntimePaths> {
    let roots = resolver.roots(profile, repo_local_root)?.service();
    Ok(RadrootsdRuntimePaths {
        config_path: roots.config.join(DEFAULT_CONFIG_FILE_NAME),
        logs_dir: roots.logs,
        identity_path: roots.secrets.join(DEFAULT_SERVICE_IDENTITY_FILE_NAME),
        transport_publish_database_path: roots.data.join(TRANSPORT_PUBLISH_DATABASE_FILE_NAME),
    })
}

pub(crate) fn default_runtime_paths_for_process() -> Result<RadrootsdRuntimePaths> {
    let (profile, repo_local_root) = process_path_selection()?;
    resolve_runtime_paths_with_resolver(
        &PathResolver::current(),
        profile,
        repo_local_root.as_deref(),
    )
}

pub(crate) fn default_transport_publish_database_path() -> PathBuf {
    default_runtime_paths_for_process()
        .expect("resolve canonical radrootsd runtime paths")
        .transport_publish_database_path
}

pub fn default_config_path_for_process() -> Result<PathBuf> {
    Ok(default_runtime_paths_for_process()?.config_path)
}

pub fn default_identity_path_for_process() -> Result<PathBuf> {
    Ok(default_runtime_paths_for_process()?.identity_path)
}

pub fn runtime_contract_for_process() -> Result<RadrootsdRuntimeContractOutput> {
    let selection = RuntimePathSelection::from_env()?;
    runtime_contract_with_selection(&PathResolver::current(), &selection)
}

pub(crate) fn runtime_contract_with_selection(
    resolver: &PathResolver,
    selection: &RuntimePathSelection,
) -> Result<RadrootsdRuntimeContractOutput> {
    let paths = resolve_runtime_paths_with_resolver(
        resolver,
        selection.profile,
        selection.repo_local_root.as_deref(),
    )?;
    Ok(RadrootsdRuntimeContractOutput {
        active_profile: selection.profile.as_str().to_owned(),
        allowed_profiles: RADROOTSD_ALLOWED_PROFILES
            .into_iter()
            .map(str::to_owned)
            .collect(),
        path_overrides: RadrootsdRuntimePathOverrideContractOutput {
            profile_source: selection.profile_source.clone(),
            root_source: if selection.profile == PathProfile::RepoLocal {
                "explicit_repo_local_root".to_owned()
            } else {
                "host_defaults".to_owned()
            },
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
        canonical_transport_publish_database_path: paths.transport_publish_database_path,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        HostEnvironment, PathProfile, PathResolver, Platform, RadrootsdRuntimeContractOutput,
        RuntimePathSelection, default_config_path_for_process, runtime_contract_for_process,
        runtime_contract_with_selection,
    };

    fn linux_resolver() -> PathResolver {
        PathResolver::new(
            Platform::Linux,
            HostEnvironment {
                home_dir: Some(PathBuf::from("/home/treesap")),
                ..HostEnvironment::default()
            },
        )
    }

    #[test]
    fn process_path_entrypoints_remain_linked_in_test_builds() {
        let _default_config_path: fn() -> anyhow::Result<PathBuf> = default_config_path_for_process;
        let _runtime_contract: fn() -> anyhow::Result<RadrootsdRuntimeContractOutput> =
            runtime_contract_for_process;
    }

    #[test]
    fn runtime_contract_output_contains_canonical_runtime_paths() {
        let contract = runtime_contract_with_selection(
            &linux_resolver(),
            &RuntimePathSelection::caller(PathProfile::InteractiveUser, None),
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
            contract.canonical_transport_publish_database_path,
            PathBuf::from(
                "/home/treesap/.radroots/data/services/radrootsd/transport_publish.sqlite"
            )
        );
    }
}
