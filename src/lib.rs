#![forbid(unsafe_code)]
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::{error::Error, fmt};

mod app;
mod core;
mod host_nostr;
#[cfg_attr(coverage_nightly, coverage(off))]
mod transport;

/// Stable, redacted failure returned by the daemon entry point.
pub struct RadrootsdRunError {
    _private: (),
}

impl fmt::Debug for RadrootsdRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RadrootsdRunError")
    }
}

impl fmt::Display for RadrootsdRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("daemon execution failed")
    }
}

impl Error for RadrootsdRunError {}

/// Runs the daemon without exposing implementation-owned modules or errors.
pub async fn run_daemon() -> Result<(), RadrootsdRunError> {
    app::run()
        .await
        .map_err(|_source| RadrootsdRunError { _private: () })
}

pub const fn crate_name() -> &'static str {
    env!("CARGO_PKG_NAME")
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::error::Error;

    #[test]
    fn crate_name_matches_package() {
        assert_eq!(super::crate_name(), "radrootsd");
    }

    #[test]
    fn run_error_is_source_free_and_redacted() {
        let error = super::RadrootsdRunError { _private: () };
        assert_eq!(format!("{error}"), "daemon execution failed");
        assert_eq!(format!("{error:?}"), "RadrootsdRunError");
        assert!(error.source().is_none());
    }
}
