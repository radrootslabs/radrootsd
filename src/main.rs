#![forbid(unsafe_code)]
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::process::ExitCode;

use anyhow::Result;

#[cfg(not(test))]
#[cfg_attr(coverage_nightly, coverage(off))]
#[tokio::main]
async fn main() -> ExitCode {
    exit_code_from_run(run().await)
}

#[cfg(test)]
fn main() -> ExitCode {
    exit_code_from_run(Ok(()))
}

fn exit_code_from_run(result: Result<()>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(error = ?err, "Fatal error");
            eprintln!("Fatal error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
static RUN_HOOK: std::sync::OnceLock<std::sync::Mutex<Option<Result<(), String>>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
fn run_hook() -> &'static std::sync::Mutex<Option<Result<(), String>>> {
    RUN_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn take_run_hook_result() -> Option<Result<(), String>> {
    run_hook()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

#[cfg(test)]
async fn run() -> Result<()> {
    if let Some(result) = take_run_hook_result() {
        return result.map_err(anyhow::Error::msg);
    }
    Err(anyhow::anyhow!("run hook not set"))
}

#[cfg(not(test))]
#[cfg_attr(coverage_nightly, coverage(off))]
async fn run() -> Result<()> {
    radrootsd::app::run().await
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::{exit_code_from_run, main, run, run_hook};
    use std::process::ExitCode;
    use std::sync::Mutex;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn test_guard() -> std::sync::MutexGuard<'static, ()> {
        let guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *run_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        guard
    }

    #[test]
    fn exit_code_from_run_maps_success_and_error() {
        assert_eq!(exit_code_from_run(Ok(())), ExitCode::SUCCESS);
        assert_eq!(
            exit_code_from_run(Err(anyhow::anyhow!("boom"))),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn main_returns_success_in_test_build() {
        assert_eq!(main(), ExitCode::SUCCESS);
    }

    #[tokio::test]
    async fn run_returns_error_when_hook_is_missing() {
        let _guard = test_guard();
        let err = run().await.expect_err("hook missing should error");
        let msg = format!("{err:#}");
        assert!(msg.contains("run hook not set"));
    }

    #[tokio::test]
    async fn run_uses_hook_result() {
        let _guard = test_guard();
        *run_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok(()));
        assert!(run().await.is_ok());
    }
}
