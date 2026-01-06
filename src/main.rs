#![forbid(unsafe_code)]

use std::process::ExitCode;

use anyhow::Result;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(error = ?err, "Fatal error");
            eprintln!("Fatal error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    radrootsd::app::run().await
}
