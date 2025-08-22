use anyhow::Result;
use radrootsd::{cli_args, config, run_radrootsd};
use tracing::info;

#[tokio::main]
async fn main() {
    if let Err(err) = setup().await {
        eprintln!("Fatal error: {err:#?}");
        std::process::exit(1);
    }
}

async fn setup() -> Result<()> {
    let (_args, settings): (cli_args, config::Settings) =
        radroots_runtime::parse_and_load_path(|a: &cli_args| Some(a.config.as_path()))?;

    radroots_runtime::init_with(&settings.config.logs_dir, None)?;

    info!("Starting radrootsd on {}", settings.config.rpc_addr);

    run_radrootsd(&settings).await
}
