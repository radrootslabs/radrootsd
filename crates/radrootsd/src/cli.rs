use std::path::PathBuf;

use clap::{Parser, ValueHint, command};

#[derive(Parser, Debug, Clone)]
#[command(
    about = env!("CARGO_PKG_DESCRIPTION"),
    author = env!("CARGO_PKG_AUTHORS"),
    version = env!("CARGO_PKG_VERSION")
)]
pub struct Args {
    #[arg(
        long,
        value_name = "PATH",
        value_hint = ValueHint::FilePath,
        default_value = "config.toml"
    )]
    pub config: PathBuf,
}
