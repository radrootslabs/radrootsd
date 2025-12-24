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
        default_value = "config.toml",
        help = "Path to the daemon configuration file (defaults to config.toml)"
    )]
    pub config: PathBuf,

    #[arg(
        long,
        value_name = "PATH",
        value_hint = ValueHint::FilePath,
        help = "Path to the daemon identity file (json, txt, or raw 32-byte key; defaults to identity.json)",
    )]
    pub identity: Option<PathBuf>,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        help = "Allow generating a new identity file if missing; if not set and identity file is absent, the daemon will fail"
    )]
    pub allow_generate_identity: bool,
}
