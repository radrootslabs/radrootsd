use std::path::PathBuf;

use clap::{ArgAction, Args as ClapArgs, Parser, Subcommand, ValueHint};

#[derive(ClapArgs, Debug, Clone)]
pub struct ServiceCliArgs {
    #[arg(
        long,
        value_name = "PATH",
        value_hint = ValueHint::FilePath,
        help = "Path to the daemon configuration file; no implicit cwd-rooted default is used"
    )]
    pub config: Option<PathBuf>,
    #[arg(
        long,
        value_name = "PATH",
        value_hint = ValueHint::FilePath,
        help = "Path to the daemon encrypted identity envelope"
    )]
    pub identity: Option<PathBuf>,
    #[arg(
        long,
        action = ArgAction::SetTrue,
        help = "Allow generating a new encrypted identity when the configured path is missing"
    )]
    pub allow_generate_identity: bool,
}

#[derive(Parser, Debug, Clone)]
#[command(
    about = env!("CARGO_PKG_DESCRIPTION"),
    author = env!("CARGO_PKG_AUTHORS"),
    version = env!("CARGO_PKG_VERSION")
)]
pub struct Args {
    #[command(flatten)]
    pub service: ServiceCliArgs,
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    TransportPublish(TransportPublishCommand),
}

#[derive(ClapArgs, Debug, Clone)]
pub struct TransportPublishCommand {
    #[command(subcommand)]
    pub command: TransportPublishSubcommand,
}

#[derive(Subcommand, Debug, Clone)]
pub enum TransportPublishSubcommand {
    Principal(PrincipalCommand),
}

#[derive(ClapArgs, Debug, Clone)]
pub struct PrincipalCommand {
    #[command(subcommand)]
    pub command: PrincipalSubcommand,
}

#[derive(Subcommand, Debug, Clone)]
pub enum PrincipalSubcommand {
    Init(PrincipalInitArgs),
}

#[derive(ClapArgs, Debug, Clone)]
pub struct PrincipalInitArgs {
    #[arg(long)]
    pub label: String,
    #[arg(long)]
    pub token_file: PathBuf,
    #[arg(long)]
    pub allowed_pubkey: Vec<String>,
    #[arg(long)]
    pub allowed_kind: Vec<u32>,
    #[arg(long)]
    pub allowed_target_policy: Vec<String>,
    #[arg(long)]
    pub allowed_explicit_transport_kind: Vec<String>,
    #[arg(long)]
    pub allowed_nostr_source_policy: Vec<String>,
    #[arg(long)]
    pub job_visibility: String,
    #[arg(long)]
    pub allow_request_targets: bool,
}
