use std::path::PathBuf;

use clap::{Args as ClapArgs, Parser, Subcommand};
use radroots_runtime::RadrootsServiceCliArgs;

#[derive(Parser, Debug, Clone)]
#[command(
    about = env!("CARGO_PKG_DESCRIPTION"),
    author = env!("CARGO_PKG_AUTHORS"),
    version = env!("CARGO_PKG_VERSION")
)]
pub struct Args {
    #[command(flatten)]
    pub service: RadrootsServiceCliArgs,
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
    pub allowed_nostr_source_policy: Vec<String>,
    #[arg(long)]
    pub job_visibility: String,
    #[arg(long)]
    pub allow_request_targets: bool,
}
