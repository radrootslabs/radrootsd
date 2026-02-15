use clap::Parser;
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
}
