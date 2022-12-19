mod config;

use clap::Parser;
use clap::Subcommand;
use log::error;

use seekr::version;
use seekr::BANNER;

use config::{IndexerConfig, ServerConfig};

pub const LOG: &str = "seekrd";

const INFO: &str = "
Seekrd is a server-side daemon Seekr.io, which manages
cluster and subscription data, orchestrates index scheduling.";

#[derive(Debug, Parser)]
#[clap(name = "Seekrd command-line interface")]
#[clap(about = INFO, before_help = BANNER, disable_version_flag = true, arg_required_else_help = true)]
struct AppOptions {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Server(ServerConfig),
    Indexer(IndexerConfig),
    Version,
}

#[actix_web::main]
async fn main() {
    let app = AppOptions::parse();

    let output = match app.command {
        Commands::Server(c) => seekr::server::run(c.into()).await,
        Commands::Indexer(c) => seekr::indexer::run(c.into()).await,
        Commands::Version => version::init(),
    };

    if let Err(e) = output {
        error!(target: LOG, "{}", e);
    }
}
