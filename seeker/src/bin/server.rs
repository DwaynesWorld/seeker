use clap::Args;
use clap::Parser;
use clap::Subcommand;
use log::error;

use seeker::logger::Level;
use seeker::version;
use seeker::BANNER;

pub const LOG: &str = "seeker";

const INFO: &str = "TODO";

#[derive(Debug, Parser)]
#[clap(name = "Seeker command-line interface")]
#[clap(about = INFO, before_help = BANNER, disable_version_flag = true, arg_required_else_help = true)]
struct AppOptions {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Start the seeker server
    Server(ServerConfig),
    Version,
}

#[derive(Args, Debug)]
struct ServerConfig {
    #[clap(
        short,
        long,
        env = "SEEKER_LOG",
        default_value = "info",
        forbid_empty_values = true,
        help = "The logging level",
        value_enum
    )]
    /// The logging level
    pub log: Level,

    #[clap(
        long = "host",
        env = "SEEKER_HOST",
        default_value = "localhost",
        forbid_empty_values = true,
        help = "Host the server will bind to"
    )]
    /// Host where server will bind to
    pub host: String,

    #[clap(
        long = "port",
        env = "SEEKER_PORT",
        default_value = "5000",
        forbid_empty_values = true,
        help = "Port the server will bind to"
    )]
    /// Port where server will bind to
    pub port: u16,
}

impl From<seeker::server::ServerConfig> for ServerConfig {
    fn from(c: seeker::server::ServerConfig) -> Self {
        Self {
            log: c.log,
            host: c.host,
            port: c.port,
        }
    }
}

impl From<ServerConfig> for seeker::server::ServerConfig {
    fn from(c: ServerConfig) -> Self {
        Self {
            log: c.log,
            host: c.host,
            port: c.port,
        }
    }
}

#[actix_web::main]
async fn main() {
    let app = AppOptions::parse();

    let output = match app.command {
        Commands::Server(c) => seeker::server::run(c.into()).await,
        Commands::Version => version::init(),
    };

    if let Err(e) = output {
        error!(target: LOG, "{}", e);
    }
}
