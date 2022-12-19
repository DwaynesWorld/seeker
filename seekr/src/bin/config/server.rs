use clap::Args;

use seekr::logger::Level;

#[derive(Args, Debug)]
pub struct ServerConfig {
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

impl From<seekr::server::ServerConfig> for ServerConfig {
    fn from(c: seekr::server::ServerConfig) -> Self {
        Self {
            log: c.log,
            host: c.host,
            port: c.port,
        }
    }
}

impl From<ServerConfig> for seekr::server::ServerConfig {
    fn from(c: ServerConfig) -> Self {
        Self {
            log: c.log,
            host: c.host,
            port: c.port,
        }
    }
}
