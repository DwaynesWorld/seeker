#[macro_use]
extern crate log;

#[macro_use]
mod macros;

pub mod clusters;
pub mod error;
pub mod id;
pub mod indexer;
pub mod logger;
pub mod server;
pub mod session;
pub mod subscriptions;
pub mod version;

pub const BANNER: &str = "
   d888888o.   8 8888888888   8 8888888888   8 8888     ,88' 8 888888888o.
 .`8888:' `88. 8 8888         8 8888         8 8888    ,88'  8 8888    `88.
 8.`8888.   Y8 8 8888         8 8888         8 8888   ,88'   8 8888     `88
 `8.`8888.     8 8888         8 8888         8 8888  ,88'    8 8888     ,88
  `8.`8888.    8 888888888888 8 888888888888 8 8888 ,88'     8 8888.   ,88'
   `8.`8888.   8 8888         8 8888         8 8888 88'      8 888888888P'
    `8.`8888.  8 8888         8 8888         8 888888<       8 8888`8b
8b   `8.`8888. 8 8888         8 8888         8 8888 `Y8.     8 8888 `8b.
`8b.  ;8.`8888 8 8888         8 8888         8 8888   `Y8.   8 8888   `8b.
 `Y8888P ,88P' 8 888888888888 8 888888888888 8 8888     `Y8. 8 8888     `88.
";

// The name and version of this build
pub const PKG_NAME: &str = env!("CARGO_PKG_NAME");
pub const PKG_VERS: &str = env!("CARGO_PKG_VERSION");
pub const RUST_VERS: &str = env!("RUST_VERSION");
pub const GIT_VERS: &str = env!("GIT_VERSION");
pub const GIT_BRANCH: &str = env!("GIT_BRANCH");
pub const GIT_SHA: &str = env!("GIT_SHA");
