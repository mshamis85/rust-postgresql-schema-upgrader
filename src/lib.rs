mod error;
mod options;
mod tls;
mod blocking;
mod async_upgrade;
mod schema_loader;
mod db_tracker;

pub use error::UpgraderError;
pub use options::{PostgresUpgraderOptions, PostgresUpgraderOptionsBuilder};
#[cfg(feature = "tls")]
pub use options::SslMode;

#[cfg(feature = "postgres")]
pub use blocking::upgrade_blocking;

#[cfg(feature = "tokio-postgres")]
pub use async_upgrade::upgrade_async;
