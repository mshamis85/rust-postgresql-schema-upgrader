mod error;
mod options;
mod tls;
#[cfg(feature = "postgres")]
mod blocking_upgrade;
#[cfg(feature = "tokio-postgres")]
mod async_upgrade;
mod schema_loader;
mod db_tracker;
mod integrity;

pub use error::UpgraderError;
pub use options::{PostgresUpgraderOptions, PostgresUpgraderOptionsBuilder};
#[cfg(feature = "tls")]
pub use options::SslMode;

#[cfg(feature = "postgres")]
pub use blocking_upgrade::upgrade_blocking;

#[cfg(feature = "tokio-postgres")]
pub use async_upgrade::upgrade_async;
