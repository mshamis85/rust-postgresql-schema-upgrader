#[cfg(feature = "tokio-postgres")]
mod async_upgrade;
#[cfg(feature = "postgres")]
mod blocking_upgrade;
mod db_tracker;
mod error;
mod integrity;
mod options;
mod schema_loader;
mod tls;

pub use error::UpgraderError;
#[cfg(feature = "tls")]
pub use options::SslMode;
pub use options::{PostgresUpgraderOptions, PostgresUpgraderOptionsBuilder};

#[cfg(feature = "postgres")]
pub use blocking_upgrade::upgrade_blocking;

#[cfg(feature = "tokio-postgres")]
pub use async_upgrade::upgrade_async;
