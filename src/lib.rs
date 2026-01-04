mod tls;
mod blocking;
mod async_upgrade;
mod schema_loader;

#[derive(Debug)]
pub enum UpgraderError {
    ConnectionError(String),
    ExecutionError(String),
    ConfigurationError(String),
    LoaderError(String),
}

impl std::fmt::Display for UpgraderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpgraderError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            UpgraderError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            UpgraderError::ConfigurationError(msg) => write!(f, "Configuration error: {}", msg),
            UpgraderError::LoaderError(msg) => write!(f, "Loader error: {}", msg),
        }
    }
}

impl std::error::Error for UpgraderError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Require,
}

#[cfg(feature = "postgres")]
pub use blocking::upgrade_blocking;

#[cfg(feature = "tokio-postgres")]
pub use async_upgrade::upgrade_async;