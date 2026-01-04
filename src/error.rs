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
