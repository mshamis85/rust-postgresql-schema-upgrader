use crate::{UpgraderError, SslMode};

#[cfg(feature = "postgres")]
pub fn upgrade_blocking(connection_string: &str, ssl_mode: SslMode) -> Result<(), UpgraderError> {
    use postgres::{Client, NoTls};

    #[cfg(feature = "tls")]
    use crate::tls::create_tls_config;

    let mut client = match ssl_mode {
        SslMode::Disable => {
            Client::connect(connection_string, NoTls)
                .map_err(|e| UpgraderError::ConnectionError(e.to_string()))?
        },
        SslMode::Require => {
            #[cfg(feature = "tls")]
            {
                let tls = create_tls_config()?;
                Client::connect(connection_string, tls)
                    .map_err(|e| UpgraderError::ConnectionError(e.to_string()))?
            }
            #[cfg(not(feature = "tls"))]
            {
                return Err(UpgraderError::ConfigurationError("TLS feature is not enabled. Enable the 'tls' feature to use SslMode::Require.".to_string()));
            }
        }
    };

    // Placeholder for upgrade logic
    client.execute("SELECT 1", &[])
        .map_err(|e| UpgraderError::ExecutionError(e.to_string()))?;

    Ok(())
}
