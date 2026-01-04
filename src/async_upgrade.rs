use crate::{UpgraderError, SslMode};

#[cfg(feature = "tokio-postgres")]
pub async fn upgrade_async(upgraders_folder: impl AsRef<std::path::Path>, connection_string: &str, ssl_mode: SslMode) -> Result<(), UpgraderError> {
    use tokio_postgres::NoTls;

    #[cfg(feature = "tls")]
    use crate::tls::create_tls_config;

    let client = match ssl_mode {
        SslMode::Disable => {
            let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
                .await
                .map_err(|e| UpgraderError::ConnectionError(e.to_string()))?;
            
            tokio::spawn(async move {
                if let Err(_e) = connection.await {
                    // Log error
                }
            });
            client
        },
        SslMode::Require => {
            #[cfg(feature = "tls")]
            {
                let tls = create_tls_config()?;
                let (client, connection) = tokio_postgres::connect(connection_string, tls)
                    .await
                    .map_err(|e| UpgraderError::ConnectionError(e.to_string()))?;
                
                tokio::spawn(async move {
                    if let Err(_e) = connection.await {
                        // Log error
                    }
                });
                client
            }
            #[cfg(not(feature = "tls"))]
            {
                 return Err(UpgraderError::ConfigurationError("TLS feature is not enabled. Enable the 'tls' feature to use SslMode::Require.".to_string()));
            }
        }
    };

    // Placeholder for upgrade logic
    client.execute("SELECT 1", &[])
        .await
        .map_err(|e| UpgraderError::ExecutionError(e.to_string()))?;

    Ok(())
}
