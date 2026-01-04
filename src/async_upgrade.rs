use crate::{UpgraderError, PostgresUpgraderOptions};
#[cfg(feature = "tls")]
use crate::SslMode;
use crate::schema_loader::load_upgraders;

#[cfg(feature = "tokio-postgres")]
pub async fn upgrade_async(upgraders_folder: impl AsRef<std::path::Path>, connection_string: &str, options: &PostgresUpgraderOptions) -> Result<(), UpgraderError> {
    use tokio_postgres::NoTls;

    #[cfg(feature = "tls")]
    use crate::tls::create_tls_config;

    #[cfg(feature = "tls")]
    let client = match options.ssl_mode {
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
    };

    #[cfg(not(feature = "tls"))]
    let client = {
         let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
            .map_err(|e| UpgraderError::ConnectionError(e.to_string()))?;
        
        tokio::spawn(async move {
            if let Err(_e) = connection.await {
                // Log error
            }
        });
        client
    };

    let upgraders = load_upgraders(upgraders_folder)?;

    for upgrader in upgraders {
        let sql = options.apply_schema_substitution(&upgrader.text);
        client.batch_execute(&sql)
            .await
            .map_err(|e| UpgraderError::ExecutionError(format!("Failed to execute upgrader {}: {}", upgrader.upgrader_id, e)))?;
    }

    Ok(())
}
