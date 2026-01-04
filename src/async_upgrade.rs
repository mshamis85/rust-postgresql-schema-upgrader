use crate::{UpgraderError, PostgresUpgraderOptions};
#[cfg(feature = "tls")]
use crate::SslMode;
use crate::schema_loader::load_upgraders;
use crate::db_tracker::async_tracker::{init_upgraders_table, lock_upgraders_table, load_applied_upgraders, record_upgrader};
use crate::integrity::verify_integrity;

#[cfg(feature = "tokio-postgres")]
pub async fn upgrade_async(upgraders_folder: impl AsRef<std::path::Path>, connection_string: &str, options: &PostgresUpgraderOptions) -> Result<(), UpgraderError> {
    use tokio_postgres::NoTls;

    #[cfg(feature = "tls")]
    use crate::tls::create_tls_config;

    #[cfg(feature = "tls")]
    let mut client = match options.ssl_mode {
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
    let mut client = {
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

    // 1. Initialize Table (Independent Transaction)
    init_upgraders_table(&mut client, options.schema.as_deref()).await?;

    // 2. Load Upgraders from Files
    let upgraders = load_upgraders(upgraders_folder)?;

    loop {
        let transaction = client.transaction().await
            .map_err(|e| UpgraderError::ConnectionError(format!("Failed to start transaction: {}", e)))?;

        lock_upgraders_table(&transaction, options.schema.as_deref()).await?;

        let applied_upgraders = load_applied_upgraders(&transaction, options.schema.as_deref()).await?;

        // Verify Integrity
        verify_integrity(&upgraders, &applied_upgraders)?;

        let upgrader_to_apply = if applied_upgraders.len() < upgraders.len() {
             Some(&upgraders[applied_upgraders.len()])
        } else {
             None
        };

        if let Some(upgrader) = upgrader_to_apply {
            let sql = options.apply_schema_substitution(&upgrader.text);

            // Execute
            transaction.batch_execute(&sql)
                .await
                .map_err(|e| UpgraderError::ExecutionError(format!("Failed to execute upgrader {}: {}", upgrader.upgrader_id, e)))?;
            
            // Record
            record_upgrader(&transaction, options.schema.as_deref(), upgrader).await?;

            transaction.commit().await
                .map_err(|e| UpgraderError::ExecutionError(format!("Failed to commit transaction: {}", e)))?;
        } else {
            // All upgraders applied
            transaction.commit().await
                .map_err(|e| UpgraderError::ExecutionError(format!("Failed to commit transaction: {}", e)))?;
            break;
        }
    }

    Ok(())
}
