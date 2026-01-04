use crate::{UpgraderError, PostgresUpgraderOptions};
#[cfg(feature = "tls")]
use crate::SslMode;
use crate::schema_loader::load_upgraders;
use crate::db_tracker::blocking::{init_upgraders_table, lock_upgraders_table, load_applied_upgraders, record_upgrader, create_schema_if_needed};
use crate::integrity::verify_integrity;

#[cfg(feature = "postgres")]
pub fn upgrade_blocking(upgraders_folder: impl AsRef<std::path::Path>, connection_string: &str, options: &PostgresUpgraderOptions) -> Result<(), UpgraderError> {
    use postgres::{Client, NoTls};

    #[cfg(feature = "tls")]
    use crate::tls::create_tls_config;

    #[cfg(feature = "tls")]
    let mut client = match options.ssl_mode {
        SslMode::Disable => {
            Client::connect(connection_string, NoTls)
                .map_err(|e| UpgraderError::ConnectionError(e.to_string()))?
        },
        SslMode::Require => {
            let tls = create_tls_config()?;
            Client::connect(connection_string, tls)
                .map_err(|e| UpgraderError::ConnectionError(e.to_string()))?
        }
    };

    #[cfg(not(feature = "tls"))]
    let mut client = Client::connect(connection_string, NoTls)
        .map_err(|e| UpgraderError::ConnectionError(e.to_string()))?;

    // 0. Create Schema (Independent)
    if options.create_schema {
        if options.schema.is_none() {
            return Err(UpgraderError::ExecutionError("create_schema is enabled but no schema name is provided.".to_string()));
        }
        create_schema_if_needed(&mut client, options.schema.as_deref())?;
    }

    // 1. Initialize Table (Independent Transaction)
    init_upgraders_table(&mut client, options.schema.as_deref())?;

    // 2. Load Upgraders from Files
    let upgraders = load_upgraders(upgraders_folder)?;

    loop {
        let mut transaction = client.transaction()
            .map_err(|e| UpgraderError::ConnectionError(format!("Failed to start transaction: {}", e)))?;

        lock_upgraders_table(&mut transaction, options.schema.as_deref())?;

        let applied_upgraders = load_applied_upgraders(&mut transaction, options.schema.as_deref())?;

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
                .map_err(|e| UpgraderError::ExecutionError(format!("Failed to execute upgrader {}: {}", upgrader.upgrader_id, e)))?;
                
            // Record
            record_upgrader(&mut transaction, options.schema.as_deref(), upgrader)?;

            transaction.commit()
                .map_err(|e| UpgraderError::ExecutionError(format!("Failed to commit transaction: {}", e)))?;
        } else {
            // All upgraders applied
            transaction.commit()
                .map_err(|e| UpgraderError::ExecutionError(format!("Failed to commit transaction: {}", e)))?;
            break;
        }
    }

    Ok(())
}
