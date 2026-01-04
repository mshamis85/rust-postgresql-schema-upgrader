use crate::{UpgraderError, PostgresUpgraderOptions};
#[cfg(feature = "tls")]
use crate::SslMode;
use crate::schema_loader::load_upgraders;

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

    let upgraders = load_upgraders(upgraders_folder)?;

    for upgrader in upgraders {
        let sql = options.apply_schema_substitution(&upgrader.text);
        // Placeholder for real execution logic (tracking table checks, etc.)
        // For now, we just execute the SQL blindly which is NOT SAFE for production but matches current progress.
        // real implementation would check if upgrader already ran.
        client.batch_execute(&sql)
            .map_err(|e| UpgraderError::ExecutionError(format!("Failed to execute upgrader {}: {}", upgrader.upgrader_id, e)))?;
    }

    Ok(())
}
