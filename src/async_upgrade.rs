#[cfg(feature = "tls")]
use crate::SslMode;
use crate::upgrade_macros::{do_await, run_upgrade_flow};
use crate::{PostgresUpgraderOptions, UpgraderError};

#[cfg(feature = "tokio-postgres")]
pub async fn upgrade_async(
    upgraders_folder: impl AsRef<std::path::Path>,
    connection_string: &str,
    options: &PostgresUpgraderOptions,
) -> Result<(), UpgraderError> {
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
        }
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

    run_upgrade_flow!(
        client,
        options,
        upgraders_folder,
        crate::db_tracker::async_tracker,
        do_await,
        &
    )
}
