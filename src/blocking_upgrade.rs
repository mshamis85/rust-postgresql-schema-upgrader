#[cfg(feature = "tls")]
use crate::SslMode;
use crate::{PostgresUpgraderOptions, UpgraderError};
use crate::upgrade_macros::{run_upgrade_flow, do_sync};

#[cfg(feature = "postgres")]
pub fn upgrade_blocking(
    upgraders_folder: impl AsRef<std::path::Path>,
    connection_string: &str,
    options: &PostgresUpgraderOptions,
) -> Result<(), UpgraderError> {
    use postgres::{Client, NoTls};

    #[cfg(feature = "tls")]
    use crate::tls::create_tls_config;

    #[cfg(feature = "tls")]
    let mut client = match options.ssl_mode {
        SslMode::Disable => Client::connect(connection_string, NoTls)
            .map_err(|e| UpgraderError::ConnectionError(e.to_string()))?,
        SslMode::Require => {
            let tls = create_tls_config()?;
            Client::connect(connection_string, tls)
                .map_err(|e| UpgraderError::ConnectionError(e.to_string()))?
        }
    };

    #[cfg(not(feature = "tls"))]
    let mut client = Client::connect(connection_string, NoTls)
        .map_err(|e| UpgraderError::ConnectionError(e.to_string()))?;

    run_upgrade_flow!(
        client,
        options,
        upgraders_folder,
        crate::db_tracker::blocking,
        do_sync,
        &mut
    )
}
