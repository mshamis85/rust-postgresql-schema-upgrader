use crate::UpgraderError;

#[cfg(feature = "tls")]
pub fn create_tls_config() -> Result<tokio_postgres_rustls::MakeRustlsConnect, UpgraderError> {
    use rustls::ClientConfig;

    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config))
}
