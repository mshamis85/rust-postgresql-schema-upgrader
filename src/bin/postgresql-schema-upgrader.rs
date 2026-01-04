use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use postgresql_schema_upgrader::{upgrade_async, PostgresUpgraderOptions, SslMode};
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Upgrade the database schema
    Upgrade(UpgradeArgs),
    /// Check the connection to the database
    CheckConnection(CheckConnectionArgs),
}

#[derive(Args)]
struct UpgradeArgs {
    #[command(flatten)]
    connection: ConnectionArgs,

    /// Path to the directory containing upgrade scripts
    #[arg(long, default_value = ".")]
    path: PathBuf,

    /// Target schema (optional)
    #[arg(long)]
    schema: Option<String>,

    /// Create schema if it does not exist
    #[arg(long, default_value_t = false)]
    create_schema: bool,

    /// Enable TLS (SSL)
    #[arg(long, default_value_t = false)]
    tls: bool,
}

#[derive(Args)]
struct CheckConnectionArgs {
    #[command(flatten)]
    connection: ConnectionArgs,

    /// Enable TLS (SSL)
    #[arg(long, default_value_t = false)]
    tls: bool,
}

#[derive(Args)]
struct ConnectionArgs {
    /// Full connection string
    #[arg(
        long,
        env = "DATABASE_URL",
        conflicts_with_all = ["host", "port", "user", "password", "database"]
    )]
    connection_string: Option<String>,

    #[arg(long, required_unless_present = "connection_string")]
    host: Option<String>,

    #[arg(long, default_value = "5432")]
    port: u16,

    #[arg(long, required_unless_present = "connection_string")]
    user: Option<String>,

    #[arg(long, env = "PGPASSWORD")]
    password: Option<String>,

    #[arg(long, required_unless_present = "connection_string")]
    database: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Commands::Upgrade(args) => {
            let connection_string = build_connection_string(&args.connection)?;
            
            let mut options_builder = PostgresUpgraderOptions::builder()
                .create_schema(args.create_schema);
            
            if let Some(schema) = args.schema {
                options_builder = options_builder.schema(schema);
            }

            if args.tls {
                #[cfg(feature = "tls")]
                {
                    options_builder = options_builder.ssl_mode(SslMode::Require);
                }
                #[cfg(not(feature = "tls"))]
                {
                    return Err(anyhow::anyhow!("TLS requested but 'tls' feature is not enabled"));
                }
            } else {
                 #[cfg(feature = "tls")]
                {
                    options_builder = options_builder.ssl_mode(SslMode::Disable);
                }
            }

            let options = options_builder.build();

            println!("Starting schema upgrade...");
            upgrade_async(args.path, &connection_string, &options).await?;
            println!("Schema upgrade completed successfully.");
        }
        Commands::CheckConnection(args) => {
            let connection_string = build_connection_string(&args.connection)?;
            check_connection(&connection_string, args.tls).await?;
        }
    }

    Ok(())
}

fn build_connection_string(args: &ConnectionArgs) -> Result<String> {
    if let Some(s) = &args.connection_string {
        return Ok(s.clone());
    }

    let host = args.host.as_ref().context("host required")?;
    let user = args.user.as_ref().context("user required")?;
    let dbname = args.database.as_ref().context("database required")?;
    let port = args.port;
    let password = args.password.as_deref().unwrap_or("");

    Ok(format!(
        "host='{}' port={} user='{}' password='{}' dbname='{}'",
        escape(host),
        port,
        escape(user),
        escape(password),
        escape(dbname)
    ))
}

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

async fn check_connection(conn_string: &str, tls: bool) -> Result<()> {
    println!("Checking connection...");

    if tls {
        #[cfg(feature = "tls")]
        {
            use rustls::ClientConfig;
            let root_store = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let config = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            let tls_connector = tokio_postgres_rustls::MakeRustlsConnect::new(config);

            let (client, connection) = tokio_postgres::connect(conn_string, tls_connector).await.context("Failed to connect with TLS")?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });
            client.simple_query("SELECT 1").await.context("Failed to execute query")?;
        }
        #[cfg(not(feature = "tls"))]
        {
            return Err(anyhow::anyhow!("TLS requested but 'tls' feature is not enabled"));
        }
    } else {
        let (client, connection) = tokio_postgres::connect(conn_string, tokio_postgres::NoTls).await.context("Failed to connect")?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        client.simple_query("SELECT 1").await.context("Failed to execute query")?;
    }

    println!("Connection successful!");
    Ok(())
}
