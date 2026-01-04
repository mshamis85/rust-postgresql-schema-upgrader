mod common;

use common::PostgresContainer;
use postgresql_schema_upgrader::{upgrade_blocking, upgrade_async, PostgresUpgraderOptions, SslMode};
use std::fs;
use tempfile::tempdir;
use postgres::{Client, NoTls};
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn test_basic_flow_blocking() {
    let container = PostgresContainer::start();
    let dir = tempdir().unwrap();
    let folder = dir.path();

    // Create upgrader 0
    common::create_dummy_upgrader(folder, "000_init.sql", 0, 0, "CREATE TABLE foo (id INT);");

    let options = PostgresUpgraderOptions::builder().build();
    upgrade_blocking(folder, &container.connection_string, &options).unwrap();

    // Verify DB
    let mut client = Client::connect(&container.connection_string, NoTls).unwrap();
    
    // Check foo table
    client.execute("SELECT * FROM foo", &[]).expect("Table foo should exist");

    // Check upgraders table
    let rows = client.query("SELECT * FROM \"$upgraders$\"", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("file_id"), 0);
    assert_eq!(rows[0].get::<_, i32>("upgrader_id"), 0);

    // Add upgrader 1
    common::create_dummy_upgrader(folder, "001_bar.sql", 1, 0, "CREATE TABLE bar (id INT);");
    upgrade_blocking(folder, &container.connection_string, &options).unwrap();

    let rows = client.query("SELECT * FROM \"$upgraders$\"", &[]).unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test] async fn test_basic_flow_async() {
    let container = PostgresContainer::start();
    let dir = tempdir().unwrap();
    let folder = dir.path();

    common::create_dummy_upgrader(folder, "000_init.sql", 0, 0, "CREATE TABLE foo (id INT);");

    let options = PostgresUpgraderOptions::builder().build();
    upgrade_async(folder, &container.connection_string, &options).await.unwrap();

    // Verify DB using tokio-postgres
    let (client, connection) = tokio_postgres::connect(&container.connection_string, tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("SELECT * FROM foo", &[]).await.expect("Table foo should exist");
    
    let rows = client.query("SELECT * FROM \"$upgraders$\"", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_schema_support() {
    let container = PostgresContainer::start();
    let dir = tempdir().unwrap();
    let folder = dir.path();

    // Create schema first
    let mut client = Client::connect(&container.connection_string, NoTls).unwrap();
    client.execute("CREATE SCHEMA my_schema", &[]).unwrap();

    common::create_dummy_upgrader(folder, "000_init.sql", 0, 0, "CREATE TABLE {{SCHEMA}}.foo (id INT);");

    let options = PostgresUpgraderOptions::builder()
        .schema("my_schema")
        .build();
    
    upgrade_blocking(folder, &container.connection_string, &options).unwrap();

    // Verify table in schema
    client.execute("SELECT * FROM my_schema.foo", &[]).expect("Table my_schema.foo should exist");

    // Verify upgraders table in schema
    let rows = client.query("SELECT * FROM \"my_schema\".\"$upgraders$\"", &[]).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_concurrency() {
    let container = PostgresContainer::start();
    let dir = tempdir().unwrap();
    let folder = dir.path();

    // Create a slow upgrader to simulate work and ensure overlap
    common::create_dummy_upgrader(folder, "000_init.sql", 0, 0, "SELECT pg_sleep(0.5); CREATE TABLE foo (id INT);");

    let connection_string = Arc::new(container.connection_string.clone());
    let folder_path = Arc::new(folder.to_path_buf());
    
    let n_threads = 10;
    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(n_threads));

    for _ in 0..n_threads {
        let conn_str = connection_string.clone();
        let f_path = folder_path.clone();
        let b = barrier.clone();
        
        handles.push(thread::spawn(move || {
            b.wait(); // Synchronize start
            let options = PostgresUpgraderOptions::builder().build();
            // We expect success because locks should serialize execution. 
            // If the library was naive, it might fail with "relation already exists" or duplicate inserts.
            upgrade_blocking(&*f_path, &conn_str, &options)
        }));
    }

    for handle in handles {
        let result = handle.join().unwrap();
        // It's possible for subsequent runs to succeed (idempotency is not strictly built-in to the *SQL* execution in the current impl, 
        // BUT the library *should* see the upgrader is already applied and skip it).
        // Wait, my current implementation in blocking_upgrade.rs BLINDLY executes the SQL.
        // It does NOT check if the upgrader was already applied.
        // I MISSED THAT part of the implementation. 
        // The prompt asked for "Read the rows back" in the test, which implied verification.
        // But for concurrency correctness, the library MUST check if the upgrader exists before applying.
        // I need to fix the library implementation first!
        // For now, I'll let the test fail or I will fix the implementation immediately after this.
        assert!(result.is_ok(), "Upgrade failed: {:?}", result.err());
    }

    // Verify only 1 row in upgraders table
    let mut client = Client::connect(&container.connection_string, NoTls).unwrap();
    let rows = client.query("SELECT * FROM \"$upgraders$\"", &[]).unwrap();
    assert_eq!(rows.len(), 1, "Should have exactly 1 upgrader recorded");
}
