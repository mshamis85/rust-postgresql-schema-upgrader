mod common;

use common::PostgresContainer;
use postgres::{Client, NoTls};
use postgresql_schema_upgrader::{PostgresUpgraderOptions, upgrade_async, upgrade_blocking};
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn test_basic_flow_blocking() {
    let container = PostgresContainer::start();

    // Step 1: Initial Schema
    let options = PostgresUpgraderOptions::builder().build();
    upgrade_blocking(
        "tests/data/basic_flow_step1",
        &container.connection_string,
        &options,
    )
    .unwrap();

    // Verify DB
    let mut client = Client::connect(&container.connection_string, NoTls).unwrap();

    // Check foo table
    client
        .execute("SELECT * FROM foo", &[])
        .expect("Table foo should exist");

    // Check upgraders table
    let rows = client.query("SELECT * FROM \"$upgraders$\"", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("file_id"), 0);
    assert_eq!(rows[0].get::<_, i32>("upgrader_id"), 0);

    // Step 2: Add second file
    upgrade_blocking(
        "tests/data/basic_flow_step2",
        &container.connection_string,
        &options,
    )
    .unwrap();

    let rows = client.query("SELECT * FROM \"$upgraders$\"", &[]).unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_basic_flow_async() {
    let container = PostgresContainer::start();

    let options = PostgresUpgraderOptions::builder().build();
    upgrade_async(
        "tests/data/basic_flow_step1",
        &container.connection_string,
        &options,
    )
    .await
    .unwrap();

    // Verify DB using tokio-postgres
    let (client, connection) =
        tokio_postgres::connect(&container.connection_string, tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
        .execute("SELECT * FROM foo", &[])
        .await
        .expect("Table foo should exist");

    let rows = client
        .query("SELECT * FROM \"$upgraders$\"", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_schema_support() {
    let container = PostgresContainer::start();

    // Create schema first
    let mut client = Client::connect(&container.connection_string, NoTls).unwrap();
    client.execute("CREATE SCHEMA my_schema", &[]).unwrap();

    let options = PostgresUpgraderOptions::builder()
        .schema("my_schema")
        .build();

    upgrade_blocking(
        "tests/data/schema_support",
        &container.connection_string,
        &options,
    )
    .unwrap();

    // Verify table in schema
    client
        .execute("SELECT * FROM my_schema.foo", &[])
        .expect("Table my_schema.foo should exist");

    // Verify upgraders table in schema
    let rows = client
        .query("SELECT * FROM \"my_schema\".\"$upgraders$\"", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_concurrency() {
    let container = PostgresContainer::start();
    let folder = "tests/data/concurrency";

    let connection_string = Arc::new(container.connection_string.clone());

    let n_threads = 10;
    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(n_threads));

    for _ in 0..n_threads {
        let conn_str = connection_string.clone();
        let b = barrier.clone();

        handles.push(thread::spawn(move || {
            b.wait(); // Synchronize start
            let options = PostgresUpgraderOptions::builder().build();
            // We expect success because locks should serialize execution.
            // If the library was naive, it might fail with "relation already exists" or duplicate inserts.
            upgrade_blocking(folder, &conn_str, &options)
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

#[test]
fn test_schema_auto_create() {
    let container = PostgresContainer::start();
    let schema_name = "auto_created_schema";

    // Ensure schema does not exist yet
    let mut client = Client::connect(&container.connection_string, NoTls).unwrap();
    let rows = client
        .query(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = $1",
            &[&schema_name],
        )
        .unwrap();
    assert_eq!(rows.len(), 0, "Schema should not exist yet");

    let options = PostgresUpgraderOptions::builder()
        .schema(schema_name)
        .create_schema(true)
        .build();

    upgrade_blocking(
        "tests/data/schema_auto_create",
        &container.connection_string,
        &options,
    )
    .unwrap();

    // Verify schema exists
    let rows = client
        .query(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = $1",
            &[&schema_name],
        )
        .unwrap();
    assert_eq!(rows.len(), 1, "Schema should have been created");

    // Verify table in schema
    client
        .execute(&format!("SELECT * FROM {}.test_table", schema_name), &[])
        .expect("Table should exist in new schema");
}
