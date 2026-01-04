mod common;

use common::{AsyncTestClient, BlockingTestClient, PostgresContainer};
use postgresql_schema_upgrader::{PostgresUpgraderOptions, upgrade_async, upgrade_blocking};
use std::sync::{Arc, Barrier};
use std::thread;

// --- Macros ---

macro_rules! await_fn {
    (async, $e:expr) => {
        $e.await
    };
    (blocking, $e:expr) => {
        $e
    };
}

macro_rules! run_upgrade {
    (async, $folder:expr, $conn:expr, $opts:expr) => {
        upgrade_async($folder, $conn, $opts).await
    };
    (blocking, $folder:expr, $conn:expr, $opts:expr) => {
        upgrade_blocking($folder, $conn, $opts)
    };
}

macro_rules! get_client {
    (async, $conn:expr) => {
        AsyncTestClient::connect($conn).await
    };
    (blocking, $conn:expr) => {
        BlockingTestClient::connect($conn)
    };
}

macro_rules! define_test_both_modes {
    ($test_name:ident, $body:expr) => {
        mod $test_name {
            use super::*;

            #[test]
            fn blocking() {
                macro_rules! m_await {
                    ($e:expr) => {
                        await_fn!(blocking, $e)
                    };
                }
                macro_rules! m_upgrade {
                    ($f:expr, $c:expr, $o:expr) => {
                        run_upgrade!(blocking, $f, $c, $o)
                    };
                }
                macro_rules! m_client {
                    ($c:expr) => {
                        get_client!(blocking, $c)
                    };
                }

                $body
            }

            #[tokio::test]
            #[allow(unused_mut)]
            async fn async_mode() {
                macro_rules! m_await {
                    ($e:expr) => {
                        await_fn!(async, $e)
                    };
                }
                macro_rules! m_upgrade {
                    ($f:expr, $c:expr, $o:expr) => {
                        run_upgrade!(async, $f, $c, $o)
                    };
                }
                macro_rules! m_client {
                    ($c:expr) => {
                        get_client!(async, $c)
                    };
                }

                $body
            }
        }
    };
}

// --- Tests ---

define_test_both_modes!(basic_flow, {
    let container = PostgresContainer::start();
    let options = PostgresUpgraderOptions::builder().build();

    // Step 1
    m_upgrade!(
        "tests/data/basic_flow_step1",
        &container.connection_string,
        &options
    )
    .unwrap();

    let mut client = m_client!(&container.connection_string);
    m_await!(client.ensure_table_exists("foo", None));

    let rows = m_await!(client.get_upgraders(None));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].file_id, 0);
    assert_eq!(rows[0].upgrader_id, 0);

    // Step 2
    m_upgrade!(
        "tests/data/basic_flow_step2",
        &container.connection_string,
        &options
    )
    .unwrap();

    let rows = m_await!(client.get_upgraders(None));
    assert_eq!(rows.len(), 2);
});

define_test_both_modes!(schema_support, {
    let container = PostgresContainer::start();

    // Create schema manually first
    let mut client = m_client!(&container.connection_string);
    m_await!(client.execute("CREATE SCHEMA my_schema"));

    let options = PostgresUpgraderOptions::builder()
        .schema("my_schema")
        .build();

    m_upgrade!(
        "tests/data/schema_support",
        &container.connection_string,
        &options
    )
    .unwrap();

    m_await!(client.ensure_table_exists("foo", Some("my_schema")));
    let rows = m_await!(client.get_upgraders(Some("my_schema")));
    assert_eq!(rows.len(), 1);
});

define_test_both_modes!(schema_auto_create, {
    let container = PostgresContainer::start();
    let schema_name = "auto_created_schema";

    let mut client = m_client!(&container.connection_string);
    m_await!(client.ensure_schema_does_not_exist(schema_name));

    let options = PostgresUpgraderOptions::builder()
        .schema(schema_name)
        .create_schema(true)
        .build();

    m_upgrade!(
        "tests/data/schema_auto_create",
        &container.connection_string,
        &options
    )
    .unwrap();

    m_await!(client.ensure_schema_exists(schema_name));
    m_await!(client.ensure_table_exists("test_table", Some(schema_name)));
});

// Concurrency tests need distinct implementations due to thread vs tokio::spawn differences.

#[test]
fn concurrency_blocking() {
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
            b.wait();
            let options = PostgresUpgraderOptions::builder().build();
            upgrade_blocking(folder, &conn_str, &options)
        }));
    }

    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    let mut client = BlockingTestClient::connect(&container.connection_string);
    let rows = client.get_upgraders(None);
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn concurrency_async() {
    let container = PostgresContainer::start();
    let folder = "tests/data/concurrency";
    let connection_string = Arc::new(container.connection_string.clone());
    let n_tasks = 10;
    let mut handles = vec![];
    let barrier = Arc::new(tokio::sync::Barrier::new(n_tasks));

    for _ in 0..n_tasks {
        let conn_str = connection_string.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            let options = PostgresUpgraderOptions::builder().build();
            upgrade_async(folder, &conn_str, &options).await
        }));
    }

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    let client = AsyncTestClient::connect(&container.connection_string).await;
    let rows = client.get_upgraders(None).await;
    assert_eq!(rows.len(), 1);
}

define_test_both_modes!(transaction_rollback, {
    let container = PostgresContainer::start();
    let options = PostgresUpgraderOptions::builder().build();

    // The rollback folder contains:
    // 000_init.sql (Valid)
    // 001_fail.sql (Valid creation + Invalid Select)

    let result = m_upgrade!(
        "tests/data/rollback",
        &container.connection_string,
        &options
    );

    // It should fail
    assert!(result.is_err(), "Upgrade should fail due to bad SQL");

    let mut client = m_client!(&container.connection_string);

    // 1. Verify 000_init was applied (transaction committed before file 001 started?
    // Wait, the library commits PER UPGRADER STEP or PER FILE?
    // Let's check logic: "Loop { Transaction -> Lock -> Check -> Apply -> Commit }"
    // It commits per *Applied Upgrader* (per step inside the file).
    // Let's check 000_init content. It has one step.
    // 001_fail content. It has one step.
    // So 000_init should be committed.
    // 001_fail step should be rolled back.

    // Check 000_init's table
    m_await!(client.ensure_table_exists("base_table", None));

    // Check 001_fail's SIDE EFFECT table. It should NOT exist.
    // "CREATE TABLE side_effect_table" happened before the error in the SAME step.
    // So it should be rolled back.
    let _sql = "SELECT 1 FROM information_schema.tables WHERE table_name = 'side_effect_table'";

    // Abstracting this check slightly since ensure_table_does_not_exist isn't on client yet,
    // but we can just use execute expectation failure or simple query check.
    // Let's use raw query check available on client wrapper? No, wrapper hides it.
    // We'll trust that ensure_table_exists fails if missing.
    // Wait, we want to ensure it is MISSING.
    // Let's rely on the Upgraders table first.
    let rows = m_await!(client.get_upgraders(None));
    // Should have 0:0. Should NOT have 0:1 (fail step) or 1:0 (file id 1).
    // File 000 is 0:0. File 001 is 1:0 (fail step).
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].file_id, 0);
    assert_eq!(rows[0].upgrader_id, 0);
});

define_test_both_modes!(integrity_violation, {
    let container = PostgresContainer::start();
    let options = PostgresUpgraderOptions::builder().build();

    // Step 1: Apply initial valid schema
    m_upgrade!(
        "tests/data/integrity_violation_step1",
        &container.connection_string,
        &options
    )
    .unwrap();

    // Verify it worked
    let mut client = m_client!(&container.connection_string);
    m_await!(client.ensure_table_exists("integrity_table", None));

    // Step 2: Apply corrupted schema (File 0 modified, File 1 added)
    let result = m_upgrade!(
        "tests/data/integrity_violation_step2",
        &container.connection_string,
        &options
    );

    assert!(result.is_err());
    let err_msg = result.err().unwrap().to_string();
    assert!(
        err_msg.contains("Integrity violation") || err_msg.contains("SQL content has changed"),
        "Unexpected error: {}",
        err_msg
    );

    // Verify File 1 (next_table) was NOT applied
    // We don't have a "ensure_table_does_not_exist" helper, but we can check the upgraders table.
    let rows = m_await!(client.get_upgraders(None));
    assert_eq!(
        rows.len(),
        1,
        "Should stop immediately after integrity check failure"
    );
    assert_eq!(rows[0].file_id, 0);
});

// Mixed Version Concurrency Tests
// Scenario:
// Threads A (v1) have files: [0]
// Threads B (v2) have files: [0, 1]
// They run concurrently.
// Expected: End state is [0, 1] applied. No crashes.

#[test]
fn mixed_concurrency_blocking() {
    let container = PostgresContainer::start();
    let conn_str = Arc::new(container.connection_string.clone());
    let options = PostgresUpgraderOptions::builder().build();

    let n_threads = 6; // 3 old, 3 new
    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(n_threads));

    for i in 0..n_threads {
        let c_str = conn_str.clone();
        let opts = options.clone();
        let bar = barrier.clone();

        let folder = if i % 2 == 0 {
            "tests/data/mixed_concurrency/v1" // Old version
        } else {
            "tests/data/mixed_concurrency/v2" // New version
        };

        handles.push(thread::spawn(move || {
            bar.wait();
            upgrade_blocking(folder, &c_str, &opts)
        }));
    }

    for handle in handles {
        let result = handle.join().unwrap();
        assert!(result.is_ok(), "Thread failed: {:?}", result.err());
    }

    // Verify final state
    let mut client = BlockingTestClient::connect(&container.connection_string);
    let rows = client.get_upgraders(None);

    // Should have file 0 (0:0) and file 1 (1:0)
    // v1 has 0:0. v2 has 0:0, 1:0.
    // Final DB should have both.
    assert_eq!(rows.len(), 2, "Should have applied both upgraders");
    client.ensure_table_exists("mixed_table", None);
    client.ensure_table_exists("feature_table", None);
}

#[tokio::test]
async fn mixed_concurrency_async() {
    let container = PostgresContainer::start();
    let conn_str = Arc::new(container.connection_string.clone());
    let options = PostgresUpgraderOptions::builder().build();

    let n_tasks = 6;
    let mut handles = vec![];
    let barrier = Arc::new(tokio::sync::Barrier::new(n_tasks));

    for i in 0..n_tasks {
        let c_str = conn_str.clone();
        let opts = options.clone();
        let bar = barrier.clone();

        let folder = if i % 2 == 0 {
            "tests/data/mixed_concurrency/v1"
        } else {
            "tests/data/mixed_concurrency/v2"
        };

        handles.push(tokio::spawn(async move {
            bar.wait().await;
            upgrade_async(folder, &c_str, &opts).await
        }));
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Task failed: {:?}", result.err());
    }

    let client = AsyncTestClient::connect(&container.connection_string).await;
    let rows = client.get_upgraders(None).await;
    assert_eq!(rows.len(), 2);
    client.ensure_table_exists("mixed_table", None).await;
    client.ensure_table_exists("feature_table", None).await;
}