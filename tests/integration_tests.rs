mod common;

use common::{AsyncTestClient, BlockingTestClient, PostgresContainer};
use postgresql_schema_upgrader::{upgrade_async, upgrade_blocking, PostgresUpgraderOptions};
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