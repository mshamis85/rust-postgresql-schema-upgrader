use std::process::Command;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

pub struct PostgresContainer {
    pub name: String,
    pub connection_string: String,
}

impl PostgresContainer {
    pub fn start() -> Self {
        // Check if docker exists
        let version_check = Command::new("docker").arg("--version").output();
        if version_check.is_err() {
            panic!("Docker is not installed or not in PATH");
        }

        let name = format!("postgres-test-{}", Uuid::new_v4());
        let password = "mysecretpassword";

        // Find a free host port
        let host_port = port_check::free_local_port().expect("No free ports available");

        // Start Postgres
        let status = Command::new("docker")
            .args(&[
                "run",
                "-d",
                "--name",
                &name,
                "-e",
                &format!("POSTGRES_PASSWORD={}", password),
                "-p",
                &format!("{}:5432", host_port),
                "postgres:18.1",
            ])
            .status()
            .expect("Failed to run docker command");

        if !status.success() {
            panic!("Failed to start postgres container");
        }

        let connection_string = format!(
            "host=localhost port={} user=postgres password={} dbname=postgres",
            host_port, password
        );

        // Wait for readiness
        let mut attempts = 0;
        while attempts < 30 {
            thread::sleep(Duration::from_secs(1));
            let status = Command::new("docker")
                .args(&["exec", &name, "pg_isready", "-U", "postgres"])
                .status();

            if let Ok(s) = status {
                if s.success() {
                    break;
                }
            }
            attempts += 1;
        }

        if attempts >= 30 {
            // Cleanup if failed
            Command::new("docker")
                .args(&["rm", "-f", &name])
                .output()
                .ok();
            panic!("Postgres container failed to become ready");
        }

        Self {
            name,
            connection_string,
        }
    }
}

impl Drop for PostgresContainer {
    fn drop(&mut self) {
        Command::new("docker")
            .args(&["rm", "-f", &self.name])
            .output()
            .ok();
    }
}

pub struct TestUpgraderRow {
    pub file_id: i32,
    pub upgrader_id: i32,
}

pub struct BlockingTestClient {
    client: postgres::Client,
}

impl BlockingTestClient {
    pub fn connect(connection_string: &str) -> Self {
        let client = postgres::Client::connect(connection_string, postgres::NoTls)
            .expect("Failed to connect to Postgres");
        Self { client }
    }

    pub fn execute(&mut self, sql: &str) {
        self.client
            .execute(sql, &[])
            .expect("Failed to execute SQL");
    }

    pub fn ensure_schema_exists(&mut self, schema: &str) {
        let sql = format!(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = '{}'",
            schema
        );
        let rows = self.client.query(&sql, &[]).expect("Query failed");
        assert!(!rows.is_empty(), "Schema {} should exist", schema);
    }

    pub fn ensure_schema_does_not_exist(&mut self, schema: &str) {
        let sql = format!(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = '{}'",
            schema
        );
        let rows = self.client.query(&sql, &[]).expect("Query failed");
        assert!(rows.is_empty(), "Schema {} should NOT exist", schema);
    }

    pub fn ensure_table_exists(&mut self, table: &str, schema: Option<&str>) {
        let table_ref = match schema {
            Some(s) => format!("{}.{}", s, table),
            None => table.to_string(),
        };
        self.client
            .execute(&format!("SELECT * FROM {}", table_ref), &[])
            .expect(&format!("Table {} should exist", table_ref));
    }

    pub fn get_upgraders(&mut self, schema: Option<&str>) -> Vec<TestUpgraderRow> {
        let table_ref = match schema {
            Some(s) => format!("\"{}\".\"$upgraders$\"", s),
            None => "\"$upgraders$\"".to_string(),
        };
        let sql = format!("SELECT file_id, upgrader_id FROM {}", table_ref);
        let rows = self.client.query(&sql, &[]).expect("Query failed");
        rows.iter()
            .map(|row| TestUpgraderRow {
                file_id: row.get("file_id"),
                upgrader_id: row.get("upgrader_id"),
            })
            .collect()
    }
}

pub struct AsyncTestClient {
    client: tokio_postgres::Client,
}

impl AsyncTestClient {
    pub async fn connect(connection_string: &str) -> Self {
        let (client, connection) =
            tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
                .await
                .expect("Failed to connect to Postgres");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Self { client }
    }

    pub async fn execute(&self, sql: &str) {
        self.client
            .execute(sql, &[])
            .await
            .expect("Failed to execute SQL");
    }

    pub async fn ensure_schema_exists(&self, schema: &str) {
        let sql = format!(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = '{}'",
            schema
        );
        let rows = self.client.query(&sql, &[]).await.expect("Query failed");
        assert!(!rows.is_empty(), "Schema {} should exist", schema);
    }

    pub async fn ensure_schema_does_not_exist(&self, schema: &str) {
        let sql = format!(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = '{}'",
            schema
        );
        let rows = self.client.query(&sql, &[]).await.expect("Query failed");
        assert!(rows.is_empty(), "Schema {} should NOT exist", schema);
    }

    pub async fn ensure_table_exists(&self, table: &str, schema: Option<&str>) {
        let table_ref = match schema {
            Some(s) => format!("{}.{}", s, table),
            None => table.to_string(),
        };
        self.client
            .execute(&format!("SELECT * FROM {}", table_ref), &[])
            .await
            .expect(&format!("Table {} should exist", table_ref));
    }

    pub async fn get_upgraders(&self, schema: Option<&str>) -> Vec<TestUpgraderRow> {
        let table_ref = match schema {
            Some(s) => format!("\"{}\".\"$upgraders$\"", s),
            None => "\"$upgraders$\"".to_string(),
        };
        let sql = format!("SELECT file_id, upgrader_id FROM {}", table_ref);
        let rows = self.client.query(&sql, &[]).await.expect("Query failed");
        rows.iter()
            .map(|row| TestUpgraderRow {
                file_id: row.get("file_id"),
                upgrader_id: row.get("upgrader_id"),
            })
            .collect()
    }
}
