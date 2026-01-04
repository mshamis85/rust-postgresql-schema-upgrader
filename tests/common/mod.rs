use std::process::Command;
use uuid::Uuid;
use std::thread;
use std::time::Duration;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

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
        
        // Start Postgres
        let status = Command::new("docker")
            .args(&[
                "run", "-d", "--name", &name, 
                "-e", &format!("POSTGRES_PASSWORD={}", password),
                "-P", // Publish all exposed ports to random ports
                "postgres:18.1"
            ])
            .status()
            .expect("Failed to run docker command");

        if !status.success() {
            panic!("Failed to start postgres container");
        }

        // Get the mapped port
        // Wait a split second for docker to assign port
        thread::sleep(Duration::from_millis(500));
        
        let output = Command::new("docker")
            .args(&["port", &name, "5432"])
            .output()
            .expect("Failed to get port mapping");
            
        let stdout = String::from_utf8(output.stdout).unwrap();
        // Output format like: 0.0.0.0:32768
        // We need to parse this. 
        // Note: It might return multiple bindings (ipv4/ipv6), usually the first line works.
        let line = stdout.lines().next().expect("No port mapping found");
        let parts: Vec<&str> = line.trim().split(':').collect();
        let host_port = parts.last().expect("Failed to parse port").trim();

        let connection_string = format!("host=localhost port={} user=postgres password={} dbname=postgres", host_port, password);
        
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
             Command::new("docker").args(&["rm", "-f", &name]).output().ok();
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

pub fn create_dummy_upgrader(folder: &Path, filename: &str, file_id: i32, upgrader_id: i32, sql: &str) {
    let path = folder.join(filename);
    let mut f = File::create(path).unwrap();
    writeln!(f, "--- {}: Description for {}\n{}", upgrader_id, upgrader_id, sql).unwrap();
}
