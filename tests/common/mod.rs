use std::process::Command;
use uuid::Uuid;
use std::thread;
use std::time::Duration;

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
                "run", "-d", "--name", &name, 
                "-e", &format!("POSTGRES_PASSWORD={}", password),
                "-p", &format!("{}:5432", host_port),
                "postgres:18.1"
            ])
            .status()
            .expect("Failed to run docker command");

        if !status.success() {
            panic!("Failed to start postgres container");
        }

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

