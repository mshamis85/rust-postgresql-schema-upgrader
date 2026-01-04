# PostgreSQL Schema Upgrader

A robust, safety-first Rust library for managing PostgreSQL database schema migrations. It supports both synchronous (blocking) and asynchronous (Tokio) execution modes, handles TLS connections via `rustls`, and enforces strict integrity checks to prevent database corruption.

## Features

- **Sequential & Atomic:** Applies schema changes in a strictly defined order. Each upgrader step is wrapped in a transaction, ensuring that a failure rolls back the entire step (no partial migrations).
- **Dual Mode:**
    - **Blocking:** Uses `postgres` crate for synchronous contexts.
    - **Async:** Uses `tokio-postgres` for asynchronous contexts.
- **Schema Isolation:** Can be configured to run all migrations within a specific PostgreSQL `SCHEMA`, allowing multiple applications to coexist in the same database or providing strict boundary control.
- **Tamper-Proof History:** Enforces strict immutability. If a previously applied migration file is modified on disk, the upgrader will refuse to run, protecting your production database from inconsistent history.
- **Strict Validation:** Enforces sequential naming conventions for migration files and upgrader steps to prevent gaps or collisions.
- **TLS Support:** Optional support for secure connections using `rustls`.

## Installation

Add the dependency to your `Cargo.toml`.

### Blocking (Synchronous)
```toml
[dependencies]
postgresql-schema-upgrader = { version = "0.1.0", features = ["postgres"] }
```

### Async (Tokio)
```toml
[dependencies]
postgresql-schema-upgrader = { version = "0.1.0", features = ["tokio-postgres"] }
```

### With TLS Support
Enable the `tls` feature to support `SslMode::Require`.
```toml
[dependencies]
postgresql-schema-upgrader = { version = "0.1.0", features = ["tokio-postgres", "tls"] }
```

## Directory Structure

The library expects a flat directory containing your migration files. Nested directories are not allowed to ensure a linear history.

**Rules:**
1. **File Naming:** Files must start with a number followed by an underscore (e.g., `000_init.sql`).
2. **File IDs:** Must start at `0` and increment sequentially without gaps (`0`, `1`, `2`, ...).

Example:
```text
upgraders/
├── 000_initial_schema.sql
├── 001_add_users.sql
└── 002_add_orders.sql
```

## Upgrader File Format

Each file can contain multiple upgrader steps. Steps are separated by a header line starting with `--- `. Segregating complex migrations into smaller steps allows for finer-grained control and easier recovery.

**Rules:**
1. **Header Format:** `--- <ID>: <Description>`
2. **Upgrader IDs:** Within each file, IDs must start at `0` and increment sequentially without gaps.

Example (`000_initial_schema.sql`):
```sql
--- 0: Create Users Table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL
);

--- 1: Create Posts Table
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    title VARCHAR(100)
);

--- 2: Add Index
CREATE INDEX idx_users_username ON users(username);
```

## Best Practices & Safety

### 1. Immutability is Key
Once an upgrader (e.g., File 0, Step 1) has been applied to a database, it is **locked**.
*   **Do not modify** the SQL or description of existing files.
*   **Do not reorder** files.
*   **Do not insert** new files in the middle of the history.

The library validates the integrity of the migration history on every run. If it detects that a file on disk differs from what was recorded in the database, it will return an error and refuse to proceed. This feature prevents "history rewriting" which can lead to catastrophic drift between environments.

### 2. Schema Isolation
You can confine your application's data to a specific schema. This is highly recommended for microservices sharing a database instance.
Use the `PostgresUpgraderOptions` builder to set the target schema. The library can also create the schema for you if it doesn't exist.

### 3. Atomic Steps
Each upgrader step (everything under a `--- ID:` header) is executed in its own transaction. If a step fails (e.g., syntax error), the transaction is rolled back, ensuring your database is never left in a half-migrated state.

## Usage

### Blocking Example

```rust
use postgresql_schema_upgrader::{upgrade_blocking, PostgresUpgraderOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection_string = "host=localhost user=postgres password=secret dbname=mydb";
    
    // Configure options
    let options = PostgresUpgraderOptions::builder()
        .schema("my_app_schema") // Isolate to this schema
        .create_schema(true)     // Create it if missing
        .build();

    // Applies upgraders from the "./upgraders" folder
    upgrade_blocking("./upgraders", connection_string, &options)?;
    
    println!("Schema upgraded successfully!");
    Ok(())
}
```

### Async Example

```rust
use postgresql_schema_upgrader::{upgrade_async, PostgresUpgraderOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection_string = "host=localhost user=postgres password=secret dbname=mydb";
    
    let options = PostgresUpgraderOptions::builder()
        .schema("my_app_schema")
        .create_schema(true)
        .build();

    // Applies upgraders from the "./upgraders" folder
    upgrade_async("./upgraders", connection_string, &options).await?;
    
    println!("Schema upgraded successfully!");
    Ok(())
}
```

## Command Line Interface (CLI)

The library includes a CLI tool for managing migrations and verifying connections from the terminal.

### Installation
Install the CLI tool using cargo:
```bash
cargo install postgresql-schema-upgrader
```

### Usage

#### Upgrade Schema
Apply migrations from a directory:
```bash
# Using a connection string
postgresql-schema-upgrader upgrade --connection-string "host=localhost user=postgres dbname=mydb" --path ./upgraders

# Using individual parameters
postgresql-schema-upgrader upgrade --host localhost --user postgres --database mydb --path ./upgraders

# With optional schema and TLS
postgresql-schema-upgrader upgrade --connection-string "..." --schema my_app --create-schema --tls
```

#### Check Connection
Verify the database is reachable:
```bash
postgresql-schema-upgrader check-connection --connection-string "..."
postgresql-schema-upgrader check-connection --host localhost --user postgres --database mydb --tls
```

### Environment Variables
The CLI supports the following environment variables:
- `DATABASE_URL`: Default for `--connection-string`
- `PGPASSWORD`: Default for `--password`

### With TLS Support

If you have the `tls` feature enabled, you can enforce SSL requirements:

```rust
use postgresql_schema_upgrader::{upgrade_async, PostgresUpgraderOptions, SslMode};

let options = PostgresUpgraderOptions::builder()
    .ssl_mode(SslMode::Require)
    .build();

upgrade_async("./upgraders", connection_string, &options).await?;
```