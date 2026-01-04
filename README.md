# PostgreSQL Schema Upgrader

A Rust library for managing PostgreSQL database schema migrations. It supports both synchronous (blocking) and asynchronous (Tokio) execution modes, and handles TLS connections via `rustls`.

## Features

- **Sequential Upgrades:** Applies schema changes in a strictly defined order based on file and upgrader IDs.
- **Dual Mode:**
    - **Blocking:** Uses `postgres` crate for synchronous contexts.
    - **Async:** Uses `tokio-postgres` for asynchronous contexts.
- **TLS Support:** Optional support for secure connections using `rustls` (via the `tls` feature).
- **Strict Validation:** Enforces sequential naming conventions for migration files and upgrader steps to prevent gaps or collisions.
- **No Type Leaks:** Encapsulates all underlying dependency types, exposing only a clean, minimal API.

## Installation

Add the dependency to your `Cargo.toml`. Choose the features that match your runtime environment.

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

The library expects a flat directory containing your migration files. Nested directories are not allowed.

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

Each file can contain multiple upgrader steps. Steps are separated by a header line starting with `--- `.

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

## Upgrader Integrity and Immutability

This library enforces a strict immutability rule for upgraders. Once an upgrader (identified by its `file_id` and `upgrader_id`) has been successfully executed against a database, it is considered **LOCKED**.

If the library detects that the SQL text of a previously executed upgrader has been modified, it will:
1.  **Refuse to proceed:** No further upgraders will be applied.
2.  **Return an error:** An error will be returned immediately before any changes are made to the database.

**Best Practice:** Once an upgrader is deployed and run against a database, its content must **NEVER** be changed. If you need to modify the schema further, create a new upgrader step or a new upgrader file.

## Usage

### Blocking Example

```rust
use postgresql_schema_upgrader::{upgrade_blocking, SslMode};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection_string = "host=localhost user=postgres password=secret dbname=mydb";
    
    // Applies upgraders from the "./upgraders" folder
    upgrade_blocking("./upgraders", connection_string, SslMode::Disable)?;
    
    println!("Schema upgraded successfully!");
    Ok(())
}
```

### Async Example

```rust
use postgresql_schema_upgrader::{upgrade_async, SslMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection_string = "host=localhost user=postgres password=secret dbname=mydb";
    
    // Applies upgraders from the "./upgraders" folder
    upgrade_async("./upgraders", connection_string, SslMode::Disable).await?;
    
    println!("Schema upgraded successfully!");
    Ok(())
}
```

### Using TLS

To enforce a secure connection, use `SslMode::Require` (requires the `tls` feature).

```rust
upgrade_async("./upgraders", connection_string, SslMode::Require).await?;
```
