use chrono::{DateTime, Utc};
use crate::UpgraderError;
use crate::schema_loader::SchemaUpgrader;

#[derive(Debug, Clone)]
pub struct AppliedUpgrader {
    pub file_id: i32,
    pub upgrader_id: i32,
    pub description: String,
    pub text: String,
    pub applied_on: DateTime<Utc>,
}

const ADVISORY_LOCK_ID: i64 = 42_00_42_00; // Arbitrary constant for serialization of CREATE TABLE

fn table_name(schema: Option<&str>) -> String {
    match schema {
        Some(s) => format!("\"{}\".\"$upgraders$\"", s),
        None => "\"$upgraders$\"".to_string(),
    }
}

#[cfg(feature = "postgres")]
pub mod blocking {
    use super::*;
    use postgres::GenericClient;

    pub fn create_schema_if_needed(client: &mut impl GenericClient, schema: Option<&str>) -> Result<(), UpgraderError> {
        if let Some(schema_name) = schema {
            let sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\";", schema_name);
            client.execute(&sql, &[])
                .map_err(|e| UpgraderError::ExecutionError(format!("Failed to create schema: {:?}", e)))?;
        }
        Ok(())
    }

    pub fn init_upgraders_table(client: &mut postgres::Client, schema: Option<&str>) -> Result<(), UpgraderError> {
        let mut transaction = client.transaction()
             .map_err(|e| UpgraderError::ConnectionError(format!("Failed to start transaction: {}", e)))?;

        // Acquire advisory lock to serialize CREATE TABLE IF NOT EXISTS logic
        transaction.execute("SELECT pg_advisory_xact_lock($1)", &[&ADVISORY_LOCK_ID])
             .map_err(|e| UpgraderError::ExecutionError(format!("Failed to acquire advisory lock: {:?}", e)))?;

        let table = table_name(schema);
        
        let create_sql = format!(r#" 
            CREATE TABLE IF NOT EXISTS {} (
                file_id INT,
                upgrader_id INT,
                description VARCHAR(500),
                text TEXT,
                applied_on TIMESTAMPTZ,
                PRIMARY KEY (file_id, upgrader_id)
            );
        "#, table);
        
        transaction.execute(&create_sql, &[])
            .map_err(|e| UpgraderError::ExecutionError(format!("Failed to create upgraders table: {:?}", e)))?;
            
        transaction.commit()
            .map_err(|e| UpgraderError::ExecutionError(format!("Failed to commit transaction: {:?}", e)))?;

        Ok(())
    }

    pub fn lock_upgraders_table(transaction: &mut postgres::Transaction, schema: Option<&str>) -> Result<(), UpgraderError> {
        let table = table_name(schema);
        let lock_sql = format!("LOCK TABLE {} IN EXCLUSIVE MODE;", table);
        
        transaction.execute(&lock_sql, &[])
            .map_err(|e| UpgraderError::ExecutionError(format!("Failed to lock upgraders table: {:?}", e)))?;
        Ok(())
    }

    pub fn load_applied_upgraders(client: &mut impl GenericClient, schema: Option<&str>) -> Result<Vec<AppliedUpgrader>, UpgraderError> {
        let table = table_name(schema);
        let select_sql = format!("SELECT file_id, upgrader_id, description, text, applied_on FROM {} ORDER BY file_id, upgrader_id;", table);

        let rows = client.query(&select_sql, &[])
            .map_err(|e| UpgraderError::ExecutionError(format!("Failed to load applied upgraders: {:?}", e)))?;

        let mut applied = Vec::new();
        for row in rows {
            applied.push(AppliedUpgrader {
                file_id: row.get("file_id"),
                upgrader_id: row.get("upgrader_id"),
                description: row.get("description"),
                text: row.get("text"),
                applied_on: row.get("applied_on"),
            });
        }
        Ok(applied)
    }

    pub fn record_upgrader(client: &mut impl GenericClient, schema: Option<&str>, upgrader: &SchemaUpgrader) -> Result<(), UpgraderError> {
        let table = table_name(schema);
        let insert_sql = format!("INSERT INTO {} (file_id, upgrader_id, description, text, applied_on) VALUES ($1, $2, $3, $4, now());", table);

        client.execute(&insert_sql, &[
            &upgrader.file_id,
            &upgrader.upgrader_id,
            &upgrader.description,
            &upgrader.text,
        ]).map_err(|e| UpgraderError::ExecutionError(format!("Failed to record upgrader {}: {:?}", upgrader.upgrader_id, e)))?;
        Ok(())
    }
}

#[cfg(feature = "tokio-postgres")]
pub mod async_tracker {
    use super::*;
    use tokio_postgres::GenericClient;

    pub async fn create_schema_if_needed(client: &impl GenericClient, schema: Option<&str>) -> Result<(), UpgraderError> {
        if let Some(schema_name) = schema {
            let sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\";", schema_name);
            client.execute(&sql, &[])
                .await
                .map_err(|e| UpgraderError::ExecutionError(format!("Failed to create schema: {:?}", e)))?;
        }
        Ok(())
    }

    pub async fn init_upgraders_table(client: &mut tokio_postgres::Client, schema: Option<&str>) -> Result<(), UpgraderError> {
        let transaction = client.transaction().await
             .map_err(|e| UpgraderError::ConnectionError(format!("Failed to start transaction: {}", e)))?;

        // Acquire advisory lock to serialize CREATE TABLE IF NOT EXISTS logic
        transaction.execute("SELECT pg_advisory_xact_lock($1)", &[&ADVISORY_LOCK_ID])
             .await
             .map_err(|e| UpgraderError::ExecutionError(format!("Failed to acquire advisory lock: {:?}", e)))?;

        let table = table_name(schema);
        
        let create_sql = format!(r#" 
            CREATE TABLE IF NOT EXISTS {} (
                file_id INT,
                upgrader_id INT,
                description VARCHAR(500),
                text TEXT,
                applied_on TIMESTAMPTZ,
                PRIMARY KEY (file_id, upgrader_id)
            );
        "#, table);

        transaction.execute(&create_sql, &[])
            .await
            .map_err(|e| UpgraderError::ExecutionError(format!("Failed to create upgraders table: {:?}", e)))?;
            
        transaction.commit().await
            .map_err(|e| UpgraderError::ExecutionError(format!("Failed to commit transaction: {:?}", e)))?;

        Ok(())
    }

    pub async fn lock_upgraders_table(transaction: &tokio_postgres::Transaction<'_>, schema: Option<&str>) -> Result<(), UpgraderError> {
        let table = table_name(schema);
        let lock_sql = format!("LOCK TABLE {} IN EXCLUSIVE MODE;", table);
        
        transaction.execute(&lock_sql, &[])
            .await
            .map_err(|e| UpgraderError::ExecutionError(format!("Failed to lock upgraders table: {:?}", e)))?;
        Ok(())
    }

    pub async fn load_applied_upgraders(client: &impl GenericClient, schema: Option<&str>) -> Result<Vec<AppliedUpgrader>, UpgraderError> {
        let table = table_name(schema);
        let select_sql = format!("SELECT file_id, upgrader_id, description, text, applied_on FROM {} ORDER BY file_id, upgrader_id;", table);

        let rows = client.query(&select_sql, &[])
            .await
            .map_err(|e| UpgraderError::ExecutionError(format!("Failed to load applied upgraders: {:?}", e)))?;

        let mut applied = Vec::new();
        for row in rows {
            applied.push(AppliedUpgrader {
                file_id: row.get("file_id"),
                upgrader_id: row.get("upgrader_id"),
                description: row.get("description"),
                text: row.get("text"),
                applied_on: row.get("applied_on"),
            });
        }
        Ok(applied)
    }

    pub async fn record_upgrader(client: &impl GenericClient, schema: Option<&str>, upgrader: &SchemaUpgrader) -> Result<(), UpgraderError> {
        let table = table_name(schema);
        let insert_sql = format!("INSERT INTO {} (file_id, upgrader_id, description, text, applied_on) VALUES ($1, $2, $3, $4, now());", table);

        client.execute(&insert_sql, &[
            &upgrader.file_id,
            &upgrader.upgrader_id,
            &upgrader.description,
            &upgrader.text,
        ]).await
        .map_err(|e| UpgraderError::ExecutionError(format!("Failed to record upgrader {}: {:?}", upgrader.upgrader_id, e)))?;
        Ok(())
    }
}