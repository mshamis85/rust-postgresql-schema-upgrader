macro_rules! do_await {
    ($e:expr) => {
        $e.await
    };
}

macro_rules! do_sync {
    ($e:expr) => {
        $e
    };
}

macro_rules! impl_create_schema_if_needed {
    ($client:ident, $schema:ident, $await_runner:ident) => {{
        if let Some(schema_name) = $schema {
            let sql = format!("CREATE SCHEMA IF NOT EXISTS \"{0}\";", schema_name);
            $await_runner!($client.execute(&sql, &[])).map_err(|e| {
                UpgraderError::ExecutionError(format!("Failed to create schema: {:?}", e))
            })?;
        }
        Ok(())
    }};
}

macro_rules! impl_init_upgraders_table {
    ($client:ident, $schema:ident, $await_runner:ident) => {
        {
            #[allow(unused_mut)]
            let mut transaction = $await_runner!($client.transaction()).map_err(|e| {
                UpgraderError::ConnectionError(format!("Failed to start transaction: {}", e))
            })?;

            $await_runner!(transaction.execute("SELECT pg_advisory_xact_lock($1)", &[&crate::db_tracker::ADVISORY_LOCK_ID]))
                .map_err(|e| {
                    UpgraderError::ExecutionError(format!("Failed to acquire advisory lock: {:?}", e))
                })?;

            let table = crate::db_tracker::table_name($schema);
            let create_sql = format!(
                r#" 
                CREATE TABLE IF NOT EXISTS {} (
                    file_id INT,
                    upgrader_id INT,
                    description VARCHAR(500),
                    text TEXT,
                    applied_on TIMESTAMPTZ,
                    PRIMARY KEY (file_id, upgrader_id)
                );
            "#,
                table
            );

            $await_runner!(transaction.execute(&create_sql, &[])).map_err(|e| {
                UpgraderError::ExecutionError(format!("Failed to create upgraders table: {:?}", e))
            })?;

            $await_runner!(transaction.commit()).map_err(|e| {
                UpgraderError::ExecutionError(format!("Failed to commit transaction: {:?}", e))
            })?;

            Ok(())
        }
    }
}

macro_rules! impl_lock_upgraders_table {
    ($transaction:ident, $schema:ident, $await_runner:ident) => {{
        let table = crate::db_tracker::table_name($schema);
        let lock_sql = format!("LOCK TABLE {} IN EXCLUSIVE MODE;", table);

        $await_runner!($transaction.execute(&lock_sql, &[])).map_err(|e| {
            UpgraderError::ExecutionError(format!("Failed to lock upgraders table: {:?}", e))
        })?;
        Ok(())
    }};
}

macro_rules! impl_load_applied_upgraders {
    ($client:ident, $schema:ident, $await_runner:ident) => {
        {
            let table = crate::db_tracker::table_name($schema);
            let select_sql = format!(
                "SELECT file_id, upgrader_id, description, text, applied_on FROM {} ORDER BY file_id, upgrader_id;",
                table
            );

            let rows = $await_runner!($client.query(&select_sql, &[])).map_err(|e| {
                UpgraderError::ExecutionError(format!("Failed to load applied upgraders: {:?}", e))
            })?;

            let mut applied = Vec::new();
            for row in rows {
                applied.push(crate::db_tracker::AppliedUpgrader {
                    file_id: row.get("file_id"),
                    upgrader_id: row.get("upgrader_id"),
                    description: row.get("description"),
                    text: row.get("text"),
                    applied_on: row.get("applied_on"),
                });
            }
            Ok(applied)
        }
    }
}

macro_rules! impl_record_upgrader {
    ($client:ident, $schema:ident, $upgrader:ident, $await_runner:ident) => {
        {
            let table = crate::db_tracker::table_name($schema);
            let insert_sql = format!(
                "INSERT INTO {} (file_id, upgrader_id, description, text, applied_on) VALUES ($1, $2, $3, $4, now());",
                table
            );

            $await_runner!($client.execute(
                &insert_sql,
                &[
                    &$upgrader.file_id,
                    &$upgrader.upgrader_id,
                    &$upgrader.description,
                    &$upgrader.text,
                ],
            ))
            .map_err(|e| {
                UpgraderError::ExecutionError(format!(
                    "Failed to record upgrader {}: {:?}",
                    $upgrader.upgrader_id, e
                ))
            })?;
            Ok(())
        }
    }
}

macro_rules! run_upgrade_flow {
    (
        $client:ident,
        $options:ident,
        $upgraders_folder:ident,
        $tracker_mod:path,
        $await_runner:ident,
        $($tx_ref:tt)*
    ) => {
        {
            use $tracker_mod::{init_upgraders_table, lock_upgraders_table, load_applied_upgraders, record_upgrader, create_schema_if_needed};
            use crate::integrity::verify_integrity;
            use crate::schema_loader::load_upgraders;

            // 0. Create Schema
            if $options.create_schema {
                if $options.schema.is_none() {
                    return Err(UpgraderError::ExecutionError("create_schema is enabled but no schema name is provided.".to_string()));
                }
                $await_runner!(create_schema_if_needed(&mut $client, $options.schema.as_deref()))?;
            }

            // 1. Initialize Table
            $await_runner!(init_upgraders_table(&mut $client, $options.schema.as_deref()))?;

            // 2. Load Upgraders from Files
            let upgraders = load_upgraders($upgraders_folder)?;

            loop {
                let mut transaction = $await_runner!($client.transaction())
                    .map_err(|e| UpgraderError::ConnectionError(format!("Failed to start transaction: {}", e)))?;

                $await_runner!(lock_upgraders_table(&mut transaction, $options.schema.as_deref()))?;

                let applied_upgraders = $await_runner!(load_applied_upgraders($($tx_ref)* transaction, $options.schema.as_deref()))?;

                // Verify Integrity
                verify_integrity(&upgraders, &applied_upgraders)?;

                let upgrader_to_apply = if applied_upgraders.len() < upgraders.len() {
                     Some(&upgraders[applied_upgraders.len()])
                } else {
                     None
                };

                if let Some(upgrader) = upgrader_to_apply {
                    let sql = $options.apply_schema_substitution(&upgrader.text);

                    // Execute
                    $await_runner!(transaction.batch_execute(&sql))
                        .map_err(|e| UpgraderError::ExecutionError(format!("Failed to execute upgrader {}: {}", upgrader.upgrader_id, e)))?;

                    // Record
                    $await_runner!(record_upgrader($($tx_ref)* transaction, $options.schema.as_deref(), upgrader))?;

                    $await_runner!(transaction.commit())
                        .map_err(|e| UpgraderError::ExecutionError(format!("Failed to commit transaction: {}", e)))?;
                } else {
                    // All upgraders applied
                    $await_runner!(transaction.commit())
                        .map_err(|e| UpgraderError::ExecutionError(format!("Failed to commit transaction: {}", e)))?;
                    break;
                }
            }
            Ok(())
        }
    }
}

pub(crate) use do_await;
pub(crate) use do_sync;
pub(crate) use impl_create_schema_if_needed;
pub(crate) use impl_init_upgraders_table;
pub(crate) use impl_load_applied_upgraders;
pub(crate) use impl_lock_upgraders_table;
pub(crate) use impl_record_upgrader;
pub(crate) use run_upgrade_flow;
