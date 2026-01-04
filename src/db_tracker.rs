use crate::UpgraderError;
use crate::schema_loader::SchemaUpgrader;
use crate::upgrade_macros::{
    do_await,
    do_sync,
    impl_create_schema_if_needed,
    impl_init_upgraders_table,
    impl_load_applied_upgraders,
    impl_lock_upgraders_table,
    impl_record_upgrader,
};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct AppliedUpgrader {
    pub file_id: i32,
    pub upgrader_id: i32,
    pub description: String,
    pub text: String,
    pub applied_on: DateTime<Utc>,
}

pub(crate) const ADVISORY_LOCK_ID: i64 = 42_00_42_00; // Arbitrary constant for serialization of CREATE TABLE

pub(crate) fn table_name(schema: Option<&str>) -> String {
    match schema {
        Some(s) => format!("\"{}\".\"$upgraders$\"", s),
        None => "\"$upgraders$\"".to_string(),
    }
}

#[cfg(feature = "postgres")]
pub mod blocking {
    use super::*;
    use postgres::GenericClient;

    pub fn create_schema_if_needed(
        client: &mut impl GenericClient,
        schema: Option<&str>,
    ) -> Result<(), UpgraderError> {
        impl_create_schema_if_needed!(client, schema, do_sync)
    }

    pub fn init_upgraders_table(
        client: &mut postgres::Client,
        schema: Option<&str>,
    ) -> Result<(), UpgraderError> {
        impl_init_upgraders_table!(client, schema, do_sync)
    }

    pub fn lock_upgraders_table(
        transaction: &mut postgres::Transaction,
        schema: Option<&str>,
    ) -> Result<(), UpgraderError> {
        impl_lock_upgraders_table!(transaction, schema, do_sync)
    }

    pub fn load_applied_upgraders(
        client: &mut impl GenericClient,
        schema: Option<&str>,
    ) -> Result<Vec<AppliedUpgrader>, UpgraderError> {
        impl_load_applied_upgraders!(client, schema, do_sync)
    }

    pub fn record_upgrader(
        client: &mut impl GenericClient,
        schema: Option<&str>,
        upgrader: &SchemaUpgrader,
    ) -> Result<(), UpgraderError> {
        impl_record_upgrader!(client, schema, upgrader, do_sync)
    }
}

#[cfg(feature = "tokio-postgres")]
pub mod async_tracker {
    use super::*;
    use tokio_postgres::GenericClient;

    pub async fn create_schema_if_needed(
        client: &impl GenericClient,
        schema: Option<&str>,
    ) -> Result<(), UpgraderError> {
        impl_create_schema_if_needed!(client, schema, do_await)
    }

    pub async fn init_upgraders_table(
        client: &mut tokio_postgres::Client,
        schema: Option<&str>,
    ) -> Result<(), UpgraderError> {
        impl_init_upgraders_table!(client, schema, do_await)
    }

    pub async fn lock_upgraders_table(
        transaction: &tokio_postgres::Transaction<'_>,
        schema: Option<&str>,
    ) -> Result<(), UpgraderError> {
        impl_lock_upgraders_table!(transaction, schema, do_await)
    }

    pub async fn load_applied_upgraders(
        client: &impl GenericClient,
        schema: Option<&str>,
    ) -> Result<Vec<AppliedUpgrader>, UpgraderError> {
        impl_load_applied_upgraders!(client, schema, do_await)
    }

    pub async fn record_upgrader(
        client: &impl GenericClient,
        schema: Option<&str>,
        upgrader: &SchemaUpgrader,
    ) -> Result<(), UpgraderError> {
        impl_record_upgrader!(client, schema, upgrader, do_await)
    }
}