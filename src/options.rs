#[cfg(feature = "tls")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Require,
}

#[cfg(feature = "tls")]
impl Default for SslMode {
    fn default() -> Self {
        SslMode::Disable
    }
}

#[derive(Debug, Clone)]
pub struct PostgresUpgraderOptions {
    #[cfg(feature = "tls")]
    pub(crate) ssl_mode: SslMode,
    pub(crate) schema: Option<String>,
    pub(crate) create_schema: bool,
}

impl Default for PostgresUpgraderOptions {
    fn default() -> Self {
        Self {
            #[cfg(feature = "tls")]
            ssl_mode: SslMode::default(),
            schema: None,
            create_schema: false,
        }
    }
}

impl PostgresUpgraderOptions {
    pub fn builder() -> PostgresUpgraderOptionsBuilder {
        PostgresUpgraderOptionsBuilder::default()
    }

    pub(crate) fn apply_schema_substitution(&self, sql: &str) -> String {
        if let Some(schema) = &self.schema {
            sql.replace("{{SCHEMA}}", schema)
        } else {
            sql.to_string()
        }
    }
}

#[derive(Default)]
pub struct PostgresUpgraderOptionsBuilder {
    #[cfg(feature = "tls")]
    ssl_mode: SslMode,
    schema: Option<String>,
    create_schema: bool,
}

impl PostgresUpgraderOptionsBuilder {
    #[cfg(feature = "tls")]
    pub fn ssl_mode(mut self, ssl_mode: SslMode) -> Self {
        self.ssl_mode = ssl_mode;
        self
    }

    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn create_schema(mut self, create: bool) -> Self {
        self.create_schema = create;
        self
    }

    pub fn build(self) -> PostgresUpgraderOptions {
        PostgresUpgraderOptions {
            #[cfg(feature = "tls")]
            ssl_mode: self.ssl_mode,
            schema: self.schema,
            create_schema: self.create_schema,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_defaults() {
        let options = PostgresUpgraderOptions::builder().build();
        assert!(options.schema.is_none());
        assert_eq!(options.create_schema, false);
        #[cfg(feature = "tls")]
        assert_eq!(options.ssl_mode, SslMode::Disable);
    }

    #[test]
    fn test_builder_custom_values() {
        let options = PostgresUpgraderOptions::builder()
            .schema("my_schema")
            .create_schema(true)
            .build();

        assert_eq!(options.schema.as_deref(), Some("my_schema"));
        assert_eq!(options.create_schema, true);
    }

    #[test]
    fn test_apply_schema_substitution_no_schema() {
        let options = PostgresUpgraderOptions::builder().build();
        let sql = "CREATE TABLE {{SCHEMA}}.test (id INT)";
        let result = options.apply_schema_substitution(sql);
        // Should remain unchanged if no schema is provided (or we might want to fail/strip?
        // Current impl returns as is, which is correct behavior for "no substitution").
        assert_eq!(result, sql);
    }

    #[test]
    fn test_apply_schema_substitution_with_schema() {
        let options = PostgresUpgraderOptions::builder()
            .schema("my_schema")
            .build();
        let sql = "CREATE TABLE {{SCHEMA}}.test (id INT)";
        let result = options.apply_schema_substitution(sql);
        assert_eq!(result, "CREATE TABLE my_schema.test (id INT)");
    }

    #[test]
    fn test_apply_schema_substitution_multiple_occurrences() {
        let options = PostgresUpgraderOptions::builder().schema("public").build();
        let sql = "SELECT * FROM {{SCHEMA}}.users JOIN {{SCHEMA}}.posts ON ...";
        let result = options.apply_schema_substitution(sql);
        assert_eq!(
            result,
            "SELECT * FROM public.users JOIN public.posts ON ..."
        );
    }
}
