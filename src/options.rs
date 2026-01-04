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
}

impl Default for PostgresUpgraderOptions {
    fn default() -> Self {
        Self {
            #[cfg(feature = "tls")]
            ssl_mode: SslMode::default(),
            schema: None,
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

    pub fn build(self) -> PostgresUpgraderOptions {
        PostgresUpgraderOptions {
            #[cfg(feature = "tls")]
            ssl_mode: self.ssl_mode,
            schema: self.schema,
        }
    }
}
