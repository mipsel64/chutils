mod error;

pub use clickhouse;
pub use clickhouse::error::Error as ClickhouseError;
pub use error::Error;

const IGNORE_TABLES: &[&str] = &["system", "information_schema", "INFORMATION_SCHEMA"];

#[async_trait::async_trait]
pub trait ClickhouseExtension: Send + Sync {
    async fn list_databases(&self) -> Result<Vec<String>, ClickhouseError>;
    async fn list_tables(&self, db: &str) -> Result<Vec<String>, ClickhouseError>;
}

#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "clap", derive(clap::Parser))]
#[cfg_attr(feature = "clap", group(id = "clickhouse-migrator"))]
pub struct Builder {
    /// Clickhouse URL
    #[cfg_attr(
        feature = "clap",
        clap(long = "clickhouse-url", env = "CLICKHOUSE_URL", default_value = "")
    )]
    #[serde(default)]
    pub url: String,

    /// Clickhouse Username
    #[cfg_attr(
        feature = "clap",
        clap(long = "clickhouse_user", env = "CLICKHOUSE_USER")
    )]
    #[serde(default)]
    pub username: Option<String>,

    /// Clickhouse Password
    #[cfg_attr(
        feature = "clap",
        clap(long = "clickhouse-password", env = "CLICKHOUSE_PASSWORD")
    )]
    #[serde(default)]
    pub password: Option<String>,

    /// Clickhouse database
    #[cfg_attr(feature = "clap", clap(long = "clickhouse-db", env = "CLICKHOUSE_DB"))]
    #[serde(default)]
    pub database: Option<String>,

    /// Clickhouse request options e.g: --request-option async_insert=1
    #[cfg_attr(
        feature = "clap",
        clap(long = "clickhouse-option", env = "CLICKHOUSE_OPTIONS", value_parser = parse_request_options, value_delimiter = ' ')
    )]
    #[serde(default)]
    pub options: Vec<(String, String)>,
}

impl Builder {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            username: None,
            password: None,
            database: None,
            options: vec![],
        }
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    pub fn with_username<T>(mut self, username: Option<T>) -> Self
    where
        T: Into<String>,
    {
        self.username = username.map(|u| u.into());
        self
    }

    pub fn with_password<T>(mut self, password: Option<T>) -> Self
    where
        T: Into<String>,
    {
        self.password = password.map(|u| u.into());
        self
    }

    pub fn with_database<T>(mut self, db: Option<T>) -> Self
    where
        T: Into<String>,
    {
        self.database = db.map(|u| u.into());
        self
    }

    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.push((name.into(), value.into()));
        self
    }

    pub fn with_options(mut self, opts: Vec<(String, String)>) -> Self {
        self.options = opts;
        self
    }

    pub fn to_client(self) -> Result<clickhouse::Client, Error> {
        if self.url.is_empty() {
            return Err(Error::EmptyUrl);
        }

        let mut inner = clickhouse::Client::default().with_url(self.url);

        if let Some(username) = self.username {
            inner = inner.with_user(username);
        }
        if let Some(password) = self.password {
            inner = inner.with_password(password);
        }
        if let Some(database) = self.database {
            inner = inner.with_database(database);
        }

        for (k, v) in self.options {
            inner = inner.with_option(k, v);
        }

        Ok(inner)
    }
}

#[async_trait::async_trait]
impl ClickhouseExtension for clickhouse::Client {
    async fn list_databases(&self) -> Result<Vec<String>, ClickhouseError> {
        let query = self
            .query("SELECT name from system.databases WHERE name NOT IN ? ORDER BY name")
            .bind(IGNORE_TABLES);
        let ret: Vec<String> = query.fetch_all().await?;
        Ok(ret)
    }

    async fn list_tables(&self, db: &str) -> Result<Vec<String>, ClickhouseError> {
        let query = self
            .query("SELECT name from system.tables WHERE database = ? AND engine NOT IN ('MaterializedView', 'View', 'Dictionary') AND name NOT LIKE '.inner_id.%' ORDER BY name")
            .bind(db);
        let ret: Vec<String> = query.fetch_all().await?;
        Ok(ret)
    }
}

pub fn parse_request_options(raw: &str) -> Result<(String, String), String> {
    raw.split_once('=')
        .and_then(|(key, value)| {
            if key.is_empty() || value.is_empty() {
                None
            } else {
                Some((key.to_owned(), value.to_owned()))
            }
        })
        .ok_or_else(|| {
            format!(
                "Invalid request option: must be in the format `key=value`. Received `{}`",
                raw
            )
        })
}
