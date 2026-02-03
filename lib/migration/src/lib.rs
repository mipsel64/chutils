pub mod error;
mod fs;

pub use error::Error;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

#[async_trait::async_trait]
pub trait Migration {
    async fn ensure_migrations_table(&self) -> Result<(), Error>;

    async fn ping(&self) -> Result<(), Error>;

    /// Create a new migration file to the source directory.
    /// If latest migration is reversible, new one will be too (unless the file mode
    /// is MigrationFileMode::Simple).
    /// Returns a list of migration files if success
    async fn add(
        &self,
        src: &str,
        name: &str,
        mode: Option<MigrationFileMode>,
    ) -> Result<Vec<String>, Error> {
        fs::gen_migration_file(src, name, mode).await
    }

    async fn run(
        &self,
        src: &str,
        dry_run: bool,
        ignore_missing: bool,
        target_version: Option<u32>,
    ) -> Result<Vec<MigrationInfo>, Error>;

    async fn revert(
        &self,
        src: &str,
        dry_run: bool,
        ignore_missing: bool,
        target_version: Option<u32>,
    ) -> Result<Vec<MigrationInfo>, Error>;

    async fn info(&self, src: &str, ignore_missing: bool) -> Result<Vec<MigrationInfo>, Error>;
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
        clap(long = "request-option", env = "REQUEST_OPTIONS", value_parser = parse_request_options)
    )]
    #[serde(default)]
    pub options: HashMap<String, String>,
}

impl Builder {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            username: None,
            password: None,
            database: None,
            options: HashMap::new(),
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

    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(name.into(), value.into());
        self
    }

    pub fn with_options(mut self, opts: HashMap<String, String>) -> Self {
        self.options = opts;
        self
    }

    pub fn to_migrator(self) -> Result<Migrator, Error> {
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

        for (k, v) in self.options {
            inner = inner.with_option(k, v);
        }

        Ok(Migrator {
            inner: inner.into(),
        })
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Deserialize, serde::Serialize, Default)]
pub enum MigrationFileMode {
    Reversible,
    #[default]
    Simple,
}

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, serde_repr::Serialize_repr, serde_repr::Deserialize_repr,
)]
#[repr(i8)]
pub enum MigrationStatus {
    Pending = 1,
    Applied = 2,
}

#[derive(Debug, Clone, clickhouse::Row, serde::Serialize, serde::Deserialize)]
pub struct MigrationInfo {
    pub version: u32,
    pub name: String,
    pub status: MigrationStatus,
    pub applied_at: chrono::DateTime<chrono::Utc>,

    #[serde(skip)]
    mode: MigrationFileMode,
    #[serde(skip)]
    src: String,
}

impl MigrationInfo {
    pub fn full_version(&self) -> String {
        format!("{:04}_{}", self.version, self.name)
    }

    pub fn file_path(&self, is_up: bool) -> String {
        fs::build_file_path(&self.src, self.version, &self.name, self.mode, is_up)
    }
}

#[derive(Debug, Clone)]
pub struct MigrationFile {
    pub path: String,
    pub name: String,
    pub mode: MigrationFileMode,
    pub src: String,
    /// Only meaning of mode is Reversible
    pub is_up: bool,
    pub seq_num: u32,
}

#[derive(Clone)]
pub struct Migrator {
    inner: Arc<clickhouse::Client>,
}

impl Migrator {
    async fn execute_migration(&self, info: &MigrationInfo, is_up: bool) -> Result<(), Error> {
        let raw = tokio::fs::read(&info.file_path(is_up)).await?;
        let content = String::from_utf8_lossy(&raw).to_string();
        let queries = content
            .split(';')
            .map(|s| s.trim())
            .filter(|s| {
                !s.is_empty()
                    && !s.starts_with("--")
                    && !s.chars().all(|c| c.is_whitespace() || c == '\n')
            })
            .collect::<Vec<&str>>();
        for query in queries {
            self.inner.query(query).execute().await.inspect_err(|err| {
                tracing::debug!(error=?err,%query, version=info.full_version(), "Failed to execute query");
            })?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Migration for Migrator {
    async fn ensure_migrations_table(&self) -> Result<(), Error> {
        self.inner
            .query(
                "
            CREATE TABLE IF NOT EXISTS _ch_migrations (
                version Uint32,
                name String,
                status Enum('pending' = 1, 'applied' = 2),
                applied_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
            ORDER BY(applied_at, version);
            ",
            )
            .execute()
            .await?;
        Ok(())
    }

    async fn ping(&self) -> Result<(), Error> {
        self.inner.query("SELECT 1 == 1 ").execute().await?;
        Ok(())
    }

    async fn run(
        &self,
        src: &str,
        dry_run: bool,
        ignore_missing: bool,
        target_version: Option<u32>,
    ) -> Result<Vec<MigrationInfo>, Error> {
        // Load all migrations in the src folder
        let migs = self.info(src, ignore_missing).await?;

        let max_applied = migs
            .iter()
            .filter(|m| m.status == MigrationStatus::Applied)
            .map(|m| m.version)
            .max()
            .unwrap_or_default();

        let mut pending: Vec<_> = migs
            .into_iter()
            .filter(|m| m.status == MigrationStatus::Pending)
            .collect();

        // Validate no pending migration should be older than max_applied
        for mig in &pending {
            if mig.version < max_applied {
                return Err(Error::MigrationCorrupted(format!(
                    "migration out of order pending version: {}, latest version: {}",
                    mig.version, max_applied
                )));
            }
        }

        if pending.is_empty() {
            return Ok(pending);
        }

        if let Some(version) = target_version {
            pending.retain(|mig| mig.version <= version);
        }

        if dry_run {
            return Ok(pending);
        }

        let mut insert = self.inner.insert::<MigrationInfo>("_ch_migrations")?;

        for mig in pending.iter_mut() {
            self.execute_migration(mig, true).await?;
            mig.status = MigrationStatus::Applied;
            mig.applied_at = chrono::Utc::now();
            insert.write(mig).await?;
        }

        insert.end().await?;
        Ok(pending)
    }

    async fn revert(
        &self,
        src: &str,
        dry_run: bool,
        ignore_missing: bool,
        target_version: Option<u32>,
    ) -> Result<Vec<MigrationInfo>, Error> {
        let migs = self.info(src, ignore_missing).await?;

        let mut targets = vec![];
        for mig in migs.into_iter().rev() {
            let file_path = mig.file_path(false);
            // Take migrations which are applied and revertable
            if mig.status == MigrationStatus::Applied && file_path.ends_with(".down.sql") {
                targets.push(mig);
            }
        }

        if let Some(version) = target_version {
            targets.retain(|mig| mig.version > version);
        } else {
            targets.truncate(1);
        }

        if targets.is_empty() || dry_run {
            return Ok(targets);
        }

        for mig in targets.iter_mut() {
            self.execute_migration(mig, false).await?;

            self.inner
                .query("DELETE FROM _ch_migrations WHERE version = ?")
                .bind(mig.version)
                .execute()
                .await?;
            mig.status = MigrationStatus::Pending;
            mig.applied_at = chrono::Utc::now();
        }

        Ok(targets)
    }

    async fn info(&self, src: &str, ignore_missing: bool) -> Result<Vec<MigrationInfo>, Error> {
        let mut migrations: BTreeMap<u32, MigrationInfo> = fs::list_migrations(src)
            .await?
            .into_iter()
            .map(|mf| (mf.seq_num, mf.into()))
            .collect();

        let mut cursor = self
            .inner
            .query("SELECT version, name, status, applied_at FROM _ch_migrations;")
            .fetch::<MigrationInfo>()?;

        while let Some(info) = cursor.next().await? {
            if let Some(mig) = migrations.get_mut(&info.version) {
                if mig.name != info.name {
                    return Err(Error::MigrationCorrupted(format!(
                        "Migration name miss match in db: '{}', local: '{}'",
                        info.name, mig.name
                    )));
                }
                mig.status = info.status;
                mig.applied_at = info.applied_at;
                continue;
            }

            if ignore_missing {
                tracing::warn!(migration=?info, "Ignoring migration {} (version={}) is existing in db but not found in local", info.name, info.version);
                continue;
            }

            return Err(Error::MigrationCorrupted(format!(
                "Migration {} (version={}) is existing in db but not found in local",
                info.name, info.version
            )));
        }

        Ok(migrations.into_values().collect())
    }
}

impl std::fmt::Display for MigrationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Applied => write!(f, "Applied"),
            Self::Pending => write!(f, "Pending"),
        }
    }
}

impl From<MigrationFile> for MigrationInfo {
    fn from(value: MigrationFile) -> Self {
        Self {
            name: value.name,
            version: value.seq_num,
            status: MigrationStatus::Pending,
            applied_at: chrono::Utc::now(),

            mode: value.mode,
            src: value.src,
        }
    }
}

fn parse_request_options(raw: &str) -> Result<(String, String), String> {
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
