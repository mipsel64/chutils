pub mod error;
mod fs;

pub use error::Error;
use std::{collections::BTreeMap, sync::Arc};

#[async_trait::async_trait]
pub trait Migration {
    async fn ensure_migrations_table(&self) -> Result<(), Error>;

    async fn ping(&self) -> Result<(), Error>;

    /// Create a new migration file to the source directory.
    /// If latest migration is reversible, new one will be too (unless the file mode
    /// is MigrationFileMode::Simple).
    /// Returns a list of migration files if success
    async fn add(
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
        if let Some(database) = self.database {
            inner = inner.with_database(database);
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
    #[serde(with = "clickhouse::serde::chrono::datetime")]
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
    /// Create a new Migrator from a clickhouse Client.
    /// This is useful for testing with mock clients.
    pub fn from_client(client: clickhouse::Client) -> Self {
        Self {
            inner: Arc::new(client),
        }
    }
}

impl Migrator {
    async fn execute_migration(&self, info: &MigrationInfo, is_up: bool) -> Result<(), Error> {
        let raw = tokio::fs::read(&info.file_path(is_up)).await?;
        let content = String::from_utf8_lossy(&raw).to_string();

        // Process content: remove full-line comments, then split by semicolon
        let queries: Vec<String> = content
            .split(';')
            .map(|stmt| {
                // For each statement, filter out comment-only lines but keep inline comments
                // (ClickHouse handles inline comments fine)
                stmt.lines()
                    .filter(|line| {
                        let trimmed = line.trim();
                        // Keep non-empty lines that don't start with --
                        !trimmed.is_empty() && !trimmed.starts_with("--")
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        for query in queries {
            self.inner.query(&query).execute().await.inspect_err(|err| {
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
                version UInt32,
                name String,
                status Enum('pending' = 1, 'applied' = 2),
                applied_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
            ORDER BY(applied_at, version)
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
            .query("SELECT version, name, status, applied_at FROM _ch_migrations")
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

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse::test;

    // ==================== Builder tests ====================

    #[test]
    fn test_builder_new() {
        let builder = Builder::new("http://localhost:8123");
        assert_eq!(builder.url, "http://localhost:8123");
        assert!(builder.username.is_none());
        assert!(builder.password.is_none());
        assert!(builder.database.is_none());
        assert!(builder.options.is_empty());
    }

    #[test]
    fn test_builder_with_url() {
        let builder = Builder::new("http://old").with_url("http://new");
        assert_eq!(builder.url, "http://new");
    }

    #[test]
    fn test_builder_with_username() {
        let builder = Builder::new("http://localhost").with_username(Some("admin"));
        assert_eq!(builder.username, Some("admin".to_string()));
    }

    #[test]
    fn test_builder_with_username_none() {
        let builder = Builder::new("http://localhost").with_username(None::<String>);
        assert!(builder.username.is_none());
    }

    #[test]
    fn test_builder_with_password() {
        let builder = Builder::new("http://localhost").with_password(Some("secret"));
        assert_eq!(builder.password, Some("secret".to_string()));
    }

    #[test]
    fn test_builder_with_option() {
        let builder = Builder::new("http://localhost")
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0");
        assert!(
            builder
                .options
                .iter()
                .any(|(k, v)| k == "async_insert" && v == "1")
        );
        assert!(
            builder
                .options
                .iter()
                .any(|(k, v)| k == "wait_for_async_insert" && v == "0")
        );
    }

    #[test]
    fn test_builder_with_options() {
        let opts = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ];

        let builder = Builder::new("http://localhost").with_options(opts);
        assert_eq!(builder.options.len(), 2);
    }

    #[test]
    fn test_builder_to_migrator_success() {
        let result = Builder::new("http://localhost:8123").to_migrator();
        assert!(result.is_ok());
    }

    #[test]
    fn test_builder_to_migrator_empty_url_error() {
        let result = Builder::new("").to_migrator();
        let Err(err) = result else {
            panic!("Expect error but got none");
        };
        assert!(matches!(err, Error::EmptyUrl));
    }

    #[test]
    fn test_builder_chaining() {
        let migrator = Builder::new("http://localhost:8123")
            .with_username(Some("user"))
            .with_password(Some("pass"))
            .with_option("async_insert", "1")
            .to_migrator();
        assert!(migrator.is_ok());
    }

    // ==================== parse_request_options tests ====================

    #[test]
    fn test_parse_request_options_valid() {
        let result = parse_request_options("async_insert=1");
        assert!(result.is_ok());
        let (key, value) = result.unwrap();
        assert_eq!(key, "async_insert");
        assert_eq!(value, "1");
    }

    #[test]
    fn test_parse_request_options_with_equals_in_value() {
        let result = parse_request_options("key=value=with=equals");
        assert!(result.is_ok());
        let (key, value) = result.unwrap();
        assert_eq!(key, "key");
        assert_eq!(value, "value=with=equals");
    }

    #[test]
    fn test_parse_request_options_no_equals() {
        let result = parse_request_options("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_request_options_empty_key() {
        let result = parse_request_options("=value");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_request_options_empty_value() {
        let result = parse_request_options("key=");
        assert!(result.is_err());
    }

    // ==================== MigrationStatus tests ====================

    #[test]
    fn test_migration_status_display() {
        assert_eq!(format!("{}", MigrationStatus::Pending), "Pending");
        assert_eq!(format!("{}", MigrationStatus::Applied), "Applied");
    }

    #[test]
    fn test_migration_status_values() {
        assert_eq!(MigrationStatus::Pending as i8, 1);
        assert_eq!(MigrationStatus::Applied as i8, 2);
    }

    // ==================== MigrationInfo tests ====================

    #[test]
    fn test_migration_info_full_version() {
        let info = MigrationInfo {
            version: 1,
            name: "create_users".to_string(),
            status: MigrationStatus::Pending,
            applied_at: chrono::Utc::now(),
            mode: MigrationFileMode::Simple,
            src: "migrations".to_string(),
        };
        assert_eq!(info.full_version(), "0001_create_users");
    }

    #[test]
    fn test_migration_info_full_version_large_number() {
        let info = MigrationInfo {
            version: 12345,
            name: "test".to_string(),
            status: MigrationStatus::Pending,
            applied_at: chrono::Utc::now(),
            mode: MigrationFileMode::Simple,
            src: "migrations".to_string(),
        };
        assert_eq!(info.full_version(), "12345_test");
    }

    #[test]
    fn test_migration_info_file_path_simple() {
        let info = MigrationInfo {
            version: 1,
            name: "create_users".to_string(),
            status: MigrationStatus::Pending,
            applied_at: chrono::Utc::now(),
            mode: MigrationFileMode::Simple,
            src: "migrations".to_string(),
        };
        // Simple mode ignores is_up
        assert_eq!(info.file_path(true), "migrations/0001_create_users.sql");
        assert_eq!(info.file_path(false), "migrations/0001_create_users.sql");
    }

    #[test]
    fn test_migration_info_file_path_reversible() {
        let info = MigrationInfo {
            version: 1,
            name: "create_users".to_string(),
            status: MigrationStatus::Pending,
            applied_at: chrono::Utc::now(),
            mode: MigrationFileMode::Reversible,
            src: "migrations".to_string(),
        };
        assert_eq!(info.file_path(true), "migrations/0001_create_users.up.sql");
        assert_eq!(
            info.file_path(false),
            "migrations/0001_create_users.down.sql"
        );
    }

    // ==================== MigrationFile to MigrationInfo conversion ====================

    #[test]
    fn test_migration_file_to_info_conversion() {
        let file = MigrationFile {
            path: "migrations/0001_create_users.sql".to_string(),
            name: "create_users".to_string(),
            mode: MigrationFileMode::Simple,
            src: "migrations".to_string(),
            is_up: false,
            seq_num: 1,
        };

        let info: MigrationInfo = file.into();
        assert_eq!(info.version, 1);
        assert_eq!(info.name, "create_users");
        assert_eq!(info.status, MigrationStatus::Pending);
        assert_eq!(info.mode, MigrationFileMode::Simple);
        assert_eq!(info.src, "migrations");
    }

    // ==================== Migrator with mock tests ====================

    fn create_mock_migrator(mock: &test::Mock) -> Migrator {
        let client = clickhouse::Client::default().with_url(mock.url());
        Migrator::from_client(client)
    }

    #[tokio::test]
    async fn test_ping_success() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        mock.add(test::handlers::record_ddl());

        let result = migrator.ping().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_ping_failure() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        mock.add(test::handlers::failure(test::status::INTERNAL_SERVER_ERROR));

        let result = migrator.ping().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ensure_migrations_table() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let recording = mock.add(test::handlers::record_ddl());

        let result = migrator.ensure_migrations_table().await;
        assert!(result.is_ok());

        let query = recording.query().await;
        assert!(query.contains("CREATE TABLE IF NOT EXISTS _ch_migrations"));
    }

    #[tokio::test]
    async fn test_info_with_local_migrations_only() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // Create local migration files
        tokio::fs::write(
            format!("{}/0001_create_users.sql", src),
            b"CREATE TABLE users",
        )
        .await
        .unwrap();
        tokio::fs::write(
            format!("{}/0002_create_posts.sql", src),
            b"CREATE TABLE posts",
        )
        .await
        .unwrap();

        // Mock empty response from database (no applied migrations)
        mock.add(test::handlers::provide::<MigrationInfo>(vec![]));

        let result = migrator.info(src, false).await;
        assert!(result.is_ok());

        let migrations = result.unwrap();
        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0].version, 1);
        assert_eq!(migrations[0].status, MigrationStatus::Pending);
        assert_eq!(migrations[1].version, 2);
        assert_eq!(migrations[1].status, MigrationStatus::Pending);
    }

    #[tokio::test]
    async fn test_info_with_applied_migrations() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // Create local migration files
        tokio::fs::write(
            format!("{}/0001_create_users.sql", src),
            b"CREATE TABLE users",
        )
        .await
        .unwrap();
        tokio::fs::write(
            format!("{}/0002_create_posts.sql", src),
            b"CREATE TABLE posts",
        )
        .await
        .unwrap();

        // Mock response with first migration applied
        let applied = vec![MigrationInfo {
            version: 1,
            name: "create_users".to_string(),
            status: MigrationStatus::Applied,
            applied_at: chrono::Utc::now(),
            mode: MigrationFileMode::Simple,
            src: String::new(),
        }];
        mock.add(test::handlers::provide(applied));

        let result = migrator.info(src, false).await;
        assert!(result.is_ok());

        let migrations = result.unwrap();
        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0].status, MigrationStatus::Applied);
        assert_eq!(migrations[1].status, MigrationStatus::Pending);
    }

    #[tokio::test]
    async fn test_info_migration_name_mismatch_error() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // Create local migration file
        tokio::fs::write(
            format!("{}/0001_create_users.sql", src),
            b"CREATE TABLE users",
        )
        .await
        .unwrap();

        // Mock response with different name for same version
        let applied = vec![MigrationInfo {
            version: 1,
            name: "different_name".to_string(), // Mismatch!
            status: MigrationStatus::Applied,
            applied_at: chrono::Utc::now(),
            mode: MigrationFileMode::Simple,
            src: String::new(),
        }];
        mock.add(test::handlers::provide(applied));

        let result = migrator.info(src, false).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::MigrationCorrupted(_)));
    }

    #[tokio::test]
    async fn test_info_missing_local_migration_error() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // No local files, but db has applied migration
        let applied = vec![MigrationInfo {
            version: 1,
            name: "create_users".to_string(),
            status: MigrationStatus::Applied,
            applied_at: chrono::Utc::now(),
            mode: MigrationFileMode::Simple,
            src: String::new(),
        }];
        mock.add(test::handlers::provide(applied));

        let result = migrator.info(src, false).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::MigrationCorrupted(_)));
    }

    #[tokio::test]
    async fn test_info_missing_local_migration_ignored() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // No local files, but db has applied migration
        let applied = vec![MigrationInfo {
            version: 1,
            name: "create_users".to_string(),
            status: MigrationStatus::Applied,
            applied_at: chrono::Utc::now(),
            mode: MigrationFileMode::Simple,
            src: String::new(),
        }];
        mock.add(test::handlers::provide(applied));

        // With ignore_missing = true
        let result = migrator.info(src, true).await;
        assert!(result.is_ok());
        // Migration is ignored, so empty result
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_run_dry_run_returns_pending() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        tokio::fs::write(
            format!("{}/0001_create_users.sql", src),
            b"CREATE TABLE users",
        )
        .await
        .unwrap();

        mock.add(test::handlers::provide::<MigrationInfo>(vec![]));

        let result = migrator.run(src, true, false, None).await;
        assert!(result.is_ok());

        let pending = result.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].version, 1);
        // Status should still be Pending in dry_run
        assert_eq!(pending[0].status, MigrationStatus::Pending);
    }

    #[tokio::test]
    async fn test_run_no_pending_migrations() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        tokio::fs::write(
            format!("{}/0001_create_users.sql", src),
            b"CREATE TABLE users",
        )
        .await
        .unwrap();

        // All migrations already applied
        let applied = vec![MigrationInfo {
            version: 1,
            name: "create_users".to_string(),
            status: MigrationStatus::Applied,
            applied_at: chrono::Utc::now(),
            mode: MigrationFileMode::Simple,
            src: String::new(),
        }];
        mock.add(test::handlers::provide(applied));

        let result = migrator.run(src, false, false, None).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_run_with_target_version() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        tokio::fs::write(format!("{}/0001_first.sql", src), b"SELECT 1")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/0002_second.sql", src), b"SELECT 2")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/0003_third.sql", src), b"SELECT 3")
            .await
            .unwrap();

        mock.add(test::handlers::provide::<MigrationInfo>(vec![]));

        // Only run up to version 2
        let result = migrator.run(src, true, false, Some(2)).await;
        assert!(result.is_ok());

        let pending = result.unwrap();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().all(|m| m.version <= 2));
    }

    #[tokio::test]
    async fn test_run_out_of_order_error() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // Create migrations with a gap
        tokio::fs::write(format!("{}/0001_first.sql", src), b"SELECT 1")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/0002_second.sql", src), b"SELECT 2")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/0003_third.sql", src), b"SELECT 3")
            .await
            .unwrap();

        // Version 3 is applied but version 2 is not (out of order)
        let applied = vec![
            MigrationInfo {
                version: 1,
                name: "first".to_string(),
                status: MigrationStatus::Applied,
                applied_at: chrono::Utc::now(),
                mode: MigrationFileMode::Simple,
                src: String::new(),
            },
            MigrationInfo {
                version: 3,
                name: "third".to_string(),
                status: MigrationStatus::Applied,
                applied_at: chrono::Utc::now(),
                mode: MigrationFileMode::Simple,
                src: String::new(),
            },
        ];
        mock.add(test::handlers::provide(applied));

        let result = migrator.run(src, false, false, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::MigrationCorrupted(_)));
        assert!(err.to_string().contains("out of order"));
    }

    #[tokio::test]
    async fn test_run_applies_migrations() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        tokio::fs::write(
            format!("{}/0001_create_users.sql", src),
            b"CREATE TABLE users (id Int32) ENGINE = Memory",
        )
        .await
        .unwrap();

        // No applied migrations
        mock.add(test::handlers::provide::<MigrationInfo>(vec![]));
        // DDL execution
        mock.add(test::handlers::record_ddl());
        // Insert recording
        let insert_recording = mock.add(test::handlers::record());

        let result = migrator.run(src, false, false, None).await;
        assert!(result.is_ok());

        let applied = result.unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].status, MigrationStatus::Applied);

        // Verify insert was called
        let inserted: Vec<MigrationInfo> = insert_recording.collect().await;
        assert_eq!(inserted.len(), 1);
        assert_eq!(inserted[0].version, 1);
    }

    #[tokio::test]
    async fn test_revert_dry_run() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // Create reversible migration
        tokio::fs::write(
            format!("{}/0001_create_users.up.sql", src),
            b"CREATE TABLE users",
        )
        .await
        .unwrap();
        tokio::fs::write(
            format!("{}/0001_create_users.down.sql", src),
            b"DROP TABLE users",
        )
        .await
        .unwrap();

        let applied = vec![MigrationInfo {
            version: 1,
            name: "create_users".to_string(),
            status: MigrationStatus::Applied,
            applied_at: chrono::Utc::now(),
            mode: MigrationFileMode::Reversible,
            src: String::new(),
        }];
        mock.add(test::handlers::provide(applied));

        let result = migrator.revert(src, true, false, None).await;
        assert!(result.is_ok());

        let targets = result.unwrap();
        assert_eq!(targets.len(), 1);
        // Still Applied in dry_run mode
        assert_eq!(targets[0].status, MigrationStatus::Applied);
    }

    #[tokio::test]
    async fn test_revert_simple_migration_not_revertable() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // Create simple (non-reversible) migration
        tokio::fs::write(
            format!("{}/0001_create_users.sql", src),
            b"CREATE TABLE users",
        )
        .await
        .unwrap();

        let applied = vec![MigrationInfo {
            version: 1,
            name: "create_users".to_string(),
            status: MigrationStatus::Applied,
            applied_at: chrono::Utc::now(),
            mode: MigrationFileMode::Simple,
            src: String::new(),
        }];
        mock.add(test::handlers::provide(applied));

        let result = migrator.revert(src, true, false, None).await;
        assert!(result.is_ok());
        // No targets because simple migrations can't be reverted
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_revert_latest_only() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // Create multiple reversible migrations
        tokio::fs::write(format!("{}/0001_first.up.sql", src), b"SELECT 1")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/0001_first.down.sql", src), b"SELECT 1")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/0002_second.up.sql", src), b"SELECT 2")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/0002_second.down.sql", src), b"SELECT 2")
            .await
            .unwrap();

        let applied = vec![
            MigrationInfo {
                version: 1,
                name: "first".to_string(),
                status: MigrationStatus::Applied,
                applied_at: chrono::Utc::now(),
                mode: MigrationFileMode::Reversible,
                src: String::new(),
            },
            MigrationInfo {
                version: 2,
                name: "second".to_string(),
                status: MigrationStatus::Applied,
                applied_at: chrono::Utc::now(),
                mode: MigrationFileMode::Reversible,
                src: String::new(),
            },
        ];
        mock.add(test::handlers::provide(applied));

        // Without target_version, should only revert the latest
        let result = migrator.revert(src, true, false, None).await;
        assert!(result.is_ok());

        let targets = result.unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].version, 2); // Latest
    }

    #[tokio::test]
    async fn test_revert_with_target_version() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // Create multiple reversible migrations
        for i in 1..=4 {
            tokio::fs::write(format!("{}/{:04}_m{}.up.sql", src, i, i), b"SELECT 1")
                .await
                .unwrap();
            tokio::fs::write(format!("{}/{:04}_m{}.down.sql", src, i, i), b"SELECT 1")
                .await
                .unwrap();
        }

        let applied: Vec<MigrationInfo> = (1..=4)
            .map(|i| MigrationInfo {
                version: i,
                name: format!("m{}", i),
                status: MigrationStatus::Applied,
                applied_at: chrono::Utc::now(),
                mode: MigrationFileMode::Reversible,
                src: String::new(),
            })
            .collect();
        mock.add(test::handlers::provide(applied));

        // Revert down to version 2 (keep 1 and 2, revert 3 and 4)
        let result = migrator.revert(src, true, false, Some(2)).await;
        assert!(result.is_ok());

        let targets = result.unwrap();
        assert_eq!(targets.len(), 2);
        assert!(targets.iter().all(|m| m.version > 2));
    }

    #[tokio::test]
    async fn test_revert_to_zero_reverts_all() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        for i in 1..=3 {
            tokio::fs::write(format!("{}/{:04}_m{}.up.sql", src, i, i), b"SELECT 1")
                .await
                .unwrap();
            tokio::fs::write(format!("{}/{:04}_m{}.down.sql", src, i, i), b"SELECT 1")
                .await
                .unwrap();
        }

        let applied: Vec<MigrationInfo> = (1..=3)
            .map(|i| MigrationInfo {
                version: i,
                name: format!("m{}", i),
                status: MigrationStatus::Applied,
                applied_at: chrono::Utc::now(),
                mode: MigrationFileMode::Reversible,
                src: String::new(),
            })
            .collect();
        mock.add(test::handlers::provide(applied));

        // Revert to 0 means revert all
        let result = migrator.revert(src, true, false, Some(0)).await;
        assert!(result.is_ok());

        let targets = result.unwrap();
        assert_eq!(targets.len(), 3);
    }

    #[tokio::test]
    async fn test_revert_executes_down_migration() {
        let mock = test::Mock::new();
        let migrator = create_mock_migrator(&mock);

        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        tokio::fs::write(format!("{}/0001_test.up.sql", src), b"CREATE TABLE test")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/0001_test.down.sql", src), b"DROP TABLE test")
            .await
            .unwrap();

        let applied = vec![MigrationInfo {
            version: 1,
            name: "test".to_string(),
            status: MigrationStatus::Applied,
            applied_at: chrono::Utc::now(),
            mode: MigrationFileMode::Reversible,
            src: String::new(),
        }];
        mock.add(test::handlers::provide(applied));

        // DDL for down migration
        let ddl_recording = mock.add(test::handlers::record_ddl());
        // DELETE query
        mock.add(test::handlers::record_ddl());

        let result = migrator.revert(src, false, false, None).await;
        assert!(result.is_ok());

        let reverted = result.unwrap();
        assert_eq!(reverted.len(), 1);
        assert_eq!(reverted[0].status, MigrationStatus::Pending);

        let query = ddl_recording.query().await;
        assert!(query.contains("DROP TABLE test"));
    }
}
