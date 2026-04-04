pub mod error;
mod fs;

use ch::clickhouse;

pub use error::Error;
use std::{collections::BTreeMap, sync::Arc};

#[async_trait::async_trait]
pub trait Migration: Send + Sync {
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
    #[serde(with = "ch::clickhouse::serde::chrono::datetime")]
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

/// Split SQL content into individual queries, respecting comments and string literals.
/// Splits on `;` only when not inside a string literal or comment.
/// Line comments (`--`) and block comments (`/* */`) are stripped from output.
fn split_queries(content: &str) -> Result<Vec<String>, Error> {
    let mut queries = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = content.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // Line comment: skip until end of line
        if i + 1 < len && chars[i] == '-' && chars[i + 1] == '-' {
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            if i < len {
                current.push('\n');
                i += 1;
            }
            continue;
        }

        // Block comment: skip until */, emit a space to preserve token separation
        if i + 1 < len && chars[i] == '/' && chars[i + 1] == '*' {
            i += 2;
            while i + 1 < len && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            if i + 1 < len {
                i += 2;
            } else {
                return Err(Error::InvalidInput("unterminated block comment".into()));
            }
            current.push(' ');
            continue;
        }

        // String literal: consume until closing quote, handling escapes
        if chars[i] == '\'' {
            current.push(chars[i]);
            i += 1;
            while i < len {
                if chars[i] == '\'' {
                    current.push(chars[i]);
                    i += 1;
                    // Escaped quote ('')
                    if i < len && chars[i] == '\'' {
                        current.push(chars[i]);
                        i += 1;
                        continue;
                    }
                    break;
                }
                if chars[i] == '\\' && i + 1 < len {
                    current.push(chars[i]);
                    current.push(chars[i + 1]);
                    i += 2;
                    continue;
                }
                current.push(chars[i]);
                i += 1;
            }
            continue;
        }

        // Semicolon: query separator
        if chars[i] == ';' {
            let trimmed = current.trim().to_string();
            if !trimmed.is_empty() {
                queries.push(trimmed);
            }
            current.clear();
            i += 1;
            continue;
        }

        current.push(chars[i]);
        i += 1;
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        queries.push(trimmed);
    }

    Ok(queries)
}

impl Migrator {
    async fn execute_migration(&self, info: &MigrationInfo, is_up: bool) -> Result<(), Error> {
        let raw = tokio::fs::read(&info.file_path(is_up)).await?;
        let content = String::from_utf8_lossy(&raw).to_string();
        let queries = split_queries(&content)?;

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

impl TryFrom<ch::Builder> for Migrator {
    type Error = ch::Error;
    fn try_from(value: ch::Builder) -> Result<Self, Self::Error> {
        let client = value.to_client()?;
        Ok(Migrator::from_client(client))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse::test;

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

    // ==================== split_queries tests ====================

    #[test]
    fn test_split_queries_basic() {
        let input = "CREATE TABLE foo (id Int32) ENGINE = Memory;\nINSERT INTO foo VALUES (1);";
        let result = split_queries(input).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "CREATE TABLE foo (id Int32) ENGINE = Memory");
        assert_eq!(result[1], "INSERT INTO foo VALUES (1)");
    }

    #[test]
    fn test_split_queries_trailing_no_semicolon() {
        let result = split_queries("SELECT 1").unwrap();
        assert_eq!(result, vec!["SELECT 1"]);
    }

    #[test]
    fn test_split_queries_empty_input() {
        let result = split_queries("").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_split_queries_only_comments() {
        let result = split_queries("-- just a comment\n-- another comment\n").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_split_queries_semicolon_in_line_comment() {
        let input = "-- Setup; initialize tables\nCREATE TABLE foo (id Int32) ENGINE = Memory;";
        let result = split_queries(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "CREATE TABLE foo (id Int32) ENGINE = Memory");
    }

    #[test]
    fn test_split_queries_semicolon_in_block_comment() {
        let input = "/* Setup; initialize tables */\nCREATE TABLE foo (id Int32) ENGINE = Memory;";
        let result = split_queries(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "CREATE TABLE foo (id Int32) ENGINE = Memory");
    }

    #[test]
    fn test_split_queries_semicolon_in_string_literal() {
        let input = "INSERT INTO foo VALUES ('hello; world');";
        let result = split_queries(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "INSERT INTO foo VALUES ('hello; world')");
    }

    #[test]
    fn test_split_queries_escaped_quotes_in_string() {
        let input = "INSERT INTO foo VALUES ('it''s a test; really');";
        let result = split_queries(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "INSERT INTO foo VALUES ('it''s a test; really')");
    }

    #[test]
    fn test_split_queries_backslash_escaped_quote() {
        let input = r"INSERT INTO foo VALUES ('it\'s; here');";
        let result = split_queries(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], r"INSERT INTO foo VALUES ('it\'s; here')");
    }

    #[test]
    fn test_split_queries_multiline_block_comment() {
        let input = "/*\n * Multi-line comment;\n * with semicolons;\n */\nSELECT 1;";
        let result = split_queries(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "SELECT 1");
    }

    #[test]
    fn test_split_queries_complex_migration() {
        let input = "\
-- Migration: create users table
-- Author: test; date: 2024-01-01
CREATE TABLE users (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id;

/* Insert default admin user;
   This is important; don't remove */
INSERT INTO users VALUES (1, 'admin; superuser');

-- Done; cleanup
SELECT 1;
";
        let result = split_queries(input).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result[0].starts_with("CREATE TABLE users"));
        assert_eq!(
            result[1],
            "INSERT INTO users VALUES (1, 'admin; superuser')"
        );
        assert_eq!(result[2], "SELECT 1");
    }

    #[test]
    fn test_split_queries_strips_comments_between_statements() {
        let input = "SELECT 1;\n-- comment between\nSELECT 2;";
        let result = split_queries(input).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "SELECT 1");
        assert_eq!(result[1], "SELECT 2");
    }

    #[test]
    fn test_split_queries_whitespace_only_segments() {
        let input = "SELECT 1;   \n  \n  ; SELECT 2;";
        let result = split_queries(input).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "SELECT 1");
        assert_eq!(result[1], "SELECT 2");
    }

    #[test]
    fn test_split_queries_inline_block_comment_preserves_spacing() {
        let input = "SELECT 1/*x*/FROM t;";
        let result = split_queries(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "SELECT 1 FROM t");
    }

    #[test]
    fn test_split_queries_unterminated_block_comment_error() {
        let input = "SELECT 1; /* unterminated comment\nSELECT 2;";
        let result = split_queries(input);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidInput(_)));
    }

    #[test]
    fn test_split_queries_real_world_migration() {
        let input = r#"
-- ============================================================================
-- app_configs
-- Stores the live configuration per service.
-- ReplacingMergeTree collapses older versions after background merges;
-- always query with FINAL to read the latest row.
-- ============================================================================
CREATE TABLE IF NOT EXISTS app_configs (
    service      Enum8('API' = 1, 'Worker' = 2, 'Scheduler' = 3),
    config_json  String,                  -- full JSON configuration blob
    updated_at   DateTime64(3, 'UTC')     -- version column for ReplacingMergeTree
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (service);

-- ============================================================================
-- jobs
-- One logical row per (service, job_id). State transitions are modelled as
-- INSERT of a new row with a newer updated_at; ReplacingMergeTree keeps only
-- the latest version after merges. Use FINAL for consistent reads.
--
-- Lifecycle:
--   Queued (pending)  ──▶  Running  (started)
--                     └──▶  is_cancelled = 1
--   Immediate         ──▶  (no pending phase; runs to completion)
-- ============================================================================
CREATE TABLE IF NOT EXISTS jobs (
    service          Enum8('API' = 1, 'Worker' = 2, 'Scheduler' = 3),
    job_id           String,                  -- e.g. '20261231235959'
    mode             Enum8('Queued' = 1, 'Running' = 2, 'Immediate' = 3),
    is_cancelled     UInt8   DEFAULT 0,       -- 1 = cancelled (only meaningful for Queued)
    config_snapshot  String  DEFAULT '',      -- snapshot of config at creation time
    total_items      UInt32  DEFAULT 0,
    success_count    UInt32  DEFAULT 0,
    failure_count    UInt32  DEFAULT 0,
    skipped_count    UInt32  DEFAULT 0,
    total_cost_usd   Float64 DEFAULT 0,
    notify_id        Nullable(UInt64),
    created_at       DateTime64(3, 'UTC'),    -- job.created_at_ms
    updated_at       DateTime64(3, 'UTC')     -- version column for ReplacingMergeTree
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (service, job_id);

-- ============================================================================
-- job_tasks
-- Denormalized task-level rows for each job. Written once per task, never
-- updated. ClickHouse native TTL handles automatic expiry:
--   - Queued jobs:   expires_at = now + queue_deadline     (2h)
--   - Finished jobs: expires_at = now + task_log_retention (7d)
-- ============================================================================
CREATE TABLE IF NOT EXISTS job_tasks (
    service            Enum8('API' = 1, 'Worker' = 2, 'Scheduler' = 3),
    job_id             String,
    task_key           String,                  -- unique identifier
    action             LowCardinality(String),  -- Process | Skip | Retry
    input_payload      String  DEFAULT '',      -- serialised input
    output_payload     Nullable(String),        -- serialised output
    cost_usd           Float64 DEFAULT 0,
    duration_ms        UInt64  DEFAULT 0,
    error_type         Nullable(String),        -- error variant name
    error_message      Nullable(String),        -- error serialised content
    note               Nullable(String),
    created_at         DateTime64(3, 'UTC'),    -- task.created_at_ms
    expires_at         DateTime('UTC')          -- for native TTL
) ENGINE = MergeTree()
ORDER BY (service, job_id, task_key)
TTL expires_at DELETE
SETTINGS merge_with_ttl_timeout = 3600;

-- ============================================================================
-- audit_events
-- Immutable audit trail for each job (e.g. config changes, manual overrides).
-- Same TTL strategy as job_tasks.
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit_events (
    service            Enum8('API' = 1, 'Worker' = 2, 'Scheduler' = 3),
    job_id             String,
    event_type         LowCardinality(String),  -- ConfigChange | Override | Rollback
    actor              String  DEFAULT '',       -- who triggered the event
    payload            String  DEFAULT '',
    success            UInt8   DEFAULT 0,        -- boolean: 1 = success, 0 = failure
    err                Nullable(String),
    created_at         DateTime64(3, 'UTC'),
    expires_at         DateTime('UTC')
) ENGINE = MergeTree()
ORDER BY (service, job_id, created_at)
TTL expires_at DELETE
SETTINGS merge_with_ttl_timeout = 3600;
"#;
        let result = split_queries(input).unwrap();
        assert_eq!(
            result.len(),
            4,
            "Expected 4 CREATE TABLE statements, got: {}",
            result.len()
        );
        assert!(
            result[0].starts_with("CREATE TABLE IF NOT EXISTS app_configs"),
            "query[0]: {}",
            &result[0][..60]
        );
        assert!(
            result[1].starts_with("CREATE TABLE IF NOT EXISTS jobs"),
            "query[1]: {}",
            &result[1][..60]
        );
        assert!(
            result[2].starts_with("CREATE TABLE IF NOT EXISTS job_tasks"),
            "query[2]: {}",
            &result[2][..60]
        );
        assert!(
            result[3].starts_with("CREATE TABLE IF NOT EXISTS audit_events"),
            "query[3]: {}",
            &result[3][..60]
        );

        // Verify inline comments are stripped but Enum string literals with quotes are preserved
        assert!(
            result[0].contains("'API' = 1"),
            "Enum values must be preserved"
        );
        assert!(
            !result[0].contains("-- full JSON"),
            "Inline comments should be stripped"
        );

        // Verify the SETTINGS clause stays attached to its CREATE TABLE (not split off)
        assert!(
            result[2].contains("SETTINGS merge_with_ttl_timeout = 3600"),
            "SETTINGS must stay with CREATE TABLE"
        );
        assert!(
            result[3].contains("SETTINGS merge_with_ttl_timeout = 3600"),
            "SETTINGS must stay with CREATE TABLE"
        );
    }
}
