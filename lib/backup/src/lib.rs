mod error;

use std::sync::Arc;

use ch::{ClickhouseExtension, clickhouse};
use clickhouse::sql::Identifier;
pub use error::Error;

#[async_trait::async_trait]
pub trait Status: Send + Sync {
    async fn status(
        &self,
        backup_ids: &[String],
        since: std::time::Duration,
    ) -> Result<Vec<BackupStatus>, Error>;
}

#[async_trait::async_trait]
pub trait Backup: Send + Sync {
    async fn backup(&self, config: BackupConfig) -> Result<Vec<(String, String)>, Error>;
}

#[async_trait::async_trait]
pub trait Restore: Send + Sync {
    async fn restore(&self, config: RestoreConfig) -> Result<Vec<(String, String)>, Error>;
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<clickhouse::Client>,
}

impl Client {
    pub fn from_client(client: clickhouse::Client) -> Self {
        Self {
            inner: Arc::new(client),
        }
    }
}

#[async_trait::async_trait]
impl Backup for Client {
    async fn backup(&self, cfg: BackupConfig) -> Result<Vec<(String, String)>, Error> {
        cfg.validate()?;

        let BackupConfig {
            db,
            tables,
            backup_to,
            options,
        } = cfg;

        // Verify database exists
        let dbs = self
            .inner
            .list_databases()
            .await
            .map_err(Error::ClickhouseError)?;

        if !dbs.contains(&db) {
            return Err(Error::InvalidInput(format!(
                "Database '{}' does not exist",
                db
            )));
        }

        // Verify tables exist
        let avail_tables = self
            .inner
            .list_tables(&db)
            .await
            .map_err(Error::ClickhouseError)?;

        // If no tables specified, backup all tables found
        let tables = if tables.is_empty() {
            avail_tables
        } else {
            // Verify specified tables exist in the database
            for table in &tables {
                if !avail_tables.contains(table) {
                    return Err(Error::InvalidInput(format!(
                        "Table '{}' not found in database '{}'",
                        table, db
                    )));
                }
            }
            tables
        };

        let options_str = if !options.is_empty() {
            format!(" SETTINGS {}", options.join(","))
        } else {
            "".to_string()
        };

        let base_url = backup_to.s3_url().unwrap_or_default();

        let mut ret = Vec::with_capacity(tables.len());
        tracing::info!("Starting backup for database '{}'", db);

        let mut buffer = "BACKUP TABLE ?.? TO ".to_string();

        match &backup_to {
            StoreMethod::S3 { .. } => {
                buffer.push_str("S3(?, ?, ?)");
            }
        }

        buffer.push_str(&options_str);
        buffer.push_str(" ASYNC");

        for table in &tables {
            tracing::info!(" - Table '{}'", table);

            let mut query = self
                .inner
                .query(&buffer)
                .bind(Identifier(&db))
                .bind(Identifier(table));

            match &backup_to {
                StoreMethod::S3 {
                    access_key,
                    secret_key,
                    ..
                } => {
                    let url = format!(
                        "{}/{}/{}",
                        base_url.trim_end_matches('/'),
                        db.trim_end_matches('/'),
                        table.trim_end_matches('/')
                    );
                    query = query.bind(url).bind(access_key).bind(secret_key);
                }
            }

            let backup_id: String = query.fetch_one().await.map_err(Error::ClickhouseError)?;
            ret.push((table.to_string(), backup_id));
        }
        Ok(ret)
    }
}

#[async_trait::async_trait]
impl Restore for Client {
    async fn restore(&self, cfg: RestoreConfig) -> Result<Vec<(String, String)>, Error> {
        cfg.validate()?;

        let RestoreConfig {
            restore_from,
            target_db,
            source_db,
            tables,
            mut options,
            mode,
        } = cfg;

        let target_db = target_db.unwrap_or_else(|| source_db.clone());

        // If no tables specified, discover all tables in the backup
        let tables = if tables.is_empty() {
            let discovered = restore_from.list_tables(&self.inner, &source_db).await?;
            if discovered.is_empty() {
                return Err(Error::InvalidInput(
                    "No tables found in the backup source".to_string(),
                ));
            }
            tracing::info!(
                "Auto-discovered {} table(s) in backup: {:?}",
                discovered.len(),
                discovered
            );
            discovered
        } else {
            tables
        };

        tracing::info!(
            "Restoring {} table(s) into database '{}' from source '{}'",
            tables.len(),
            target_db,
            source_db
        );

        if let Some(mode) = mode {
            match mode {
                RestoreMode::StructureOnly => {
                    options.push("structure_only=1".to_string());
                }
                RestoreMode::DataOnly => {
                    options.push("struct_only=0".to_string());
                    options.push("allow_non_empty_tables=1".to_string());
                }
            }
        }

        let options_str = if !options.is_empty() {
            format!(" SETTINGS {}", options.join(","))
        } else {
            "".to_string()
        };

        let base_url = restore_from.s3_url().unwrap_or_default();

        // When restoring to a different database, we use:
        //   RESTORE TABLE source_db.table AS target_db.table FROM S3(...)
        // When restoring to the same database:
        //   RESTORE TABLE db.table FROM S3(...)
        let cross_db = target_db != source_db;
        let mut buffer = if cross_db {
            "RESTORE TABLE ?.? AS ?.? FROM ".to_string()
        } else {
            "RESTORE TABLE ?.? FROM ".to_string()
        };

        match &restore_from {
            StoreMethod::S3 { .. } => {
                buffer.push_str("S3(?, ?, ?)");
            }
        }

        buffer.push_str(&options_str);
        buffer.push_str(" ASYNC");

        let mut ret: Vec<(String, String)> = Vec::with_capacity(tables.len());

        for table in &tables {
            tracing::info!(" - Table '{}'", table);

            let mut query = self.inner.query(&buffer);

            // Bind source table identifiers
            query = query.bind(Identifier(&source_db)).bind(Identifier(table));

            // If cross-db, also bind target table identifiers
            if cross_db {
                query = query.bind(Identifier(&target_db)).bind(Identifier(table));
            }

            // The S3 URL for restore must match what backup() wrote:
            // {base_url}/{source_db}/{table}
            match &restore_from {
                StoreMethod::S3 {
                    access_key,
                    secret_key,
                    ..
                } => {
                    let url = format!(
                        "{}/{}/{}",
                        base_url.trim_end_matches('/'),
                        source_db.trim_end_matches('/'),
                        table.trim_end_matches('/')
                    );
                    query = query.bind(url).bind(access_key).bind(secret_key);
                }
            }

            let backup_id: String = query.fetch_one().await.map_err(Error::ClickhouseError)?;
            ret.push((table.to_string(), backup_id));
        }

        Ok(ret)
    }
}

#[async_trait::async_trait]
impl Status for Client {
    async fn status(
        &self,
        backup_ids: &[String],
        since: std::time::Duration,
    ) -> Result<Vec<BackupStatus>, Error> {
        let mut buffer = "SELECT id,
                    name,
                    query_id,
                    status,
                    total_size,
                    num_files,
                    files_read,
                    bytes_read,
                    start_time,
                    end_time,
                    error
                FROM system.backups
                WHERE system.backups.start_time >= fromUnixTimestamp64Second(?)"
            .to_string();

        if !backup_ids.is_empty() {
            buffer.push_str(" AND id IN ?");
        }
        buffer.push_str("\nORDER BY start_time DESC");

        let mut query = self
            .inner
            .query(&buffer)
            .bind((chrono::Utc::now() - since).timestamp());
        if !backup_ids.is_empty() {
            query = query.bind(backup_ids);
        }

        let ret = query
            .fetch_all()
            .await
            .map_err(crate::Error::ClickhouseError)?;

        Ok(ret)
    }
}

impl TryFrom<ch::Builder> for Client {
    type Error = ch::Error;

    fn try_from(value: ch::Builder) -> Result<Self, Self::Error> {
        let client = value.to_client()?;
        Ok(Self::from_client(client))
    }
}

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, serde_repr::Serialize_repr, serde_repr::Deserialize_repr,
)]
#[repr(i8)]
pub enum StatusStage {
    CreatingBackup = 0,
    BackupCreated = 1,
    BackupFailed = 2,
    Restoring = 3,
    Restored = 4,
    RestoreFailed = 5,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, clickhouse::Row)]
pub struct BackupStatus {
    #[serde(default)]
    pub id: String,

    #[serde(default)]
    pub name: String,

    #[serde(default)]
    pub query_id: String,

    pub status: StatusStage,

    #[serde(default)]
    pub total_size: u64,

    #[serde(default)]
    pub num_files: u64,

    #[serde(default)]
    pub files_read: u64,

    #[serde(default)]
    pub bytes_read: u64,

    #[serde(with = "clickhouse::serde::chrono::datetime64::micros")]
    pub start_time: chrono::DateTime<chrono::Utc>,

    #[serde(with = "clickhouse::serde::chrono::datetime64::micros")]
    pub end_time: chrono::DateTime<chrono::Utc>,

    #[serde(default)]
    pub error: String,
}

impl BackupStatus {
    pub fn progress(&self) -> f64 {
        if self.total_size == 0 {
            0.0
        } else {
            (self.bytes_read as f64 / self.total_size as f64) * 100.0
        }
    }

    pub fn is_completed(&self) -> bool {
        self.end_time > self.start_time
    }
}

#[derive(Debug, Clone)]
pub struct BackupConfig {
    pub db: String,
    pub tables: Vec<String>,
    pub backup_to: StoreMethod,
    pub options: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum RestoreMode {
    StructureOnly,
    DataOnly,
}

#[derive(Debug, Clone)]
pub struct RestoreConfig {
    pub restore_from: StoreMethod,
    pub target_db: Option<String>,
    pub source_db: String,
    pub tables: Vec<String>,
    pub options: Vec<String>,
    pub mode: Option<RestoreMode>,
}

impl BackupConfig {
    pub fn new(method: StoreMethod, db: impl Into<String>) -> Self {
        Self {
            db: db.into(),
            tables: vec![],
            backup_to: method,
            options: vec![],
        }
    }

    pub fn store_method(mut self, method: StoreMethod) -> Self {
        self.backup_to = method;
        self
    }

    pub fn tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    pub fn db(mut self, db: impl Into<String>) -> Self {
        self.db = db.into();
        self
    }

    pub fn add_table(mut self, table: impl Into<String>) -> Self {
        self.tables.push(table.into());
        self
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.db.is_empty() {
            return Err(Error::InvalidInput(
                "Database name must be specified".to_string(),
            ));
        }

        self.backup_to.validate()?;
        Ok(())
    }

    pub fn options(mut self, options: Vec<String>) -> Self {
        self.options = options;
        self
    }

    pub fn add_option(mut self, option: impl Into<String>) -> Self {
        self.options.push(option.into());
        self
    }
}

impl RestoreConfig {
    pub fn new(method: StoreMethod, src_db: impl Into<String>) -> Self {
        Self {
            restore_from: method,
            source_db: src_db.into(),
            tables: vec![],
            options: vec![],
            mode: None,
            target_db: None,
        }
    }

    pub fn store_method(mut self, method: StoreMethod) -> Self {
        self.restore_from = method;
        self
    }

    pub fn target_db<T>(mut self, db: Option<T>) -> Self
    where
        T: Into<String>,
    {
        self.target_db = db.map(|d| d.into());
        self
    }

    pub fn source_db(mut self, db: impl Into<String>) -> Self {
        self.source_db = db.into();
        self
    }

    pub fn tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    pub fn add_table(mut self, table: impl Into<String>) -> Self {
        self.tables.push(table.into());
        self
    }

    pub fn options(mut self, options: Vec<String>) -> Self {
        self.options = options;
        self
    }

    pub fn add_option(mut self, option: impl Into<String>) -> Self {
        self.options.push(option.into());
        self
    }

    pub fn mode(mut self, mode: RestoreMode) -> Self {
        self.mode = Some(mode);
        self
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.source_db.is_empty() {
            return Err(Error::InvalidInput(
                "Source database name must be specified".to_string(),
            ));
        }

        self.restore_from.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum StoreMethod {
    S3 {
        url: String,
        access_key: String,
        secret_key: String,
        prefix_path: Option<String>,
    },
}

impl StoreMethod {
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            StoreMethod::S3 {
                url,
                access_key,
                secret_key,
                ..
            } => {
                if url.is_empty() {
                    return Err(Error::InvalidInput("S3 URL must be specified".to_string()));
                }

                if access_key.is_empty() {
                    return Err(Error::InvalidInput(
                        "S3 Access Key must be specified".to_string(),
                    ));
                }

                if secret_key.is_empty() {
                    return Err(Error::InvalidInput(
                        "S3 Secret Key must be specified".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    fn s3_url(&self) -> Option<String> {
        match self {
            StoreMethod::S3 {
                url, prefix_path, ..
            } => {
                let url = if let Some(prefix) = prefix_path {
                    format!(
                        "{}/{}",
                        url.trim_end_matches('/'),
                        prefix.trim_start_matches('/')
                    )
                } else {
                    url.clone()
                };
                Some(url)
            }
        }
    }

    /// Discover table names present in a backup by scanning metadata files.
    ///
    /// ClickHouse backup layout (per-table, as written by `backup()`):
    ///   `{base}/{db}/{table}/metadata/{db}/{table}.sql`
    ///
    /// We glob across all per-table subdirectories and extract the table
    /// name from the `.sql` filename.
    pub async fn list_tables(
        &self,
        client: &clickhouse::Client,
        source_db: &str,
    ) -> Result<Vec<String>, Error> {
        let extract_expr =
            concat!("replaceRegexpOne(_path, '.*/metadata/[^/]+/([^/]+)\\.sql$', '\\\\1')",);

        match self {
            StoreMethod::S3 {
                access_key,
                secret_key,
                ..
            } => {
                let glob_url = format!(
                    "{}/{}/*/metadata/{}/*.sql",
                    self.s3_url().unwrap_or_default().trim_end_matches('/'),
                    source_db,
                    source_db,
                );

                let query_str = format!(
                    "SELECT DISTINCT {} AS table_name FROM s3(?, ?, ?, 'One') ORDER BY table_name",
                    extract_expr,
                );

                let tables: Vec<String> = client
                    .query(&query_str)
                    .bind(&glob_url)
                    .bind(access_key)
                    .bind(secret_key)
                    .fetch_all()
                    .await
                    .map_err(Error::ClickhouseError)?;

                Ok(tables)
            }
        }
    }
}

impl std::fmt::Display for StatusStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatusStage::CreatingBackup => write!(f, "CREATING_BACKUP"),
            StatusStage::BackupCreated => write!(f, "BACKUP_CREATED"),
            StatusStage::BackupFailed => write!(f, "BACKUP_FAILED"),
            StatusStage::Restoring => write!(f, "RESTORING"),
            StatusStage::Restored => write!(f, "RESTORED"),
            StatusStage::RestoreFailed => write!(f, "RESTORE_FAILED"),
        }
    }
}
