use std::collections::{HashMap, HashSet};

use backup::{Backup, Status};
use eyre::Context;

#[derive(clap::Parser)]
pub struct Command {
    /// ClickHouse server URL (e.g., http://localhost:8123)
    #[clap(long = "clickhouse-url", short = 'c', env = "CLICKHOUSE_URL")]
    pub url: String,

    /// ClickHouse username for authentication
    #[clap(long = "clickhouse-user", short = 'u', env = "CLICKHOUSE_USER")]
    pub username: Option<String>,

    /// ClickHouse password for authentication
    #[clap(long = "clickhouse-password", short = 'p', env = "CLICKHOUSE_PASSWORD")]
    pub password: Option<String>,

    /// Additional ClickHouse request options (space-delimited key=value pairs)
    #[clap(long = "clickhouse-option", short='o', env = "CLICKHOUSE_OPTIONS", value_parser = ch::parse_request_options, value_delimiter = ',')]
    pub options: Vec<(String, String)>,

    /// ClickHouse database to back up
    #[clap(long = "db", short = 'd', env = "BACKUP_DB")]
    pub database: String,

    /// Comma-separated list of tables to back up. If not set, all tables in
    /// the database will be backed up.
    #[clap(
        long = "table",
        short = 't',
        env = "BACKUP_TABLES",
        value_delimiter = ','
    )]
    pub tables: Vec<String>,

    /// Comma-separated list of backup options (e.g., "s3_max_connections=1000")
    #[clap(
        long = "backup-option",
        short = 'O',
        env = "BACKUP_OPTIONS",
        value_delimiter = ','
    )]
    pub backup_options: Vec<String>,

    /// Wait for the backup operation to complete. If not set,
    /// the command will return immediately after initiating the backup.
    #[clap(long, short = 'W', env = "BACKUP_WAIT", default_value_t = false)]
    pub wait: bool,

    /// S3 URL to store the backups
    #[clap(long, env = "S3_URL")]
    pub s3_url: String,

    /// S3 Access Key
    #[clap(long, env = "S3_ACCESS_KEY")]
    pub s3_access_key: String,

    /// S3 Secret Key
    #[clap(long, env = "S3_SECRET_KEY")]
    pub s3_secret_key: String,

    /// Optional S3 prefix for the backup files
    #[clap(long, env = "S3_PREFIX")]
    pub s3_prefix: Option<String>,
}

impl Command {
    pub async fn execute(self) -> eyre::Result<()> {
        let Command {
            url,
            username,
            password,
            options,
            database,
            wait,
            tables,
            backup_options,
            s3_url,
            s3_access_key,
            s3_secret_key,
            s3_prefix,
        } = self;

        let builder = ch::Builder::new(url)
            .with_username(username)
            .with_password(password)
            .with_options(options);

        let ch_client = builder
            .to_client()
            .wrap_err_with(|| "Failed to build ClickHouse client")?;

        let t0 = std::time::Instant::now();
        let client = backup::Client::from_client(ch_client);
        let backup_ids = client
            .backup(backup::BackupConfig {
                backup_to: backup::StoreMethod::S3 {
                    url: s3_url,
                    access_key: s3_access_key,
                    secret_key: s3_secret_key,
                    prefix_path: s3_prefix,
                },
                db: database,
                options: backup_options,
                tables,
            })
            .await?;

        eprintln!("Initalized backup successfully. Backup IDs:");
        for (table, backup_id) in &backup_ids {
            eprintln!("- {}: {}", table, backup_id);
        }

        if !wait {
            return Ok(());
        }

        let backups: HashMap<String, String> = HashMap::from_iter(backup_ids.into_iter());

        let ids = backups.values().cloned().collect::<Vec<String>>();

        // Loop status until all backups are complete
        let mut completed = HashSet::new();
        while completed.len() < backups.len() {
            let status = client
                .status(&ids, t0.elapsed())
                .await
                .wrap_err_with(|| "Getting backup status")?;

            for s in status {
                if completed.contains(&s.id) {
                    continue;
                }

                let Some(table) = backups.get(&s.id) else {
                    tracing::warn!(backup_id = s.id, "Received status for unknown backup ID");
                    continue;
                };

                eprintln!("{}: {:.2}% ({})", table, s.progress(), s.status,);
                if s.end_time > s.start_time {
                    // Completed
                    eprint!("Table {} backup completed. Final status: ", table);
                    crate::status::print_status_inline(&s);
                    completed.insert(s.id.clone());
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
        // Print status
        eprintln!("All backups completed successfully took {:?}", t0.elapsed());
        eprintln!("Backup details:");
        let status = client
            .status(&ids, t0.elapsed())
            .await
            .wrap_err_with(|| "Getting final backup status")?;
        crate::status::print_statuses(&status);
        Ok(())
    }
}
