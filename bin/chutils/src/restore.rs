use std::collections::{HashMap, HashSet};

use backup::{Restore, Status};
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

    /// Source database name to backup. This is required.
    #[clap(long = "src-db", short = 's', env = "RESTORE_SRC_DB")]
    pub source_db: String,

    /// Optional destination database name. If not set, the backup will be restored
    /// to the same database name as the source.
    #[clap(long = "dst-db", short = 'd', env = "RESTORE_DST_DB")]
    pub destination_db: Option<String>,

    /// Comma-separated list of tables to backup. If not set,
    /// all tables in the source database will be backed up.
    #[clap(
        long = "table",
        short = 't',
        env = "RESTORE_TABLE",
        value_delimiter = ','
    )]
    pub tables: Vec<String>,

    /// Comma-separated list of restore options (e.g., "s3_max_connections=1000")
    #[clap(
        long = "restore-option",
        short = 'O',
        env = "RESTORE_OPTIONS",
        value_delimiter = ','
    )]
    pub restore_options: Vec<String>,

    /// If set, only the structure of the tables will be backed up, without any data.
    /// Note: This option is mutually exclusive with --data-only. If both are set, the command will return an error.
    #[clap(long = "structure-only", short = 'S', env = "RESTORE_STRUCTURE_ONLY")]
    pub structure_only: bool,

    /// If set, only the data of the tables will be backed up, without any structure.
    /// Note: This option is mutually exclusive with --structure-only. If both are set, the command will return an error.
    #[clap(long = "data-only", short = 'D', env = "RESTORE_DATA_ONLY")]
    pub data_only: bool,

    /// Wait for the backup operation to complete. If not set,
    /// the command will return immediately after initiating the backup.
    #[clap(long, env = "RESTORE_WAIT", short = 'W', default_value_t = false)]
    pub wait: bool,

    /// S3 URL to store the backup (e.g., s3://my-bucket/backups/)
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
            source_db,
            destination_db,
            tables,
            restore_options,
            structure_only,
            data_only,
            wait,
            s3_url,
            s3_access_key,
            s3_secret_key,
            s3_prefix,
        } = self;

        let mode = match (structure_only, data_only) {
            (true, true) => {
                return Err(eyre::eyre!(
                    "Cannot set both --structure-only and --data-only options"
                ));
            }
            (true, false) => Some(backup::RestoreMode::StructureOnly),
            (false, true) => Some(backup::RestoreMode::DataOnly),
            (false, false) => None,
        };

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
            .restore(backup::RestoreConfig {
                restore_from: backup::StoreMethod::S3 {
                    url: s3_url,
                    access_key: s3_access_key,
                    secret_key: s3_secret_key,
                    prefix_path: s3_prefix,
                },
                source_db,
                target_db: destination_db,
                options: restore_options,
                mode,
                tables,
            })
            .await?;

        eprintln!("Initalized restore successfully. Backup IDs:");
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
                    eprint!("Table {} has been restored. Final status: ", table);
                    crate::status::print_status_inline(&s);
                    completed.insert(s.id.clone());
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
        // Print status
        eprintln!(
            "All backups has been restored successfully took {:?}",
            t0.elapsed()
        );
        eprintln!("Backup details:");
        let status = client
            .status(&ids, t0.elapsed())
            .await
            .wrap_err_with(|| "Getting final backup status")?;
        crate::status::print_statuses(&status);
        Ok(())
    }
}
