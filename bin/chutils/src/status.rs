use backup::Status;
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
    #[clap(long = "clickhouse-option", short = 'o', env = "CLICKHOUSE_OPTIONS", value_parser = ch::parse_request_options, value_delimiter = ',')]
    pub options: Vec<(String, String)>,

    /// Comma-separated list of backup IDs to check status for
    #[clap(long, short, required = true, value_delimiter = ',')]
    pub backup_ids: Vec<String>,

    /// Wait until all backups are complete
    #[clap(long, short = 'W')]
    pub wait: bool,

    /// Filter backup since duration (e.g., "10m" for 10 minutes)
    #[clap(long, short = 's', default_value = "24h", value_parser = humantime::parse_duration)]
    pub since: std::time::Duration,
}

impl Command {
    pub async fn execute(self) -> eyre::Result<()> {
        let Command {
            url,
            username,
            password,
            options,
            backup_ids,
            wait,
            since,
        } = self;

        let builder = ch::Builder::new(url)
            .with_username(username)
            .with_password(password)
            .with_options(options);

        let ch_client = builder
            .to_client()
            .wrap_err_with(|| "Failed to build ClickHouse client")?;

        let client = backup::Client::from_client(ch_client);
        let status = client
            .status(&backup_ids, since)
            .await
            .wrap_err_with(|| "Failed to get backup status")?;

        let all_completed = status.iter().all(|s| s.is_completed());

        print_statuses(&status);
        if !wait || all_completed {
            return Ok(());
        }

        loop {
            let status = client
                .status(&backup_ids, since)
                .await
                .wrap_err_with(|| "Failed to get backup status")?;
            print_statuses(&status);

            let all_completed = status.iter().all(|s| s.is_completed());
            if all_completed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
        Ok(())
    }
}

pub fn print_statuses(statuses: &[backup::BackupStatus]) {
    for status in statuses {
        eprintln!("id: {}", status.id);
        eprintln!("query_id: {}", status.query_id);
        eprintln!("status: {}", status.status);
        eprintln!("progress: {:.2}", status.progress());
        eprintln!(
            "bytes_read: {}",
            human_bytes::human_bytes(status.bytes_read as f64)
        );
        eprintln!(
            "total_size: {}",
            human_bytes::human_bytes(status.total_size as f64)
        );
        eprintln!("start_time: {}", status.start_time.to_rfc3339());
        eprintln!(
            "end_time: {}",
            if status.start_time > status.end_time {
                "NOT_COMPLETED".to_string()
            } else {
                status.end_time.to_rfc3339()
            }
        );
        eprintln!(
            "error: {}",
            if !status.error.is_empty() {
                &status.error
            } else {
                "NOME"
            }
        );
        eprintln!("----------------------------------------");
    }
}

pub fn print_status_inline(status: &backup::BackupStatus) {
    eprint!(
        "\rBackup ID: {} | Status: {} | Progress: {:.2}% ({}/{}) | Error: {}",
        status.id,
        status.status,
        status.progress(),
        human_bytes::human_bytes(status.bytes_read as f64),
        human_bytes::human_bytes(status.total_size as f64),
        if !status.error.is_empty() {
            &status.error
        } else {
            "NONE"
        }
    );
}
