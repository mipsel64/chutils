use clap::Parser;
use eyre::Context;
use migration::Migration;
use tracing_subscriber::EnvFilter;

#[derive(clap::Parser)]
struct Cli {
    /// ClickHouse server URL (e.g., http://localhost:8123)
    #[clap(
        long = "clickhouse-url",
        env = "CLICKHOUSE_URL",
        default_value = "",
        global = true
    )]
    pub url: String,

    /// ClickHouse username for authentication
    #[clap(long = "clickhouse-user", env = "CLICKHOUSE_USER", global = true)]
    pub username: Option<String>,

    /// ClickHouse password for authentication
    #[clap(
        long = "clickhouse-password",
        env = "CLICKHOUSE_PASSWORD",
        global = true
    )]
    pub password: Option<String>,

    /// ClickHouse database name to use
    #[clap(long = "clickhouse-db", env = "CLICKHOUSE_DB", global = true)]
    pub database: Option<String>,

    /// Additional ClickHouse request options (space-delimited key=value pairs)
    #[clap(long = "clickhouse-option", env = "CLICKHOUSE_OPTIONS", value_parser = migration::parse_request_options, global = true, value_delimiter = ' ')]
    pub options: Vec<(String, String)>,

    /// Directory path containing migration files
    #[clap(long, env = "MIGRATION_SOURCE", default_value = "migrations/")]
    pub source: String,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(clap::Parser)]
enum Commands {
    /// Create a new migration file
    Add {
        /// Name of the migration (will be prefixed with version number)
        name: String,
        /// Create a reversible migration with up/down scripts
        #[clap(long, short = 'r')]
        reversible: bool,
        /// Create a simple migration with a single script
        #[clap(long, short = 's')]
        simple: bool,
    },
    /// Display migration status information
    Info {
        /// Skip validation of missing local migration files
        #[clap(long, short = 'I')]
        ignore_missing: bool,
    },
    /// Apply pending migrations
    Up {
        /// Preview migrations without applying them
        #[clap(long, short = 'd')]
        dry_run: bool,
        /// Skip validation of missing local migration files
        #[clap(long, short = 'I')]
        ignore_missing: bool,
        /// Migrate up to a specific version (inclusive)
        #[clap(long, short = 't')]
        target_version: Option<u32>,
    },
    /// Revert applied migrations
    Down {
        /// Preview migrations without reverting them
        #[clap(long, short = 'd')]
        dry_run: bool,
        /// Skip validation of missing local migration files
        #[clap(long, short = 'I')]
        ignore_missing: bool,
        /// Migrate down to a specific version (exclusive)
        #[clap(long, short = 't')]
        target_version: Option<u32>,
    },
}

async fn run() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let Cli {
        username,
        password,
        url,
        database,
        options,
        source,
        command,
    } = Cli::parse();

    if let Commands::Add {
        name,
        reversible,
        simple,
    } = command
    {
        return add(&source, &name, reversible, simple).await;
    }

    if url.is_empty() {
        eyre::bail!("--clickhouse-url must be specified");
    }

    let builder = migration::Builder::new(url)
        .with_username(username)
        .with_password(password)
        .with_database(database)
        .with_options(options);

    let migrator = builder
        .to_migrator()
        .wrap_err_with(|| "Failed to build migrator")?;

    migrator
        .ping()
        .await
        .wrap_err_with(|| "Failed to ping ClickHouse")?;

    match command {
        Commands::Up {
            dry_run,
            ignore_missing,
            target_version,
        } => up(&migrator, &source, dry_run, ignore_missing, target_version).await?,
        Commands::Down {
            dry_run,
            ignore_missing,
            target_version,
        } => down(&migrator, &source, dry_run, ignore_missing, target_version).await?,
        Commands::Info { ignore_missing } => info(&migrator, &source, ignore_missing).await?,
        _ => unreachable!(),
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        tracing::error!(error=?err, "Execute migration failed");
        eprintln!("Execute migration failed, got error: {}", err.root_cause());
        std::process::exit(1);
    }
}

async fn add(src: &str, name: &str, reversible: bool, simple: bool) -> eyre::Result<()> {
    let mode = match (reversible, simple) {
        (true, false) => Some(migration::MigrationFileMode::Reversible),
        (false, true) => Some(migration::MigrationFileMode::Simple),
        (false, false) => None,
        (true, true) => {
            eyre::bail!("Only one of --reversible or --simple can be specified at one time")
        }
    };

    let file_paths = migration::Migrator::add(src, name, mode).await?;
    for fp in file_paths {
        println!("Added migration file to {}", fp)
    }
    Ok(())
}

async fn up(
    migrator: &impl migration::Migration,
    src: &str,
    dry_run: bool,
    ignore_missing: bool,
    target_version: Option<u32>,
) -> eyre::Result<()> {
    let installed = migrator
        .run(src, dry_run, ignore_missing, target_version)
        .await?;

    eprintln!(
        "{}Installed {} migration(s)!",
        if dry_run { "(Prepare) " } else { "" },
        installed.len()
    );
    print_migrations_info(&installed);
    Ok(())
}

async fn down(
    migrator: &impl migration::Migration,
    src: &str,
    dry_run: bool,
    ignore_missing: bool,
    target_version: Option<u32>,
) -> eyre::Result<()> {
    let uninstalled = migrator
        .revert(src, dry_run, ignore_missing, target_version)
        .await?;
    eprintln!(
        "{}Uninstalled {} migration(s)!",
        if dry_run { "(Prepare) " } else { "" },
        uninstalled.len()
    );
    print_migrations_info(&uninstalled);
    Ok(())
}

async fn info(
    migrator: &impl migration::Migration,
    src: &str,
    ignore_missing: bool,
) -> eyre::Result<()> {
    let migrations = migrator.info(src, ignore_missing).await?;
    eprintln!("Migration status");
    print_migrations_info(&migrations);
    Ok(())
}

fn print_migrations_info(migrations: &[migration::MigrationInfo]) {
    for mig in migrations {
        println!(
            "{} {} at {}",
            mig.full_version(),
            match mig.status {
                migration::MigrationStatus::Pending => "pending",
                migration::MigrationStatus::Applied => "applied",
            },
            if mig.status != migration::MigrationStatus::Pending {
                mig.applied_at.to_rfc3339()
            } else {
                "N/A".to_string()
            }
        )
    }
}
