mod backup;
mod cluster;
mod migration;
mod restore;
mod status;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "chutils")]
pub struct CLI {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(clap::Subcommand)]
pub enum Command {
    /// Run database migrations
    Migrate(migration::Command),
    /// Backup the database
    Backup(backup::Command),
    /// Restore the database from a backup
    Restore(restore::Command),
    /// Show backup status
    Status(status::Command),
    /// Manage database cluster
    Cluster(cluster::Command),
}

fn check_version_flag() -> bool {
    std::env::args().any(|arg| arg == "--version" || arg == "-V")
}

async fn run() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    if check_version_flag() {
        println!("{}", info::version());
        return Ok(());
    }

    let cli = CLI::parse();
    match cli.command {
        Command::Migrate(cmd) => cmd.execute().await?,
        Command::Backup(cmd) => cmd.execute().await?,
        Command::Restore(cmd) => cmd.execute().await?,
        Command::Status(cmd) => cmd.execute().await?,
        Command::Cluster(cmd) => cmd.execute().await?,
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        tracing::error!(error=?err, "Execute failed");
        std::process::exit(1);
    }
}
