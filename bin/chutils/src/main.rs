mod backup;
mod migration;
mod restore;
mod status;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
pub enum Command {
    /// Run database migrations
    Migrate(migration::Command),
    /// Backup the database
    Backup(backup::Command),
    /// Restore the database from a backup
    Restore(restore::Command),
    /// Show backup status
    Status(status::Command),
}

async fn run() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let command = Command::parse();
    match command {
        Command::Migrate(cmd) => cmd.execute().await?,
        Command::Backup(cmd) => cmd.execute().await?,
        Command::Restore(cmd) => cmd.execute().await?,
        Command::Status(cmd) => cmd.execute().await?,
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
