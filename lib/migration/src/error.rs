#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Input Clickhouse URL is empty")]
    EmptyUrl,

    #[error("Migration Corrupted: {0}")]
    MigrationCorrupted(String),

    #[error("Invalid Input: {0}")]
    InvalidInput(String),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    ClickhouseError(#[from] clickhouse::error::Error),
}
