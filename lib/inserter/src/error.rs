#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Send Error: {0}")]
    SendError(String),

    #[error(transparent)]
    ClickhouseError(#[from] ch::ClickhouseError),

    #[error("Max attempts exceeded: {0}")]
    MaxAttemptsExceeded(Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T> = std::result::Result<T, Error>;
