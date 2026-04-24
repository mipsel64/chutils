mod error;

pub use error::*;

pub use macros::Table;

use ch::clickhouse;
use std::{sync::Arc, time::Duration};

use serde::Serialize;
use tokio_util::sync::CancellationToken;

const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Configuration for the buffered inserter.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Max number of rows before triggering a flush.
    pub max_rows: Option<usize>,
    /// Max buffered bytes (estimated via `std::mem::size_of::<T>() * row_count`)
    /// before triggering a flush.
    pub max_bytes: Option<u64>,
    /// Max time between flushes.
    pub flush_interval: Option<Duration>,

    /// Whether to drop buffered rows on flush failure after exhausting retries.
    /// If false, the buffer will be remained and the inserter will continue retrying
    /// on the next flush trigger.
    pub drop_on_failure: bool,

    /// Number of retry attempts on flush failure.
    pub max_retries: usize,
    /// Maximum backoff duration between retries. The backoff starts at `INITIAL_BACKOFF`
    pub max_backoff: Duration,
    /// Optional initial backoff duration between retries. If not set, defaults to `INITIAL_BACKOFF`.
    pub initial_backoff: Duration,
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = Some(max_rows);
        self
    }

    pub fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    pub fn with_flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = Some(flush_interval);
        self
    }

    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn with_max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    pub fn with_initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.initial_backoff = initial_backoff;
        self
    }

    pub fn with_drop_on_failure(mut self, drop_on_failure: bool) -> Self {
        self.drop_on_failure = drop_on_failure;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_rows: Some(1000),
            max_bytes: None,
            flush_interval: Some(Duration::from_secs(5)),
            max_retries: 5,
            max_backoff: MAX_BACKOFF,
            initial_backoff: INITIAL_BACKOFF,
            drop_on_failure: false,
        }
    }
}

#[async_trait::async_trait]
pub trait Insert: Send + Sync {
    type Row: clickhouse::Row + Serialize + Send + Sync + 'static;

    async fn insert(&self, row: Self::Row) -> Result<()>;
    async fn insert_many(&self, rows: Vec<Self::Row>) -> Result<()>;
}

/// A handle used by callers to send rows. Cheaply cloneable.
#[derive(Clone)]
pub struct Inserter<T> {
    tx: async_channel::Sender<T>,
}

impl<T: Send + 'static> Inserter<T> {
    /// Send a single row to the background buffer.
    pub async fn write(&self, row: T) -> Result<()> {
        self.tx
            .send(row)
            .await
            .map_err(|err| Error::SendError(err.to_string()))
    }

    /// Send multiple rows to the background buffer.
    pub async fn write_many(&self, rows: impl IntoIterator<Item = T>) -> Result<()> {
        for row in rows {
            self.write(row).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> Insert for Inserter<T>
where
    T: clickhouse::Row + Serialize + Send + Sync + 'static,
{
    type Row = T;

    async fn insert(&self, row: Self::Row) -> Result<()> {
        self.write(row).await
    }

    async fn insert_many(&self, rows: Vec<Self::Row>) -> Result<()> {
        self.write_many(rows).await
    }
}

#[derive(Clone)]
pub struct NoInserter<T> {
    ch_client: Arc<clickhouse::Client>,
    table: Arc<String>,
    config: Config,
    _phantom: std::marker::PhantomData<T>,
}

#[async_trait::async_trait]
impl<T> Insert for NoInserter<T>
where
    T: clickhouse::Row + Serialize + Send + Sync + 'static,
{
    type Row = T;

    async fn insert(&self, row: Self::Row) -> Result<()> {
        flush_with_retry(&self.ch_client, &self.table, &[row], &self.config).await?;
        Ok(())
    }

    async fn insert_many(&self, rows: Vec<Self::Row>) -> Result<()> {
        flush_with_retry(&self.ch_client, &self.table, &rows, &self.config).await?;
        Ok(())
    }
}

/// Spawn a background inserter task. Returns a sender handle.
///
/// The background task:
/// 1. Receives rows from the channel into an internal `Vec<T>` buffer.
/// 2. When `max_rows`, `max_bytes`, or `flush_interval` thresholds are met,
///    flushes the buffer via `clickhouse::Client::insert()`.
/// 3. On flush failure, retries with exponential backoff, re-writing all rows
///    each attempt (the `insert()` API creates a fresh INSERT per call).
/// 4. On cancellation, flushes any remaining rows before exiting.
pub fn spawn_with_table<T>(
    client: Arc<clickhouse::Client>,
    table: &str,
    config: Config,
    cancel: CancellationToken,
) -> Arc<dyn Insert<Row = T>>
where
    T: clickhouse::Row + Serialize,
    T: Send + Sync + 'static,
{
    if config.max_rows.is_none() && config.max_bytes.is_none() && config.flush_interval.is_none() {
        tracing::warn!(
            table,
            "No flush thresholds configured, flushing on every insert",
        );
        return Arc::new(NoInserter {
            ch_client: client,
            table: Arc::new(table.to_string()),
            config,
            _phantom: std::marker::PhantomData,
        });
    }

    let (tx, rx) = async_channel::unbounded::<T>();
    let table = table.to_string();

    tokio::spawn(async move {
        run_background(client, &table, config, rx, cancel).await;
    });

    Arc::new(Inserter { tx })
}

/// Spawn a background inserter task. Returns a sender handle.
///
/// The destination table is inferred from `T` via the [`macros::Table`] trait.
///
/// The background task:
/// 1. Receives rows from the channel into an internal `Vec<T>` buffer.
/// 2. When `max_rows`, `max_bytes`, or `flush_interval` thresholds are met,
///    flushes the buffer via `clickhouse::Client::insert()`.
/// 3. On flush failure, retries with exponential backoff, re-writing all rows
///    each attempt (the `insert()` API creates a fresh INSERT per call).
/// 4. On cancellation, flushes any remaining rows before exiting.
pub fn spawn<T>(
    client: Arc<clickhouse::Client>,
    config: Config,
    cancel: CancellationToken,
) -> Arc<dyn Insert<Row = T>>
where
    T: clickhouse::Row + Serialize + macros::Table + Send + Sync + 'static,
{
    let table = T::table_name();
    spawn_with_table(client, table, config, cancel)
}

async fn run_background<T>(
    client: Arc<clickhouse::Client>,
    table: &str,
    config: Config,
    rx: async_channel::Receiver<T>,
    cancel: CancellationToken,
) where
    T: clickhouse::Row + Serialize,
    T: Send + Sync + 'static,
{
    let flush_interval = config.flush_interval.unwrap_or(Duration::from_secs(1));
    let mut tick = tokio::time::interval(flush_interval);
    // The first tick fires immediately, consume it so the first real tick is after the interval.
    tick.tick().await;

    let mut buffer: Vec<T> = Vec::new();
    let row_size = std::mem::size_of::<T>();

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                // Drain any remaining messages from the channel.
                rx.close();
                while let Ok(row) = rx.try_recv() {
                    buffer.push(row);
                }

                if !buffer.is_empty() {
                    tracing::info!(
                        table,
                        rows = buffer.len(),
                        "Flushing remaining rows before shutdown",
                    );
                    if let Err(err) = flush_with_retry(&client, table, &buffer, &config).await {
                        tracing::error!(table, error = ?err, "Failed to flush remaining rows on shutdown");
                    }
                }
                return;
            }

            _ = tick.tick() => {

                if buffer.is_empty() {
                    continue;
                }

                let result = flush_with_retry(&client, table, &buffer, &config).await;

                if result.is_ok() || config.drop_on_failure {
                    buffer.clear();
                }

                if let Err(err) = result {
                    tracing::error!(table, error = ?err, "Failed to flush rows on interval");
                    if config.drop_on_failure {
                        tracing::warn!(table, "Buffer cleared due to flush failure and drop_on_failure=true");
                    }
                }
            }

            msg = rx.recv() => {
                let Ok(row) = msg else {
                    // Channel closed — flush remainder.
                    if !buffer.is_empty() {
                        if let Err(err) = flush_with_retry(&client, table, &buffer, &config).await {
                            tracing::error!(table, error = ?err, "Failed to flush rows on channel close");
                        }
                    }

                    return;
                };

                buffer.push(row);

                if should_flush(&buffer, row_size, &config) {
                    let result = flush_with_retry(&client, table, &buffer, &config).await;

                    if result.is_ok() || config.drop_on_failure {
                        buffer.clear();
                    }

                    if let Err(err) = result {
                        tracing::error!(table, error = ?err, "Failed to flush rows on threshold");
                        if config.drop_on_failure {
                            tracing::warn!(table, "Buffer cleared due to flush failure and drop_on_failure=true");
                        }
                    }
                }
            }
        }
    }
}

fn should_flush<T>(buffer: &[T], row_size: usize, config: &Config) -> bool {
    if config.max_rows.is_none() && config.max_bytes.is_none() && config.flush_interval.is_none() {
        return true;
    }

    if matches!(config.max_rows, Some(max_rows) if max_rows >= buffer.len()) {
        return true;
    }

    let estimated_bytes = (buffer.len() * row_size) as u64;
    if matches!(config.max_bytes, Some(max_bytes) if max_bytes >= estimated_bytes) {
        return true;
    }

    false
}

/// Flush the given rows using `clickhouse::Client::insert()`.
///
/// Each retry creates a fresh INSERT statement and re-writes all rows, so a
/// prior failed flush doesn't corrupt state. The caller is responsible for
/// clearing the buffer based on the result and `Config::drop_on_failure`.
async fn flush_with_retry<T>(
    client: &clickhouse::Client,
    table: &str,
    buffer: &[T],
    cfg: &Config,
) -> Result<()>
where
    T: clickhouse::Row + Serialize + 'static,
{
    if buffer.is_empty() {
        return Ok(());
    }

    let mut backoff = cfg.initial_backoff;

    for attempt in 0..=cfg.max_retries {
        match try_flush(client, table, buffer).await {
            Ok(()) => {
                tracing::debug!(table, rows = buffer.len(), "Flush successful");
                return Ok(());
            }
            Err(err) => {
                if attempt >= cfg.max_retries {
                    tracing::error!(
                        table,
                        attempt = attempt + 1,
                        max_attempts = cfg.max_retries + 1,
                        rows = buffer.len(),
                        error = ?err,
                        "Flush failed, max attempts exceeded. Drop rows.",
                    );
                    return Err(Error::MaxAttemptsExceeded(Box::new(err)));
                }
                tracing::warn!(
                    table,
                    attempt = attempt + 1,
                    max_attempts = cfg.max_retries + 1,
                    rows = buffer.len(),
                    error = ?err,
                    backoff_ms = backoff.as_millis(),
                    "Flush failed, retrying",
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(cfg.max_backoff);
            }
        }
    }

    unreachable!("loop always returns")
}

async fn try_flush<T>(client: &clickhouse::Client, table: &str, rows: &[T]) -> Result<()>
where
    T: clickhouse::Row + Serialize + 'static,
{
    let mut insert = client.insert::<T>(table)?;

    for row in rows {
        insert.write(row).await?;
    }

    insert.end().await?;
    Ok(())
}
