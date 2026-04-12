use ch::clickhouse;
use clickhouse::Row;
use inserter::{Config, Insert};
use serde::Serialize;
use serial_test::serial;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Atomic counter to generate unique table suffixes for test isolation.
static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

fn unique_table_name() -> String {
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("test_inserter_{}_{}", std::process::id(), id)
}

/// A simple row type used across all tests.
#[derive(Debug, Clone, Row, Serialize)]
struct TestRow {
    id: u32,
    name: String,
}

/// Build a raw clickhouse client from TEST_CLICKHOUSE_URL.
/// Returns None if the env var is not set, allowing tests to be skipped.
fn build_client() -> Option<clickhouse::Client> {
    let url = std::env::var("TEST_CLICKHOUSE_URL").ok()?;
    if url.is_empty() {
        return None;
    }

    let mut client = clickhouse::Client::default().with_url(url);

    if let Ok(user) = std::env::var("TEST_CLICKHOUSE_USER") {
        client = client.with_user(user);
    }
    if let Ok(password) = std::env::var("TEST_CLICKHOUSE_PASSWORD") {
        client = client.with_password(password);
    }

    Some(client)
}

macro_rules! require_clickhouse {
    () => {
        match build_client() {
            Some(client) => client,
            _ => {
                eprintln!("Skipping test: TEST_CLICKHOUSE_URL not set");
                return;
            }
        }
    };
}

/// Create a fresh test table and return its name. Drops any leftover table
/// from a previous failed run first to guarantee a clean starting state.
async fn create_test_table(client: &clickhouse::Client) -> String {
    let table = unique_table_name();
    // Drop first to clean up leftovers from a previous failed run where
    // drop_test_table may not have executed due to a panic.
    client
        .query(&format!("DROP TABLE IF EXISTS {}", table))
        .execute()
        .await
        .ok();
    client
        .query(&format!(
            "CREATE TABLE {} (id UInt32, name String) ENGINE = Memory",
            table
        ))
        .execute()
        .await
        .expect("Failed to create test table");
    table
}

async fn drop_test_table(client: &clickhouse::Client, table: &str) {
    client
        .query(&format!("DROP TABLE IF EXISTS {}", table))
        .execute()
        .await
        .ok();
}

async fn count_rows(client: &clickhouse::Client, table: &str) -> u64 {
    client
        .query(&format!("SELECT count() FROM {}", table))
        .fetch_one()
        .await
        .unwrap()
}

fn test_rows(n: u32) -> Vec<TestRow> {
    (0..n)
        .map(|i| TestRow {
            id: i,
            name: format!("row_{}", i),
        })
        .collect()
}

// ==================== Basic Spawn + Insert Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_spawn_insert_single_row() {
    let client = require_clickhouse!();
    let table = create_test_table(&client).await;

    let cancel = CancellationToken::new();
    let config = Config::new().with_flush_interval(Duration::from_secs(60));
    let inserter =
        inserter::spawn::<TestRow>(Arc::new(client.clone()), &table, config, cancel.clone());

    inserter
        .insert(TestRow {
            id: 1,
            name: "alice".into(),
        })
        .await
        .unwrap();

    // Cancel triggers a final flush of buffered rows.
    cancel.cancel();
    // Give the background task time to flush.
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(count_rows(&client, &table).await, 1);

    drop_test_table(&client, &table).await;
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_spawn_insert_many() {
    let client = require_clickhouse!();
    let table = create_test_table(&client).await;

    let cancel = CancellationToken::new();
    let config = Config::new().with_flush_interval(Duration::from_secs(60));
    let inserter =
        inserter::spawn::<TestRow>(Arc::new(client.clone()), &table, config, cancel.clone());

    inserter.insert_many(test_rows(50)).await.unwrap();

    cancel.cancel();
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(count_rows(&client, &table).await, 50);

    drop_test_table(&client, &table).await;
}

// ==================== Flush Threshold Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_flush_on_max_rows_threshold() {
    let client = require_clickhouse!();
    let table = create_test_table(&client).await;

    let cancel = CancellationToken::new();
    let config = Config::new()
        .with_max_rows(10)
        .with_flush_interval(Duration::from_secs(60));
    let inserter =
        inserter::spawn::<TestRow>(Arc::new(client.clone()), &table, config, cancel.clone());

    // Write exactly max_rows to trigger a threshold flush.
    inserter.insert_many(test_rows(10)).await.unwrap();

    // Wait for the flush to complete (no cancellation needed).
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(count_rows(&client, &table).await, 10);

    cancel.cancel();
    tokio::time::sleep(Duration::from_secs(1)).await;

    drop_test_table(&client, &table).await;
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_flush_on_interval() {
    let client = require_clickhouse!();
    let table = create_test_table(&client).await;

    let cancel = CancellationToken::new();
    let config = Config::new()
        .with_max_rows(1000) // high threshold — won't trigger
        .with_flush_interval(Duration::from_millis(200));
    let inserter =
        inserter::spawn::<TestRow>(Arc::new(client.clone()), &table, config, cancel.clone());

    inserter.insert_many(test_rows(5)).await.unwrap();

    // Wait for the interval to fire and flush.
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(count_rows(&client, &table).await, 5);

    cancel.cancel();
    tokio::time::sleep(Duration::from_secs(1)).await;

    drop_test_table(&client, &table).await;
}

// ==================== Cancellation Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_cancel_flushes_remaining_rows() {
    let client = require_clickhouse!();
    let table = create_test_table(&client).await;

    let cancel = CancellationToken::new();
    let config = Config::new()
        .with_max_rows(1000) // high threshold — won't trigger
        .with_flush_interval(Duration::from_secs(60)); // long interval — won't trigger
    let inserter =
        inserter::spawn::<TestRow>(Arc::new(client.clone()), &table, config, cancel.clone());

    inserter.insert_many(test_rows(7)).await.unwrap();

    // Neither threshold nor interval should have fired.
    // Cancel should flush the remaining 7 rows.
    cancel.cancel();
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(count_rows(&client, &table).await, 7);

    drop_test_table(&client, &table).await;
}

// ==================== NoInserter (no thresholds) Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_no_thresholds_flushes_immediately() {
    let client = require_clickhouse!();
    let table = create_test_table(&client).await;

    let cancel = CancellationToken::new();
    let config = Config {
        max_rows: None,
        max_bytes: None,
        flush_interval: None,
        ..Config::default()
    };
    let inserter =
        inserter::spawn::<TestRow>(Arc::new(client.clone()), &table, config, cancel.clone());

    // With no thresholds, each insert flushes synchronously via NoInserter.
    inserter
        .insert(TestRow {
            id: 1,
            name: "immediate".into(),
        })
        .await
        .unwrap();

    // Row should be visible immediately — no need to cancel or sleep.
    assert_eq!(count_rows(&client, &table).await, 1);

    inserter.insert_many(test_rows(3)).await.unwrap();
    assert_eq!(count_rows(&client, &table).await, 4);

    cancel.cancel();
    drop_test_table(&client, &table).await;
}

// ==================== Multiple Batch Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_multiple_batches() {
    let client = require_clickhouse!();
    let table = create_test_table(&client).await;

    let cancel = CancellationToken::new();
    let config = Config::new()
        .with_max_rows(5)
        .with_flush_interval(Duration::from_secs(60));
    let inserter =
        inserter::spawn::<TestRow>(Arc::new(client.clone()), &table, config, cancel.clone());

    // First batch: triggers flush at 5 rows.
    inserter.insert_many(test_rows(5)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(count_rows(&client, &table).await, 5);

    // Second batch: another 5 rows.
    let batch2: Vec<TestRow> = (10..15)
        .map(|i| TestRow {
            id: i,
            name: format!("row_{}", i),
        })
        .collect();
    inserter.insert_many(batch2).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(count_rows(&client, &table).await, 10);

    cancel.cancel();
    tokio::time::sleep(Duration::from_secs(1)).await;

    drop_test_table(&client, &table).await;
}

// ==================== Clone Handle Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_insert_trait_is_send_sync() {
    let client = require_clickhouse!();
    let table = create_test_table(&client).await;

    let cancel = CancellationToken::new();
    let config = Config::new().with_flush_interval(Duration::from_secs(60));
    let inserter: Arc<dyn Insert<Row = TestRow>> =
        inserter::spawn::<TestRow>(Arc::new(client.clone()), &table, config, cancel.clone());

    // Use the trait object from two spawned tasks to verify Send + Sync.
    let ins1 = inserter.clone();
    let ins2 = inserter.clone();

    let h1 = tokio::spawn(async move {
        for i in 0..5 {
            ins1.insert(TestRow {
                id: i,
                name: format!("task1_{}", i),
            })
            .await
            .unwrap();
        }
    });

    let h2 = tokio::spawn(async move {
        for i in 100..105 {
            ins2.insert(TestRow {
                id: i,
                name: format!("task2_{}", i),
            })
            .await
            .unwrap();
        }
    });

    h1.await.unwrap();
    h2.await.unwrap();

    cancel.cancel();
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(count_rows(&client, &table).await, 10);

    drop_test_table(&client, &table).await;
}

// ==================== Drop-on-channel-close Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_drop_inserter_flushes_on_channel_close() {
    let client = require_clickhouse!();
    let table = create_test_table(&client).await;

    let cancel = CancellationToken::new();
    let config = Config::new()
        .with_max_rows(1000)
        .with_flush_interval(Duration::from_secs(60));

    // Scope the inserter so it drops (closing the channel).
    {
        let inserter =
            inserter::spawn::<TestRow>(Arc::new(client.clone()), &table, config, cancel.clone());
        inserter.insert_many(test_rows(3)).await.unwrap();
        // inserter (and its Sender) drops here.
    }

    // The background task should detect channel close and flush.
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(count_rows(&client, &table).await, 3);

    cancel.cancel();
    drop_test_table(&client, &table).await;
}

// ==================== Config Builder Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_config_max_bytes_threshold() {
    let client = require_clickhouse!();
    let table = create_test_table(&client).await;

    let cancel = CancellationToken::new();
    let row_size = std::mem::size_of::<TestRow>() as u64;
    // Set max_bytes to hold ~5 rows worth.
    let config = Config::new()
        .with_max_rows(1000) // high — won't trigger
        .with_max_bytes(row_size * 5)
        .with_flush_interval(Duration::from_secs(60));
    let inserter =
        inserter::spawn::<TestRow>(Arc::new(client.clone()), &table, config, cancel.clone());

    // Write 5 rows — should hit the byte threshold.
    inserter.insert_many(test_rows(5)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(count_rows(&client, &table).await, 5);

    cancel.cancel();
    tokio::time::sleep(Duration::from_secs(1)).await;

    drop_test_table(&client, &table).await;
}
