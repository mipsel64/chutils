//! Integration tests for `chutils copy`.
//!
//! Tests drive the compiled `chutils` binary via `std::process::Command` and
//! exercise against a real ClickHouse reachable at `TEST_CLICKHOUSE_URL`.
//! A single ClickHouse instance stands in for "two clusters" by using two
//! distinct databases per test; the copy command still uses its independent
//! `--src-*`/`--dst-*` endpoint flags, so the code path is the same as a
//! genuine cross-cluster run.
//!
//! Every test skips gracefully if `TEST_CLICKHOUSE_URL` is not set, so the
//! suite is safe to run in environments without a ClickHouse available.

use ch::clickhouse;
use serial_test::serial;
use std::process::Command;
use std::sync::LazyLock;

/// Unique run id so a failed test's leftover databases can't collide with a
/// concurrent or subsequent run.
static RUN_ID: LazyLock<u128> = LazyLock::new(|| {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
});

const BIN: &str = env!("CARGO_BIN_EXE_chutils");

fn clickhouse_url() -> Option<String> {
    let url = std::env::var("TEST_CLICKHOUSE_URL").ok()?;
    if url.is_empty() { None } else { Some(url) }
}

fn test_user() -> Option<String> {
    std::env::var("TEST_CLICKHOUSE_USER").ok()
}

fn test_password() -> Option<String> {
    std::env::var("TEST_CLICKHOUSE_PASSWORD").ok()
}

fn raw_client(url: &str) -> clickhouse::Client {
    let mut c = clickhouse::Client::default().with_url(url);
    if let Some(u) = test_user() {
        c = c.with_user(u);
    }
    if let Some(p) = test_password() {
        c = c.with_password(p);
    }
    c
}

async fn exec(client: &clickhouse::Client, sql: &str) {
    client
        .query(sql)
        .execute()
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
}

/// Drop DB if exists, create it, create a MergeTree `events` table, and
/// populate it with `rows` rows of synthetic data.
async fn setup_events_db(client: &clickhouse::Client, db: &str, rows: u32) {
    exec(client, &format!("DROP DATABASE IF EXISTS {db}")).await;
    exec(client, &format!("CREATE DATABASE {db}")).await;
    exec(
        client,
        &format!(
            "CREATE TABLE {db}.events (
                id UInt64,
                payload String
            ) ENGINE = MergeTree() ORDER BY id"
        ),
    )
    .await;
    exec(
        client,
        &format!(
            "INSERT INTO {db}.events (id, payload)
             SELECT number AS id, concat('p-', toString(number)) AS payload
             FROM numbers({rows})"
        ),
    )
    .await;
}

/// Create an empty destination `events` table with the same schema as
/// `setup_events_db` produces.
async fn setup_empty_dst(client: &clickhouse::Client, db: &str) {
    exec(client, &format!("DROP DATABASE IF EXISTS {db}")).await;
    exec(client, &format!("CREATE DATABASE {db}")).await;
    exec(
        client,
        &format!(
            "CREATE TABLE {db}.events (
                id UInt64,
                payload String
            ) ENGINE = MergeTree() ORDER BY id"
        ),
    )
    .await;
}

async fn count_rows(client: &clickhouse::Client, db: &str, table: &str) -> u64 {
    client
        .query(&format!("SELECT count() FROM {db}.{table}"))
        .fetch_one::<u64>()
        .await
        .expect("count() failed")
}

async fn drop_db(client: &clickhouse::Client, db: &str) {
    exec(client, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

/// Build the common `src-*` / `dst-*` arg set pointing both sides at the same
/// URL but different databases.
fn base_args(url: &str, src_db: &str, dst_db: &str) -> Vec<String> {
    let mut args = vec![
        "copy".to_string(),
        "--src-url".to_string(),
        url.to_string(),
        "--dst-url".to_string(),
        url.to_string(),
        "--src-db".to_string(),
        src_db.to_string(),
        "--dst-db".to_string(),
        dst_db.to_string(),
        "--dst-table".to_string(),
        "events".to_string(),
    ];
    if let Some(u) = test_user() {
        args.push("--src-user".to_string());
        args.push(u.clone());
        args.push("--dst-user".to_string());
        args.push(u);
    }
    if let Some(p) = test_password() {
        args.push("--src-password".to_string());
        args.push(p.clone());
        args.push("--dst-password".to_string());
        args.push(p);
    }
    args
}

macro_rules! require_ch {
    () => {
        match clickhouse_url() {
            Some(url) => url,
            None => {
                eprintln!("Skipping: TEST_CLICKHOUSE_URL not set");
                return;
            }
        }
    };
}

/// End-to-end single-shot copy: 30 rows go through `chutils copy` and land
/// in the destination table intact.
#[tokio::test]
#[serial]
async fn copy_full_table() {
    let url = require_ch!();
    let ch = raw_client(&url);
    let src_db = format!("chutils_copy_full_src_{}", *RUN_ID);
    let dst_db = format!("chutils_copy_full_dst_{}", *RUN_ID);

    setup_events_db(&ch, &src_db, 30).await;
    setup_empty_dst(&ch, &dst_db).await;

    let mut args = base_args(&url, &src_db, &dst_db);
    args.push("--query".to_string());
    args.push("SELECT * FROM events".to_string());

    let out = Command::new(BIN)
        .args(&args)
        .output()
        .expect("spawn chutils");
    assert!(
        out.status.success(),
        "chutils copy failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    assert_eq!(count_rows(&ch, &dst_db, "events").await, 30);

    drop_db(&ch, &src_db).await;
    drop_db(&ch, &dst_db).await;
}

/// A filtered query copies only the matching rows.
#[tokio::test]
#[serial]
async fn copy_with_where_clause() {
    let url = require_ch!();
    let ch = raw_client(&url);
    let src_db = format!("chutils_copy_where_src_{}", *RUN_ID);
    let dst_db = format!("chutils_copy_where_dst_{}", *RUN_ID);

    setup_events_db(&ch, &src_db, 100).await;
    setup_empty_dst(&ch, &dst_db).await;

    let mut args = base_args(&url, &src_db, &dst_db);
    args.push("--query".to_string());
    args.push("SELECT * FROM events WHERE id < 25".to_string());

    let out = Command::new(BIN)
        .args(&args)
        .output()
        .expect("spawn chutils");
    assert!(
        out.status.success(),
        "chutils copy failed\nstderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    assert_eq!(count_rows(&ch, &dst_db, "events").await, 25);

    drop_db(&ch, &src_db).await;
    drop_db(&ch, &dst_db).await;
}

/// Schema pre-check rejects a copy whose source projects a column that does
/// not exist on the destination, BEFORE any INSERT is attempted.
#[tokio::test]
#[serial]
async fn copy_schema_check_rejects_missing_destination_column() {
    let url = require_ch!();
    let ch = raw_client(&url);
    let src_db = format!("chutils_copy_schema_src_{}", *RUN_ID);
    let dst_db = format!("chutils_copy_schema_dst_{}", *RUN_ID);

    setup_events_db(&ch, &src_db, 5).await;

    // Destination is missing the `payload` column.
    exec(&ch, &format!("DROP DATABASE IF EXISTS {dst_db}")).await;
    exec(&ch, &format!("CREATE DATABASE {dst_db}")).await;
    exec(
        &ch,
        &format!(
            "CREATE TABLE {dst_db}.events (id UInt64)
             ENGINE = MergeTree() ORDER BY id"
        ),
    )
    .await;

    let mut args = base_args(&url, &src_db, &dst_db);
    args.extend(["--query".to_string(), "SELECT * FROM events".to_string()]);

    let out = Command::new(BIN)
        .args(&args)
        .output()
        .expect("spawn chutils");
    assert!(
        !out.status.success(),
        "expected failure but chutils exited 0"
    );
    // `tracing_subscriber::fmt()` writes to stdout by default, so check both
    // streams — this test only cares that the failure was explained somewhere.
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stderr.contains("Schema check failed")
            || stderr.contains("payload")
            || stdout.contains("Schema check failed")
            || stdout.contains("payload"),
        "output did not mention schema failure\nstdout: {stdout}\nstderr: {stderr}"
    );

    // Destination must be empty — no INSERT should have run.
    assert_eq!(count_rows(&ch, &dst_db, "events").await, 0);

    drop_db(&ch, &src_db).await;
    drop_db(&ch, &dst_db).await;
}
