use backup::{Backup, BackupConfig, Client, Restore, RestoreConfig, Status, StoreMethod};
use serial_test::serial;
use std::sync::LazyLock;
use std::time::Duration;

/// Unique prefix per test run so S3 paths never collide with previous runs.
static RUN_ID: LazyLock<u128> = LazyLock::new(|| {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
});

/// Build a backup Client from TEST_CLICKHOUSE_URL environment variable.
/// Returns None if the env var is not set, allowing tests to be skipped.
fn build_backup_client() -> Option<Client> {
    let url = std::env::var("TEST_CLICKHOUSE_URL").ok()?;
    if url.is_empty() {
        return None;
    }

    let mut builder = ch::Builder::new(&url);

    if let Ok(user) = std::env::var("TEST_CLICKHOUSE_USER") {
        builder = builder.with_username(Some(user));
    }

    if let Ok(password) = std::env::var("TEST_CLICKHOUSE_PASSWORD") {
        builder = builder.with_password(Some(password));
    }

    let client: Client = builder.try_into().expect("Failed to build backup client");
    Some(client)
}

/// Build a raw clickhouse client for test setup/teardown queries
fn build_raw_client() -> clickhouse::Client {
    let url = std::env::var("TEST_CLICKHOUSE_URL").unwrap();
    let mut client = clickhouse::Client::default().with_url(url);

    if let Ok(user) = std::env::var("TEST_CLICKHOUSE_USER") {
        client = client.with_user(user);
    }
    if let Ok(password) = std::env::var("TEST_CLICKHOUSE_PASSWORD") {
        client = client.with_password(password);
    }

    client
}

/// Build a StoreMethod::S3 from TEST_S3_URL environment variable.
/// Returns None if the env var is not set, allowing S3 tests to be skipped.
fn build_s3_store(prefix: &str) -> Option<StoreMethod> {
    let url = std::env::var("TEST_S3_URL").ok()?;
    if url.is_empty() {
        return None;
    }

    let access_key =
        std::env::var("TEST_S3_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key =
        std::env::var("TEST_S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());

    Some(StoreMethod::S3 {
        url,
        access_key,
        secret_key,
        prefix_path: Some(format!("{}/{}", *RUN_ID, prefix)),
    })
}

/// Macro to skip test if ClickHouse is not available
macro_rules! require_clickhouse {
    () => {
        match build_backup_client() {
            Some(client) => client,
            None => {
                eprintln!("Skipping test: TEST_CLICKHOUSE_URL not set");
                return;
            }
        }
    };
}

/// Macro to skip test if S3 (MinIO) is not available
macro_rules! require_s3 {
    ($prefix:expr) => {
        match build_s3_store($prefix) {
            Some(store) => store,
            None => {
                eprintln!("Skipping test: TEST_S3_URL not set");
                return;
            }
        }
    };
}

/// Poll backup status until all backup IDs are completed or failed.
/// Returns the final statuses.
async fn wait_for_backup_completion(
    client: &Client,
    backup_ids: &[String],
    timeout: Duration,
) -> Vec<backup::BackupStatus> {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!(
                "Backup did not complete within {:?} for IDs: {:?}",
                timeout, backup_ids
            );
        }

        let statuses = client
            .status(backup_ids, Duration::from_secs(3600))
            .await
            .expect("Failed to fetch backup status");

        let all_done = !statuses.is_empty()
            && statuses.iter().all(|s| {
                s.status.to_string() == "BACKUP_CREATED"
                    || s.status.to_string() == "RESTORED"
                    || s.status.to_string() == "BACKUP_FAILED"
                    || s.status.to_string() == "RESTORE_FAILED"
            });

        if all_done {
            return statuses;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Drop the database if it exists, then create it fresh with a test table and sample data.
async fn setup_test_table(raw: &clickhouse::Client, db: &str, table: &str) {
    raw.query(&format!("DROP DATABASE IF EXISTS {}", db))
        .execute()
        .await
        .expect("Failed to drop test database");

    raw.query(&format!("CREATE DATABASE {}", db))
        .execute()
        .await
        .expect("Failed to create test database");

    raw.query(&format!(
        "CREATE TABLE {}.{} (id UInt32, name String) ENGINE = MergeTree() ORDER BY id",
        db, table
    ))
    .execute()
    .await
    .expect("Failed to create test table");

    raw.query(&format!(
        "INSERT INTO {}.{} (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
        db, table
    ))
    .execute()
    .await
    .expect("Failed to insert test data");
}

// ==================== BackupConfig Validation Tests ====================

#[tokio::test]
async fn test_backup_config_validates_empty_db() {
    let cfg = BackupConfig::new(StoreMethod::File("/tmp/test".to_string()), "");
    let result = cfg.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Database name must be specified")
    );
}

#[tokio::test]
async fn test_backup_config_validates_s3_empty_url() {
    let store = StoreMethod::S3 {
        url: "".to_string(),
        access_key: "key".to_string(),
        secret_key: "secret".to_string(),
        prefix_path: None,
    };
    let cfg = BackupConfig::new(store, "mydb");
    let result = cfg.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("S3 URL must be specified")
    );
}

#[tokio::test]
async fn test_backup_config_validates_s3_empty_access_key() {
    let store = StoreMethod::S3 {
        url: "http://localhost:9000/bucket".to_string(),
        access_key: "".to_string(),
        secret_key: "secret".to_string(),
        prefix_path: None,
    };
    let cfg = BackupConfig::new(store, "mydb");
    let result = cfg.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("S3 Access Key must be specified")
    );
}

#[tokio::test]
async fn test_backup_config_validates_s3_empty_secret_key() {
    let store = StoreMethod::S3 {
        url: "http://localhost:9000/bucket".to_string(),
        access_key: "key".to_string(),
        secret_key: "".to_string(),
        prefix_path: None,
    };
    let cfg = BackupConfig::new(store, "mydb");
    let result = cfg.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("S3 Secret Key must be specified")
    );
}

#[tokio::test]
async fn test_restore_config_validates_empty_source_db() {
    let cfg = RestoreConfig::new(StoreMethod::File("/tmp/test".to_string()), "");
    let result = cfg.validate();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Source database name must be specified")
    );
}

#[tokio::test]
async fn test_backup_config_builder() {
    let store = StoreMethod::File("/tmp/test".to_string());
    let cfg = BackupConfig::new(store, "mydb")
        .add_table("users")
        .add_table("orders")
        .add_option("base_backup=''");

    assert_eq!(cfg.db, "mydb");
    assert_eq!(cfg.tables, vec!["users", "orders"]);
    assert_eq!(cfg.options, vec!["base_backup=''"]);
}

#[tokio::test]
async fn test_restore_config_builder() {
    let store = StoreMethod::File("/tmp/test".to_string());
    let cfg = RestoreConfig::new(store, "source_db")
        .target_db(Some("target_db"))
        .add_table("users")
        .mode(backup::RestoreMode::StructureOnly);

    assert_eq!(cfg.source_db, "source_db");
    assert_eq!(cfg.target_db, Some("target_db".to_string()));
    assert_eq!(cfg.tables, vec!["users"]);
    assert!(cfg.mode.is_some());
}

// ==================== Backup to S3 Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_backup_nonexistent_database() {
    let client = require_clickhouse!();
    let _store = require_s3!("test_backup_nonexistent_database");

    let store = build_s3_store("test_backup_nonexistent_database").unwrap();
    let cfg = BackupConfig::new(store, "nonexistent_db_12345").add_table("some_table");

    let result = client.backup(cfg).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_backup_nonexistent_table() {
    let client = require_clickhouse!();
    let _store = require_s3!("test_backup_nonexistent_table");
    let raw = build_raw_client();

    let db = "test_backup_nonexistent_table";
    raw.query(&format!("DROP DATABASE IF EXISTS {}", db))
        .execute()
        .await
        .unwrap();
    raw.query(&format!("CREATE DATABASE {}", db))
        .execute()
        .await
        .unwrap();

    let store = build_s3_store("test_backup_nonexistent_table").unwrap();
    let cfg = BackupConfig::new(store, db).add_table("nonexistent_table_12345");

    let result = client.backup(cfg).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_backup_single_table_to_s3() {
    let client = require_clickhouse!();
    let _store = require_s3!("test_backup_single_table_to_s3");
    let raw = build_raw_client();

    let db = "test_backup_single_table_to_s3";
    let table = "data";
    setup_test_table(&raw, db, table).await;

    let store = build_s3_store("test_backup_single_table_to_s3").unwrap();
    let cfg = BackupConfig::new(store, db).add_table(table);

    let backup_ids = client.backup(cfg).await.expect("Backup should succeed");
    assert_eq!(backup_ids.len(), 1, "Should return one backup ID per table");
    assert!(!backup_ids[0].is_empty(), "Backup ID should not be empty");

    let statuses = wait_for_backup_completion(&client, &backup_ids, Duration::from_secs(30)).await;
    assert_eq!(statuses.len(), 1);
    assert_eq!(
        statuses[0].status.to_string(),
        "BACKUP_CREATED",
        "Backup should complete successfully, got: {} (error: {:?})",
        statuses[0].status,
        statuses[0].error
    );
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_backup_multiple_tables_to_s3() {
    let client = require_clickhouse!();
    let _store = require_s3!("test_backup_multiple_tables_to_s3");
    let raw = build_raw_client();

    let db = "test_backup_multiple_tables_to_s3";
    raw.query(&format!("DROP DATABASE IF EXISTS {}", db))
        .execute()
        .await
        .unwrap();
    raw.query(&format!("CREATE DATABASE {}", db))
        .execute()
        .await
        .unwrap();

    raw.query(&format!(
        "CREATE TABLE {}.users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id",
        db
    ))
    .execute()
    .await
    .unwrap();

    raw.query(&format!(
        "CREATE TABLE {}.orders (id UInt32, amount Float64) ENGINE = MergeTree() ORDER BY id",
        db
    ))
    .execute()
    .await
    .unwrap();

    raw.query(&format!(
        "INSERT INTO {}.users (id, name) VALUES (1, 'alice')",
        db
    ))
    .execute()
    .await
    .unwrap();

    raw.query(&format!(
        "INSERT INTO {}.orders (id, amount) VALUES (1, 99.99)",
        db
    ))
    .execute()
    .await
    .unwrap();

    let store = build_s3_store("test_backup_multiple_tables_to_s3").unwrap();
    let cfg = BackupConfig::new(store, db)
        .add_table("users")
        .add_table("orders");

    let backup_ids = client.backup(cfg).await.expect("Backup should succeed");
    assert_eq!(backup_ids.len(), 2, "Should return one backup ID per table");

    let statuses = wait_for_backup_completion(&client, &backup_ids, Duration::from_secs(30)).await;
    assert!(
        statuses
            .iter()
            .all(|s| s.status.to_string() == "BACKUP_CREATED"),
        "All backups should complete successfully: {:?}",
        statuses.iter().map(|s| &s.status).collect::<Vec<_>>()
    );
}

// ==================== Status Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_status_with_no_backups() {
    let client = require_clickhouse!();

    let statuses = client
        .status(
            &["00000000-0000-0000-0000-000000000000".to_string()],
            Duration::from_secs(3600),
        )
        .await
        .expect("Status query should succeed");

    assert!(
        statuses.is_empty(),
        "Should return empty for non-existent backup IDs"
    );
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_status_after_backup() {
    let client = require_clickhouse!();
    let _store = require_s3!("test_status_after_backup");
    let raw = build_raw_client();

    let db = "test_status_after_backup";
    let table = "data";
    setup_test_table(&raw, db, table).await;

    let store = build_s3_store("test_status_after_backup").unwrap();
    let cfg = BackupConfig::new(store, db).add_table(table);

    let backup_ids = client.backup(cfg).await.expect("Backup should succeed");
    assert!(!backup_ids.is_empty());

    let statuses = wait_for_backup_completion(&client, &backup_ids, Duration::from_secs(30)).await;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].status.to_string(), "BACKUP_CREATED");
    assert_eq!(statuses[0].id, backup_ids[0]);
}

// ==================== Restore from S3 Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_backup_and_restore_single_table() {
    let client = require_clickhouse!();
    let _store = require_s3!("test_backup_and_restore_single_table");
    let raw = build_raw_client();

    let db = "test_backup_and_restore_single_table";
    let table = "data";
    setup_test_table(&raw, db, table).await;

    // Backup to S3
    let store = build_s3_store("test_backup_and_restore_single_table").unwrap();
    let backup_cfg = BackupConfig::new(store, db).add_table(table);

    let backup_ids = client
        .backup(backup_cfg)
        .await
        .expect("Backup should succeed");

    let statuses = wait_for_backup_completion(&client, &backup_ids, Duration::from_secs(30)).await;
    assert!(
        statuses
            .iter()
            .all(|s| s.status.to_string() == "BACKUP_CREATED"),
        "Backup should succeed before restore"
    );

    // Drop the table so we can restore into it
    raw.query(&format!("DROP TABLE IF EXISTS {}.{}", db, table))
        .execute()
        .await
        .unwrap();

    // Restore from S3
    let restore_store = build_s3_store("test_backup_and_restore_single_table").unwrap();
    let restore_cfg = RestoreConfig::new(restore_store, db)
        .target_db(Some(db))
        .add_table(table);

    let restore_ids = client
        .restore(restore_cfg)
        .await
        .expect("Restore should succeed");

    assert_eq!(restore_ids.len(), 1, "Should return one restore ID");

    let statuses = wait_for_backup_completion(&client, &restore_ids, Duration::from_secs(30)).await;
    assert!(
        statuses.iter().all(|s| s.status.to_string() == "RESTORED"),
        "Restore should complete successfully: {:?}",
        statuses
            .iter()
            .map(|s| (&s.status, &s.error))
            .collect::<Vec<_>>()
    );

    // Verify data was restored
    let count: u64 = raw
        .query(&format!("SELECT count() FROM {}.{}", db, table))
        .fetch_one()
        .await
        .expect("Should be able to query restored table");

    assert_eq!(count, 3, "Restored table should have 3 rows");
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_backup_and_restore_to_different_database() {
    let client = require_clickhouse!();
    let _store = require_s3!("test_backup_and_restore_to_different_database");
    let raw = build_raw_client();

    let src_db = "test_restore_diff_db_src";
    let target_db = "test_restore_diff_db_dst";
    let table = "data";
    setup_test_table(&raw, src_db, table).await;

    // Clean target db from any previous run
    raw.query(&format!("DROP DATABASE IF EXISTS {}", target_db))
        .execute()
        .await
        .unwrap();

    // Backup source to S3
    let store = build_s3_store("test_backup_and_restore_to_different_database").unwrap();
    let backup_cfg = BackupConfig::new(store, src_db).add_table(table);

    let backup_ids = client
        .backup(backup_cfg)
        .await
        .expect("Backup should succeed");

    let statuses = wait_for_backup_completion(&client, &backup_ids, Duration::from_secs(30)).await;
    assert!(
        statuses
            .iter()
            .all(|s| s.status.to_string() == "BACKUP_CREATED"),
        "Backup should succeed"
    );

    // Create target database
    raw.query(&format!("CREATE DATABASE {}", target_db))
        .execute()
        .await
        .unwrap();

    // Restore into a different database
    let restore_store = build_s3_store("test_backup_and_restore_to_different_database").unwrap();
    let restore_cfg = RestoreConfig::new(restore_store, src_db)
        .target_db(Some(target_db))
        .add_table(table);

    let restore_ids = client
        .restore(restore_cfg)
        .await
        .expect("Restore should succeed");

    let statuses = wait_for_backup_completion(&client, &restore_ids, Duration::from_secs(30)).await;
    assert!(
        statuses.iter().all(|s| s.status.to_string() == "RESTORED"),
        "Restore should complete successfully: {:?}",
        statuses
            .iter()
            .map(|s| (&s.status, &s.error))
            .collect::<Vec<_>>()
    );

    // Verify data in target database
    let count: u64 = raw
        .query(&format!("SELECT count() FROM {}.{}", target_db, table))
        .fetch_one()
        .await
        .expect("Should be able to query restored table in target db");

    assert_eq!(count, 3, "Restored table should have 3 rows");
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_backup_and_restore_structure_only() {
    let client = require_clickhouse!();
    let _store = require_s3!("test_backup_and_restore_structure_only");
    let raw = build_raw_client();

    let db = "test_backup_and_restore_structure_only";
    let table = "data";
    setup_test_table(&raw, db, table).await;

    // Backup to S3
    let store = build_s3_store("test_backup_and_restore_structure_only").unwrap();
    let backup_cfg = BackupConfig::new(store, db).add_table(table);

    let backup_ids = client
        .backup(backup_cfg)
        .await
        .expect("Backup should succeed");

    let statuses = wait_for_backup_completion(&client, &backup_ids, Duration::from_secs(30)).await;
    assert!(
        statuses
            .iter()
            .all(|s| s.status.to_string() == "BACKUP_CREATED")
    );

    // Drop the table
    raw.query(&format!("DROP TABLE IF EXISTS {}.{}", db, table))
        .execute()
        .await
        .unwrap();

    // Restore structure only
    let restore_store = build_s3_store("test_backup_and_restore_structure_only").unwrap();
    let restore_cfg = RestoreConfig::new(restore_store, db)
        .target_db(Some(db))
        .add_table(table)
        .mode(backup::RestoreMode::StructureOnly);

    let restore_ids = client
        .restore(restore_cfg)
        .await
        .expect("Restore structure only should succeed");

    let statuses = wait_for_backup_completion(&client, &restore_ids, Duration::from_secs(30)).await;
    assert!(
        statuses.iter().all(|s| s.status.to_string() == "RESTORED"),
        "Structure-only restore should succeed: {:?}",
        statuses
            .iter()
            .map(|s| (&s.status, &s.error))
            .collect::<Vec<_>>()
    );

    // Table should exist but be empty
    let count: u64 = raw
        .query(&format!("SELECT count() FROM {}.{}", db, table))
        .fetch_one()
        .await
        .expect("Table should exist after structure-only restore");

    assert_eq!(
        count, 0,
        "Table should be empty after structure-only restore"
    );
}

// ==================== Full Workflow Test ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_full_backup_restore_workflow() {
    let client = require_clickhouse!();
    let _store = require_s3!("test_full_backup_restore_workflow");
    let raw = build_raw_client();

    let db = "test_full_backup_restore_workflow";
    raw.query(&format!("DROP DATABASE IF EXISTS {}", db))
        .execute()
        .await
        .unwrap();
    raw.query(&format!("CREATE DATABASE {}", db))
        .execute()
        .await
        .unwrap();

    raw.query(&format!(
        "CREATE TABLE {}.users (id UInt32, name String) ENGINE = MergeTree() ORDER BY id",
        db
    ))
    .execute()
    .await
    .unwrap();

    raw.query(&format!(
        "CREATE TABLE {}.orders (id UInt32, user_id UInt32, amount Float64) ENGINE = MergeTree() ORDER BY id",
        db
    ))
    .execute()
    .await
    .unwrap();

    raw.query(&format!(
        "INSERT INTO {}.users (id, name) VALUES (1, 'alice'), (2, 'bob')",
        db
    ))
    .execute()
    .await
    .unwrap();

    raw.query(&format!(
        "INSERT INTO {}.orders (id, user_id, amount) VALUES (1, 1, 50.0), (2, 2, 75.0), (3, 1, 25.0)",
        db
    ))
    .execute()
    .await
    .unwrap();

    // Backup both tables to S3
    let store = build_s3_store("test_full_backup_restore_workflow").unwrap();
    let backup_cfg = BackupConfig::new(store, db)
        .add_table("users")
        .add_table("orders");

    let backup_ids = client
        .backup(backup_cfg)
        .await
        .expect("Backup should succeed");
    assert_eq!(backup_ids.len(), 2);

    let statuses = wait_for_backup_completion(&client, &backup_ids, Duration::from_secs(30)).await;
    assert!(
        statuses
            .iter()
            .all(|s| s.status.to_string() == "BACKUP_CREATED"),
        "All backups should succeed: {:?}",
        statuses
            .iter()
            .map(|s| (&s.status, &s.error))
            .collect::<Vec<_>>()
    );

    // Verify status reports correct info
    let final_statuses = client
        .status(&backup_ids, Duration::from_secs(3600))
        .await
        .expect("Status should work");
    assert_eq!(final_statuses.len(), 2);
    assert!(
        final_statuses
            .iter()
            .all(|s| s.status.to_string() == "BACKUP_CREATED")
    );

    // Drop source tables
    raw.query(&format!("DROP TABLE IF EXISTS {}.users", db))
        .execute()
        .await
        .unwrap();
    raw.query(&format!("DROP TABLE IF EXISTS {}.orders", db))
        .execute()
        .await
        .unwrap();

    // Restore both tables
    let restore_store = build_s3_store("test_full_backup_restore_workflow").unwrap();
    let restore_cfg = RestoreConfig::new(restore_store, db)
        .target_db(Some(db))
        .add_table("users")
        .add_table("orders");

    let restore_ids = client
        .restore(restore_cfg)
        .await
        .expect("Restore should succeed");
    assert_eq!(restore_ids.len(), 2);

    let statuses = wait_for_backup_completion(&client, &restore_ids, Duration::from_secs(30)).await;
    assert!(
        statuses.iter().all(|s| s.status.to_string() == "RESTORED"),
        "All restores should succeed: {:?}",
        statuses
            .iter()
            .map(|s| (&s.status, &s.error))
            .collect::<Vec<_>>()
    );

    // Verify data integrity
    let user_count: u64 = raw
        .query(&format!("SELECT count() FROM {}.users", db))
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(user_count, 2, "Users table should have 2 rows");

    let order_count: u64 = raw
        .query(&format!("SELECT count() FROM {}.orders", db))
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(order_count, 3, "Orders table should have 3 rows");
}
