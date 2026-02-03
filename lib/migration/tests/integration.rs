use migration::{Builder, Migration, MigrationFileMode, MigrationStatus};
use serial_test::serial;
use std::sync::atomic::{AtomicU32, Ordering};

/// Atomic counter to generate unique table suffixes for parallel test isolation
static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Helper to get a unique test ID for table naming
fn unique_test_id() -> u32 {
    TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Build a ClickHouse client from TEST_CLICKHOUSE_URL environment variable.
/// Returns None if the env var is not set, allowing tests to be skipped.
fn build_clickhouse_client() -> Option<migration::Migrator> {
    let url = std::env::var("TEST_CLICKHOUSE_URL").ok()?;
    if url.is_empty() {
        return None;
    }

    let mut builder = Builder::new(&url);

    if let Ok(user) = std::env::var("TEST_CLICKHOUSE_USER") {
        builder = builder.with_username(Some(user));
    }

    if let Ok(password) = std::env::var("TEST_CLICKHOUSE_PASSWORD") {
        builder = builder.with_password(Some(password));
    }

    let migrator = builder.to_migrator().expect("Failed to build migrator");

    Some(migrator)
}

/// Build a raw clickhouse client for test verification queries
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

/// Clear the migrations table to ensure test isolation
async fn clear_migrations_table() {
    let client = build_raw_client();
    // TRUNCATE is synchronous and immediate
    client
        .query("TRUNCATE TABLE _ch_migrations")
        .execute()
        .await
        .ok();
}

/// Macro to skip test if ClickHouse is not available
macro_rules! require_clickhouse {
    () => {
        match build_clickhouse_client() {
            Some(client) => client,
            None => {
                eprintln!("Skipping test: TEST_CLICKHOUSE_URL not set");
                return;
            }
        }
    };
}

// ==================== Connection Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_ping() {
    let migrator = require_clickhouse!();
    let result = migrator.ping().await;
    assert!(result.is_ok(), "ping failed: {:?}", result.err());
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_ensure_migrations_table() {
    let migrator = require_clickhouse!();

    let result = migrator.ensure_migrations_table().await;
    assert!(
        result.is_ok(),
        "ensure_migrations_table failed: {:?}",
        result.err()
    );
}

// ==================== Info Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_info_empty_directory() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    let result = migrator.info(src, false).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_info_with_local_migrations() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    // Create local migration files
    tokio::fs::write(format!("{}/0001_create_users.sql", src), b"SELECT 1")
        .await
        .unwrap();
    tokio::fs::write(format!("{}/0002_create_posts.sql", src), b"SELECT 2")
        .await
        .unwrap();

    let result = migrator.info(src, false).await;
    assert!(result.is_ok());

    let migrations = result.unwrap();
    assert_eq!(migrations.len(), 2);
    assert_eq!(migrations[0].version, 1);
    assert_eq!(migrations[0].name, "create_users");
    assert_eq!(migrations[0].status, MigrationStatus::Pending);
    assert_eq!(migrations[1].version, 2);
    assert_eq!(migrations[1].name, "create_posts");
    assert_eq!(migrations[1].status, MigrationStatus::Pending);
}

// ==================== Add Migration Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_add_simple_migration() {
    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    let result =
        migration::Migrator::add(src, "create_users", Some(MigrationFileMode::Simple)).await;
    assert!(result.is_ok());

    let files = result.unwrap();
    assert_eq!(files.len(), 1);
    assert!(files[0].ends_with("0001_create_users.sql"));

    // Verify file exists
    assert!(tokio::fs::metadata(&files[0]).await.is_ok());
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_add_reversible_migration() {
    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    let result =
        migration::Migrator::add(src, "create_users", Some(MigrationFileMode::Reversible)).await;
    assert!(result.is_ok());

    let files = result.unwrap();
    assert_eq!(files.len(), 2);
    assert!(files.iter().any(|f| f.ends_with(".up.sql")));
    assert!(files.iter().any(|f| f.ends_with(".down.sql")));
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_add_migration_increments_version() {
    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    // Add first migration
    migration::Migrator::add(src, "first", Some(MigrationFileMode::Simple))
        .await
        .unwrap();

    // Add second migration
    let result = migration::Migrator::add(src, "second", Some(MigrationFileMode::Simple)).await;
    assert!(result.is_ok());

    let files = result.unwrap();
    assert!(files[0].contains("0002_second"));
}

// ==================== Run Migration Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_run_dry_run() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    // Create a migration file
    tokio::fs::write(format!("{}/0001_test.sql", src), b"SELECT 1")
        .await
        .unwrap();

    // Dry run should return pending migrations without applying
    let result = migrator.run(src, true, false, None).await;
    assert!(result.is_ok());

    let pending = result.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, MigrationStatus::Pending);

    // Verify migration is still pending after dry run
    let info = migrator.info(src, false).await.unwrap();
    assert_eq!(info[0].status, MigrationStatus::Pending);
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_run_applies_migrations() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    let test_id = unique_test_id();
    let table_name = format!("test_users_{}", test_id);

    // Create migration that creates a table
    let migration_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (id UInt32) ENGINE = Memory",
        table_name
    );
    tokio::fs::write(
        format!("{}/0001_create_table.sql", src),
        migration_sql.as_bytes(),
    )
    .await
    .unwrap();

    // Run migration
    let result = migrator.run(src, false, false, None).await;
    assert!(result.is_ok(), "run failed: {:?}", result.err());

    let applied = result.unwrap();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].status, MigrationStatus::Applied);

    // Verify migration is now applied
    let info = migrator.info(src, false).await.unwrap();
    assert_eq!(info[0].status, MigrationStatus::Applied);

    // Cleanup: drop the test table
    let client = build_raw_client();
    client
        .query(&format!("DROP TABLE IF EXISTS {}", table_name))
        .execute()
        .await
        .ok();
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_run_with_target_version() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    // Create multiple migration files
    tokio::fs::write(format!("{}/0001_first.sql", src), b"SELECT 1")
        .await
        .unwrap();
    tokio::fs::write(format!("{}/0002_second.sql", src), b"SELECT 2")
        .await
        .unwrap();
    tokio::fs::write(format!("{}/0003_third.sql", src), b"SELECT 3")
        .await
        .unwrap();

    // Run only up to version 2 (dry run to avoid side effects)
    let result = migrator.run(src, true, false, Some(2)).await;
    assert!(result.is_ok());

    let pending = result.unwrap();
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().all(|m| m.version <= 2));
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_run_no_pending_migrations() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    // Empty directory - no migrations to run
    let result = migrator.run(src, false, false, None).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_run_multiple_queries_in_single_file() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    let test_id = unique_test_id();
    let table1 = format!("test_table1_{}", test_id);
    let table2 = format!("test_table2_{}", test_id);

    // Create migration with multiple statements
    let migration_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (id UInt32) ENGINE = Memory;\n\
         CREATE TABLE IF NOT EXISTS {} (id UInt32) ENGINE = Memory;",
        table1, table2
    );
    tokio::fs::write(
        format!("{}/0001_create_tables.sql", src),
        migration_sql.as_bytes(),
    )
    .await
    .unwrap();

    // Run migration
    let result = migrator.run(src, false, false, None).await;
    assert!(result.is_ok(), "run failed: {:?}", result.err());

    // Cleanup
    let client = build_raw_client();
    client
        .query(&format!("DROP TABLE IF EXISTS {}", table1))
        .execute()
        .await
        .ok();
    client
        .query(&format!("DROP TABLE IF EXISTS {}", table2))
        .execute()
        .await
        .ok();
}

// ==================== Revert Migration Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_revert_dry_run() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    let test_id = unique_test_id();
    let table_name = format!("test_revert_{}", test_id);

    // Create reversible migration
    let up_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (id UInt32) ENGINE = Memory",
        table_name
    );
    let down_sql = format!("DROP TABLE IF EXISTS {}", table_name);

    tokio::fs::write(
        format!("{}/0001_create_table.up.sql", src),
        up_sql.as_bytes(),
    )
    .await
    .unwrap();
    tokio::fs::write(
        format!("{}/0001_create_table.down.sql", src),
        down_sql.as_bytes(),
    )
    .await
    .unwrap();

    // Apply migration first
    migrator.run(src, false, false, None).await.unwrap();

    // Dry run revert
    let result = migrator.revert(src, true, false, None).await;
    assert!(result.is_ok());

    let targets = result.unwrap();
    assert_eq!(targets.len(), 1);
    // Status should still be Applied in dry run
    assert_eq!(targets[0].status, MigrationStatus::Applied);

    // Verify migration is still applied
    let info = migrator.info(src, false).await.unwrap();
    assert_eq!(info[0].status, MigrationStatus::Applied);

    // Cleanup
    let client = build_raw_client();
    client
        .query(&format!("DROP TABLE IF EXISTS {}", table_name))
        .execute()
        .await
        .ok();
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_revert_simple_migration_not_revertable() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    // Create simple (non-reversible) migration
    tokio::fs::write(format!("{}/0001_simple.sql", src), b"SELECT 1")
        .await
        .unwrap();

    // Apply migration
    migrator.run(src, false, false, None).await.unwrap();

    // Try to revert - should return empty (no revertable migrations)
    let result = migrator.revert(src, true, false, None).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_revert_executes_down_migration() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    let test_id = unique_test_id();
    let table_name = format!("test_revert_exec_{}", test_id);

    // Create reversible migration
    let up_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (id UInt32) ENGINE = Memory",
        table_name
    );
    let down_sql = format!("DROP TABLE IF EXISTS {}", table_name);

    tokio::fs::write(format!("{}/0001_test.up.sql", src), up_sql.as_bytes())
        .await
        .unwrap();
    tokio::fs::write(format!("{}/0001_test.down.sql", src), down_sql.as_bytes())
        .await
        .unwrap();

    // Apply migration
    migrator.run(src, false, false, None).await.unwrap();

    // Verify table exists
    let client = build_raw_client();
    let exists_result = client
        .query(&format!("EXISTS TABLE {}", table_name))
        .fetch_one::<u8>()
        .await;
    assert!(exists_result.is_ok());
    assert_eq!(exists_result.unwrap(), 1);

    // Revert migration
    let result = migrator.revert(src, false, false, None).await;
    assert!(result.is_ok(), "revert failed: {:?}", result.err());

    let reverted = result.unwrap();
    assert_eq!(reverted.len(), 1);
    assert_eq!(reverted[0].status, MigrationStatus::Pending);

    // Verify table no longer exists
    let exists_result = client
        .query(&format!("EXISTS TABLE {}", table_name))
        .fetch_one::<u8>()
        .await;
    assert!(exists_result.is_ok());
    assert_eq!(exists_result.unwrap(), 0);

    // Verify migration status is now pending
    let info = migrator.info(src, false).await.unwrap();
    assert_eq!(info[0].status, MigrationStatus::Pending);
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_revert_with_target_version() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    // Create multiple reversible migrations
    for i in 1..=3 {
        tokio::fs::write(format!("{}/{:04}_m{}.up.sql", src, i, i), b"SELECT 1")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/{:04}_m{}.down.sql", src, i, i), b"SELECT 1")
            .await
            .unwrap();
    }

    // Apply all migrations
    migrator.run(src, false, false, None).await.unwrap();

    // Revert down to version 1 (keep 1, revert 2 and 3) - dry run
    let result = migrator.revert(src, true, false, Some(1)).await;
    assert!(result.is_ok());

    let targets = result.unwrap();
    assert_eq!(targets.len(), 2);
    assert!(targets.iter().all(|m| m.version > 1));
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_revert_latest_only() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    // Create multiple reversible migrations
    for i in 1..=3 {
        tokio::fs::write(format!("{}/{:04}_m{}.up.sql", src, i, i), b"SELECT 1")
            .await
            .unwrap();
        tokio::fs::write(format!("{}/{:04}_m{}.down.sql", src, i, i), b"SELECT 1")
            .await
            .unwrap();
    }

    // Apply all migrations
    migrator.run(src, false, false, None).await.unwrap();

    // Revert without target_version should only revert the latest
    let result = migrator.revert(src, true, false, None).await;
    assert!(result.is_ok());

    let targets = result.unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].version, 3); // Latest
}

// ==================== Full Workflow Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_full_migration_workflow() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    let test_id = unique_test_id();
    let table_name = format!("test_workflow_{}", test_id);

    // 1. Add a reversible migration
    let files = migration::Migrator::add(src, "create_table", Some(MigrationFileMode::Reversible))
        .await
        .unwrap();
    assert_eq!(files.len(), 2);

    // 2. Write migration content
    let up_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (id UInt32, name String) ENGINE = Memory",
        table_name
    );
    let down_sql = format!("DROP TABLE IF EXISTS {}", table_name);

    let up_file = files.iter().find(|f| f.ends_with(".up.sql")).unwrap();
    let down_file = files.iter().find(|f| f.ends_with(".down.sql")).unwrap();

    tokio::fs::write(up_file, up_sql.as_bytes()).await.unwrap();
    tokio::fs::write(down_file, down_sql.as_bytes())
        .await
        .unwrap();

    // 3. Check info - should be pending
    let info = migrator.info(src, false).await.unwrap();
    assert_eq!(info.len(), 1);
    assert_eq!(info[0].status, MigrationStatus::Pending);

    // 4. Run migration
    let applied = migrator.run(src, false, false, None).await.unwrap();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].status, MigrationStatus::Applied);

    // 5. Check info - should be applied
    let info = migrator.info(src, false).await.unwrap();
    assert_eq!(info[0].status, MigrationStatus::Applied);

    // 6. Run again - should be no-op
    let applied = migrator.run(src, false, false, None).await.unwrap();
    assert!(applied.is_empty());

    // 7. Revert migration
    let reverted = migrator.revert(src, false, false, None).await.unwrap();
    assert_eq!(reverted.len(), 1);
    assert_eq!(reverted[0].status, MigrationStatus::Pending);

    // 8. Check info - should be pending again
    let info = migrator.info(src, false).await.unwrap();
    assert_eq!(info[0].status, MigrationStatus::Pending);

    // 9. Run migration again
    let applied = migrator.run(src, false, false, None).await.unwrap();
    assert_eq!(applied.len(), 1);

    // Cleanup
    let client = build_raw_client();
    client
        .query(&format!("DROP TABLE IF EXISTS {}", table_name))
        .execute()
        .await
        .ok();
}

// ==================== Error Handling Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_run_invalid_sql_fails() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    // Create migration with invalid SQL
    tokio::fs::write(
        format!("{}/0001_invalid.sql", src),
        b"THIS IS NOT VALID SQL AT ALL",
    )
    .await
    .unwrap();

    // Run should fail
    let result = migrator.run(src, false, false, None).await;
    assert!(result.is_err());
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_info_nonexistent_directory_fails() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let result = migrator.info("/nonexistent/path/12345", false).await;
    assert!(result.is_err());
}

// ==================== SQL Comments Tests ====================

#[tokio::test]
#[serial(clickhouse)]
async fn test_run_migration_with_comments() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    let test_id = unique_test_id();
    let table_name = format!("test_comments_{}", test_id);

    // Create migration with various comment styles
    let migration_sql = format!(
        r#"-- This is a comment at the start
-- Another comment line

-- Create the main table
CREATE TABLE IF NOT EXISTS {} (
    id UInt32,  -- inline comment for id
    name String -- inline comment for name
) ENGINE = Memory;

-- This comment is between statements

-- Insert some test data
INSERT INTO {} (id, name) VALUES (1, 'test'); -- trailing comment
"#,
        table_name, table_name
    );

    tokio::fs::write(
        format!("{}/0001_with_comments.sql", src),
        migration_sql.as_bytes(),
    )
    .await
    .unwrap();

    // Run migration
    let result = migrator.run(src, false, false, None).await;
    assert!(result.is_ok(), "run failed: {:?}", result.err());

    // Verify table was created and data inserted
    let client = build_raw_client();

    let count: u64 = client
        .query(&format!("SELECT count() FROM {}", table_name))
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 1);

    // Cleanup
    client
        .query(&format!("DROP TABLE IF EXISTS {}", table_name))
        .execute()
        .await
        .ok();
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_revert_migration_with_comments() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    let test_id = unique_test_id();
    let table_name = format!("test_revert_comments_{}", test_id);

    // Create up migration with comments
    let up_sql = format!(
        r#"-- Migration: Create test table
-- Author: Test
-- Date: 2024-01-01

-- Create the table with proper structure
CREATE TABLE IF NOT EXISTS {} (
    id UInt32,      -- Primary identifier
    name String,    -- User name
    created_at DateTime DEFAULT now()  -- Timestamp
) ENGINE = Memory;

-- End of up migration
"#,
        table_name
    );

    // Create down migration with comments
    let down_sql = format!(
        r#"-- Revert migration: Drop test table
-- This will remove all data!

-- Drop the table
DROP TABLE IF EXISTS {};

-- Cleanup complete
"#,
        table_name
    );

    tokio::fs::write(format!("{}/0001_test.up.sql", src), up_sql.as_bytes())
        .await
        .unwrap();
    tokio::fs::write(format!("{}/0001_test.down.sql", src), down_sql.as_bytes())
        .await
        .unwrap();

    // Run up migration
    let result = migrator.run(src, false, false, None).await;
    assert!(result.is_ok(), "run failed: {:?}", result.err());

    // Verify table exists
    let client = build_raw_client();

    let exists: u8 = client
        .query(&format!("EXISTS TABLE {}", table_name))
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(exists, 1, "Table should exist after up migration");

    // Run down migration (revert)
    let result = migrator.revert(src, false, false, None).await;
    assert!(result.is_ok(), "revert failed: {:?}", result.err());

    // Verify table no longer exists
    let exists: u8 = client
        .query(&format!("EXISTS TABLE {}", table_name))
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(exists, 0, "Table should not exist after down migration");
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_migration_with_only_comments_skipped() {
    let migrator = require_clickhouse!();
    migrator.ensure_migrations_table().await.unwrap();
    clear_migrations_table().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let src = temp_dir.path().to_str().unwrap();

    // Create migration that only contains comments (no actual SQL)
    let migration_sql = r#"-- This migration is intentionally empty
-- It serves as a placeholder
-- For future development

-- TODO: Add actual migration content
"#;

    tokio::fs::write(
        format!("{}/0001_comments_only.sql", src),
        migration_sql.as_bytes(),
    )
    .await
    .unwrap();

    // Run migration - should succeed (no queries to execute)
    let result = migrator.run(src, false, false, None).await;
    assert!(result.is_ok(), "run failed: {:?}", result.err());

    // Migration should be recorded as applied
    let info = migrator.info(src, false).await.unwrap();
    assert_eq!(info.len(), 1);
    assert_eq!(info[0].status, MigrationStatus::Applied);
}
