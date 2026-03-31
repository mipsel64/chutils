# chutils

A CLI tool and Rust library for managing ClickHouse databases -- migrations, backups, restores, and cluster inspection.

## Features

- **Migrations**: Simple (irreversible) and reversible (up/down) migrations with dry-run and version targeting
- **Backup to S3**: Back up entire databases or specific tables to S3-compatible storage
- **Restore from S3**: Restore databases from S3 backups with structure-only or data-only options
- **Backup Status**: Monitor backup/restore operation progress
- **Cluster Inspection**: List databases and tables on a ClickHouse server
- **Cross-Platform**: Binaries available for Linux and macOS (amd64/arm64)
- **Docker Support**: Multi-arch container images (linux/amd64, linux/arm64)
- **Library Usage**: Use as a Rust library in your own applications

## Installation

### Binary

Download the latest release from [GitHub Releases](https://github.com/mipsel64/chutils/releases):

```bash
# Linux (amd64)
curl -LO https://github.com/mipsel64/chutils/releases/latest/download/chutils-linux-amd64
chmod +x chutils-linux-amd64
sudo mv chutils-linux-amd64 /usr/local/bin/chutils

# Linux (arm64)
curl -LO https://github.com/mipsel64/chutils/releases/latest/download/chutils-linux-arm64
chmod +x chutils-linux-arm64
sudo mv chutils-linux-arm64 /usr/local/bin/chutils

# macOS (Apple Silicon)
curl -LO https://github.com/mipsel64/chutils/releases/latest/download/chutils-darwin-arm64
chmod +x chutils-darwin-arm64
sudo mv chutils-darwin-arm64 /usr/local/bin/chutils

# macOS (Intel)
curl -LO https://github.com/mipsel64/chutils/releases/latest/download/chutils-darwin-amd64
chmod +x chutils-darwin-amd64
sudo mv chutils-darwin-amd64 /usr/local/bin/chutils
```

### Docker

```bash
docker pull ghcr.io/mipsel64/chutils:latest

# Run migrations with mounted migrations directory
docker run --rm -v $(pwd)/migrations:/migrations \
  ghcr.io/mipsel64/chutils:latest \
  migrate \
  --clickhouse-url http://host.docker.internal:8123 \
  --source /migrations \
  up
```

### Cargo (from source)

```bash
cargo install --git https://github.com/mipsel64/chutils --bin chutils
```

## Quick Start

### Migrations

```bash
# Set connection details via environment variables
export CLICKHOUSE_URL=http://localhost:8123
export CLICKHOUSE_USER=admin
export CLICKHOUSE_PASSWORD=secret

# Create your first migration
chutils migrate add create_users_table --reversible

# Edit the generated files
# migrations/0001_create_users_table.up.sql
# migrations/0001_create_users_table.down.sql

# Check migration status
chutils migrate info

# Apply pending migrations
chutils migrate up

# Revert the latest migration
chutils migrate down
```

### Backup & Restore

```bash
# Back up a database to S3
chutils backup \
  --clickhouse-url http://localhost:8123 \
  --db mydb \
  --s3-url s3://my-bucket/backups/ \
  --s3-access-key AKIAIOSFODNN7EXAMPLE \
  --s3-secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  --wait

# Restore a database from S3
chutils restore \
  --clickhouse-url http://localhost:8123 \
  --src-db mydb \
  --s3-url s3://my-bucket/backups/ \
  --s3-access-key AKIAIOSFODNN7EXAMPLE \
  --s3-secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  --wait

# Check backup status
chutils status \
  --clickhouse-url http://localhost:8123 \
  --backup-ids backup_id_1,backup_id_2
```

### Cluster Inspection

```bash
# List all databases
chutils cluster --clickhouse-url http://localhost:8123 list-databases

# List tables in a database
chutils cluster --clickhouse-url http://localhost:8123 list-tables --database mydb
```

## CLI Reference

```
Usage: chutils <COMMAND>

Commands:
  migrate  Run database migrations
  backup   Backup the database
  restore  Restore the database from a backup
  status   Show backup status
  cluster  Manage database cluster
```

---

### `chutils migrate` - Run database migrations

Shared options for all `migrate` subcommands:

| Flag                    | Short | Environment Variable | Description                                    | Default       |
| ----------------------- | ----- | -------------------- | ---------------------------------------------- | ------------- |
| `--clickhouse-url`      | `-c`  | `CLICKHOUSE_URL`     | ClickHouse server URL                          | (empty)       |
| `--clickhouse-user`     | `-u`  | `CLICKHOUSE_USER`    | Username for authentication                    | None          |
| `--clickhouse-password` | `-p`  | `CLICKHOUSE_PASSWORD`| Password for authentication                    | None          |
| `--clickhouse-db`       | `-d`  | `CLICKHOUSE_DB`      | Database name                                  | None          |
| `--clickhouse-option`   | `-o`  | `CLICKHOUSE_OPTIONS` | Additional options (space-delimited key=value)  | None          |
| `--source`              | `-s`  | `MIGRATION_SOURCE`   | Path to migrations directory                   | `migrations/` |

#### `migrate add <name>` - Create a new migration

```bash
# Create a simple (irreversible) migration
chutils migrate add create_logs --simple

# Create a reversible migration with up/down scripts
chutils migrate add create_users --reversible

# Auto-detect mode from existing migrations
chutils migrate add add_email_column
```

| Flag           | Short | Description                                 |
| -------------- | ----- | ------------------------------------------- |
| `--reversible` | `-r`  | Create reversible migration (up/down files) |
| `--simple`     | `-s`  | Create simple migration (single file)       |

#### `migrate info` - Display migration status

```bash
chutils migrate info
```

| Flag               | Short | Description                            |
| ------------------ | ----- | -------------------------------------- |
| `--ignore-missing` | `-I`  | Skip validation of missing local files |

#### `migrate up` - Apply pending migrations

```bash
# Apply all pending migrations
chutils migrate up

# Preview without applying
chutils migrate up --dry-run

# Apply up to version 5 (inclusive)
chutils migrate up --target-version 5
```

| Flag               | Short | Description                                |
| ------------------ | ----- | ------------------------------------------ |
| `--dry-run`        |       | Preview without applying                   |
| `--ignore-missing` | `-I`  | Skip validation of missing local files     |
| `--target-version` | `-t`  | Migrate up to specific version (inclusive)  |

#### `migrate down` - Revert applied migrations

```bash
# Revert the latest migration
chutils migrate down

# Preview without reverting
chutils migrate down --dry-run

# Revert all migrations after version 3 (exclusive)
chutils migrate down --target-version 3
```

| Flag               | Short | Description                                 |
| ------------------ | ----- | ------------------------------------------- |
| `--dry-run`        |       | Preview without reverting                   |
| `--ignore-missing` | `-I`  | Skip validation of missing local files      |
| `--target-version` | `-t`  | Revert down to specific version (exclusive) |

---

### `chutils backup` - Backup the database

Back up a ClickHouse database (or specific tables) to S3-compatible storage.

```bash
chutils backup \
  --clickhouse-url http://localhost:8123 \
  --db mydb \
  --s3-url s3://my-bucket/backups/ \
  --s3-access-key AKIAEXAMPLE \
  --s3-secret-key wJalrXUtnFEMIEXAMPLEKEY \
  --wait
```

| Flag                    | Short | Environment Variable | Description                                                        | Required |
| ----------------------- | ----- | -------------------- | ------------------------------------------------------------------ | -------- |
| `--clickhouse-url`      | `-c`  | `CLICKHOUSE_URL`     | ClickHouse server URL                                              | Yes      |
| `--clickhouse-user`     | `-u`  | `CLICKHOUSE_USER`    | Username for authentication                                        | No       |
| `--clickhouse-password` | `-p`  | `CLICKHOUSE_PASSWORD`| Password for authentication                                        | No       |
| `--clickhouse-option`   | `-o`  | `CLICKHOUSE_OPTIONS` | Additional options (space-delimited key=value)                      | No       |
| `--db`                  | `-d`  | `BACKUP_DB`          | Database to back up                                                | Yes      |
| `--table`               | `-t`  | `BACKUP_TABLES`      | Comma-separated list of tables (default: all tables)                | No       |
| `--backup-option`       | `-O`  | `BACKUP_OPTIONS`     | Comma-separated backup options (e.g., `s3_max_connections=1000`)    | No       |
| `--wait`                | `-W`  | `BACKUP_WAIT`        | Wait for the backup to complete before returning                    | No       |
| `--s3-url`              |       | `S3_URL`             | S3 URL for storing backups                                          | Yes      |
| `--s3-access-key`       |       | `S3_ACCESS_KEY`      | S3 access key                                                      | Yes      |
| `--s3-secret-key`       |       | `S3_SECRET_KEY`      | S3 secret key                                                      | Yes      |
| `--s3-prefix`           |       | `S3_PREFIX`          | Optional S3 prefix for backup files                                | No       |

---

### `chutils restore` - Restore from a backup

Restore a ClickHouse database from an S3 backup.

```bash
# Restore to the same database name
chutils restore \
  --clickhouse-url http://localhost:8123 \
  --src-db mydb \
  --s3-url s3://my-bucket/backups/ \
  --s3-access-key AKIAEXAMPLE \
  --s3-secret-key wJalrXUtnFEMIEXAMPLEKEY \
  --wait

# Restore to a different database
chutils restore \
  --clickhouse-url http://localhost:8123 \
  --src-db mydb \
  --dst-db mydb_restored \
  --s3-url s3://my-bucket/backups/ \
  --s3-access-key AKIAEXAMPLE \
  --s3-secret-key wJalrXUtnFEMIEXAMPLEKEY \
  --wait

# Restore structure only (no data)
chutils restore \
  --clickhouse-url http://localhost:8123 \
  --src-db mydb \
  --structure-only \
  --s3-url s3://my-bucket/backups/ \
  --s3-access-key AKIAEXAMPLE \
  --s3-secret-key wJalrXUtnFEMIEXAMPLEKEY
```

| Flag                    | Short | Environment Variable   | Description                                                            | Required |
| ----------------------- | ----- | ---------------------- | ---------------------------------------------------------------------- | -------- |
| `--clickhouse-url`      | `-c`  | `CLICKHOUSE_URL`       | ClickHouse server URL                                                  | Yes      |
| `--clickhouse-user`     | `-u`  | `CLICKHOUSE_USER`      | Username for authentication                                            | No       |
| `--clickhouse-password` | `-p`  | `CLICKHOUSE_PASSWORD`  | Password for authentication                                            | No       |
| `--clickhouse-option`   | `-o`  | `CLICKHOUSE_OPTIONS`   | Additional options (space-delimited key=value)                          | No       |
| `--src-db`              | `-s`  | `RESTORE_SRC_DB`       | Source database name to restore from                                    | Yes      |
| `--dst-db`              | `-d`  | `RESTORE_DST_DB`       | Destination database name (defaults to source name)                     | No       |
| `--table`               | `-t`  | `RESTORE_TABLE`        | Comma-separated list of tables (default: all tables)                    | No       |
| `--restore-option`      | `-O`  | `RESTORE_OPTIONS`      | Comma-separated restore options (e.g., `s3_max_connections=1000`)       | No       |
| `--structure-only`      | `-S`  | `RESTORE_STRUCTURE_ONLY`| Restore table structure only (mutually exclusive with `--data-only`)   | No       |
| `--data-only`           | `-D`  | `RESTORE_DATA_ONLY`    | Restore data only (mutually exclusive with `--structure-only`)          | No       |
| `--wait`                | `-W`  | `RESTORE_WAIT`         | Wait for the restore to complete before returning                       | No       |
| `--s3-url`              |       | `S3_URL`               | S3 URL for the backup location                                          | Yes      |
| `--s3-access-key`       |       | `S3_ACCESS_KEY`        | S3 access key                                                          | Yes      |
| `--s3-secret-key`       |       | `S3_SECRET_KEY`        | S3 secret key                                                          | Yes      |
| `--s3-prefix`           |       | `S3_PREFIX`            | Optional S3 prefix for backup files                                    | No       |

---

### `chutils status` - Show backup status

Check the status of backup or restore operations.

```bash
# Check status of specific backups
chutils status \
  --clickhouse-url http://localhost:8123 \
  --backup-ids backup_id_1,backup_id_2

# Wait until all backups complete
chutils status \
  --clickhouse-url http://localhost:8123 \
  --backup-ids backup_id_1 \
  --wait

# Filter backups from the last 10 minutes
chutils status \
  --clickhouse-url http://localhost:8123 \
  --backup-ids backup_id_1 \
  --since 10m
```

| Flag                    | Short | Environment Variable | Description                                                   | Required |
| ----------------------- | ----- | -------------------- | ------------------------------------------------------------- | -------- |
| `--clickhouse-url`      | `-c`  | `CLICKHOUSE_URL`     | ClickHouse server URL                                         | Yes      |
| `--clickhouse-user`     | `-u`  | `CLICKHOUSE_USER`    | Username for authentication                                   | No       |
| `--clickhouse-password` | `-p`  | `CLICKHOUSE_PASSWORD`| Password for authentication                                   | No       |
| `--clickhouse-option`   | `-o`  | `CLICKHOUSE_OPTIONS` | Additional options (space-delimited key=value)                 | No       |
| `--backup-ids`          | `-b`  |                      | Comma-separated list of backup IDs to check                   | Yes      |
| `--wait`                | `-W`  |                      | Wait until all backups are complete                            | No       |
| `--since`               | `-s`  |                      | Filter backups since duration (e.g., `10m`, `1h`)              | No (default: `24h`) |

---

### `chutils cluster` - Manage database cluster

Shared options for all `cluster` subcommands:

| Flag                    | Short | Environment Variable | Description                                    | Default |
| ----------------------- | ----- | -------------------- | ---------------------------------------------- | ------- |
| `--clickhouse-url`      | `-c`  | `CLICKHOUSE_URL`     | ClickHouse server URL                          | (empty) |
| `--clickhouse-user`     | `-u`  | `CLICKHOUSE_USER`    | Username for authentication                    | None    |
| `--clickhouse-password` | `-p`  | `CLICKHOUSE_PASSWORD`| Password for authentication                    | None    |
| `--clickhouse-option`   | `-o`  | `CLICKHOUSE_OPTIONS` | Additional options (space-delimited key=value)  | None    |

#### `cluster list-databases` - List all databases

```bash
chutils cluster --clickhouse-url http://localhost:8123 list-databases
```

#### `cluster list-tables` - List all tables in a database

```bash
chutils cluster --clickhouse-url http://localhost:8123 list-tables --database mydb
```

| Flag         | Short | Description                    | Required |
| ------------ | ----- | ------------------------------ | -------- |
| `--database` | `-d`  | Database name to list tables from | Yes      |

## Migration File Format

### Naming Convention

**Simple migrations** (irreversible):

```
{NNNN}_{name}.sql
```

**Reversible migrations**:

```
{NNNN}_{name}.up.sql    # Applied when running `up`
{NNNN}_{name}.down.sql  # Applied when running `down`
```

- `NNNN`: 4-digit zero-padded sequence number (e.g., `0001`, `0042`)
- `name`: Descriptive name using alphanumeric characters and underscores

### Examples

```
migrations/
├── 0001_create_users.up.sql
├── 0001_create_users.down.sql
├── 0002_add_email_column.up.sql
├── 0002_add_email_column.down.sql
└── 0003_create_audit_log.sql        # Simple/irreversible
```

### SQL File Content

```sql
-- migrations/0001_create_users.up.sql
CREATE TABLE users (
    id UInt64,
    name String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

-- migrations/0001_create_users.down.sql
DROP TABLE IF EXISTS users;
```

- Multiple statements separated by semicolons are supported
- Full-line comments (`-- comment`) are stripped before execution
- Empty migrations (comments only) are allowed

## Library Usage

The workspace provides several crates that can be used independently:

```toml
[dependencies]
# Migration library
migration = { git = "https://github.com/mipsel64/chutils", package = "migration" }

# Backup/restore library
backup = { git = "https://github.com/mipsel64/chutils", package = "backup" }

# Shared ClickHouse client library
ch = { git = "https://github.com/mipsel64/chutils", package = "ch" }
```

### Migration Example

```rust
use migration::{Migration, MigrationFileMode};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // Build a ClickHouse client
    let client = ch::Builder::new("http://localhost:8123")
        .with_username(Some("admin".into()))
        .with_password(Some("secret".into()))
        .with_database(Some("mydb".into()))
        .to_client()?;

    let migrator = migration::Migrator::from_client(client);

    // Ensure migrations table exists
    migrator.ensure_migrations_table().await?;

    // Get migration status
    let migrations = migrator.info("migrations/", false).await?;
    for m in &migrations {
        println!("{}: {:?}", m.full_version(), m.status);
    }

    // Apply pending migrations
    let applied = migrator.run("migrations/", false, false, None).await?;
    println!("Applied {} migrations", applied.len());

    // Revert latest migration
    let reverted = migrator.revert("migrations/", false, false, None).await?;
    println!("Reverted {} migrations", reverted.len());

    Ok(())
}
```

### Feature Flags

| Feature           | Description                         | Default |
| ----------------- | ----------------------------------- | ------- |
| `rustls-tls`      | Use rustls for TLS                  | Yes     |
| `rustls-tls-ring` | Use rustls with ring crypto backend | No      |
| `native-tls`      | Use native TLS implementation       | No      |

## How It Works

### Migrations Table

chutils tracks applied migrations in a `_ch_migrations` table:

```sql
CREATE TABLE IF NOT EXISTS _ch_migrations (
    version UInt32,
    name String,
    status Enum('pending' = 1, 'applied' = 2),
    applied_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (applied_at, version)
```

### Migration Execution

1. **Discovery**: Scans the migrations directory for `.sql` files
2. **Validation**: Ensures local files match database records
3. **Execution**: Runs each pending migration in sequence order
4. **Recording**: Marks migrations as applied in `_ch_migrations`

### Revert Behavior

- Only reversible migrations (with `.down.sql`) can be reverted
- Without `--target-version`, only the latest migration is reverted
- Migrations are reverted in reverse order

---

## Project Structure

```
chutils/
├── bin/chutils/          # CLI binary
│   └── src/
│       ├── main.rs       # Entry point, argument parsing
│       ├── migration.rs  # Migrate subcommand
│       ├── backup.rs     # Backup subcommand
│       ├── restore.rs    # Restore subcommand
│       ├── status.rs     # Status subcommand
│       └── cluster.rs    # Cluster subcommand
├── lib/
│   ├── migration/        # Migration library
│   │   └── src/
│   │       ├── lib.rs    # Migration trait, Migrator
│   │       ├── fs.rs     # File system operations
│   │       └── error.rs  # Error types
│   ├── backup/           # Backup/restore library
│   │   └── src/
│   │       ├── lib.rs    # Backup and restore logic
│   │       └── error.rs  # Error types
│   └── ch/               # Shared ClickHouse client library
│       └── src/
│           ├── lib.rs    # Client builder, helpers
│           └── error.rs  # Error types
├── Dockerfile            # Multi-arch container build
└── .github/workflows/
    ├── ci.yaml           # CI pipeline (lint, test, build)
    └── release.yaml      # Release pipeline (binaries, Docker, GitHub release)
```

---

## Development

### Prerequisites

- Rust 1.89+
- ClickHouse server (for integration tests)

### Building

```bash
cargo build --release
```

### Testing

```bash
# Start ClickHouse
docker run -d --name clickhouse \
  -p 8123:8123 \
  -e CLICKHOUSE_USER=admin \
  -e CLICKHOUSE_PASSWORD=admin \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  clickhouse/clickhouse-server:latest

# Run tests
export TEST_CLICKHOUSE_URL=http://localhost:8123
export TEST_CLICKHOUSE_USER=admin
export TEST_CLICKHOUSE_PASSWORD=admin
cargo test
```

### Linting

```bash
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all --check
```

## License

MIT License - see [LICENSE](LICENSE) for details.
