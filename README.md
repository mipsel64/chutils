# ch-migrator

A CLI tool and Rust library for managing ClickHouse database migrations.

## Features

- **Simple & Reversible Migrations**: Support for one-way (simple) and two-way (up/down) migrations
- **Dry Run**: Preview migrations before applying them
- **Version Targeting**: Migrate up or down to a specific version
- **Cross-Platform**: Binaries available for Linux and macOS (amd64/arm64)
- **Docker Support**: Multi-arch container images (linux/amd64, linux/arm64)
- **Library Usage**: Use as a Rust library in your own applications

## Installation

### Binary

Download the latest release from [GitHub Releases](https://github.com/mipsel64/ch-migrator/releases):

```bash
# Linux (amd64)
curl -LO https://github.com/mipsel64/ch-migrator/releases/latest/download/ch-migrator-linux-amd64
chmod +x ch-migrator-linux-amd64
sudo mv ch-migrator-linux-amd64 /usr/local/bin/ch-migrator

# Linux (arm64)
curl -LO https://github.com/mipsel64/ch-migrator/releases/latest/download/ch-migrator-linux-arm64
chmod +x ch-migrator-linux-arm64
sudo mv ch-migrator-linux-arm64 /usr/local/bin/ch-migrator

# macOS (Apple Silicon)
curl -LO https://github.com/mipsel64/ch-migrator/releases/latest/download/ch-migrator-darwin-arm64
chmod +x ch-migrator-darwin-arm64
sudo mv ch-migrator-darwin-arm64 /usr/local/bin/ch-migrator

# macOS (Intel)
curl -LO https://github.com/mipsel64/ch-migrator/releases/latest/download/ch-migrator-darwin-amd64
chmod +x ch-migrator-darwin-amd64
sudo mv ch-migrator-darwin-amd64 /usr/local/bin/ch-migrator
```

### Docker

```bash
docker pull ghcr.io/mipsel64/ch-migrator:latest

# Run with mounted migrations directory
docker run --rm -v $(pwd)/migrations:/migrations \
  ghcr.io/mipsel64/ch-migrator:latest \
  --clickhouse-url http://host.docker.internal:8123 \
  --source /migrations \
  up
```

### Cargo (from source)

```bash
cargo install --git https://github.com/mipsel64/ch-migrator --bin cli
```

## Quick Start

```bash
# Set connection details via environment variables
export CLICKHOUSE_URL=http://localhost:8123
export CLICKHOUSE_USER=admin
export CLICKHOUSE_PASSWORD=secret

# Create your first migration
ch-migrator add create_users_table --reversible

# Edit the generated files
# migrations/0001_create_users_table.up.sql
# migrations/0001_create_users_table.down.sql

# Check migration status
ch-migrator info

# Apply pending migrations
ch-migrator up

# Revert the latest migration
ch-migrator down
```

## CLI Reference

### Global Options

| Flag | Environment Variable | Description | Default |
|------|---------------------|-------------|---------|
| `--clickhouse-url` | `CLICKHOUSE_URL` | ClickHouse server URL | Required |
| `--clickhouse-user` | `CLICKHOUSE_USER` | Username for authentication | None |
| `--clickhouse-password` | `CLICKHOUSE_PASSWORD` | Password for authentication | None |
| `--clickhouse-db` | `CLICKHOUSE_DB` | Database name | None |
| `--clickhouse-option` | `CLICKHOUSE_OPTIONS` | Additional options (space-delimited key=value) | None |
| `--source` | `MIGRATION_SOURCE` | Path to migrations directory | `migrations/` |

### Commands

#### `add <name>` - Create a new migration

```bash
# Create a simple (irreversible) migration
ch-migrator add create_logs --simple

# Create a reversible migration with up/down scripts
ch-migrator add create_users --reversible

# Auto-detect mode from existing migrations
ch-migrator add add_email_column
```

| Flag | Short | Description |
|------|-------|-------------|
| `--reversible` | `-r` | Create reversible migration (up/down files) |
| `--simple` | `-s` | Create simple migration (single file) |

#### `info` - Display migration status

```bash
ch-migrator info
```

| Flag | Short | Description |
|------|-------|-------------|
| `--ignore-missing` | `-I` | Skip validation of missing local files |

#### `up` - Apply pending migrations

```bash
# Apply all pending migrations
ch-migrator up

# Preview without applying
ch-migrator up --dry-run

# Apply up to version 5 (inclusive)
ch-migrator up --target-version 5
```

| Flag | Short | Description |
|------|-------|-------------|
| `--dry-run` | `-d` | Preview without applying |
| `--ignore-missing` | `-I` | Skip validation of missing local files |
| `--target-version` | `-t` | Migrate up to specific version (inclusive) |

#### `down` - Revert applied migrations

```bash
# Revert the latest migration
ch-migrator down

# Preview without reverting
ch-migrator down --dry-run

# Revert all migrations after version 3 (exclusive)
ch-migrator down --target-version 3
```

| Flag | Short | Description |
|------|-------|-------------|
| `--dry-run` | `-d` | Preview without reverting |
| `--ignore-missing` | `-I` | Skip validation of missing local files |
| `--target-version` | `-t` | Revert down to specific version (exclusive) |

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

Add to your `Cargo.toml`:

```toml
[dependencies]
migration = { git = "https://github.com/mipsel64/ch-migrator", package = "migration" }
```

### Example

```rust
use migration::{Builder, Migration, MigrationFileMode};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // Build a migrator
    let migrator = Builder::new("http://localhost:8123")
        .with_username(Some("admin"))
        .with_password(Some("secret"))
        .with_database(Some("mydb"))
        .to_migrator()?;

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

| Feature | Description | Default |
|---------|-------------|---------|
| `rustls-tls` | Use rustls for TLS | Yes |
| `rustls-tls-ring` | Use rustls with ring crypto backend | No |
| `native-tls` | Use native TLS implementation | No |

## How It Works

### Migrations Table

ch-migrator tracks applied migrations in a `_ch_migrations` table:

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
ch-migrator/
├── bin/cli/              # CLI binary
│   └── src/main.rs       # Entry point, argument parsing
├── lib/migration/        # Core library
│   └── src/
│       ├── lib.rs        # Migration trait, Migrator, Builder
│       ├── fs.rs         # File system operations
│       └── error.rs      # Error types
├── Dockerfile            # Multi-arch container build
└── .github/workflows/
    ├── ci.yaml           # CI pipeline (lint, test, build)
    └── release.yaml      # Release pipeline (binaries, Docker, GitHub release)
```

### Key Types

| Type | Description |
|------|-------------|
| `Builder` | Configures and creates a `Migrator` instance |
| `Migrator` | Main migration executor, wraps ClickHouse client |
| `Migration` | Trait defining migration operations |
| `MigrationInfo` | Migration metadata and status |
| `MigrationFileMode` | Enum: `Simple` or `Reversible` |
| `MigrationStatus` | Enum: `Pending` or `Applied` |

---

## Development

### Prerequisites

- Rust 1.85+
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
