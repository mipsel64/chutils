use std::path::Path;

pub async fn gen_migration_file(
    src: &str,
    name: &str,
    mode: Option<crate::MigrationFileMode>,
) -> Result<Vec<String>, crate::Error> {
    let src = src.strip_suffix("/").unwrap_or(src);

    let name = sanitize_name(name);
    if name.is_empty() {
        return Err(crate::Error::InvalidInput(format!(
            "migration name '{}' is empty",
            name
        )));
    }

    let (seq, mode) = match get_latest_migration_file(src).await? {
        Some(latest) => match mode {
            Some(mode) => (latest.seq_num + 1, mode),
            _ => (latest.seq_num + 1, latest.mode),
        },
        _ => match mode {
            Some(mode) => (1, mode),
            _ => (1, crate::MigrationFileMode::Simple),
        },
    };

    let filenames = match mode {
        crate::MigrationFileMode::Reversible => vec![
            build_file_path(src, seq, &name, mode, true),
            build_file_path(src, seq, &name, mode, false),
        ],
        crate::MigrationFileMode::Simple => vec![build_file_path(src, seq, &name, mode, false)],
    };

    for filename in &filenames {
        tokio::fs::write(filename, b"").await?;
    }

    Ok(filenames)
}

pub fn build_file_path(
    src: &str,
    seq: u32,
    name: &str,
    mode: crate::MigrationFileMode,
    is_up: bool,
) -> String {
    if mode == crate::MigrationFileMode::Simple {
        return format!("{}/{:04}_{}.sql", src, seq, name);
    }

    if is_up {
        format!("{}/{:04}_{}.up.sql", src, seq, name)
    } else {
        format!("{}/{:04}_{}.down.sql", src, seq, name)
    }
}

pub async fn get_latest_migration_file(
    src: &str,
) -> Result<Option<crate::MigrationFile>, crate::Error> {
    Ok(list_migrations(src).await?.pop())
}

pub async fn list_migrations(src: &str) -> Result<Vec<crate::MigrationFile>, crate::Error> {
    let mut it = tokio::fs::read_dir(src).await?;
    let mut files = vec![];
    while let Some(entry) = it.next_entry().await? {
        if entry
            .file_type()
            .await
            .map(|e| e.is_file())
            .unwrap_or_default()
        {
            if let Some(file) = parse_migration_file(src, &entry.path()) {
                files.push(file);
            }
        }
    }

    // Sort by seq desc
    files.sort_unstable_by(|a, b| a.seq_num.cmp(&b.seq_num));
    Ok(files)
}

fn parse_migration_file(src: &str, path: &Path) -> Option<crate::MigrationFile> {
    // File format: xxxx_name.up/down.sql with mode as reversible
    //              xxxx_name.sql with mode as simple
    // with xxxx is a sequence number
    let filename = path.file_stem()?.to_str()?;
    let ext = path.extension()?.to_str()?;

    if ext != "sql" {
        return None;
    }

    if let Some(stem) = filename.strip_suffix(".up") {
        let (seq, name) = parse_sequence_and_name(stem)?;
        return Some(crate::MigrationFile {
            path: path.display().to_string(),
            mode: crate::MigrationFileMode::Reversible,
            is_up: true,
            seq_num: seq,
            name: name.to_string(),
            src: src.to_string(),
        });
    }

    if let Some(stem) = filename.strip_suffix(".down") {
        let (seq, name) = parse_sequence_and_name(stem)?;
        return Some(crate::MigrationFile {
            path: path.display().to_string(),
            mode: crate::MigrationFileMode::Reversible,
            is_up: false,
            seq_num: seq,
            name: name.to_string(),
            src: src.to_string(),
        });
    }

    let (seq, name) = parse_sequence_and_name(filename)?;
    Some(crate::MigrationFile {
        path: filename.to_string(),
        name: name.to_string(),
        mode: crate::MigrationFileMode::Simple,
        is_up: false,
        seq_num: seq,
        src: src.to_string(),
    })
}

fn parse_sequence_and_name(filename: &str) -> Option<(u32, &str)> {
    let (seq_str, name) = filename.split_once('_')?;
    let seq = seq_str.parse::<u32>().ok()?;
    Some((seq, name))
}

fn sanitize_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();

    // Collapse consecutive underscores
    let mut result = String::with_capacity(sanitized.len());
    let mut prev_underscore = false;
    for c in sanitized.chars() {
        if c == '_' {
            if !prev_underscore {
                result.push(c);
            }
            prev_underscore = true;
        } else {
            result.push(c);
            prev_underscore = false;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MigrationFileMode;
    use std::path::PathBuf;

    // ==================== sanitize_name tests ====================

    #[test]
    fn test_sanitize_name_simple() {
        assert_eq!(sanitize_name("create_users"), "create_users");
    }

    #[test]
    fn test_sanitize_name_with_spaces() {
        assert_eq!(sanitize_name("create users table"), "create_users_table");
    }

    #[test]
    fn test_sanitize_name_with_special_chars() {
        assert_eq!(sanitize_name("add-email@column!"), "add_email_column_");
    }

    #[test]
    fn test_sanitize_name_collapses_consecutive_underscores() {
        assert_eq!(sanitize_name("create___users"), "create_users");
        assert_eq!(sanitize_name("a  b  c"), "a_b_c");
    }

    #[test]
    fn test_sanitize_name_mixed_special_chars() {
        assert_eq!(sanitize_name("add--email..column"), "add_email_column");
    }

    #[test]
    fn test_sanitize_name_alphanumeric_preserved() {
        assert_eq!(sanitize_name("migration123"), "migration123");
    }

    #[test]
    fn test_sanitize_name_empty() {
        assert_eq!(sanitize_name(""), "");
    }

    #[test]
    fn test_sanitize_name_only_special_chars() {
        assert_eq!(sanitize_name("---"), "_");
        assert_eq!(sanitize_name("@#$"), "_");
    }

    // ==================== parse_sequence_and_name tests ====================

    #[test]
    fn test_parse_sequence_and_name_valid() {
        assert_eq!(
            parse_sequence_and_name("0001_create_users"),
            Some((1, "create_users"))
        );
        assert_eq!(
            parse_sequence_and_name("0123_add_email"),
            Some((123, "add_email"))
        );
    }

    #[test]
    fn test_parse_sequence_and_name_large_sequence() {
        assert_eq!(
            parse_sequence_and_name("9999_last_migration"),
            Some((9999, "last_migration"))
        );
    }

    #[test]
    fn test_parse_sequence_and_name_no_underscore() {
        assert_eq!(parse_sequence_and_name("0001createusers"), None);
    }

    #[test]
    fn test_parse_sequence_and_name_invalid_sequence() {
        assert_eq!(parse_sequence_and_name("abcd_create_users"), None);
    }

    #[test]
    fn test_parse_sequence_and_name_empty_name() {
        assert_eq!(parse_sequence_and_name("0001_"), Some((1, "")));
    }

    // ==================== build_file_path tests ====================

    #[test]
    fn test_build_file_path_simple() {
        let path = build_file_path("migrations", 1, "create_users", MigrationFileMode::Simple, false);
        assert_eq!(path, "migrations/0001_create_users.sql");
    }

    #[test]
    fn test_build_file_path_simple_ignores_is_up() {
        // Simple mode should ignore is_up parameter
        let path_up = build_file_path("migrations", 1, "create_users", MigrationFileMode::Simple, true);
        let path_down = build_file_path("migrations", 1, "create_users", MigrationFileMode::Simple, false);
        assert_eq!(path_up, path_down);
        assert_eq!(path_up, "migrations/0001_create_users.sql");
    }

    #[test]
    fn test_build_file_path_reversible_up() {
        let path = build_file_path("migrations", 1, "create_users", MigrationFileMode::Reversible, true);
        assert_eq!(path, "migrations/0001_create_users.up.sql");
    }

    #[test]
    fn test_build_file_path_reversible_down() {
        let path = build_file_path("migrations", 1, "create_users", MigrationFileMode::Reversible, false);
        assert_eq!(path, "migrations/0001_create_users.down.sql");
    }

    #[test]
    fn test_build_file_path_pads_sequence() {
        let path = build_file_path("migrations", 42, "test", MigrationFileMode::Simple, false);
        assert_eq!(path, "migrations/0042_test.sql");
    }

    #[test]
    fn test_build_file_path_large_sequence() {
        let path = build_file_path("migrations", 12345, "test", MigrationFileMode::Simple, false);
        assert_eq!(path, "migrations/12345_test.sql");
    }

    // ==================== parse_migration_file tests ====================

    #[test]
    fn test_parse_migration_file_simple() {
        let path = PathBuf::from("migrations/0001_create_users.sql");
        let result = parse_migration_file("migrations", &path);

        assert!(result.is_some());
        let file = result.unwrap();
        assert_eq!(file.seq_num, 1);
        assert_eq!(file.name, "create_users");
        assert_eq!(file.mode, MigrationFileMode::Simple);
        assert!(!file.is_up);
    }

    #[test]
    fn test_parse_migration_file_reversible_up() {
        let path = PathBuf::from("migrations/0001_create_users.up.sql");
        let result = parse_migration_file("migrations", &path);

        assert!(result.is_some());
        let file = result.unwrap();
        assert_eq!(file.seq_num, 1);
        assert_eq!(file.name, "create_users");
        assert_eq!(file.mode, MigrationFileMode::Reversible);
        assert!(file.is_up);
    }

    #[test]
    fn test_parse_migration_file_reversible_down() {
        let path = PathBuf::from("migrations/0001_create_users.down.sql");
        let result = parse_migration_file("migrations", &path);

        assert!(result.is_some());
        let file = result.unwrap();
        assert_eq!(file.seq_num, 1);
        assert_eq!(file.name, "create_users");
        assert_eq!(file.mode, MigrationFileMode::Reversible);
        assert!(!file.is_up);
    }

    #[test]
    fn test_parse_migration_file_non_sql_extension() {
        let path = PathBuf::from("migrations/0001_create_users.txt");
        let result = parse_migration_file("migrations", &path);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_migration_file_invalid_sequence() {
        let path = PathBuf::from("migrations/abcd_create_users.sql");
        let result = parse_migration_file("migrations", &path);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_migration_file_no_underscore() {
        let path = PathBuf::from("migrations/0001createusers.sql");
        let result = parse_migration_file("migrations", &path);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_migration_file_stores_src() {
        let path = PathBuf::from("custom/path/0001_test.sql");
        let result = parse_migration_file("custom/path", &path);

        assert!(result.is_some());
        assert_eq!(result.unwrap().src, "custom/path");
    }

    // ==================== async tests for list_migrations and gen_migration_file ====================

    #[tokio::test]
    async fn test_list_migrations_empty_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = list_migrations(temp_dir.path().to_str().unwrap()).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_migrations_sorted_by_sequence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // Create files out of order
        tokio::fs::write(format!("{}/0003_third.sql", src), b"").await.unwrap();
        tokio::fs::write(format!("{}/0001_first.sql", src), b"").await.unwrap();
        tokio::fs::write(format!("{}/0002_second.sql", src), b"").await.unwrap();

        let result = list_migrations(src).await.unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].seq_num, 1);
        assert_eq!(result[1].seq_num, 2);
        assert_eq!(result[2].seq_num, 3);
    }

    #[tokio::test]
    async fn test_list_migrations_ignores_non_sql_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        tokio::fs::write(format!("{}/0001_valid.sql", src), b"").await.unwrap();
        tokio::fs::write(format!("{}/0002_invalid.txt", src), b"").await.unwrap();
        tokio::fs::write(format!("{}/readme.md", src), b"").await.unwrap();

        let result = list_migrations(src).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "valid");
    }

    #[tokio::test]
    async fn test_list_migrations_ignores_directories() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        tokio::fs::write(format!("{}/0001_valid.sql", src), b"").await.unwrap();
        tokio::fs::create_dir(format!("{}/0002_subdir.sql", src)).await.unwrap();

        let result = list_migrations(src).await.unwrap();

        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_list_migrations_handles_reversible_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        tokio::fs::write(format!("{}/0001_create.up.sql", src), b"").await.unwrap();
        tokio::fs::write(format!("{}/0001_create.down.sql", src), b"").await.unwrap();

        let result = list_migrations(src).await.unwrap();

        // Both files should be listed
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|f| f.seq_num == 1));
        assert!(result.iter().any(|f| f.is_up));
        assert!(result.iter().any(|f| !f.is_up));
    }

    #[tokio::test]
    async fn test_get_latest_migration_file_returns_highest_seq() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        tokio::fs::write(format!("{}/0001_first.sql", src), b"").await.unwrap();
        tokio::fs::write(format!("{}/0003_third.sql", src), b"").await.unwrap();
        tokio::fs::write(format!("{}/0002_second.sql", src), b"").await.unwrap();

        let result = get_latest_migration_file(src).await.unwrap();

        assert!(result.is_some());
        assert_eq!(result.unwrap().seq_num, 3);
    }

    #[tokio::test]
    async fn test_get_latest_migration_file_empty_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = get_latest_migration_file(temp_dir.path().to_str().unwrap()).await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_gen_migration_file_first_simple() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        let result = gen_migration_file(src, "create_users", None).await.unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0].ends_with("0001_create_users.sql"));
        assert!(tokio::fs::metadata(&result[0]).await.is_ok());
    }

    #[tokio::test]
    async fn test_gen_migration_file_first_reversible() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        let result = gen_migration_file(src, "create_users", Some(MigrationFileMode::Reversible))
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|f| f.ends_with("0001_create_users.up.sql")));
        assert!(result.iter().any(|f| f.ends_with("0001_create_users.down.sql")));
    }

    #[tokio::test]
    async fn test_gen_migration_file_increments_sequence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // Create first migration
        tokio::fs::write(format!("{}/0001_first.sql", src), b"").await.unwrap();

        let result = gen_migration_file(src, "second", None).await.unwrap();

        assert!(result[0].contains("0002_second"));
    }

    #[tokio::test]
    async fn test_gen_migration_file_inherits_mode_from_latest() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // Create reversible migration
        tokio::fs::write(format!("{}/0001_first.up.sql", src), b"").await.unwrap();
        tokio::fs::write(format!("{}/0001_first.down.sql", src), b"").await.unwrap();

        // Create without specifying mode
        let result = gen_migration_file(src, "second", None).await.unwrap();

        // Should inherit reversible mode
        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|f| f.ends_with(".up.sql")));
        assert!(result.iter().any(|f| f.ends_with(".down.sql")));
    }

    #[tokio::test]
    async fn test_gen_migration_file_override_mode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        // Create reversible migration
        tokio::fs::write(format!("{}/0001_first.up.sql", src), b"").await.unwrap();
        tokio::fs::write(format!("{}/0001_first.down.sql", src), b"").await.unwrap();

        // Override with simple mode
        let result = gen_migration_file(src, "second", Some(MigrationFileMode::Simple))
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0].ends_with("0002_second.sql"));
    }

    #[tokio::test]
    async fn test_gen_migration_file_sanitizes_name() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        let result = gen_migration_file(src, "create users table", None).await.unwrap();

        assert!(result[0].contains("create_users_table"));
    }

    #[tokio::test]
    async fn test_gen_migration_file_empty_name_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = temp_dir.path().to_str().unwrap();

        let result = gen_migration_file(src, "", None).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_gen_migration_file_strips_trailing_slash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let src = format!("{}/", temp_dir.path().to_str().unwrap());

        let result = gen_migration_file(&src, "test", None).await.unwrap();

        // Should not have double slashes
        assert!(!result[0].contains("//"));
    }

    #[tokio::test]
    async fn test_list_migrations_nonexistent_dir() {
        let result = list_migrations("/nonexistent/path").await;
        assert!(result.is_err());
    }
}
