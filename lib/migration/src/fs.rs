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
