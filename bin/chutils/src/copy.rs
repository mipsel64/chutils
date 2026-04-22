use eyre::Context;

/// Literal token the user can place inside `--query` to mark where the
/// partition predicate should be substituted. Using `{}` avoids clashing with
/// ClickHouse's own `{name:Type}` query parameter syntax.
const FILTER_PLACEHOLDER: &str = "{filter}";

#[derive(clap::Parser)]
pub struct Command {
    /// Source ClickHouse URL (e.g., http://src-host:8123)
    #[clap(long = "src-url", env = "SRC_CLICKHOUSE_URL")]
    pub src_url: String,

    /// Source ClickHouse username
    #[clap(long = "src-user", env = "SRC_CLICKHOUSE_USER")]
    pub src_username: Option<String>,

    /// Source ClickHouse password
    #[clap(long = "src-password", env = "SRC_CLICKHOUSE_PASSWORD")]
    pub src_password: Option<String>,

    /// Source ClickHouse database. Applied as the default DB for the SELECT
    /// and count queries (unqualified table names resolve against it).
    #[clap(long = "src-db", env = "SRC_CLICKHOUSE_DB")]
    pub src_database: Option<String>,

    /// Destination ClickHouse URL (e.g., http://dst-host:8123)
    #[clap(long = "dst-url", env = "DST_CLICKHOUSE_URL")]
    pub dst_url: String,

    /// Destination ClickHouse username
    #[clap(long = "dst-user", env = "DST_CLICKHOUSE_USER")]
    pub dst_username: Option<String>,

    /// Destination ClickHouse password
    #[clap(long = "dst-password", env = "DST_CLICKHOUSE_PASSWORD")]
    pub dst_password: Option<String>,

    /// Destination ClickHouse database. Applied as the default DB for the
    /// INSERT (an unqualified --dst-table resolves against it).
    #[clap(long = "dst-db", env = "DST_CLICKHOUSE_DB")]
    pub dst_database: Option<String>,

    /// Fully-qualified destination table (e.g. db.table).
    #[clap(long = "dst-table", short = 'd')]
    pub dst_table: String,

    /// SELECT query to stream from the source. The FORMAT clause is appended
    /// automatically — do not include it here.
    ///
    /// May contain the literal token `{filter}`. When --partition-column is
    /// set, each iteration substitutes `{filter}` with the per-day predicate
    /// (`col >= 'day' AND col < 'next_day'`); otherwise it is substituted
    /// with `1 = 1`. Use the placeholder when your SELECT list does not
    /// project the partition column — put `{filter}` inside your own WHERE
    /// clause so the predicate is applied against the source table directly,
    /// not against the subquery's output.
    ///
    /// Examples:
    ///
    /// 1. Simple copy without partitioning — everything goes in one stream:
    ///      --query "SELECT * FROM events"
    ///
    /// 2. Partitioned copy where the partition column IS in the projection —
    ///    no placeholder needed; the tool wraps the query as a subquery and
    ///    appends `WHERE created_at >= 'day' AND created_at < 'next_day'`:
    ///      --query "SELECT * FROM events" \
    ///      --partition-column created_at
    ///
    /// 3. Partitioned copy where the partition column is NOT projected —
    ///    use `{filter}` so the predicate reaches the table scan. Expands to
    ///    `SELECT id, payload FROM events WHERE created_at >= 'day' AND
    ///    created_at < 'next_day'`:
    ///      --query "SELECT id, payload FROM events WHERE {filter}" \
    ///      --partition-column created_at
    ///
    /// 4. Partitioned copy combined with your own WHERE conditions — the
    ///    placeholder slots in next to your predicates:
    ///      --query "SELECT id, payload FROM events \
    ///               WHERE {filter} AND tenant_id = 42" \
    ///      --partition-column created_at
    ///
    /// 5. `{filter}` without --partition-column — the placeholder is
    ///    substituted with `1 = 1`, so the query still runs as one stream
    ///    with your other conditions intact:
    ///      --query "SELECT id FROM events WHERE {filter} AND tenant_id = 42"
    ///      # becomes: SELECT id FROM events WHERE 1 = 1 AND tenant_id = 42
    #[clap(long, short = 'q')]
    pub query: String,

    /// Transfer format. Native is fastest and preserves types exactly.
    #[clap(long, default_value = "Native")]
    pub format: String,

    /// Date/DateTime column used to split the copy into daily chunks. When
    /// set, the tool iterates one day at a time instead of a single stream.
    #[clap(long)]
    pub partition_column: Option<String>,

    /// Inclusive start date for --partition-column (YYYY-MM-DD). If omitted,
    /// discovered from the source via min(--partition-column).
    #[clap(long, requires = "partition_column")]
    pub start: Option<chrono::NaiveDate>,

    /// Exclusive end date for --partition-column (YYYY-MM-DD). If omitted,
    /// discovered from the source via max(--partition-column) + 1 day.
    #[clap(long, requires = "partition_column")]
    pub end: Option<chrono::NaiveDate>,

    /// Skip the pre-copy schema check. By default the tool runs
    /// `DESCRIBE (<query>)` on the source and `DESCRIBE TABLE <dst_table>` on
    /// the destination, and refuses to start if the source produces columns
    /// that do not exist on the destination.
    #[clap(long)]
    pub skip_schema_check: bool,
}

impl Command {
    /// Entry point for the `copy` subcommand. Dispatches to either a single
    /// streaming copy or a daily-chunked loop depending on whether
    /// `--partition-column` was supplied.
    pub async fn execute(self) -> eyre::Result<()> {
        let Command {
            src_url,
            src_username,
            src_password,
            src_database,
            dst_url,
            dst_username,
            dst_password,
            dst_database,
            dst_table,
            query,
            format,
            partition_column,
            start,
            end,
            skip_schema_check,
        } = self;

        let http = reqwest::Client::builder()
            .build()
            .wrap_err("Failed to build HTTP client")?;

        let src = Endpoint {
            url: &src_url,
            username: src_username.as_deref(),
            password: src_password.as_deref(),
            database: src_database.as_deref(),
        };
        let dst = Endpoint {
            url: &dst_url,
            username: dst_username.as_deref(),
            password: dst_password.as_deref(),
            database: dst_database.as_deref(),
        };

        tracing::info!(%dst_table, %format, "Copying");

        if !skip_schema_check {
            check_schemas(&http, &src, &dst, &query, &dst_table).await?;
        } else {
            tracing::warn!("Skipping schema check");
        }

        let Some(column) = partition_column else {
            // If partition_column is omitted, skip any extra filter
            let effective_query = apply_no_filter(&query);
            return copy_chunk(&http, &src, &dst, &dst_table, &effective_query, &format).await;
        };

        let (range_start, range_end) =
            resolve_range(&http, &src, &query, &column, start, end).await?;
        if range_start >= range_end {
            tracing::info!(%range_start, %range_end, "Date range is empty — nothing to copy");
            return Ok(());
        }

        let total_days = (range_end - range_start).num_days();
        tracing::info!(
            %range_start,
            %range_end,
            total_days,
            "Breaking into daily chunks"
        );
        let overall_t0 = std::time::Instant::now();

        let mut day = range_start;
        while day < range_end {
            let next_day = day.succ_opt().expect("date overflow");
            let predicate = format!("{column} >= '{day}' AND {column} < '{next_day}'");

            let day_query = apply_filter(&query, &predicate);
            tracing::info!(%day, "Copying chunk");
            copy_chunk(&http, &src, &dst, &dst_table, &day_query, &format)
                .await
                .wrap_err_with(|| format!("Failed copying chunk {day}"))?;
            day = next_day;
        }

        tracing::info!(
            total_days,
            elapsed = ?overall_t0.elapsed(),
            "All chunks completed"
        );
        Ok(())
    }
}

/// Connection parameters for one side of the copy (source or destination).
/// Borrowed so the caller can reuse the owned `String`s from `Command`.
#[derive(Clone, Copy)]
struct Endpoint<'a> {
    url: &'a str,
    username: Option<&'a str>,
    password: Option<&'a str>,
    database: Option<&'a str>,
}

/// Copy the rows produced by `query` from `src` into `dst_table` on `dst`.
///
/// Runs a count first and skips the transfer entirely if it is zero. Otherwise
/// opens a streaming `SELECT ... FORMAT {format}` against the source and pipes
/// the response body straight into the destination's `INSERT ... FORMAT
/// {format}` request body via `reqwest::Body::wrap_stream`. Memory usage stays
/// constant regardless of row count — bytes never buffer fully in the client.
async fn copy_chunk(
    http: &reqwest::Client,
    src: &Endpoint<'_>,
    dst: &Endpoint<'_>,
    dst_table: &str,
    query: &str,
    format: &str,
) -> eyre::Result<()> {
    let select_query = format!("{query} FORMAT {format}");
    let insert_query = format!("INSERT INTO {dst_table} FORMAT {format}");
    let count_query = format!("SELECT count() FROM ({query}) FORMAT TabSeparated");

    tracing::debug!(%count_query, "Estimating row count on source");
    let estimated_rows = fetch_count(http, src, &count_query)
        .await
        .wrap_err("Failed to estimate source row count")?;
    tracing::info!(estimated_rows, "Counted source rows");
    if estimated_rows == 0 {
        return Ok(());
    }

    tracing::debug!(%select_query, "Opening source stream");
    let mut src_req = http.post(src.url);
    if let Some(db) = src.database {
        src_req = src_req.query(&[("database", db)]);
    }
    src_req = src_req.body(select_query);
    if let Some(user) = src.username {
        src_req = src_req.basic_auth(user, src.password);
    }
    let src_resp = src_req
        .send()
        .await
        .wrap_err("Failed to initiate source SELECT")?;

    let src_status = src_resp.status();
    if !src_status.is_success() {
        let body = src_resp.text().await.unwrap_or_default();
        eyre::bail!("Source SELECT failed ({src_status}): {body}");
    }

    let byte_stream = src_resp.bytes_stream();

    tracing::debug!(%insert_query, "Streaming to destination");
    let t0 = std::time::Instant::now();

    let mut dst_req = http.post(dst.url).query(&[("query", &insert_query)]);
    if let Some(db) = dst.database {
        dst_req = dst_req.query(&[("database", db)]);
    }
    if let Some(user) = dst.username {
        dst_req = dst_req.basic_auth(user, dst.password);
    }
    let dst_resp = dst_req
        .body(reqwest::Body::wrap_stream(byte_stream))
        .send()
        .await
        .wrap_err("Failed to send destination INSERT")?;

    let dst_status = dst_resp.status();
    if !dst_status.is_success() {
        let body = dst_resp.text().await.unwrap_or_default();
        eyre::bail!("Destination INSERT failed ({dst_status}): {body}");
    }

    tracing::info!(elapsed = ?t0.elapsed(), "Chunk done");
    Ok(())
}

/// Run `count_query` against `src` and parse the single-number response.
///
/// The caller is responsible for building a query that wraps the user's
/// SELECT as a subquery (`SELECT count() FROM (<query>) FORMAT TabSeparated`)
/// so the count reflects exactly what will be streamed.
async fn fetch_count(
    http: &reqwest::Client,
    src: &Endpoint<'_>,
    count_query: &str,
) -> eyre::Result<u64> {
    let body = send_query(http, src, count_query)
        .await
        .wrap_err("Failed to send count query")?;
    body.trim()
        .parse::<u64>()
        .wrap_err_with(|| format!("Count response was not a number: {body:?}"))
}

/// Determine the `[start, end)` date range for daily chunking.
///
/// If both bounds are provided by the user, returns them unchanged without
/// hitting the source. Otherwise runs `min`/`max` over the user's query
/// (wrapped as a subquery so any WHERE clause is respected) and fills in
/// whichever side was omitted. The discovered `max` is bumped by one day so
/// the returned range is end-exclusive, matching the loop in `execute`.
async fn resolve_range(
    http: &reqwest::Client,
    src: &Endpoint<'_>,
    query: &str,
    column: &str,
    start: Option<chrono::NaiveDate>,
    end: Option<chrono::NaiveDate>,
) -> eyre::Result<(chrono::NaiveDate, chrono::NaiveDate)> {
    if let (Some(s), Some(e)) = (start, end) {
        return Ok((s, e));
    }

    let inner = apply_no_filter(query);
    let q = format!(
        "SELECT toDate(min({column})), toDate(max({column})) FROM ({inner}) FORMAT TabSeparated"
    );
    let body = send_query(http, src, &q)
        .await
        .wrap_err("Failed to discover date range")?;

    let line = body.trim();
    let mut parts = line.split('\t');
    let min_s = parts.next().unwrap_or_default();
    let max_s = parts.next().unwrap_or_default();
    if min_s.is_empty() || max_s.is_empty() {
        eyre::bail!("Source query returned no rows; cannot discover date range");
    }
    let min = chrono::NaiveDate::parse_from_str(min_s, "%Y-%m-%d")
        .wrap_err_with(|| format!("Invalid min date: {min_s:?}"))?;
    let max = chrono::NaiveDate::parse_from_str(max_s, "%Y-%m-%d")
        .wrap_err_with(|| format!("Invalid max date: {max_s:?}"))?;

    let s = start.unwrap_or(min);
    let e = end.unwrap_or_else(|| max.succ_opt().expect("date overflow"));
    Ok((s, e))
}

/// One row from a ClickHouse `DESCRIBE` result. `default_kind` captures the
/// 3rd `DESCRIBE` column — empty or `DEFAULT` for a regular insertable
/// column, or `MATERIALIZED` / `ALIAS` / `EPHEMERAL` for columns that cannot
/// appear in an `INSERT` and must be excluded from the schema check.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Column {
    name: String,
    data_type: String,
    default_kind: String,
}

impl Column {
    /// Whether this column can be the target of an `INSERT`. Materialized,
    /// alias, and ephemeral columns are computed by the server and are not
    /// accepted in the input stream, so the pre-check skips them.
    fn is_insertable(&self) -> bool {
        matches!(self.default_kind.as_str(), "" | "DEFAULT")
    }
}

/// Run the source and destination schema pre-check.
///
/// Fetches `DESCRIBE (<query>)` from the source (with `{filter}` substituted
/// by a no-op) and `DESCRIBE TABLE <dst_table>` from the destination, then
/// compares them. Logs warnings for type differences and extra destination
/// columns, and returns an error if any source column is missing on the
/// destination — that case would fail the INSERT for sure.
async fn check_schemas(
    http: &reqwest::Client,
    src: &Endpoint<'_>,
    dst: &Endpoint<'_>,
    query: &str,
    dst_table: &str,
) -> eyre::Result<()> {
    let src_cols = describe_query(http, src, query)
        .await
        .wrap_err("Failed to DESCRIBE source query")?;
    let dst_cols = describe_table(http, dst, dst_table)
        .await
        .wrap_err_with(|| format!("Failed to DESCRIBE destination table {dst_table}"))?;

    tracing::info!(
        source_columns = src_cols.len(),
        destination_columns = dst_cols.len(),
        "Schema check"
    );
    compare_schemas(&src_cols, &dst_cols)
}

/// Return `DESCRIBE (<query>)` column metadata from the source, with
/// `{filter}` neutralized so the describe query is always valid.
async fn describe_query(
    http: &reqwest::Client,
    src: &Endpoint<'_>,
    query: &str,
) -> eyre::Result<Vec<Column>> {
    let inner = apply_no_filter(query);
    let q = format!("DESCRIBE ({inner}) FORMAT TabSeparated");
    let body = send_query(http, src, &q).await?;
    Ok(drop_non_insertable(parse_describe(&body)?))
}

/// Return `DESCRIBE TABLE <dst_table>` column metadata from the destination,
/// with `MATERIALIZED`/`ALIAS`/`EPHEMERAL` columns stripped so the comparator
/// neither requires the source to produce them nor flags them as "extra".
async fn describe_table(
    http: &reqwest::Client,
    dst: &Endpoint<'_>,
    dst_table: &str,
) -> eyre::Result<Vec<Column>> {
    let q = format!("DESCRIBE TABLE {dst_table} FORMAT TabSeparated");
    let body = send_query(http, dst, &q).await?;
    Ok(drop_non_insertable(parse_describe(&body)?))
}

/// Filter a DESCRIBE result down to columns that can appear in an INSERT.
/// Non-insertable columns are printed so the user can see what was skipped.
fn drop_non_insertable(cols: Vec<Column>) -> Vec<Column> {
    cols.into_iter()
        .filter(|c| {
            let keep = c.is_insertable();
            if !keep {
                tracing::info!(
                    name = %c.name,
                    data_type = %c.data_type,
                    kind = %c.default_kind,
                    "Skipping non-insertable column"
                );
            }
            keep
        })
        .collect()
}

/// Parse ClickHouse's `DESCRIBE ... FORMAT TabSeparated` output. Each line
/// has seven tab-separated fields (name, type, default_type,
/// default_expression, comment, codec_expression, ttl_expression); only the
/// first two are used for the schema comparison.
fn parse_describe(body: &str) -> eyre::Result<Vec<Column>> {
    body.lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let mut parts = line.split('\t');
            let name = parts
                .next()
                .ok_or_else(|| eyre::eyre!("DESCRIBE row missing name: {line:?}"))?;
            let data_type = parts
                .next()
                .ok_or_else(|| eyre::eyre!("DESCRIBE row missing type: {line:?}"))?;
            // `default_type` is the 3rd field; older/newer ClickHouse versions
            // always emit it but it may be empty for regular columns.
            let default_kind = parts.next().unwrap_or("");
            Ok(Column {
                name: name.to_string(),
                data_type: data_type.to_string(),
                default_kind: default_kind.to_string(),
            })
        })
        .collect()
}

/// Compare two column lists by name. A source column absent from the
/// destination is a hard error. Type differences and extra destination
/// columns are only logged so safe coercions (e.g. `UInt32` → `UInt64`) and
/// destination-side defaults keep working.
fn compare_schemas(src: &[Column], dst: &[Column]) -> eyre::Result<()> {
    use std::collections::{HashMap, HashSet};

    let dst_by_name: HashMap<&str, &str> = dst
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.as_str()))
        .collect();
    let src_names: HashSet<&str> = src.iter().map(|c| c.name.as_str()).collect();

    let mut missing: Vec<&Column> = Vec::new();
    let mut type_mismatches: Vec<(&Column, &str)> = Vec::new();

    for col in src {
        match dst_by_name.get(col.name.as_str()) {
            None => missing.push(col),
            Some(dst_type) if *dst_type != col.data_type.as_str() => {
                type_mismatches.push((col, dst_type));
            }
            Some(_) => {}
        }
    }

    for (src_col, dst_type) in &type_mismatches {
        tracing::warn!(
            column = %src_col.name,
            source_type = %src_col.data_type,
            destination_type = %dst_type,
            "Column type differs between source and destination"
        );
    }
    for col in dst.iter().filter(|c| !src_names.contains(c.name.as_str())) {
        tracing::info!(
            column = %col.name,
            data_type = %col.data_type,
            "Destination column not produced by source — default will be used"
        );
    }
    for col in &missing {
        tracing::error!(
            column = %col.name,
            data_type = %col.data_type,
            "Source column has no match on destination"
        );
    }

    if !missing.is_empty() {
        eyre::bail!(
            "Schema check failed: {} source column(s) missing on destination. \
             Fix the destination schema or re-run with --skip-schema-check.",
            missing.len()
        );
    }

    Ok(())
}

/// Substitute `{filter}` in the user's query with the per-day predicate, or
/// — if the placeholder is absent — fall back to wrapping the query as a
/// subquery with an outer `WHERE`.
///
/// The placeholder form lets the predicate sit next to the source-table
/// scan, so the partition column does not need to appear in the SELECT
/// projection (and ClickHouse can still use the primary key index). The
/// subquery-wrap fallback only works when the partition column is projected.
fn apply_filter(query: &str, predicate: &str) -> String {
    if query.contains(FILTER_PLACEHOLDER) {
        query.replace(FILTER_PLACEHOLDER, predicate)
    } else {
        format!("SELECT * FROM ({query}) WHERE {predicate}")
    }
}

/// Neutralize `{filter}` for queries that should run without any date filter
/// applied (single-shot copies and range-discovery). Substitutes a no-op
/// predicate so the query is syntactically valid; queries that do not
/// contain the placeholder are returned unchanged.
fn apply_no_filter(query: &str) -> String {
    if query.contains(FILTER_PLACEHOLDER) {
        query.replace(FILTER_PLACEHOLDER, "1 = 1")
    } else {
        query.to_string()
    }
}

/// POST a simple (non-streaming) query to ClickHouse and return the full
/// response body as a string.
///
/// The query goes in the request body rather than the `?query=` URL param
/// because ClickHouse requires a `Content-Length` or chunked `Transfer-
/// Encoding` on POST requests, and reqwest does not set `Content-Length: 0`
/// for an empty POST.
async fn send_query(
    http: &reqwest::Client,
    src: &Endpoint<'_>,
    query: &str,
) -> eyre::Result<String> {
    let mut req = http.post(src.url);
    if let Some(db) = src.database {
        req = req.query(&[("database", db)]);
    }
    req = req.body(query.to_string());
    if let Some(user) = src.username {
        req = req.basic_auth(user, src.password);
    }
    let resp = req.send().await?;
    let status = resp.status();
    let body = resp.text().await.wrap_err("Failed to read response body")?;
    if !status.is_success() {
        eyre::bail!("Query failed ({status}): {body}");
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREDICATE: &str = "created_at >= '2026-01-01' AND created_at < '2026-01-02'";

    #[test]
    fn apply_filter_substitutes_placeholder() {
        let q = "SELECT id FROM events WHERE {filter}";
        assert_eq!(
            apply_filter(q, PREDICATE),
            format!("SELECT id FROM events WHERE {PREDICATE}"),
        );
    }

    #[test]
    fn apply_filter_replaces_every_occurrence() {
        let q =
            "SELECT id FROM events WHERE {filter} AND tenant IN (SELECT t FROM x WHERE {filter})";
        let out = apply_filter(q, PREDICATE);
        assert!(
            !out.contains("{filter}"),
            "placeholder should be gone: {out}"
        );
        assert_eq!(out.matches(PREDICATE).count(), 2);
    }

    #[test]
    fn apply_filter_wraps_when_placeholder_missing() {
        let q = "SELECT * FROM events";
        assert_eq!(
            apply_filter(q, PREDICATE),
            format!("SELECT * FROM ({q}) WHERE {PREDICATE}"),
        );
    }

    #[test]
    fn apply_filter_does_not_mutate_input_without_placeholder() {
        let q = "SELECT * FROM events";
        let _ = apply_filter(q, PREDICATE);
        assert_eq!(q, "SELECT * FROM events");
    }

    #[test]
    fn apply_no_filter_substitutes_with_noop() {
        let q = "SELECT id FROM events WHERE {filter} AND tenant = 7";
        assert_eq!(
            apply_no_filter(q),
            "SELECT id FROM events WHERE 1 = 1 AND tenant = 7",
        );
    }

    #[test]
    fn apply_no_filter_returns_unchanged_without_placeholder() {
        let q = "SELECT * FROM events WHERE tenant = 7";
        assert_eq!(apply_no_filter(q), q);
    }

    #[test]
    fn apply_no_filter_replaces_every_occurrence() {
        let q = "SELECT a FROM t1 WHERE {filter} UNION ALL SELECT a FROM t2 WHERE {filter}";
        let out = apply_no_filter(q);
        assert!(!out.contains("{filter}"));
        assert_eq!(out.matches("1 = 1").count(), 2);
    }

    fn col(name: &str, ty: &str) -> Column {
        Column {
            name: name.to_string(),
            data_type: ty.to_string(),
            default_kind: String::new(),
        }
    }

    fn col_kind(name: &str, ty: &str, kind: &str) -> Column {
        Column {
            name: name.to_string(),
            data_type: ty.to_string(),
            default_kind: kind.to_string(),
        }
    }

    #[test]
    fn parse_describe_extracts_name_type_and_kind() {
        let body = "id\tUInt64\t\t\t\t\t\n\
                    payload\tString\tDEFAULT\t''\t\t\t\n\
                    row_hash\tUInt64\tMATERIALIZED\tcityHash64(payload)\t\t\t\n";
        let cols = parse_describe(body).unwrap();
        assert_eq!(
            cols,
            vec![
                col_kind("id", "UInt64", ""),
                col_kind("payload", "String", "DEFAULT"),
                col_kind("row_hash", "UInt64", "MATERIALIZED"),
            ]
        );
    }

    #[test]
    fn parse_describe_ignores_blank_lines() {
        let body = "id\tUInt64\n\npayload\tString\n\n";
        let cols = parse_describe(body).unwrap();
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn drop_non_insertable_keeps_regular_and_default_columns() {
        let cols = vec![
            col_kind("id", "UInt64", ""),
            col_kind("status", "String", "DEFAULT"),
            col_kind("hash", "UInt64", "MATERIALIZED"),
            col_kind("full_name", "String", "ALIAS"),
            col_kind("tmp", "UInt8", "EPHEMERAL"),
        ];
        let kept: Vec<String> = drop_non_insertable(cols)
            .into_iter()
            .map(|c| c.name)
            .collect();
        assert_eq!(kept, vec!["id".to_string(), "status".to_string()]);
    }

    #[test]
    fn compare_schemas_accepts_identical() {
        let schema = vec![col("id", "UInt64"), col("payload", "String")];
        compare_schemas(&schema, &schema).unwrap();
    }

    #[test]
    fn compare_schemas_allows_extra_destination_columns() {
        let src = vec![col("id", "UInt64")];
        let dst = vec![col("id", "UInt64"), col("inserted_at", "DateTime")];
        compare_schemas(&src, &dst).unwrap();
    }

    #[test]
    fn compare_schemas_tolerates_type_mismatch_with_warning() {
        let src = vec![col("id", "UInt32")];
        let dst = vec![col("id", "UInt64")];
        // Different types are warnings, not errors: ClickHouse can coerce.
        compare_schemas(&src, &dst).unwrap();
    }

    #[test]
    fn compare_schemas_rejects_missing_destination_column() {
        let src = vec![col("id", "UInt64"), col("extra", "String")];
        let dst = vec![col("id", "UInt64")];
        let err = compare_schemas(&src, &dst).unwrap_err();
        assert!(err.to_string().contains("extra") || err.to_string().contains("missing"));
    }
}
