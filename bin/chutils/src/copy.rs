use eyre::Context;

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
    #[clap(long, short = 'q')]
    pub query: String,

    /// Transfer format. Native is fastest and preserves types exactly.
    #[clap(long, default_value = "Native")]
    pub format: String,
}

impl Command {
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
        } = self;

        let select_query = format!("{query} FORMAT {format}");
        let insert_query = format!("INSERT INTO {dst_table} FORMAT {format}");
        let count_query = format!("SELECT count() FROM ({query}) FORMAT TabSeparated");

        let http = reqwest::Client::builder()
            .build()
            .wrap_err("Failed to build HTTP client")?;

        eprintln!("Copying to {dst_table} (format={format})");

        tracing::info!(%count_query, "Estimating row count on source");
        let estimated_rows = fetch_count(
            &http,
            &src_url,
            src_username.as_deref(),
            src_password.as_deref(),
            src_database.as_deref(),
            &count_query,
        )
        .await
        .wrap_err("Failed to estimate source row count")?;
        eprintln!("Estimated {estimated_rows} rows to copy");

        tracing::info!(%select_query, "Opening source stream");

        let mut src_req = http.post(&src_url);
        if let Some(db) = &src_database {
            src_req = src_req.query(&[("database", db)]);
        }
        src_req = src_req.body(select_query.clone());
        if let Some(user) = &src_username {
            src_req = src_req.basic_auth(user, src_password.as_deref());
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

        tracing::info!(%insert_query, "Streaming to destination");
        let t0 = std::time::Instant::now();

        let mut dst_req = http.post(&dst_url).query(&[("query", &insert_query)]);
        if let Some(db) = &dst_database {
            dst_req = dst_req.query(&[("database", db)]);
        }
        if let Some(user) = &dst_username {
            dst_req = dst_req.basic_auth(user, dst_password.as_deref());
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

        eprintln!("Copy completed in {:?}", t0.elapsed());
        Ok(())
    }
}

async fn fetch_count(
    http: &reqwest::Client,
    url: &str,
    username: Option<&str>,
    password: Option<&str>,
    database: Option<&str>,
    count_query: &str,
) -> eyre::Result<u64> {
    let mut req = http.post(url);
    if let Some(db) = database {
        req = req.query(&[("database", db)]);
    }
    req = req.body(count_query.to_string());
    if let Some(user) = username {
        req = req.basic_auth(user, password);
    }
    let resp = req.send().await.wrap_err("Failed to send count query")?;
    let status = resp.status();
    let body = resp.text().await.wrap_err("Failed to read count response")?;
    if !status.is_success() {
        eyre::bail!("Count query failed ({status}): {body}");
    }
    body.trim()
        .parse::<u64>()
        .wrap_err_with(|| format!("Count response was not a number: {body:?}"))
}
