//! This module implements store behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use anyhow::{Context, Result};
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;
use tracing::warn;

use crate::aggregate::Snapshot;

const METRICS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS metrics (
    time          TIMESTAMPTZ NOT NULL,
    session_id    TEXT        NOT NULL,
    contestant_id TEXT        NOT NULL,
    wave_index    INT         NOT NULL,
    p50_ns        BIGINT,
    p90_ns        BIGINT,
    p99_ns        BIGINT,
    p999_ns       BIGINT,
    rt_p50_ns     BIGINT,
    rt_p90_ns     BIGINT,
    rt_p99_ns     BIGINT,
    tps_1s        DOUBLE PRECISION,
    error_rate    DOUBLE PRECISION,
    hdr_encoded   BYTEA,
    rt_hdr_encoded   BYTEA,
    slip_hdr_encoded BYTEA,
    match_hdr_encoded BYTEA
);";

const METRICS_PARTIAL_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS metrics_partial (
    time          TIMESTAMPTZ NOT NULL,
    shard         TEXT        NOT NULL,
    session_id    TEXT        NOT NULL,
    contestant_id TEXT        NOT NULL,
    wave_index    INT         NOT NULL,
    p50_ns        BIGINT,
    p90_ns        BIGINT,
    p99_ns        BIGINT,
    p999_ns       BIGINT,
    rt_p50_ns     BIGINT,
    rt_p90_ns     BIGINT,
    rt_p99_ns     BIGINT,
    tps_1s        DOUBLE PRECISION,
    error_rate    DOUBLE PRECISION,
    offered       BIGINT,
    errors        BIGINT,
    hdr_encoded   BYTEA,
    rt_hdr_encoded   BYTEA,
    slip_hdr_encoded BYTEA,
    match_hdr_encoded BYTEA
);";

const ADD_RT_COLUMNS: &str = "\
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS rt_p50_ns BIGINT;
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS rt_p90_ns BIGINT;
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS rt_p99_ns BIGINT;
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS rt_hdr_encoded BYTEA;
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS slip_hdr_encoded BYTEA;
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS match_hdr_encoded BYTEA;
ALTER TABLE metrics_partial ADD COLUMN IF NOT EXISTS match_hdr_encoded BYTEA;";

const TIMESCALE_SETUP: &[&str] = &[
    "CREATE EXTENSION IF NOT EXISTS timescaledb;",
    "SELECT create_hypertable('metrics', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 hour');",
    "CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_10s \
     WITH (timescaledb.continuous) AS \
     SELECT time_bucket('10 seconds', time) AS bucket, contestant_id, \
            avg(p99_ns) AS avg_p99, max(tps_1s) AS peak_tps, avg(error_rate) AS avg_error_rate \
     FROM metrics GROUP BY bucket, contestant_id WITH NO DATA;",
    "SELECT add_continuous_aggregate_policy('metrics_10s', \
        start_offset => INTERVAL '1 hour', end_offset => INTERVAL '10 seconds', \
        schedule_interval => INTERVAL '10 seconds');",
    // metrics_partial is transient rollup staging: the rollup merges it into
    // `metrics` and never reads old rows again, so drop chunks after 6h to stop
    // unbounded growth at high TPS. 6h is far larger than any run + rollup lag.
    // NOTE: `metrics` itself is intentionally NOT compressed — telemetry-rollup
    // re-creates its unique index (for the UPSERT ON CONFLICT) on every boot, and
    // TimescaleDB forbids CREATE INDEX on a compressed hypertable, which would
    // crashloop the rollup. Growth of `metrics` is bounded per-run instead.
    "SELECT add_retention_policy('metrics_partial', INTERVAL '6 hours', if_not_exists => TRUE);",
];

/// INSERT_SQL writes per-shard partial rows for later rollup into metrics.
/// The final values include the shard id so the rollup can merge cumulative
/// histograms across independent ingester shards.
const INSERT_SQL: &str = "\
INSERT INTO metrics_partial
    (time, session_id, contestant_id, wave_index, p50_ns, p90_ns, p99_ns, p999_ns, rt_p50_ns, rt_p90_ns, rt_p99_ns, tps_1s, error_rate, hdr_encoded, rt_hdr_encoded, slip_hdr_encoded, offered, errors, shard, match_hdr_encoded)
VALUES (to_timestamp($1::double precision / 1e9), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)";

/// Store stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct Store {
    pool: Pool,
    shard: String,
}

impl Store {
    /// connect opens the Timescale/Postgres pool used by telemetry ingestion.
    /// It carries the shard id that is written with each partial metrics row.
    pub async fn connect(url: &str, shard: String) -> Result<Self> {
        let pg_config: tokio_postgres::Config = url.parse().context("parse TIMESCALE_URL")?;
        let mgr = deadpool_postgres::Manager::from_config(
            pg_config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr)
            .max_size(8)
            .runtime(Runtime::Tokio1)
            .build()
            .context("build timescale pool")?;
        Ok(Self { pool, shard })
    }

    /// init_schema performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub async fn init_schema(&self) -> Result<()> {
        let client = self.pool.get().await.context("get timescale conn")?;
        client
            .batch_execute(METRICS_TABLE)
            .await
            .context("create metrics table")?;
        client
            .batch_execute(METRICS_PARTIAL_TABLE)
            .await
            .context("create metrics_partial table")?;
        client
            .batch_execute(ADD_RT_COLUMNS)
            .await
            .context("add response-time columns")?;
        if let Err(e) = client
            .batch_execute(
                "SELECT create_hypertable('metrics_partial', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 hour');",
            )
            .await
        {
            warn!(error = %e, "metrics_partial hypertable setup failed (non-fatal)");
        }
        for stmt in TIMESCALE_SETUP {
            if let Err(e) = client.batch_execute(stmt).await {
                warn!(error = %e, stmt = %stmt.split_whitespace().take(3).collect::<Vec<_>>().join(" "),
                      "timescale setup step failed (non-fatal — extension/policy may be unavailable)");
            }
        }
        Ok(())
    }

    /// write performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub async fn write(&self, snaps: &[Snapshot]) -> Result<()> {
        if snaps.is_empty() {
            return Ok(());
        }
        let client = self.pool.get().await.context("get timescale conn")?;
        let stmt = client
            .prepare_cached(INSERT_SQL)
            .await
            .context("prepare insert")?;
        for s in snaps {
            client
                .execute(
                    &stmt,
                    &[
                        &(s.time_ns as f64),
                        &s.session_id,
                        &s.contestant_id,
                        &(s.wave_index as i32),
                        &(s.p50_ns as i64),
                        &(s.p90_ns as i64),
                        &(s.p99_ns as i64),
                        &(s.p999_ns as i64),
                        &(s.rt_p50_ns as i64),
                        &(s.rt_p90_ns as i64),
                        &(s.rt_p99_ns as i64),
                        &s.tps_1s,
                        &s.error_rate,
                        &s.hdr_encoded,
                        &s.rt_hdr_encoded,
                        &s.slip_hdr_encoded,
                        &(s.offered as i64),
                        &(s.errors as i64),
                        &self.shard,
                        &s.match_hdr_encoded,
                    ],
                )
                .await
                .context("insert metrics_partial row")?;
        }
        Ok(())
    }
}
