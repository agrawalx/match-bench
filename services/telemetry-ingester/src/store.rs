//! TimescaleDB sink for metric snapshots.
//!
//! Schema is created on startup (no migration tool), mirroring the Go services'
//! `createTableSQL` pattern. The `metrics` hypertable is the must-have; the
//! `metrics_10s` continuous aggregate + refresh policy are best-effort (a fresh
//! TimescaleDB has the extension; re-creates are idempotent).

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
    hdr_encoded   BYTEA
);";

/// Adds the response-time (r9 - t0, the coordinated-omission-aware round trip)
/// percentile columns to an EXISTING metrics table. CREATE TABLE IF NOT EXISTS
/// above only covers fresh clusters; this backfills the columns where the table
/// already exists. Idempotent (ADD COLUMN IF NOT EXISTS).
const ADD_RT_COLUMNS: &str = "\
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS rt_p50_ns BIGINT;
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS rt_p90_ns BIGINT;
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS rt_p99_ns BIGINT;";

/// Best-effort, idempotent TimescaleDB setup. Each runs independently; failures
/// (e.g. continuous-aggregate policy already exists) are logged, not fatal.
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
];

const INSERT_SQL: &str = "\
INSERT INTO metrics
    (time, session_id, contestant_id, wave_index, p50_ns, p90_ns, p99_ns, p999_ns, rt_p50_ns, rt_p90_ns, rt_p99_ns, tps_1s, error_rate, hdr_encoded)
VALUES (to_timestamp($1::double precision / 1e9), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)";

pub struct Store {
    pool: Pool,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self> {
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
        Ok(Self { pool })
    }

    /// Create the metrics table (required) + the TimescaleDB hypertable/aggregate
    /// (best-effort). Returns an error only if the base table can't be created.
    pub async fn init_schema(&self) -> Result<()> {
        let client = self.pool.get().await.context("get timescale conn")?;
        client
            .batch_execute(METRICS_TABLE)
            .await
            .context("create metrics table")?;
        client
            .batch_execute(ADD_RT_COLUMNS)
            .await
            .context("add response-time columns")?;
        for stmt in TIMESCALE_SETUP {
            if let Err(e) = client.batch_execute(stmt).await {
                warn!(error = %e, stmt = %stmt.split_whitespace().take(3).collect::<Vec<_>>().join(" "),
                      "timescale setup step failed (non-fatal — extension/policy may be unavailable)");
            }
        }
        Ok(())
    }

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
                    ],
                )
                .await
                .context("insert metrics row")?;
        }
        Ok(())
    }
}
