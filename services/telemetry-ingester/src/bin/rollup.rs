//! telemetry-rollup binary: stage-2 merge of per-shard partials (metrics_partial)
//! into the canonical `metrics` table. Run one of these alongside the (scaled)
//! telemetry-ingester replicas. Config via env (shares TIMESCALE_URL with the
//! ingester); ROLLUP_INTERVAL_MS controls the merge cadence (default 1s).

use std::time::Duration;

use anyhow::Result;
use iicpc_logger_rust::loki;
use iicpc_telemetry_ingester::{config::Config, metrics, rollup};

/// main starts the telemetry rollup worker binary.
/// It reads environment configuration, initializes logging and metrics, and
/// delegates the periodic merge loop to the rollup library.
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let _loki_guard = loki::init("telemetry-rollup");
    metrics::start_server();
    let cfg = Config::from_env();
    let interval_ms = std::env::var("ROLLUP_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1000);
    rollup::run(&cfg.timescale_url, Duration::from_millis(interval_ms)).await
}
