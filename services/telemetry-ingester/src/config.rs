//! Environment configuration. Mirrors the env-only pattern used across the
//! platform (no config files, no flags).

use std::env;

use crate::aggregate::DEFAULT_WAVE_NS;

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_brokers: String,
    pub consumer_group: String,
    /// PostgreSQL/TimescaleDB connection string (libpq URL).
    pub timescale_url: String,
    /// Redis connection URL, e.g. redis://redis.data.svc.cluster.local:6379.
    pub redis_url: String,
    pub wave_ns: u64,
    pub snapshot_interval_ms: u64,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            kafka_brokers: env_or("KAFKA_BROKERS", "localhost:9092"),
            consumer_group: env_or("KAFKA_CONSUMER_GROUP", "telemetry-ingester"),
            timescale_url: env_or(
                "TIMESCALE_URL",
                "postgres://iicpc:iicpc@localhost:5434/metrics",
            ),
            redis_url: env_or("REDIS_URL", "redis://localhost:6379"),
            wave_ns: env_u64("WAVE_DURATION_MS", DEFAULT_WAVE_NS / 1_000_000)
                .saturating_mul(1_000_000),
            snapshot_interval_ms: env_u64("SNAPSHOT_INTERVAL_MS", 1000).max(1),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
