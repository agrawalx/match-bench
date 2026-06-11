//! This module implements config behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::env;

use crate::aggregate::DEFAULT_WAVE_NS;

#[derive(Debug, Clone)]
/// Config stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct Config {
    pub kafka_brokers: String,
    pub consumer_group: String,
    pub timescale_url: String,
    pub redis_url: String,
    pub wave_ns: u64,
    pub snapshot_interval_ms: u64,
    /// This replica's shard identity. Each ingester replica writes its partial
    /// per-(session,wave) aggregates to metrics_partial tagged with this, so a
    /// session sharded across replicas produces non-colliding partial rows that the
    /// rollup merges. Defaults to the pod name (HOSTNAME); INGESTER_SHARD overrides.
    pub shard: String,
}

impl Config {
    /// from_env performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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
            shard: env::var("INGESTER_SHARD")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .or_else(|| env::var("HOSTNAME").ok().filter(|v| !v.trim().is_empty()))
                .unwrap_or_else(|| "ingester-0".to_string()),
        }
    }
}

/// env_or performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn env_or(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// env_u64 performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
