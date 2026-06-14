//! This module implements config behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::{env, process, time::Duration};

use iicpc_schemas_rust::{
    TOPIC_BARRIER, TOPIC_BOT_READY, TOPIC_ORDERS_SENT, TOPIC_WORKLOAD_ASSIGNMENTS,
    TOPIC_WORKLOAD_FAILED,
};

#[derive(Debug, Clone)]
/// Config stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct Config {
    pub worker_id: String,
    pub kafka_brokers: String,
    pub consumer_group: String,
    pub workload_topic: String,
    pub barrier_topic: String,
    pub ready_topic: String,
    pub workload_failed_topic: String,
    pub orders_sent_topic: String,
    pub orders_partitions: i32,
    pub telemetry_flush_interval: Duration,
    pub telemetry_batch_size: usize,
    pub telemetry_channel_capacity: usize,
    pub max_bots_per_worker: usize,
    pub max_poll_interval: Duration,
}

pub const DEFAULT_MAX_POLL_INTERVAL: Duration = Duration::from_millis(1_800_000);

impl Default for Config {
    /// default performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn default() -> Self {
        Self {
            worker_id: format!("bot-fleet-local-{}", process::id()),
            kafka_brokers: "localhost:9092".to_string(),
            consumer_group: "bot-fleet".to_string(),
            workload_topic: TOPIC_WORKLOAD_ASSIGNMENTS.to_string(),
            barrier_topic: TOPIC_BARRIER.to_string(),
            ready_topic: TOPIC_BOT_READY.to_string(),
            workload_failed_topic: TOPIC_WORKLOAD_FAILED.to_string(),
            orders_sent_topic: TOPIC_ORDERS_SENT.to_string(),
            orders_partitions: 24,
            telemetry_flush_interval: Duration::from_millis(5),
            telemetry_batch_size: 1000,
            telemetry_channel_capacity: 65536,
            max_bots_per_worker: 1000,
            max_poll_interval: DEFAULT_MAX_POLL_INTERVAL,
        }
    }
}

impl Config {
    /// from_env performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            worker_id: env::var("HOSTNAME")
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or(default.worker_id),
            kafka_brokers: env_or("KAFKA_BROKERS", default.kafka_brokers),
            consumer_group: env_or("KAFKA_CONSUMER_GROUP", default.consumer_group),
            workload_topic: env_or("WORKLOAD_TOPIC", default.workload_topic),
            barrier_topic: env_or("BARRIER_TOPIC", default.barrier_topic),
            ready_topic: env_or("READY_TOPIC", default.ready_topic),
            workload_failed_topic: env_or("WORKLOAD_FAILED_TOPIC", default.workload_failed_topic),
            orders_sent_topic: env_or("ORDERS_SENT_TOPIC", default.orders_sent_topic),
            orders_partitions: env::var("ORDERS_PARTITIONS")
                .ok()
                .and_then(|v| v.parse::<i32>().ok())
                .filter(|n| *n > 0)
                .unwrap_or(default.orders_partitions),
            telemetry_flush_interval: env::var("TELEMETRY_FLUSH_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_millis)
                .unwrap_or(default.telemetry_flush_interval),
            telemetry_batch_size: env::var("TELEMETRY_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(default.telemetry_batch_size),
            telemetry_channel_capacity: env::var("TELEMETRY_CHANNEL_CAPACITY")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(default.telemetry_channel_capacity),
            max_bots_per_worker: env::var("MAX_BOTS_PER_WORKER")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(default.max_bots_per_worker),
            max_poll_interval: env::var("MAX_POLL_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_millis)
                .unwrap_or(default.max_poll_interval),
        }
    }

    /// validate performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn validate(&self) -> Result<(), String> {
        if self.worker_id.trim().is_empty() {
            return Err("worker_id cannot be empty".to_string());
        }
        if self.kafka_brokers.trim().is_empty() {
            return Err("kafka_brokers cannot be empty".to_string());
        }
        if self.consumer_group.trim().is_empty() {
            return Err("consumer_group cannot be empty".to_string());
        }
        if self.workload_topic.trim().is_empty() {
            return Err("workload_topic cannot be empty".to_string());
        }
        if self.barrier_topic.trim().is_empty() {
            return Err("barrier_topic cannot be empty".to_string());
        }
        if self.ready_topic.trim().is_empty() {
            return Err("ready_topic cannot be empty".to_string());
        }
        if self.workload_failed_topic.trim().is_empty() {
            return Err("workload_failed_topic cannot be empty".to_string());
        }
        if self.orders_sent_topic.trim().is_empty() {
            return Err("orders_sent_topic cannot be empty".to_string());
        }
        if self.telemetry_batch_size == 0 {
            return Err("telemetry_batch_size must be greater than zero".to_string());
        }
        if self.telemetry_channel_capacity == 0 {
            return Err("telemetry_channel_capacity must be greater than zero".to_string());
        }
        if self.max_bots_per_worker == 0 {
            return Err("max_bots_per_worker must be greater than zero".to_string());
        }
        if self.telemetry_channel_capacity < self.telemetry_batch_size {
            return Err(format!(
                "telemetry_channel_capacity ({}) cannot be less than telemetry_batch_size ({})",
                self.telemetry_channel_capacity, self.telemetry_batch_size
            ));
        }
        Ok(())
    }
}

/// env_or performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn env_or(key: &str, default: String) -> String {
    env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// default_topics_come_from_schema_contract performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn default_topics_come_from_schema_contract() {
        let config = Config::default();
        assert_eq!(config.workload_topic, TOPIC_WORKLOAD_ASSIGNMENTS);
        assert_eq!(config.barrier_topic, TOPIC_BARRIER);
        assert_eq!(config.ready_topic, TOPIC_BOT_READY);
        assert_eq!(config.workload_failed_topic, TOPIC_WORKLOAD_FAILED);
        assert_eq!(config.orders_sent_topic, TOPIC_ORDERS_SENT);
    }

    #[test]
    /// validate_rejects_undersized_telemetry_channel performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn validate_rejects_undersized_telemetry_channel() {
        let config = Config {
            telemetry_batch_size: 100,
            telemetry_channel_capacity: 10,
            ..Config::default()
        };

        let err = config.validate().expect_err("config should be rejected");
        assert!(err.contains("telemetry_channel_capacity"));
    }

    #[test]
    /// default_max_poll_interval_covers_ramp_worst_case performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn default_max_poll_interval_covers_ramp_worst_case() {
        let ramp_worst_case = Duration::from_secs(120 + 180 + 5);
        assert!(
            DEFAULT_MAX_POLL_INTERVAL > ramp_worst_case,
            "default poll interval {DEFAULT_MAX_POLL_INTERVAL:?} must exceed the ramp worst case {ramp_worst_case:?}"
        );
        assert_eq!(DEFAULT_MAX_POLL_INTERVAL, Duration::from_millis(1_800_000));
    }

    #[test]
    /// default_telemetry_batch_size_matches_publish_chunk_ceiling performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn default_telemetry_batch_size_matches_publish_chunk_ceiling() {
        let config = Config::default();
        assert_eq!(config.telemetry_batch_size, 1000);
        assert_eq!(
            config.telemetry_batch_size,
            crate::telemetry::MAX_EVENTS_PER_BATCH
        );
        assert!(config.telemetry_channel_capacity >= config.telemetry_batch_size);
    }
}
