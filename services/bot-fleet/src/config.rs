use std::{env, process, time::Duration};

/// Config contains the bot-fleet runtime knobs loaded from environment.
/// Defaults target local development while keeping workload fan-out bounded.
#[derive(Debug, Clone)]
pub struct Config {
    pub worker_id: String,
    pub kafka_brokers: String,
    pub consumer_group: String,
    pub workload_topic: String,
    pub barrier_topic: String,
    pub ready_topic: String,
    pub workload_failed_topic: String,
    pub orders_sent_topic: String,
    pub telemetry_flush_interval: Duration,
    pub telemetry_batch_size: usize,
    pub telemetry_channel_capacity: usize,
    pub max_bots_per_worker: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            worker_id: format!("bot-fleet-local-{}", process::id()),
            kafka_brokers: "localhost:9092".to_string(),
            consumer_group: "bot-fleet".to_string(),
            workload_topic: "workload.assignments".to_string(),
            barrier_topic: "barrier".to_string(),
            ready_topic: "bot.ready".to_string(),
            workload_failed_topic: "workload.failed".to_string(),
            orders_sent_topic: "orders.sent".to_string(),
            telemetry_flush_interval: Duration::from_millis(5),
            telemetry_batch_size: 4096,
            telemetry_channel_capacity: 65536,
            max_bots_per_worker: 200,
        }
    }
}

impl Config {
    /// from_env builds a Config from process environment variables.
    /// Missing or invalid values fall back to safe defaults.
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
        }
    }

    /// validate checks config bounds to avoid division-by-zero or queue starvation downstream.
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

/// env_or returns a non-empty environment value or the provided default.
fn env_or(key: &str, default: String) -> String {
    env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or(default)
}
