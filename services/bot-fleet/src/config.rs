use std::{env, time::Duration};

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
    pub orders_sent_topic: String,
    pub telemetry_flush_interval: Duration,
    pub telemetry_batch_size: usize,
    pub telemetry_channel_capacity: usize,
    pub max_bots_per_worker: usize,
}

impl Config {
    /// from_env builds a Config from process environment variables.
    /// Missing or invalid values fall back to safe defaults.
    pub fn from_env() -> Self {
        Self {
            worker_id: "bot-fleet-local".to_string(),
            kafka_brokers: env_or("KAFKA_BROKERS", "localhost:9092".to_string()),
            consumer_group: "bot-fleet".to_string(),
            workload_topic: "workload.assignments".to_string(),
            barrier_topic: "barrier".to_string(),
            ready_topic: "bot.ready".to_string(),
            orders_sent_topic: "orders.sent".to_string(),
            telemetry_flush_interval: Duration::from_millis(5),
            telemetry_batch_size: 4096,
            telemetry_channel_capacity: 65536,
            max_bots_per_worker: 1000,
        }
    }
}

/// env_or returns a non-empty environment value or the provided default.
fn env_or(key: &str, default: String) -> String {
    env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or(default)
}
