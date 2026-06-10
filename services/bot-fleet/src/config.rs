use std::{env, process, time::Duration};

use iicpc_schemas_rust::{
    TOPIC_BARRIER, TOPIC_BOT_READY, TOPIC_ORDERS_SENT, TOPIC_WORKLOAD_ASSIGNMENTS,
    TOPIC_WORKLOAD_FAILED,
};

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
    /// max_poll_interval is the consumer's `max.poll.interval.ms` bound. The
    /// workload-assignment offset is committed only after the whole workload
    /// finishes (barrier wait + scenario duration + drain); if that wall time
    /// exceeds this bound Kafka rebalances mid-run and re-delivers the
    /// assignment, causing duplicate execution. validate_spec rejects a spec
    /// whose worst-case wall time would breach it (L39). Kept in sync with the
    /// value passed to the rdkafka consumer in kafka.rs.
    pub max_poll_interval: Duration,
}

/// DEFAULT_MAX_POLL_INTERVAL is the consumer's `max.poll.interval.ms` default.
/// Shared between the Config default and the rdkafka consumer so the L39 guard
/// and the broker bound never drift apart.
///
/// 30 minutes, because the worker does not poll while a workload runs and the
/// guard compares the workload's WORST-CASE wall time against this bound:
///
///   barrier wait (120s) + max(start_offset + duration) + response drain (5s)
///
/// The ramp scenario's ~180s task span makes that ≈305s — the old 300s
/// default sat just below it, so every ramp run was either rejected by the
/// guard or rebalanced mid-run and re-delivered (duplicate execution).
/// 1_800_000 ms leaves ~1675s of task span for long high-RPS runs and matches
/// the deployment manifest's MAX_POLL_INTERVAL_MS. Liveness is unaffected:
/// librdkafka's background heartbeat (session.timeout.ms=10s) still detects a
/// dead pod; only the allowed processing-between-polls window grows.
pub const DEFAULT_MAX_POLL_INTERVAL: Duration = Duration::from_millis(1_800_000);

impl Default for Config {
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
            telemetry_flush_interval: Duration::from_millis(5),
            // Aggregator flush threshold, kept equal to the sink's
            // per-message chunk ceiling (telemetry.rs MAX_EVENTS_PER_BATCH)
            // so a steady-state flush is exactly one Kafka message:
            // ≈360 B/event msgpack-named ⇒ 1000 events ≈ 360 KB < 1 MiB
            // max.message.bytes. The interim 200 ceiling capped the sink at
            // ~50-100k ev/s and silently dropped events at the scoring waves.
            telemetry_batch_size: 1000,
            telemetry_channel_capacity: 65536,
            // The per-pod task ceiling. Must be >= the controller's
            // MAX_TASKS_PER_WORKER (default 1000): the controller shards a
            // scenario into specs of up to that many tasks, and a smaller value
            // here makes the worker reject valid specs, dropping the workload so
            // the controller's bot.ready fan-in times out. Raise both together
            // (MAX_BOTS_PER_WORKER here, MAX_TASKS_PER_WORKER on the controller)
            // to pin more load onto a single pod for capacity testing.
            max_bots_per_worker: 1000,
            max_poll_interval: DEFAULT_MAX_POLL_INTERVAL,
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
            max_poll_interval: env::var("MAX_POLL_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_millis)
                .unwrap_or(default.max_poll_interval),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_topics_come_from_schema_contract() {
        let config = Config::default();
        assert_eq!(config.workload_topic, TOPIC_WORKLOAD_ASSIGNMENTS);
        assert_eq!(config.barrier_topic, TOPIC_BARRIER);
        assert_eq!(config.ready_topic, TOPIC_BOT_READY);
        assert_eq!(config.workload_failed_topic, TOPIC_WORKLOAD_FAILED);
        assert_eq!(config.orders_sent_topic, TOPIC_ORDERS_SENT);
    }

    #[test]
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
    fn default_max_poll_interval_covers_ramp_worst_case() {
        // The L39 guard (worker.rs validate_spec) rejects any spec whose
        // worst-case wall time meets or exceeds this default: 120s barrier
        // wait + the ramp scenario's ~180s task span + 5s response drain
        // ≈ 305s. The old 300s default sat BELOW that, so every ramp run
        // either got rejected up front or — without the guard — hit a Kafka
        // rebalance mid-run and was re-delivered (duplicate execution).
        let ramp_worst_case = Duration::from_secs(120 + 180 + 5);
        assert!(
            DEFAULT_MAX_POLL_INTERVAL > ramp_worst_case,
            "default poll interval {DEFAULT_MAX_POLL_INTERVAL:?} must exceed the ramp worst case {ramp_worst_case:?}"
        );
        // 30 min, matching the deployment manifest's MAX_POLL_INTERVAL_MS.
        assert_eq!(DEFAULT_MAX_POLL_INTERVAL, Duration::from_millis(1_800_000));
    }

    #[test]
    fn default_telemetry_batch_size_matches_publish_chunk_ceiling() {
        // The aggregator flushes at telemetry_batch_size and the sink splits
        // each flush into MAX_EVENTS_PER_BATCH-sized Kafka messages. Keeping
        // them equal means a steady-state flush is exactly one message — a
        // smaller batch size silently caps sink throughput (the old 200
        // ceiling capped it at ~50-100k ev/s, dropping events at the scoring
        // waves), a larger one always pays the multi-chunk path.
        let config = Config::default();
        assert_eq!(config.telemetry_batch_size, 1000);
        assert_eq!(
            config.telemetry_batch_size,
            crate::telemetry::MAX_EVENTS_PER_BATCH
        );
        assert!(config.telemetry_channel_capacity >= config.telemetry_batch_size);
    }
}
