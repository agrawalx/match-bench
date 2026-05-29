use serde::{Deserialize, Serialize};

pub const TOPIC_SUBMISSION_BUILD_REQUESTED: &str = "submission.build.requested";
pub const TOPIC_SUBMISSION_STATUS_UPDATED: &str = "submission.status.updated";
pub const TOPIC_BENCHMARK_REQUESTED: &str = "benchmark.requested";
pub const TOPIC_BENCHMARK_STATUS_UPDATED: &str = "benchmark.status.updated";
pub const TOPIC_WORKLOAD_ASSIGNMENTS: &str = "workload.assignments";
pub const TOPIC_BARRIER: &str = "barrier";
pub const TOPIC_BOT_READY: &str = "bot.ready";
pub const TOPIC_WORKLOAD_FAILED: &str = "workload.failed";
pub const TOPIC_ORDERS_SENT: &str = "orders.sent";
pub const TOPIC_ORDERS_ACKED: &str = "orders.acked";
pub const TOPIC_SCORES_CORRECTNESS: &str = "scores.correctness";
pub const TOPIC_LEADERBOARD_UPDATES: &str = "leaderboard.updates";

/// Protocol identifies the transport a bot-fleet worker should use.
/// Serialized as FIX, REST, or WS to match controller payloads.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Protocol {
    Fix,
    Rest,
    Ws,
}

/// BotProfile is the participant archetype a task simulates. Per-bot RPS and
/// order-shape biasing are determined by the profile; the controller does not
/// dictate a per-message mix.
///
/// v1 archetypes match the hackathon spec: HFT market maker, Retail trader,
/// Institutional. Per-bot RPS values (midpoints of the arch_v2 ranges) live
/// in the scenarios table and are tunable by judges without code changes.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BotProfile {
    Hft,
    Retail,
    Institutional,
}

/// WorkloadSpec is published to "workload.assignments" by the test controller.
/// Each message is keyed by session_id:worker_index and consumed by one worker.
///
/// The flat (bot_count, orders_per_bot, profile_mix) shape is replaced by a
/// list of TaskSpec values. Every TaskSpec is one tokio task in the worker =
/// one TCP connection = one constant-rate sender. Load-pattern variation
/// (spike, ramp) emerges from the schedule of TaskSpecs: tasks start at their
/// start_offset_ns and stop after duration_ns. Bots never change behaviour
/// mid-flight.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WorkloadSpec {
    pub session_id: String,
    pub submission_id: String,
    #[serde(default)]
    pub contestant_id: String,
    pub target_host: String,
    pub target_port: u16,
    pub protocol: Protocol,
    pub worker_index: u32,
    pub worker_count: u32,
    pub global_seed: u64,
    #[serde(default = "default_fix_version")]
    pub fix_version: String,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,
    // barrier_epoch_ns was historically carried here as a fallback for a
    // lost BarrierEvent, but workers never read it — wait_for_barrier reads
    // BarrierEvent.target_epoch_unix_nanos. The controller now publishes a
    // fresh epoch AFTER fan-in, so the BarrierEvent value is the only one
    // workers need. Default to 0 if older controllers still send the field.
    #[serde(default)]
    pub barrier_epoch_ns: u64,
    /// This worker's slice of the scenario's task list.
    pub tasks: Vec<TaskSpec>,
}

/// TaskSpec is one sender: one tokio task, one TCP connection, one constant rate.
///
/// All tasks pre-open their TCP connection at barrier time (avoids cold-start
/// jitter contaminating spike measurements). Each task sleeps until
/// barrier_epoch_ns + start_offset_ns, then sends at target_rps via
/// fixed-interval pacing until barrier_epoch_ns + start_offset_ns + duration_ns.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskSpec {
    pub task_id: u32,
    pub profile: BotProfile,
    pub target_rps: u32,
    pub start_offset_ns: u64,
    pub duration_ns: u64,
}

/// BarrierEvent is published to "barrier" once all workers have reported ready.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BarrierEvent {
    pub session_id: String,
    pub target_epoch_unix_nanos: u64,
}

/// ReadySignal is published by each bot-fleet worker to "bot.ready".
/// It tells the controller how many local tasks connected before the barrier.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReadySignal {
    pub session_id: String,
    pub submission_id: String,
    pub worker_id: String,
    pub worker_index: u32,
    pub worker_count: u32,
    /// Number of TaskSpecs assigned to this worker (replaces the old bot_count).
    pub task_count: u32,
    /// Number of tasks that successfully pre-opened their TCP connection.
    pub connected_count: u32,
    pub ready_at_unix_nanos: u64,
}

/// OrderSentEvent records one outbound order with the three bot-side
/// timestamps the telemetry-ingester needs to detect coordinated omission.
///
/// Timestamp definitions (all CLOCK_REALTIME nanoseconds, bot-side):
///   - target_send_ts_ns (t0): the schedule's intended fire time for this
///     order. Computed deterministically from
///     `barrier_epoch_ns + task.start_offset_ns + seq * (1e9 / target_rps)`.
///     Captured BEFORE sleep_until — never a clock read. The gap
///     `send_ts_ns - target_send_ts_ns` IS coordinated omission, by definition.
///   - send_ts_ns (t1): wall-clock immediately after the TCP write returned.
///   - recv_done_ts_ns (r9): wall-clock immediately after the FIRST response
///     for this order was read off the socket. Subsequent ExecutionReports
///     for the same ClOrdID (partial fills, final fills) are ignored — r9 is
///     "I heard back."
///
/// Emission semantics:
///   - Emitted only ONCE per order, either when r9 is captured (timed_out=false)
///     or when the watchdog evicts the order at the 5s deadline (timed_out=true,
///     recv_done_ts_ns=0).
///   - Late emission: events lag the actual write by up to RESPONSE_TIMEOUT
///     (5s). Live latency dashboards must source from order.service.events
///     (algo-side, no delay); orders.sent is the CO-detection stream.
///
/// REST/WS caveat (v1): the response read path is FIX-only. For REST and WS
/// the bot emits with recv_done_ts_ns=0 and timed_out=false immediately after
/// the write, matching the legacy behaviour. Ingesters can distinguish by
/// checking `recv_done_ts_ns > 0 || timed_out`.
#[derive(Debug, Clone, Serialize)]
pub struct OrderSentEvent {
    pub session_id: String,
    pub submission_id: String,
    pub worker_id: String,
    pub task_id: u32,
    pub order_id: String,
    pub target_send_ts_ns: u64,
    pub send_ts_ns: u64,
    pub recv_done_ts_ns: u64,
    pub timed_out: bool,
    pub price: u64,
    pub qty: u64,
    pub side: Side,
}

/// OrderSentBatch is MessagePack-encoded on "orders.sent".
/// Batching keeps Kafka traffic proportional to flush rate instead of order rate.
#[derive(Debug, Clone, Serialize)]
pub struct OrderSentBatch {
    pub session_id: String,
    pub worker_id: String,
    pub events: Vec<OrderSentEvent>,
}

/// Side is serialized as BUY or SELL in telemetry payloads.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

/// default_fix_version supplies FIX.4.2 when a workload omits the field.
fn default_fix_version() -> String {
    "FIX.4.2".to_string()
}

/// default_connect_timeout_ms bounds initial TCP/WebSocket connection setup.
fn default_connect_timeout_ms() -> u64 {
    1500
}

/// default_write_timeout_ms bounds per-order socket writes.
fn default_write_timeout_ms() -> u64 {
    250
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_constants_match_platform_contract() {
        let topics = [
            TOPIC_SUBMISSION_BUILD_REQUESTED,
            TOPIC_SUBMISSION_STATUS_UPDATED,
            TOPIC_BENCHMARK_REQUESTED,
            TOPIC_BENCHMARK_STATUS_UPDATED,
            TOPIC_WORKLOAD_ASSIGNMENTS,
            TOPIC_BARRIER,
            TOPIC_BOT_READY,
            TOPIC_WORKLOAD_FAILED,
            TOPIC_ORDERS_SENT,
            TOPIC_ORDERS_ACKED,
            TOPIC_SCORES_CORRECTNESS,
            TOPIC_LEADERBOARD_UPDATES,
        ];

        for topic in topics {
            assert!(!topic.trim().is_empty());
            assert!(
                topic.bytes().all(|b| b.is_ascii_lowercase()
                    || b.is_ascii_digit()
                    || matches!(b, b'.' | b'-')),
                "topic {topic} contains unsupported characters"
            );
        }
    }

    #[test]
    fn workload_spec_decodes_go_controller_payload() {
        let payload = br#"{
            "session_id":"sess-1",
            "submission_id":"sub-1",
            "contestant_id":"team-1",
            "target_host":"algo-sess-1.sandbox.svc.cluster.local",
            "target_port":8080,
            "protocol":"FIX",
            "worker_index":0,
            "worker_count":1,
            "global_seed":42,
            "fix_version":"FIX.4.2",
            "connect_timeout_ms":1500,
            "write_timeout_ms":250,
            "tasks":[
                {"task_id":1,"profile":"hft","target_rps":50,"start_offset_ns":0,"duration_ns":1000000000}
            ]
        }"#;

        let spec: WorkloadSpec = serde_json::from_slice(payload).expect("decode workload spec");
        assert_eq!(spec.session_id, "sess-1");
        assert_eq!(spec.submission_id, "sub-1");
        assert_eq!(spec.protocol, Protocol::Fix);
        assert_eq!(spec.tasks.len(), 1);
        assert_eq!(spec.tasks[0].profile, BotProfile::Hft);
    }

    #[test]
    fn ready_signal_encodes_go_controller_fields() {
        let signal = ReadySignal {
            session_id: "sess-1".into(),
            submission_id: "sub-1".into(),
            worker_id: "worker-1".into(),
            worker_index: 0,
            worker_count: 1,
            task_count: 10,
            connected_count: 10,
            ready_at_unix_nanos: 123,
        };

        let value = serde_json::to_value(signal).expect("encode ready signal");
        assert_eq!(value["session_id"], "sess-1");
        assert_eq!(value["submission_id"], "sub-1");
        assert_eq!(value["worker_id"], "worker-1");
        assert_eq!(value["task_count"], 10);
        assert_eq!(value["ready_at_unix_nanos"], 123);
    }
}
