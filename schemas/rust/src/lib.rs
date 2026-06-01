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
pub const TELEMETRY_PRICE_SCALE: u64 = 1_000_000_000;

/// Protocol identifies the transport a bot-fleet worker should use.
/// Serialized as FIX, REST, or WS to match controller payloads.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Protocol {
    Fix,
    Rest,
    Ws,
}

/// PayloadType identifies the order lifecycle operation carried by an event.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum PayloadType {
    New,
    Cancel,
    Replace,
}

/// OrdType is the FIX OrdType (tag 40) of the order an event pertains to:
/// Limit (40=2) or Market (40=1). The bot records it so the correctness
/// validator can replay market orders (immediate execution) vs limit orders
/// (rest in the book) without re-parsing tag 40 from the wire on the algo side.
/// Cancel/Replace events carry the OrdType of the resting order they act on,
/// which is always Limit in v1 (market orders never rest).
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrdType {
    Limit,
    Market,
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
    /// Order-type mix as a percentage of messages sent by this task. The limit
    /// fraction is implied: 100 - market_pct - cancel_pct - replace_pct. Source:
    /// architecture_v2.md Bot Profiles. `#[serde(default)]` so older
    /// WorkloadSpecs without these fields decode as all-limit (0/0/0).
    #[serde(default)]
    pub market_pct: u8,
    #[serde(default)]
    pub cancel_pct: u8,
    #[serde(default)]
    pub replace_pct: u8,
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
    /// NEW | CANCEL | REPLACE. Lets the validator/ingester separate cancels and
    /// replaces from new orders (e.g. the HFT cancel-throughput metric).
    pub payload_type: PayloadType,
    /// LIMIT | MARKET (FIX tag 40). Distinguishes market from limit new orders,
    /// which share payload_type=NEW, so the validator can replay them correctly.
    pub ord_type: OrdType,
}

/// OrderSentBatch is MessagePack-encoded on "orders.sent".
/// Batching keeps Kafka traffic proportional to flush rate instead of order rate.
#[derive(Debug, Clone, Serialize)]
pub struct OrderSentBatch {
    pub session_id: String,
    pub worker_id: String,
    pub events: Vec<OrderSentEvent>,
}

/// OrderAckedEvent records the request/response boundary timestamps for one FIX
/// ClOrdID. One order may produce SEVERAL events — one per response packet (ACK,
/// then each partial fill) — all sharing the request's t3.
///
/// Timestamp definitions (the kernel stamps CLOCK_MONOTONIC via bpf_ktime_get_ns;
/// the ebpf-latency userspace adds a sampled realtime-minus-monotonic offset, so
/// the published values are CLOCK_REALTIME, matching the bot fleet's t0/t1/r9):
///   - t3_xdp_ingress_ns: captured by XDP when the request segment enters the
///     algo pod's veth.
///   - t7_xdp_egress_ns: captured by tc egress when the response segment leaves
///     the algo pod's veth.
/// The telemetry ingester joins this stream with `orders.sent` on
/// `(session_id, order_id)`. The primary contestant latency metric is
/// `pod_service_time_ns = max(0, t7_xdp_egress_ns - t3_xdp_ingress_ns)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderAckedEvent {
    pub session_id: String,
    pub contestant_id: String,
    pub order_id: String,
    pub src_ip: u32,
    pub src_port: u16,
    pub tcp_seq: u32,
    pub t3_xdp_ingress_ns: u64,
    pub t7_xdp_egress_ns: u64,
    pub pod_service_time_ns: u64,
    pub exec_type: String,
    pub fill_qty: u64,
    /// Fill price as a fixed-point integer scaled by TELEMETRY_PRICE_SCALE.
    pub fill_price: u64,
    pub orig_order_id: String,
    pub reordering_detected: bool,
    pub retransmission_count: u32,
}

/// Borrowed serialization view for `OrderAckedEvent`.
///
/// Producers that already carry `session_id` and `contestant_id` at the batch
/// level can use this to avoid allocating cloned id strings for every event.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct OrderAckedEventRef<'a> {
    pub session_id: &'a str,
    pub contestant_id: &'a str,
    pub order_id: &'a str,
    pub src_ip: u32,
    pub src_port: u16,
    pub tcp_seq: u32,
    pub t3_xdp_ingress_ns: u64,
    pub t7_xdp_egress_ns: u64,
    pub pod_service_time_ns: u64,
    pub exec_type: &'a str,
    pub fill_qty: u64,
    pub fill_price: u64,
    pub orig_order_id: &'a str,
    pub reordering_detected: bool,
    pub retransmission_count: u32,
}

/// OrderAckedBatch is MessagePack-encoded on "orders.acked".
/// Batching keeps one Kafka record per drain interval instead of one record
/// per response packet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderAckedBatch {
    pub session_id: String,
    pub contestant_id: String,
    pub events: Vec<OrderAckedEvent>,
}

/// Borrowed serialization view for `OrderAckedBatch`.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct OrderAckedBatchRef<'a> {
    pub session_id: &'a str,
    pub contestant_id: &'a str,
    pub events: &'a [OrderAckedEventRef<'a>],
}

/// Side is serialized as BUY or SELL in telemetry payloads.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
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
