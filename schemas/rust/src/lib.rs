//! This module defines shared schema contracts for lib.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
/// Protocol enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum Protocol {
    Fix,
    Rest,
    Ws,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
/// PayloadType enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum PayloadType {
    New,
    Cancel,
    Replace,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
/// OrdType enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum OrdType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// BotProfile enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum BotProfile {
    Hft,
    Retail,
    Institutional,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// WorkloadSpec stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
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
    #[serde(default)]
    pub barrier_epoch_ns: u64,
    pub tasks: Vec<TaskSpec>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// TaskSpec stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct TaskSpec {
    pub task_id: u32,
    pub profile: BotProfile,
    pub target_rps: u32,
    pub start_offset_ns: u64,
    pub duration_ns: u64,
    #[serde(default)]
    pub market_pct: u8,
    #[serde(default)]
    pub cancel_pct: u8,
    #[serde(default)]
    pub replace_pct: u8,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// BarrierEvent stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct BarrierEvent {
    pub session_id: String,
    pub target_epoch_unix_nanos: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// ReadySignal stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct ReadySignal {
    pub session_id: String,
    pub submission_id: String,
    pub worker_id: String,
    pub worker_index: u32,
    pub worker_count: u32,
    pub task_count: u32,
    pub connected_count: u32,
    pub ready_at_unix_nanos: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// OrderSentEvent stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
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
    pub payload_type: PayloadType,
    pub ord_type: OrdType,
    #[serde(default)]
    pub orig_order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// OrderSentBatch stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct OrderSentBatch {
    pub session_id: String,
    pub worker_id: String,
    pub events: Vec<OrderSentEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// OrderAckedEvent stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
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
    pub fill_price: u64,
    pub orig_order_id: String,
    pub reordering_detected: bool,
    pub retransmission_count: u32,
}

#[derive(Debug, Clone, Copy, Serialize)]
/// OrderAckedEventRef stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
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

#[derive(Debug, Clone, Serialize, Deserialize)]
/// OrderAckedBatch stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct OrderAckedBatch {
    pub session_id: String,
    pub contestant_id: String,
    pub events: Vec<OrderAckedEvent>,
}

#[derive(Debug, Clone, Copy, Serialize)]
/// OrderAckedBatchRef stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct OrderAckedBatchRef<'a> {
    pub session_id: &'a str,
    pub contestant_id: &'a str,
    pub events: &'a [OrderAckedEventRef<'a>],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// CorrectnessScoreEvent stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct CorrectnessScoreEvent {
    pub session_id: String,
    pub contestant_id: String,
    pub valid_fills: u64,
    pub total_fills: u64,
    pub correctness_score: f64,
    pub violation_count: u32,
    pub computed_at_ns: u64,
    #[serde(default)]
    pub sent_count: u64,
    #[serde(default)]
    pub acked_count: u64,
    #[serde(default)]
    pub matched_count: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
/// Side enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum Side {
    Buy,
    Sell,
}

/// default_fix_version performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn default_fix_version() -> String {
    "FIX.4.2".to_string()
}

/// default_connect_timeout_ms performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn default_connect_timeout_ms() -> u64 {
    1500
}

/// default_write_timeout_ms performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn default_write_timeout_ms() -> u64 {
    250
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// topic_constants_match_platform_contract performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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
    /// workload_spec_decodes_go_controller_payload performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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
    /// correctness_score_event_decodes_go_validator_payload performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn correctness_score_event_decodes_go_validator_payload() {
        let payload = br#"{
            "session_id":"sess-1",
            "contestant_id":"team-1",
            "valid_fills":2,
            "total_fills":4,
            "correctness_score":0.5,
            "violation_count":2,
            "computed_at_ns":123,
            "sent_count":1000,
            "acked_count":950,
            "matched_count":940
        }"#;

        let ev: CorrectnessScoreEvent =
            serde_json::from_slice(payload).expect("decode correctness score event");
        assert_eq!(ev.session_id, "sess-1");
        assert_eq!(ev.contestant_id, "team-1");
        assert_eq!(ev.valid_fills, 2);
        assert_eq!(ev.total_fills, 4);
        assert_eq!(ev.correctness_score, 0.5);
        assert_eq!(ev.violation_count, 2);
        assert_eq!(ev.computed_at_ns, 123);
        assert_eq!(ev.sent_count, 1000);
        assert_eq!(ev.acked_count, 950);
        assert_eq!(ev.matched_count, 940);

        let legacy = br#"{
            "session_id":"sess-1",
            "contestant_id":"team-1",
            "valid_fills":2,
            "total_fills":4,
            "correctness_score":0.5,
            "violation_count":2,
            "computed_at_ns":123
        }"#;
        let ev: CorrectnessScoreEvent =
            serde_json::from_slice(legacy).expect("decode legacy correctness score event");
        assert_eq!(ev.sent_count, 0);
        assert_eq!(ev.acked_count, 0);
        assert_eq!(ev.matched_count, 0);
    }

    #[test]
    /// ready_signal_encodes_go_controller_fields performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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
