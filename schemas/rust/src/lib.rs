use std::sync::Arc;
use serde::{Deserialize, Serialize};

pub const STATUS_UPLOADED: &str = "uploaded";
pub const STATUS_BUILDING: &str = "building";
pub const STATUS_SCANNED: &str = "scanned";
pub const STATUS_SBOM_READY: &str = "sbom_ready";
pub const STATUS_READY: &str = "ready";
pub const STATUS_FAILED: &str = "failed";

pub const RUN_STATUS_REQUESTED: &str = "requested";
pub const RUN_STATUS_DEPLOYING: &str = "deploying";
pub const RUN_STATUS_WAITING_READY: &str = "waiting_ready";
pub const RUN_STATUS_BARRIER_FIRED: &str = "barrier_fired";
pub const RUN_STATUS_RUNNING: &str = "running";
pub const RUN_STATUS_COMPLETED: &str = "completed";
pub const RUN_STATUS_FAILED: &str = "failed";

/// Protocol identifies the transport a bot-fleet worker should use.
/// Serialized as FIX, REST, or WS to match controller payloads.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Protocol {
    Fix,
    Rest,
    Ws,
}

/// BotProfile selects the deterministic order-shape strategy for a bot.
/// Serialized as snake_case to match workload assignment documents.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BotProfile {
    MarketMaker,
    AggressiveTaker,
    Canceller,
}

/// BotProfileWeight assigns a relative share of bots to a profile.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BotProfileWeight {
    pub profile: BotProfile,
    pub weight: u32,
}

/// WorkloadSpec is published to "workload.assignments" by the test controller.
/// Each message is keyed by session_id:worker_index and consumed by one worker.
///
/// OrdersPerBot and TargetRatePerBot are mutually exclusive control modes:
///   - If TargetRatePerBot > 0, the worker fires at that rate (orders/sec) for a
///     fixed 60-second window, ignoring OrdersPerBot entirely.
///   - If TargetRatePerBot == 0/absent, the worker sends exactly OrdersPerBot orders as
///     fast as the target allows (unbounded rate, count-limited).
///
/// FIXVersion defaults to "FIX.4.2". This default matches the Go controller side and
/// acts as a fallback.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
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
    pub bot_count: u32,
    pub orders_per_bot: u32,
    #[serde(default)]
    pub target_rate_per_bot: Option<u32>,
    pub global_seed: u64,
    #[serde(default = "default_fix_version")]
    pub fix_version: String,
    #[serde(default = "default_profile_mix")]
    pub profile_mix: Vec<BotProfileWeight>,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,
}

/// PayloadType identifies the order lifecycle operation carried by an event.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum PayloadType {
    New,
    Cancel,
    Replace,
}

/// Side is serialized as BUY or SELL in telemetry payloads.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

/// BarrierEvent is published to "barrier" once all workers have reported ready.
///
/// TargetEpochUnixNanos is the absolute nanosecond timestamp at which all workers
/// must simultaneously open fire. Consumers must handle the case where this timestamp
/// is in the past at the time of consumption (e.g. slow consumer, Kafka replay):
/// in that case the worker should begin firing immediately rather than waiting.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BarrierEvent {
    pub session_id: String,
    pub target_epoch_unix_nanos: u64,
}

/// ReadySignal is published by each bot-fleet worker to "bot.ready".
/// It tells the controller how many local bots connected before the barrier.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ReadySignal {
    pub session_id: String,
    pub submission_id: String,
    pub worker_id: String,
    pub worker_index: u32,
    pub worker_count: u32,
    pub bot_count: u32,
    pub connected_count: u32,
    pub ready_at_unix_nanos: u64,
}

/// OrderSentEvent records one outbound order timestamp from the worker.
/// Events are batched before publishing to avoid per-order Kafka writes.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OrderSentEvent {
    pub session_id: Arc<str>,
    pub submission_id: Arc<str>,
    pub worker_id: Arc<str>,
    pub bot_id: u64,
    pub order_id: String,
    pub send_ts_ns: u64,
    pub payload_type: PayloadType,
    pub price: u64,
    pub qty: u64,
    pub side: Side,
    pub protocol: Protocol,
}

/// WorkloadFailedEvent is published when a worker cannot complete an assignment.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkloadFailedEvent {
    pub session_id: String,
    pub submission_id: String,
    pub worker_id: String,
    pub worker_index: u32,
    pub reason: String,
    pub failed_at_unix_nanos: u64,
}

/// OrderSentBatch is MessagePack-encoded on "orders.sent".
/// Batching keeps Kafka traffic proportional to flush rate instead of order rate.
///
/// Note: session_id, submission_id, and worker_id are duplicated in the event envelope
/// so that individual events remain self-contained for downstream analytical pipelines.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OrderSentBatch {
    pub session_id: String,
    pub worker_id: String,
    pub events: Vec<OrderSentEvent>,
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

/// default_profile_mix matches the standard benchmark profile distribution.
fn default_profile_mix() -> Vec<BotProfileWeight> {
    vec![
        BotProfileWeight {
            profile: BotProfile::MarketMaker,
            weight: 40,
        },
        BotProfileWeight {
            profile: BotProfile::AggressiveTaker,
            weight: 40,
        },
        BotProfileWeight {
            profile: BotProfile::Canceller,
            weight: 20,
        },
    ]
}
