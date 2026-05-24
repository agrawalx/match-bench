use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BotProfile {
    MarketMaker,
    AggressiveTaker,
    Canceller,
}

/// BotProfileWeight assigns a relative share of bots to a profile.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BotProfileWeight {
    pub profile: BotProfile,
    pub weight: u32,
}

/// WorkloadSpec is published to "workload.assignments" by the test controller.
/// Each message is keyed by session_id:worker_index and consumed by one worker.
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
    pub bot_count: u32,
    pub orders_per_bot: u32,
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

/// BarrierEvent is published to "barrier" once all workers have reported ready.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BarrierEvent {
    pub session_id: String,
    pub target_epoch_unix_nanos: u64,
}

/// ReadySignal is published by each bot-fleet worker to "bot.ready".
/// It tells the controller how many local bots connected before the barrier.
#[derive(Debug, Clone, Deserialize, Serialize)]
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
#[derive(Debug, Clone, Serialize)]
pub struct OrderSentEvent {
    pub session_id: String,
    pub submission_id: String,
    pub worker_id: String,
    pub bot_id: u64,
    pub order_id: String,
    pub send_ts_ns: u64,
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
