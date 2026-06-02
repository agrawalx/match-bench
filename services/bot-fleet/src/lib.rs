//! iicpc-bot-fleet library surface.
//!
//! main.rs is the binary entrypoint; lib.rs re-exports the internal modules
//! so integration tests and examples (e.g. examples/kafka_roundtrip.rs) can
//! exercise the same code the binary runs. Nothing here is public-facing;
//! it exists strictly to let tests reach module internals without
//! duplicating them.

pub mod config;
pub mod content;
pub mod errors;
pub mod fix;
pub mod kafka;
pub mod metrics;
pub mod telemetry;
pub mod time;
pub mod worker;
