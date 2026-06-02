//! Telemetry ingester: consumes the bot-side `orders.sent` and the eBPF-side
//! `orders.acked` streams into per-`(session, wave)` latency aggregates and
//! snapshots them to TimescaleDB + Redis once a second.

pub mod aggregate;
pub mod config;
pub mod ingester;
pub mod join;
pub mod kafka;
pub mod redis_sink;
pub mod store;
