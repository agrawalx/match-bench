//! This module implements kafka behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};

/// build_consumer performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn build_consumer(brokers: &str, group: &str, topics: &[&str]) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "1000")
        .set("auto.offset.reset", "latest")
        .set("fetch.wait.max.ms", "100")
        .set("session.timeout.ms", "10000")
        .set("max.poll.interval.ms", "300000")
        .create()
        .context("create telemetry kafka consumer")?;
    consumer
        .subscribe(topics)
        .context("subscribe telemetry topics")?;
    Ok(consumer)
}
