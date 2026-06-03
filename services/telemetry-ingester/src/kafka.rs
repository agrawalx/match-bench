//! Kafka consumer for the two telemetry streams.
//!
//! Telemetry is loss-tolerant (the producers use acks=1), so unlike the
//! control-plane consumers this one uses librdkafka auto-commit and starts at
//! `latest` — on restart we resume with live data rather than reprocessing a
//! session's history into the time-series.

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};

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
