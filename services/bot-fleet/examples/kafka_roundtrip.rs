//! This module implements kafka roundtrip behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::time::Duration;

use anyhow::{Context, Result};
use iicpc_bot_fleet::kafka;

const BROKERS: &str = "localhost:9092";
const TOPIC: &str = "kafka.smoke";
const GROUP: &str = "kafka.smoke.roundtrip";
const COUNT: usize = 5;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
/// main performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn main() -> Result<()> {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| BROKERS.to_string());

    let consumer = kafka::consumer(
        &brokers,
        GROUP,
        &[TOPIC],
        std::time::Duration::from_secs(300),
    )
    .context("create consumer")?;
    let producer = kafka::producer(&brokers).context("create producer")?;

    println!("Publishing {COUNT} messages to {TOPIC}");
    let mut expected: Vec<String> = Vec::with_capacity(COUNT);
    for i in 0..COUNT {
        let key = format!("k{i}");
        let payload = format!("roundtrip-payload-{i}-{}", chrono_like_ts());
        expected.push(payload.clone());
        kafka::publish_bytes(&producer, TOPIC, &key, payload.as_bytes())
            .await
            .with_context(|| format!("publish msg {i}"))?;
        println!("  -> {key}={payload}");
    }

    println!("\nConsuming back from {TOPIC}");
    let mut got: Vec<String> = Vec::with_capacity(COUNT);
    while got.len() < COUNT {
        let payload = tokio::time::timeout(Duration::from_secs(10), kafka::recv_payload(&consumer))
            .await
            .context("consume timeout")??
            .context("consumed a tombstone/empty message")?;

        let s = String::from_utf8(payload).context("payload was not utf8")?;
        println!("  <- {s}");
        got.push(s);
    }

    expected.sort();
    got.sort();
    assert_eq!(expected, got, "round-trip payload mismatch");
    println!("\nOK — {COUNT}/{COUNT} messages round-tripped through rdkafka");

    Ok(())
}

/// chrono_like_ts performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn chrono_like_ts() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
