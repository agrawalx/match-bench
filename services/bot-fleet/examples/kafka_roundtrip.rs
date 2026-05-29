//! kafka_roundtrip — end-to-end verification that bot-fleet's rdkafka
//! producer + consumer can talk to a live broker. Produces 5 messages on a
//! dedicated test topic, consumes them back through the exact same kafka.rs
//! primitives the bot-fleet uses, and asserts every payload matches.
//!
//! Run via testing/08_rdkafka_roundtrip.sh (which pre-creates the topic).

use std::time::Duration;

use anyhow::{Context, Result};
use iicpc_bot_fleet::kafka;

const BROKERS: &str = "localhost:9092";
const TOPIC: &str = "kafka.smoke";
const GROUP: &str = "kafka.smoke.roundtrip";
const COUNT: usize = 5;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| BROKERS.to_string());

    // Set up consumer FIRST so its group offset is known before we publish.
    // Without this, "auto.offset.reset=earliest" still works because the
    // commit_message path advances offsets, but ordering this way matches
    // how production callers (worker.rs) do it.
    let consumer = kafka::consumer(&brokers, GROUP, &[TOPIC])
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
        // 10s timeout per message is generous; first read includes consumer
        // group join + offset reset, which can take a second or two.
        let payload = tokio::time::timeout(
            Duration::from_secs(10),
            kafka::recv_payload(&consumer),
        )
        .await
        .context("consume timeout")??
        .context("consumed a tombstone/empty message")?;

        let s = String::from_utf8(payload).context("payload was not utf8")?;
        println!("  <- {s}");
        got.push(s);
    }

    // Order is not asserted (Kafka guarantees within a partition; with 1
    // partition and one producer we'd get strict order, but the test only
    // needs to confirm the message *set* round-tripped).
    expected.sort();
    got.sort();
    assert_eq!(expected, got, "round-trip payload mismatch");
    println!("\nOK — {COUNT}/{COUNT} messages round-tripped through rdkafka");

    Ok(())
}

/// chrono_like_ts gives a millisecond-precision wall-clock stamp without
/// pulling chrono in just for this example.
fn chrono_like_ts() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
