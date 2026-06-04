use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use iicpc_bot_fleet::kafka;
use iicpc_schemas_rust::{TOPIC_BOT_READY, TOPIC_ORDERS_SENT};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct TestEvent {
    id: String,
    value: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_integration_round_trips_control_and_telemetry_records() -> Result<()> {
    let Some(brokers) = brokers() else {
        eprintln!("skipping real Kafka integration test: KAFKA_BROKERS is not set");
        return Ok(());
    };

    let suffix = suffix();
    let group = format!("itest.bot-fleet.kafka.group.{suffix}");
    kafka::ensure_topics(&brokers, &[TOPIC_BOT_READY, TOPIC_ORDERS_SENT])
        .await
        .context("ensure real Kafka contract topics")?;

    let consumer = kafka::consumer(
        &brokers,
        &group,
        &[TOPIC_BOT_READY, TOPIC_ORDERS_SENT],
        std::time::Duration::from_secs(300),
    )
    .context("create consumer")?;
    let control = kafka::control_producer(&brokers).context("create control producer")?;
    let telemetry = kafka::telemetry_producer(&brokers).context("create telemetry producer")?;

    let control_key = format!("control-{suffix}");
    let control_event = TestEvent {
        id: control_key.clone(),
        value: "ready".to_string(),
    };
    kafka::publish_json(&control, TOPIC_BOT_READY, &control_key, &control_event)
        .await
        .context("publish control json")?;

    let telemetry_key = format!("telemetry-{suffix}");
    let telemetry_payload = format!("orders.sent batch bytes {suffix}");
    kafka::publish_bytes(
        &telemetry,
        TOPIC_ORDERS_SENT,
        &telemetry_key,
        telemetry_payload.as_bytes(),
    )
    .await
    .context("publish telemetry bytes")?;

    let mut saw_control = false;
    let mut saw_telemetry = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !(saw_control && saw_telemetry) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("timed out waiting for real Kafka round-trip records");
        }
        let msg = tokio::time::timeout(remaining, kafka::recv_message(&consumer))
            .await
            .context("receive timeout")??;
        let Some(payload) = msg.payload.as_deref() else {
            kafka::commit_message(&consumer, &msg).context("commit tombstone")?;
            continue;
        };
        if !saw_control {
            if let Ok(event) = serde_json::from_slice::<TestEvent>(payload) {
                if event == control_event {
                    saw_control = true;
                    kafka::commit_message(&consumer, &msg).context("commit control")?;
                    continue;
                }
            }
        }
        if payload == telemetry_payload.as_bytes() {
            saw_telemetry = true;
        }
        kafka::commit_message(&consumer, &msg).context("commit message")?;
    }

    Ok(())
}

fn brokers() -> Option<String> {
    std::env::var("KAFKA_BROKERS")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}
