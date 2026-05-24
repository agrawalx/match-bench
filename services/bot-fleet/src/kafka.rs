use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use kafka::{
    consumer::{Consumer, FetchOffset, GroupOffsetStorage},
    producer::{Producer, Record, RequiredAcks},
};

use iicpc_schemas_rust::{BarrierEvent, ReadySignal};

/// KafkaProducer wraps the blocking kafka crate producer behind a mutex so
/// async tasks can publish through spawn_blocking without sharing it unsafely.
#[derive(Clone)]
pub struct KafkaProducer {
    inner: Arc<Mutex<Producer>>,
}

/// KafkaConsumer wraps the blocking kafka crate consumer behind a mutex so one
/// async poll path owns offset advancement for a subscribed topic set.
#[derive(Clone)]
pub struct KafkaConsumer {
    inner: Arc<Mutex<Consumer>>,
}

/// producer creates a Kafka producer for the configured broker list.
/// Messages require one broker ack and use a bounded ack timeout.
pub fn producer(brokers: &str) -> Result<KafkaProducer> {
    let producer = Producer::from_hosts(parse_brokers(brokers))
        .with_required_acks(RequiredAcks::One)
        .with_ack_timeout(Duration::from_secs(5))
        .create()
        .context("create kafka producer")?;
    Ok(KafkaProducer {
        inner: Arc::new(Mutex::new(producer)),
    })
}

/// consumer creates a Kafka consumer group subscription for the requested topics.
/// Offsets are stored in Kafka and new groups start at the latest offset.
pub fn consumer(brokers: &str, group: &str, topics: &[&str]) -> Result<KafkaConsumer> {
    let mut builder = Consumer::from_hosts(parse_brokers(brokers))
        .with_group(group.to_string())
        .with_fallback_offset(FetchOffset::Latest)
        .with_offset_storage(Some(GroupOffsetStorage::Kafka));

    for topic in topics {
        builder = builder.with_topic((*topic).to_string());
    }

    let consumer = builder.create().context("create kafka consumer")?;
    Ok(KafkaConsumer {
        inner: Arc::new(Mutex::new(consumer)),
    })
}

/// publish_json serializes a value as JSON and publishes it with the provided key.
/// Used for control-plane messages such as ready signals.
pub async fn publish_json<T: serde::Serialize>(
    producer: &KafkaProducer,
    topic: &str,
    key: &str,
    value: &T,
) -> Result<()> {
    let payload = serde_json::to_vec(value).context("serialize kafka json payload")?;
    publish_bytes(producer, topic, key, &payload).await
}

/// publish_bytes sends a pre-encoded Kafka payload from an async context.
/// The blocking producer call is isolated on Tokio's blocking thread pool.
pub async fn publish_bytes(
    producer: &KafkaProducer,
    topic: &str,
    key: &str,
    payload: &[u8],
) -> Result<()> {
    let producer = producer.inner.clone();
    let topic = topic.to_string();
    let key = key.as_bytes().to_vec();
    let payload = payload.to_vec();

    tokio::task::spawn_blocking(move || {
        let mut producer = producer
            .lock()
            .map_err(|err| crate::errors::BotFleetError::KafkaError(format!("kafka producer mutex poisoned: {err}")))?;
        producer
            .send(&Record::from_key_value(&topic, key, payload))
            .context("publish kafka message")
    })
    .await
    .context("join kafka producer task")?
}

/// wait_for_barrier polls the barrier topic until the matching session event arrives.
/// Malformed or unrelated messages are ignored until the timeout expires.
pub async fn wait_for_barrier(
    consumer: &KafkaConsumer,
    session_id: &str,
    wait_timeout: Duration,
) -> Result<BarrierEvent> {
    let deadline = std::time::Instant::now() + wait_timeout;

    loop {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for barrier");
        }

        let Some(payload) = recv_payload(consumer).await? else {
            continue;
        };
        let event: BarrierEvent = match serde_json::from_slice(&payload) {
            Ok(event) => event,
            Err(_) => continue,
        };
        if event.session_id == session_id {
            return Ok(event);
        }
    }
}

/// decode_workload decodes a workload assignment consumed from "workload.assignments".
pub fn decode_workload(payload: &[u8]) -> Result<iicpc_schemas_rust::WorkloadSpec> {
    serde_json::from_slice(payload).context("decode workload assignment")
}

/// ready_key builds the Kafka key used for per-worker ready signals.
pub fn ready_key(signal: &ReadySignal) -> String {
    format!("{}:{}", signal.session_id, signal.worker_id)
}

/// recv_payload polls one Kafka message and commits only that message's offset.
/// Returns None when no message is currently available.
pub async fn recv_payload(consumer: &KafkaConsumer) -> Result<Option<Vec<u8>>> {
    let consumer = consumer.inner.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut consumer = consumer
            .lock()
            .map_err(|err| crate::errors::BotFleetError::KafkaError(format!("kafka consumer mutex poisoned: {err}")))?;
        let sets = consumer.poll().context("poll kafka consumer")?;

        for set in sets.iter() {
            if let Some(message) = set.messages().first() {
                let payload = message.value.to_vec();
                consumer
                    .consume_message(set.topic(), set.partition(), message.offset)
                    .context("consume kafka message")?;
                consumer.commit_consumed().context("commit kafka offset")?;
                return Ok(Some(payload));
            }
        }

        // thread::sleep blocked the Tokio blocking thread
        // pool; yield back immediately and let the async caller sleep instead
        Ok(None)
    })
    .await
    .context("join kafka consumer task")?;

    // sleep in async context instead of blocking a
    // thread pool thread, preventing pool starvation under concurrent polls
    if matches!(&result, Ok(None)) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    result
}

/// parse_brokers normalizes a comma-separated broker list into kafka crate hosts.
fn parse_brokers(brokers: &str) -> Vec<String> {
    brokers
        .split(',')
        .map(str::trim)
        .filter(|broker| !broker.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}
