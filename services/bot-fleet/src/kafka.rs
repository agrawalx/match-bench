use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use rdkafka::{
    config::ClientConfig,
    consumer::{BaseConsumer, CommitMode, Consumer, StreamConsumer},
    message::Message,
    producer::{FutureProducer, FutureRecord, Producer},
    Offset, TopicPartitionList,
};
use tracing::{debug, warn};

use iicpc_schemas_rust::{BarrierEvent, ReadySignal};

/// KafkaProducer wraps rdkafka's async FutureProducer.
#[derive(Clone)]
pub struct KafkaProducer {
    inner: FutureProducer,
}

/// KafkaConsumer wraps rdkafka's async StreamConsumer inside an Arc.
#[derive(Clone)]
pub struct KafkaConsumer {
    inner: Arc<StreamConsumer>,
}

/// KafkaMessage is the payload plus enough offset metadata to commit it only
/// after the caller has decided how the message was handled.
pub struct KafkaMessage {
    pub payload: Option<Vec<u8>>,
    topic: String,
    partition: i32,
    offset: i64,
}

/// ensure_topics asks Kafka for metadata for each configured topic so startup
/// fails fast when explicit topic initialization has not run.
pub fn ensure_topics(brokers: &str, topics: &[&str]) -> Result<()> {
    let client: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("create temp metadata consumer")?;

    for topic in topics {
        let metadata = client
            .fetch_metadata(Some(topic), Duration::from_secs(5))
            .with_context(|| format!("fetch kafka metadata for topic '{topic}'"))?;
        let exists = metadata.topics().iter().any(|t| t.name() == *topic);
        if !exists {
            anyhow::bail!("kafka topic '{topic}' does not exist");
        }
    }
    Ok(())
}

/// producer creates a Kafka producer for the configured broker list.
/// Messages require all in-sync replicas to ack and use a bounded ack timeout.
pub fn producer(brokers: &str) -> Result<KafkaProducer> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("acks", "all")
        .set("queue.buffering.max.ms", "5")
        .set("batch.size", "65536") // 64KB
        .set("compression.type", "lz4")
        .set("request.timeout.ms", "5000")
        .create()
        .context("create kafka producer")?;

    Ok(KafkaProducer { inner: producer })
}

pub fn consumer(brokers: &str, group: &str, topics: &[&str]) -> Result<KafkaConsumer> {
    consumer_with_fallback(brokers, group, topics, "latest")
}

/// workload_consumer creates the assignment consumer. New groups replay retained
/// assignments so scale-up races do not drop single-shot controller messages.
pub fn workload_consumer(brokers: &str, group: &str, topics: &[&str]) -> Result<KafkaConsumer> {
    consumer_with_fallback(brokers, group, topics, "earliest")
}

/// consumer_with_fallback creates a Kafka consumer group subscription for the
/// requested topics. Offsets are stored in Kafka only when explicitly committed
/// by the caller after it has handled the message.
fn consumer_with_fallback(
    brokers: &str,
    group: &str,
    topics: &[&str],
    fallback_offset: &str,
) -> Result<KafkaConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset_reset", fallback_offset)
        .create()
        .context("create kafka consumer")?;

    consumer
        .subscribe(topics)
        .context("subscribe to kafka topics")?;

    Ok(KafkaConsumer {
        inner: Arc::new(consumer),
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
///
/// The timeout bounds how long rdkafka may wait to enqueue into its internal
/// producer queue. It is not an end-to-end broker delivery timeout, so callers
/// on hot paths should treat a slow/full producer queue as backpressure.
pub async fn publish_bytes(
    producer: &KafkaProducer,
    topic: &str,
    key: &str,
    payload: &[u8],
) -> Result<()> {
    let record = FutureRecord::to(topic).key(key).payload(payload);

    producer
        .inner
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(err, _)| {
            crate::errors::BotFleetError::KafkaError(format!("publish error: {err}"))
        })?;

    Ok(())
}

/// wait_for_barrier polls the barrier topic until the matching session event arrives.
/// Malformed or unrelated messages are ignored until the timeout expires.
pub async fn wait_for_barrier(
    consumer: &KafkaConsumer,
    session_id: &str,
    wait_timeout: Duration,
) -> Result<BarrierEvent> {
    let deadline = tokio::time::Instant::now() + wait_timeout;

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            anyhow::bail!("timed out waiting for barrier");
        }
        let time_remaining = deadline - now;

        let message_res = tokio::time::timeout(time_remaining, recv_message(consumer)).await;
        let message = match message_res {
            Ok(Ok(message)) => message,
            Ok(Err(err)) => return Err(err),
            Err(_) => anyhow::bail!("timed out waiting for barrier"),
        };
        let Some(payload) = message.payload.as_deref() else {
            warn!("skipping null barrier message");
            commit_message(consumer, &message).context("commit null barrier message")?;
            continue;
        };

        let event: BarrierEvent = match serde_json::from_slice(payload) {
            Ok(event) => event,
            Err(err) => {
                let prefix = String::from_utf8_lossy(&payload[..payload.len().min(256)]);
                warn!(error = %err, payload_prefix = %prefix, "skipping malformed barrier message");
                commit_message(consumer, &message).context("commit malformed barrier message")?;
                continue;
            }
        };
        if event.session_id == session_id {
            commit_message(consumer, &message).context("commit matching barrier message")?;
            return Ok(event);
        }
        debug!(
            expected_session_id = session_id,
            actual_session_id = %event.session_id,
            "skipping unrelated barrier message"
        );
        commit_message(consumer, &message).context("commit unrelated barrier message")?;
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

/// recv_message polls one Kafka message without committing its offset.
pub async fn recv_message(consumer: &KafkaConsumer) -> Result<KafkaMessage> {
    match consumer.inner.recv().await {
        Ok(msg) => Ok(KafkaMessage {
            payload: msg.payload().map(|p| p.to_vec()),
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
        }),
        Err(err) => {
            anyhow::bail!("failed to receive message from kafka: {err}");
        }
    }
}

/// commit_message records that the consumed Kafka message has been fully
/// handled. Kafka commits the next offset, not the current message offset.
pub fn commit_message(consumer: &KafkaConsumer, message: &KafkaMessage) -> Result<()> {
    let mut offsets = TopicPartitionList::new();
    offsets
        .add_partition_offset(
            &message.topic,
            message.partition,
            Offset::Offset(message.offset + 1),
        )
        .context("build kafka commit offset")?;
    consumer
        .inner
        .commit(&offsets, CommitMode::Async)
        .context("commit kafka message offset")
}

/// flush_producer waits for rdkafka's internal queue to deliver enqueued
/// messages. Use this before treating final telemetry/control-plane publishes
/// as durable.
pub fn flush_producer(producer: &KafkaProducer, timeout: Duration) -> Result<()> {
    producer
        .inner
        .flush(timeout)
        .context("flush kafka producer")
}
