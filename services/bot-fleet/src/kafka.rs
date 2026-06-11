//! This module implements kafka behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
    producer::{FutureProducer, FutureRecord, Producer},
    Offset, TopicPartitionList,
};

use iicpc_schemas_rust::{BarrierEvent, ReadySignal};

const TOPIC_REPLICATION_FACTOR: i32 = 3;
const DEFAULT_TOPIC_PARTITIONS: i32 = 3;
const HIGH_THROUGHPUT_TOPIC_PARTITIONS: i32 = 24;

#[derive(Clone)]
/// KafkaProducer stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct KafkaProducer {
    inner: FutureProducer,
}

/// KafkaConsumer stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct KafkaConsumer {
    inner: StreamConsumer,
}

/// KafkaMessage stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct KafkaMessage {
    pub payload: Option<Vec<u8>>,
    topic: String,
    partition: i32,
    offset: i64,
}

/// ensure_topics performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub async fn ensure_topics(brokers: &str, topics: &[&str]) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("create kafka admin client")?;

    let new_topics: Vec<NewTopic> = topics
        .iter()
        .map(|topic| {
            NewTopic::new(
                topic,
                topic_partitions(topic),
                TopicReplication::Fixed(TOPIC_REPLICATION_FACTOR),
            )
            .set("min.insync.replicas", "2")
            .set("retention.ms", topic_retention_ms(topic))
            .set("max.message.bytes", "1048576")
        })
        .collect();

    admin
        .create_topics(&new_topics, &AdminOptions::new())
        .await
        .context("create topics")?;

    Ok(())
}

/// topic_partitions performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn topic_partitions(topic: &str) -> i32 {
    match topic {
        iicpc_schemas_rust::TOPIC_ORDERS_ACKED
        | iicpc_schemas_rust::TOPIC_ORDERS_SENT
        | iicpc_schemas_rust::TOPIC_WORKLOAD_ASSIGNMENTS => HIGH_THROUGHPUT_TOPIC_PARTITIONS,
        _ => DEFAULT_TOPIC_PARTITIONS,
    }
}

/// topic_retention_ms performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn topic_retention_ms(topic: &str) -> &'static str {
    match topic {
        iicpc_schemas_rust::TOPIC_ORDERS_ACKED
        | iicpc_schemas_rust::TOPIC_ORDERS_SENT
        | iicpc_schemas_rust::TOPIC_WORKLOAD_ASSIGNMENTS
        | iicpc_schemas_rust::TOPIC_BARRIER
        | iicpc_schemas_rust::TOPIC_BOT_READY => "86400000",
        iicpc_schemas_rust::TOPIC_SCORES_CORRECTNESS => "2592000000",
        _ => "604800000",
    }
}

/// control_producer performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn control_producer(brokers: &str) -> Result<KafkaProducer> {
    let inner: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("linger.ms", "0")
        .set("retries", "2147483647")
        .set("retry.backoff.ms", "100")
        .set("delivery.timeout.ms", "10000")
        .create()
        .context("create kafka control producer")?;
    Ok(KafkaProducer { inner })
}

/// telemetry_producer performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn telemetry_producer(brokers: &str) -> Result<KafkaProducer> {
    let inner: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("acks", "1")
        .set("linger.ms", "2")
        .set("compression.type", "lz4")
        .set("retries", "3")
        .set("retry.backoff.ms", "25")
        .set("delivery.timeout.ms", "5000")
        .create()
        .context("create kafka telemetry producer")?;
    Ok(KafkaProducer { inner })
}

/// producer performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn producer(brokers: &str) -> Result<KafkaProducer> {
    telemetry_producer(brokers)
}

/// consumer performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn consumer(
    brokers: &str,
    group: &str,
    topics: &[&str],
    max_poll_interval: Duration,
) -> Result<KafkaConsumer> {
    let inner: StreamConsumer = consumer_client_config(brokers, group, max_poll_interval)
        .create()
        .context("create kafka consumer")?;
    inner.subscribe(topics).context("subscribe to topics")?;
    Ok(KafkaConsumer { inner })
}

/// consumer_client_config performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn consumer_client_config(brokers: &str, group: &str, max_poll_interval: Duration) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("fetch.min.bytes", "1")
        .set("fetch.wait.max.ms", "100")
        .set(
            "max.poll.interval.ms",
            max_poll_interval.as_millis().to_string(),
        )
        .set("session.timeout.ms", "10000")
        .set("partition.assignment.strategy", "roundrobin");
    config
}

/// publish_json performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub async fn publish_json<T: serde::Serialize>(
    producer: &KafkaProducer,
    topic: &str,
    key: &str,
    value: &T,
) -> Result<()> {
    let payload = serde_json::to_vec(value).context("serialize kafka json payload")?;
    publish_bytes(producer, topic, key, &payload).await
}

/// publish_bytes performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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
        .map_err(|(err, _msg)| anyhow!("publish kafka message: {err}"))?;
    Ok(())
}

pub fn topic_partition_count(producer: &KafkaProducer, topic: &str) -> Option<i32> {
    let md = producer
        .inner
        .client()
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .ok()?;
    let n = md
        .topics()
        .iter()
        .find(|t| t.name() == topic)?
        .partitions()
        .len();
    (n > 0).then_some(n as i32)
}

pub async fn publish_to_partition(
    producer: &KafkaProducer,
    topic: &str,
    partition: i32,
    key: &str,
    payload: &[u8],
) -> Result<()> {
    let record = FutureRecord::to(topic)
        .key(key)
        .payload(payload)
        .partition(partition);
    producer
        .inner
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(err, _msg)| anyhow!("publish kafka message to partition {partition}: {err}"))?;
    Ok(())
}

pub async fn wait_for_barrier(
    consumer: &KafkaConsumer,
    session_id: &str,
    wait_timeout: Duration,
) -> Result<BarrierEvent> {
    let deadline = tokio::time::Instant::now() + wait_timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("timed out waiting for barrier");
        }

        let message = match tokio::time::timeout(remaining, recv_message(consumer)).await {
            Ok(result) => result?,
            Err(_) => anyhow::bail!("timed out waiting for barrier"),
        };
        let Some(payload) = message.payload.as_deref() else {
            commit_message(consumer, &message).context("commit null barrier message")?;
            continue;
        };

        let event: BarrierEvent = match serde_json::from_slice(payload) {
            Ok(event) => event,
            Err(_) => {
                commit_message(consumer, &message).context("commit malformed barrier message")?;
                continue;
            }
        };
        if event.session_id == session_id {
            commit_message(consumer, &message).context("commit matching barrier message")?;
            return Ok(event);
        }
        commit_message(consumer, &message).context("commit unrelated barrier message")?;
    }
}

/// decode_workload performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn decode_workload(payload: &[u8]) -> Result<iicpc_schemas_rust::WorkloadSpec> {
    serde_json::from_slice(payload).context("decode workload assignment")
}

/// ready_key performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn ready_key(signal: &ReadySignal) -> String {
    format!("{}:{}", signal.session_id, signal.worker_id)
}

/// recv_message performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub async fn recv_message(consumer: &KafkaConsumer) -> Result<KafkaMessage> {
    let mut stream = consumer.inner.stream();
    let msg = stream
        .next()
        .await
        .ok_or_else(|| anyhow!("consumer stream ended unexpectedly"))?
        .context("read message")?;

    Ok(KafkaMessage {
        payload: msg.payload().map(|b| b.to_vec()),
        topic: msg.topic().to_string(),
        partition: msg.partition(),
        offset: msg.offset(),
    })
}

/// commit_message performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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
        .commit(&offsets, CommitMode::Sync)
        .context("commit kafka offset")?;
    Ok(())
}

/// recv_payload performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub async fn recv_payload(consumer: &KafkaConsumer) -> Result<Option<Vec<u8>>> {
    let message = recv_message(consumer).await?;
    let payload = message.payload.clone();
    commit_message(consumer, &message)?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// consumer_config_spreads_partitions_roundrobin performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn consumer_config_spreads_partitions_roundrobin() {
        let config = consumer_client_config(
            "localhost:9092",
            "bot-fleet",
            Duration::from_millis(1_800_000),
        );
        assert_eq!(
            config.get("partition.assignment.strategy"),
            Some("roundrobin")
        );
        assert_eq!(config.get("max.poll.interval.ms"), Some("1800000"));
        assert_eq!(config.get("enable.auto.commit"), Some("false"));
        assert_eq!(config.get("auto.offset.reset"), Some("earliest"));
    }
}
