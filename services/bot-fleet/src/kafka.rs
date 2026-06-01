use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
    producer::{FutureProducer, FutureRecord},
    Offset, TopicPartitionList,
};

use iicpc_schemas_rust::{BarrierEvent, ReadySignal};

const TOPIC_REPLICATION_FACTOR: i32 = 3;
const DEFAULT_TOPIC_PARTITIONS: i32 = 3;
const HIGH_THROUGHPUT_TOPIC_PARTITIONS: i32 = 24;

/// KafkaProducer wraps rdkafka's FutureProducer. FutureProducer is already
/// Clone + Send + Sync, so we keep it as a thin newtype rather than wrapping
/// in Arc<Mutex<_>> — the kafka-rust era of locked-producer-behind-mutex is
/// gone with the migration to librdkafka.
#[derive(Clone)]
pub struct KafkaProducer {
    inner: FutureProducer,
}

/// KafkaConsumer holds a StreamConsumer. rdkafka's StreamConsumer pulls
/// messages asynchronously off librdkafka's internal poll loop; the bot-fleet
/// no longer pays for a tokio spawn_blocking per poll. Manual commit mode is
/// used so we keep at-least-once delivery semantics — commit fires only
/// after the caller has finished processing the message.
pub struct KafkaConsumer {
    inner: StreamConsumer,
}

/// KafkaMessage carries a consumed payload plus offset metadata. Callers must
/// commit it only after the message has been fully handled.
pub struct KafkaMessage {
    pub payload: Option<Vec<u8>>,
    topic: String,
    partition: i32,
    offset: i64,
}

/// ensure_topics creates each topic if it doesn't already exist.
///
/// Problem: the old fallback could create topics with one partition and RF=1,
/// which violates the schema-defined Kafka contract. Fix: mirror the explicit
/// topic-init partition, replication, ISR, retention, and message-size policy.
///
/// This is a developer fallback; production/local compose should provision the
/// same topics before services start.
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

    // create_topics returns errors per topic, including "already exists";
    // librdkafka surfaces them but does not fail the whole call. We don't
    // need to inspect them — the next operation (producer/consumer) will
    // fail loudly if a topic genuinely doesn't exist.
    Ok(())
}

fn topic_partitions(topic: &str) -> i32 {
    match topic {
        iicpc_schemas_rust::TOPIC_ORDERS_ACKED
        | iicpc_schemas_rust::TOPIC_ORDERS_SENT
        | iicpc_schemas_rust::TOPIC_WORKLOAD_ASSIGNMENTS => HIGH_THROUGHPUT_TOPIC_PARTITIONS,
        _ => DEFAULT_TOPIC_PARTITIONS,
    }
}

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

/// control_producer builds a FutureProducer tuned for control-plane messages
/// (ready signals on bot.ready). Per architecture §6.5, every control topic
/// (benchmark.requested, barrier, benchmark.status.updated, workload.assignments,
/// bot.ready) must publish with `acks=all` so that a leader failure between
/// ack and replication cannot silently drop the message. Tuning:
///
///   - `acks=all`: every in-sync replica acks. Costs ~ms of latency, gains
///     no-loss-on-leader-failure. Required for control plane.
///   - `enable.idempotence=true`: librdkafka attaches PID + sequence so a
///     retry after broker error doesn't duplicate. Free when acks=all.
///   - `linger.ms=0`: no batching. Ready signals are one-per-worker-per-session,
///     not a stream — coalescing buys nothing and adds wakeup latency.
///   - `delivery.timeout.ms=10000`: 10s upper bound. Higher than telemetry's
///     5s because retries with idempotent producer can take longer.
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

/// telemetry_producer builds a FutureProducer tuned for the orders.sent
/// stream. Telemetry is high-volume, loss-tolerant (rare drops show up as
/// gaps in the HDR histogram, not as wrong scores), so we prioritise
/// throughput over durability. Tuning:
///
///   - `acks=1`: leader-only ack. Rare loss is acceptable for telemetry.
///   - `linger.ms=2`: 2ms batching window coalesces bursts of small
///     OrderSentBatch messages into one TCP write.
///   - `compression.type=lz4`: cheap CPU, ~3x compression on JSON. lz4 is
///     faster than zstd at our message sizes (~200 bytes) and avoids the
///     zstd librdkafka feature dependency.
///   - `delivery.timeout.ms=5000`: 5s upper bound from send() to
///     delivery-failure. Matches the old kafka-rust ack_timeout.
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

/// producer is preserved as an alias for telemetry_producer to keep the
/// integration test (examples/bot_worker_fix_roundtrip.rs) and any other
/// historical callers compiling. New code should pick the named variant
/// explicitly.
pub fn producer(brokers: &str) -> Result<KafkaProducer> {
    telemetry_producer(brokers)
}

/// consumer builds a StreamConsumer subscribed to the requested topics under
/// the given consumer group. Tuning notes:
///
///   - `enable.auto.commit=false`: we commit explicitly after processing each
///     message so that on a worker crash the message is re-delivered. The
///     workload-assignment path is idempotent at the controller level (same
///     session_id is recognised), so re-delivery is safe.
///   - `auto.offset.reset=earliest`: new consumer groups start from the
///     beginning of the partition. Bot-workers do not exist before the
///     controller publishes a workload, so "earliest" effectively means
///     "the message that was just produced". Switching to `latest` would
///     mean the worker can miss a workload that landed before its consumer
///     attached.
///   - `session.timeout.ms=10000` keeps rebalances tight when KEDA scales
///     the worker pool.
pub fn consumer(brokers: &str, group: &str, topics: &[&str]) -> Result<KafkaConsumer> {
    let inner: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("fetch.min.bytes", "1")
        .set("fetch.wait.max.ms", "100")
        .set("max.poll.interval.ms", "300000")
        .set("session.timeout.ms", "10000")
        .create()
        .context("create kafka consumer")?;
    inner.subscribe(topics).context("subscribe to topics")?;
    Ok(KafkaConsumer { inner })
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

/// publish_bytes sends a pre-encoded payload through the FutureProducer.
/// Awaits delivery confirmation; returns the librdkafka error on failure.
pub async fn publish_bytes(
    producer: &KafkaProducer,
    topic: &str,
    key: &str,
    payload: &[u8],
) -> Result<()> {
    let record = FutureRecord::to(topic).key(key).payload(payload);
    // 5s queue timeout matches the producer's delivery.timeout.ms; if the
    // internal queue is full for longer than this, we surface an error
    // rather than blocking the caller indefinitely.
    producer
        .inner
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(err, _msg)| anyhow!("publish kafka message: {err}"))?;
    Ok(())
}

/// wait_for_barrier polls the barrier topic until the matching session
/// event arrives or the wait timeout expires. Malformed or unrelated
/// messages are dropped and the loop continues.
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

/// decode_workload decodes a workload assignment consumed from "workload.assignments".
pub fn decode_workload(payload: &[u8]) -> Result<iicpc_schemas_rust::WorkloadSpec> {
    serde_json::from_slice(payload).context("decode workload assignment")
}

/// ready_key builds the Kafka key used for per-worker ready signals.
pub fn ready_key(signal: &ReadySignal) -> String {
    format!("{}:{}", signal.session_id, signal.worker_id)
}

/// recv_message awaits the next message on the consumer's subscribed topics
/// without committing its offset. Call commit_message after the payload is
/// handled or intentionally discarded.
///
/// Cancellation is the caller's responsibility — wrap this call in a
/// `tokio::select!` or `tokio::time::timeout` to bound the wait.
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
        .commit(&offsets, CommitMode::Sync)
        .context("commit kafka offset")?;
    Ok(())
}

/// recv_payload is kept for examples that do not need manual commit control.
pub async fn recv_payload(consumer: &KafkaConsumer) -> Result<Option<Vec<u8>>> {
    let message = recv_message(consumer).await?;
    let payload = message.payload.clone();
    commit_message(consumer, &message)?;
    Ok(payload)
}
