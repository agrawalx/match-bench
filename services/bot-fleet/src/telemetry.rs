use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::{
    sync::mpsc::{self, error::TrySendError, Sender},
    task::JoinHandle,
    time,
};
use tracing::{error, warn};

use iicpc_schemas_rust::{partition_for, OrderSentEvent};

use crate::{
    kafka::{self, KafkaProducer},
    metrics,
};

/// TelemetrySink accepts per-order telemetry from bot tasks and forwards it to
/// one background aggregator for batched Kafka publishing.
#[derive(Clone)]
pub struct TelemetrySink {
    tx: Sender<OrderSentEvent>,
    handle: Arc<Mutex<Option<JoinHandle<Result<()>>>>>,
}

impl TelemetrySink {
    /// new starts the background telemetry aggregator for one workload session.
    /// The channel capacity and flush cadence come from Config.
    pub fn new(
        producer: KafkaProducer,
        topic: String,
        session_id: String,
        worker_id: String,
        capacity: usize,
        flush_interval: Duration,
        batch_size: usize,
        num_partitions: i32,
    ) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        let handle = tokio::spawn(run_aggregator(
            rx,
            producer,
            topic,
            session_id,
            worker_id,
            flush_interval,
            batch_size,
            num_partitions,
        ));
        Self {
            tx,
            handle: Arc::new(Mutex::new(Some(handle))),
        }
    }

    /// record queues one outbound order timestamp without blocking bot writes.
    /// Events are dropped only when the bounded channel is full.
    pub async fn record(&self, event: OrderSentEvent) {
        match self.tx.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                metrics::telemetry_dropped();
                warn!("dropping telemetry event because channel is full");
            }
            Err(TrySendError::Closed(_)) => {
                metrics::telemetry_dropped();
                error!("dropping telemetry event because aggregator channel is closed");
            }
        }
    }

    /// close drops the final sender and waits for the aggregator to flush.
    /// Telemetry is loss-tolerant; publish failures are logged by the
    /// aggregator and must not block workload offset commits.
    pub async fn close(self) -> Result<()> {
        drop(self.tx);

        let handle = self
            .handle
            .lock()
            .map_err(|err| {
                crate::errors::BotFleetError::TelemetryError(format!(
                    "telemetry join handle mutex poisoned: {err}"
                ))
            })?
            .take();

        if let Some(handle) = handle {
            handle.await.context("join telemetry aggregator")??;
        }

        Ok(())
    }
}

/// run_aggregator drains telemetry events into batches and publishes them by
/// size or interval, whichever arrives first.
async fn run_aggregator(
    mut rx: mpsc::Receiver<OrderSentEvent>,
    producer: KafkaProducer,
    topic: String,
    session_id: String,
    worker_id: String,
    flush_interval: Duration,
    batch_size: usize,
    num_partitions: i32,
) -> Result<()> {
    let mut ticker = time::interval(flush_interval);
    let mut events = Vec::with_capacity(batch_size);

    loop {
        // biased select ensures recv is drained before
        // timer flushes, preventing stale partial batches under high throughput
        tokio::select! {
            biased;
            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        events.push(event);
                        if events.len() >= batch_size {
                            if let Err(err) = flush(&producer, &topic, &session_id, &worker_id, num_partitions, &mut events).await {
                                error!(error = %err, "failed to flush telemetry batch");
                            }
                        }
                    }
                    None => {
                        if let Err(err) = flush(&producer, &topic, &session_id, &worker_id, num_partitions, &mut events).await {
                            error!(error = %err, "failed to flush final telemetry batch");
                            metrics::telemetry_dropped();
                            events.clear();
                        }
                        return Ok(());
                    }
                }
            }
            _ = ticker.tick() => {
                if let Err(err) = flush(&producer, &topic, &session_id, &worker_id, num_partitions, &mut events).await {
                    error!(error = %err, "failed to flush telemetry batch");
                }
            }
        }
    }
}

/// OrderSentBatchRef mirrors OrderSentBatch for zero-copy encoding of a chunk
/// of the aggregator's event buffer.
#[derive(Serialize)]
struct OrderSentBatchRef<'a> {
    session_id: &'a str,
    worker_id: &'a str,
    events: &'a [OrderSentEvent],
}

/// flush shards the batch by destination partition and publishes one sub-batch
/// per partition to its EXPLICIT partition. partition_for(order_id) is the same
/// hash the eBPF capture uses for orders.acked, so an order's sent event lands on
/// the same partition as its acked event — the co-partitioning a multi-replica
/// telemetry-ingester needs to join sent⋈acked on a single consumer.
///
/// Within a partition, events are split into size-bounded chunks (one Kafka
/// message each, under max.message.bytes). Every publish is PIPELINED (join_all)
/// so a flush pays ≈ one broker RTT, not one per chunk.
///
/// Failure semantics: events whose publish failed are pushed back into `events`
/// (retained for the next flush); published events are dropped. The first error
/// is returned after reconciliation.
/// shard_events groups events by destination partition via partition_for(order_id),
/// moving them out of the input buffer. The eBPF capture shards orders.acked by the
/// same hash, so an order's sent and acked events land on the same partition.
/// Exposed for unit-testing the co-partition invariant.
fn shard_events(
    events: &mut Vec<OrderSentEvent>,
    num_partitions: i32,
) -> BTreeMap<i32, Vec<OrderSentEvent>> {
    let mut by_part: BTreeMap<i32, Vec<OrderSentEvent>> = BTreeMap::new();
    for e in events.drain(..) {
        by_part
            .entry(partition_for(&e.order_id, num_partitions))
            .or_default()
            .push(e);
    }
    by_part
}

async fn flush(
    producer: &KafkaProducer,
    topic: &str,
    session_id: &str,
    worker_id: &str,
    num_partitions: i32,
    events: &mut Vec<OrderSentEvent>,
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    // Shard by destination partition (move events out — no clone).
    let by_part = shard_events(events, num_partitions);

    // One message per (partition, size-bounded chunk); each owns its events so a
    // failed publish can be retained without re-encoding.
    let mut msgs: Vec<(i32, Vec<u8>, Vec<OrderSentEvent>)> = Vec::new();
    for (part, mut group) in by_part {
        while !group.is_empty() {
            let take = group.len().min(MAX_EVENTS_PER_BATCH);
            let chunk: Vec<OrderSentEvent> = group.drain(..take).collect();
            let payload = rmp_serde::to_vec_named(&OrderSentBatchRef {
                session_id,
                worker_id,
                events: &chunk,
            })
            .context("encode orders.sent messagepack")?;
            msgs.push((part, payload, chunk));
        }
    }

    // Pipeline every publish, then await together (≈ one broker RTT).
    let results = futures::future::join_all(
        msgs.iter().map(|(part, payload, _)| {
            kafka::publish_to_partition(producer, topic, *part, session_id, payload)
        }),
    )
    .await;

    let mut first_err = None;
    for ((_, _, chunk), result) in msgs.into_iter().zip(results) {
        match result {
            Ok(()) => metrics::telemetry_flushed(chunk.len()),
            Err(err) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
                events.extend(chunk); // retain for next flush
            }
        }
    }
    match first_err {
        None => Ok(()),
        Some(err) => Err(err),
    }
}

/// Max events per published orders.sent Kafka message. Size math:
/// msgpack-named events repeat field names, ≈360 B/event at realistic
/// identifier sizes, so 1000 events ≈ 360 KB — comfortably under the topic's
/// 1 MiB max.message.bytes (topic-init / kafka.rs ensure_topics both set
/// max.message.bytes=1048576) with headroom for larger identifiers. Kept
/// equal to Config::telemetry_batch_size so a steady-state flush is exactly
/// one Kafka message.
pub(crate) const MAX_EVENTS_PER_BATCH: usize = 1000;

#[cfg(test)]
mod tests {
    use super::*;
    use iicpc_schemas_rust::{OrdType, OrderSentBatch, PayloadType, Side};

    /// test_event builds an event with production-realistic identifier sizes
    /// (UUID session/submission ids, k8s pod-name worker id) so the encoded
    /// payload size assertions track what the broker actually sees.
    fn test_event(i: usize) -> OrderSentEvent {
        OrderSentEvent {
            session_id: "01890dd2-71f3-7abc-9def-0123456789ab".to_string(),
            submission_id: "01890dd2-71f3-7abc-9def-0123456789ac".to_string(),
            worker_id: "bot-fleet-7d9f8c6b5-x2k4j".to_string(),
            task_id: 42,
            order_id: format!("01890dd2-71f3-7abc-9def-0123456789ab_42_{i}_O"),
            target_send_ts_ns: 1_770_000_000_000_000_000 + i as u64,
            send_ts_ns: 1_770_000_000_000_000_100 + i as u64,
            recv_done_ts_ns: 1_770_000_000_000_000_900 + i as u64,
            timed_out: false,
            price: 10_000,
            qty: 25,
            side: Side::Buy,
            payload_type: PayloadType::New,
            ord_type: OrdType::Limit,
            orig_order_id: String::new(),
            barrier_epoch_ns: 1_770_000_000_000_000_000,
        }
    }

    // Co-partition invariant: every event in group[p] hashes to p, and sharding
    // preserves all events. This is what guarantees an order's sent batch and (via
    // the same partition_for hash on the eBPF side) its acked batch land together.
    #[test]
    fn shard_events_groups_every_event_to_its_partition() {
        let n = 24;
        let mut events: Vec<OrderSentEvent> = (0..5_000).map(test_event).collect();
        let total = events.len();

        let by_part = shard_events(&mut events, n);

        assert!(events.is_empty(), "shard_events must drain the input buffer");
        let regrouped: usize = by_part.values().map(Vec::len).sum();
        assert_eq!(regrouped, total, "no events lost in sharding");
        for (part, group) in &by_part {
            for e in group {
                assert_eq!(
                    partition_for(&e.order_id, n),
                    *part,
                    "event {} placed in wrong partition",
                    e.order_id
                );
            }
        }
        assert!(by_part.len() > 1, "events should spread across partitions");
    }

    // A per-task order_id stream (all same order_id prefix differing by seq) must
    // still spread across partitions — confirms we shard by full order_id, not by a
    // coarse prefix that would funnel a session to one partition.
    #[test]
    fn shard_events_spreads_a_single_session_across_partitions() {
        let n = 24;
        let mut events: Vec<OrderSentEvent> = (0..2_000).map(test_event).collect();
        let by_part = shard_events(&mut events, n);
        assert!(
            by_part.len() >= n as usize / 2,
            "a busy session must use many partitions, used {}",
            by_part.len()
        );
    }

    // Size math behind the 1000-event ceiling: msgpack-named events repeat field
    // names, ≈360 B/event at realistic identifier sizes, so a full chunk is ≈360 KB
    // — comfortably under the topic's 1 MiB max.message.bytes.
    #[test]
    fn full_chunk_stays_under_broker_message_ceiling() {
        let events: Vec<OrderSentEvent> = (0..MAX_EVENTS_PER_BATCH).map(test_event).collect();
        let payload = rmp_serde::to_vec_named(&OrderSentBatchRef {
            session_id: "sess",
            worker_id: "worker",
            events: &events,
        })
        .expect("encode chunk");

        let decoded: OrderSentBatch = rmp_serde::from_slice(&payload).expect("decode chunk");
        assert_eq!(decoded.events.len(), MAX_EVENTS_PER_BATCH);
        assert!(
            payload.len() < 1_048_576,
            "full chunk is {} bytes, must stay under 1 MiB",
            payload.len()
        );
    }
}
