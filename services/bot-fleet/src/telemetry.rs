//! This module implements telemetry behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use tokio::{
    sync::mpsc::{self, Sender},
    task::JoinHandle,
    time,
};
use tracing::error;

use iicpc_schemas_rust::{partition_for, OrderSentEvent};

use crate::{
    kafka::{self, KafkaProducer},
    metrics,
};

#[derive(Clone)]
/// TelemetrySink stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct TelemetrySink {
    tx: Sender<OrderSentEvent>,
    handle: Arc<Mutex<Option<JoinHandle<Result<()>>>>>,
}

impl TelemetrySink {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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

    /// record performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    /// LOSSLESS: blocks on a full channel (backpressure) instead of dropping, so the
    /// generator self-paces to the sustainable telemetry rate rather than discarding
    /// measurement data. Only errors if the aggregator is gone (shutdown), which is
    /// the single case we still count as dropped.
    pub async fn record(&self, event: OrderSentEvent) {
        if self.tx.send(event).await.is_err() {
            metrics::telemetry_dropped();
            error!("telemetry channel closed; dropping event");
        }
    }

    /// close performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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

/// Max events pulled from the channel per aggregator wake (recv_many), amortizing
/// the select!/wake overhead across a slice instead of paying it per event.
const RECV_BATCH: usize = 4096;

/// AckFuture is a pending delivery, tagged with the number of events in its batch
/// so completion can be accounted (flushed vs lost) without awaiting it inline.
type AckFuture = Pin<Box<dyn Future<Output = (usize, Result<(), String>)> + Send>>;

/// run_aggregator drains the event channel and publishes per-partition batches to
/// Kafka WITHOUT blocking on each batch's delivery. The old design awaited delivery
/// per flush (one broker round-trip serialized the whole drain → ~5.7k/s ceiling and
/// 90% drops). Here a single `select!` loop interleaves three things: draining the
/// channel into a per-partition batcher, enqueuing full chunks (rdkafka pipelines +
/// batches them in the background), and accounting completed deliveries from an
/// `inflight` set — so drain rate is decoupled from delivery latency. Backpressure
/// is lossless: a full producer queue makes `enqueue_chunk` poll+retry, which stalls
/// the drain, fills the channel, and blocks `record()` — the generator self-paces
/// instead of dropping evidence.
async fn run_aggregator(
    mut rx: mpsc::Receiver<OrderSentEvent>,
    producer: KafkaProducer,
    topic: String,
    session_id: String,
    worker_id: String,
    flush_interval: Duration,
    _batch_size: usize,
    num_partitions: i32,
) -> Result<()> {
    let mut ticker = time::interval(flush_interval);
    let mut batcher = PartitionBatcher::new(num_partitions);
    let mut inflight: FuturesUnordered<AckFuture> = FuturesUnordered::new();
    // Drain the channel in bulk (recv_many) rather than one event per wake: at high
    // rates a single aggregator paid the select!/wake overhead per event, which was
    // the next drain ceiling after the await-per-flush fix. One wake now pulls up to
    // RECV_BATCH events and feeds them straight through the batcher.
    let mut buf: Vec<OrderSentEvent> = Vec::with_capacity(RECV_BATCH);

    loop {
        tokio::select! {
            biased;
            // Account finished deliveries first so `inflight` stays bounded.
            Some((n, res)) = inflight.next(), if !inflight.is_empty() => account_delivery(n, res),
            count = rx.recv_many(&mut buf, RECV_BATCH) => {
                if count == 0 {
                    break; // sink closed → drain to completion below
                }
                for event in buf.drain(..) {
                    if let Some((part, chunk)) = batcher.push(event) {
                        enqueue_chunk(&producer, &topic, &session_id, &worker_id, part, chunk, &mut inflight).await;
                    }
                }
            }
            _ = ticker.tick() => {
                for (part, chunk) in batcher.drain_ready() {
                    enqueue_chunk(&producer, &topic, &session_id, &worker_id, part, chunk, &mut inflight).await;
                }
            }
        }
    }

    // Shutdown: flush whatever is buffered, then wait for every in-flight delivery.
    for (part, chunk) in batcher.drain_ready() {
        enqueue_chunk(&producer, &topic, &session_id, &worker_id, part, chunk, &mut inflight).await;
    }
    while let Some((n, res)) = inflight.next().await {
        account_delivery(n, res);
    }
    Ok(())
}

fn account_delivery(n: usize, res: Result<(), String>) {
    match res {
        Ok(()) => metrics::telemetry_flushed(n),
        Err(err) => {
            metrics::telemetry_dropped_n(n);
            error!(error = %err, count = n, "telemetry batch failed delivery after retries");
        }
    }
}

/// enqueue_chunk encodes one partition's chunk and enqueues it without awaiting
/// delivery, retrying on a full producer queue (poll to drain, then retry) so events
/// are never silently dropped. rdkafka owns retry/ordering for the in-flight message;
/// the tagged DeliveryFuture is pushed to `inflight` for async accounting.
async fn enqueue_chunk(
    producer: &KafkaProducer,
    topic: &str,
    session_id: &str,
    worker_id: &str,
    partition: i32,
    chunk: Vec<OrderSentEvent>,
    inflight: &mut FuturesUnordered<AckFuture>,
) {
    let n = chunk.len();
    let payload = match rmp_serde::to_vec_named(&OrderSentBatchRef {
        session_id,
        worker_id,
        events: &chunk,
    }) {
        Ok(p) => p,
        Err(err) => {
            error!(error = %err, "encode orders.sent messagepack; dropping batch");
            metrics::telemetry_dropped_n(n);
            return;
        }
    };
    loop {
        match kafka::enqueue_to_partition(producer, topic, partition, session_id, &payload) {
            Ok(Some(fut)) => {
                inflight.push(Box::pin(async move {
                    // DeliveryFuture resolves to Result<OwnedDeliveryResult, Canceled>;
                    // OwnedDeliveryResult is Result<(partition,offset), (KafkaError, msg)>.
                    match fut.await {
                        Ok(Ok(_)) => (n, Ok(())),
                        Ok(Err((err, _))) => (n, Err(err.to_string())),
                        Err(canceled) => (n, Err(canceled.to_string())),
                    }
                }));
                return;
            }
            Ok(None) => {
                // Producer queue full — drain it and retry (lossless backpressure).
                kafka::poll_producer(producer, Duration::from_millis(10));
                time::sleep(Duration::from_millis(1)).await;
            }
            Err(err) => {
                error!(error = %err, "enqueue orders.sent; dropping batch");
                metrics::telemetry_dropped_n(n);
                return;
            }
        }
    }
}

#[derive(Serialize)]
/// OrderSentBatchRef stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct OrderSentBatchRef<'a> {
    session_id: &'a str,
    worker_id: &'a str,
    events: &'a [OrderSentEvent],
}

/// PartitionBatcher buffers events per destination partition (co-partitioned by
/// order_id, same contract as the ingester) and emits a chunk the instant a
/// partition reaches MAX_EVENTS_PER_BATCH, so the aggregator can enqueue it without
/// waiting for a timer. `drain_ready` returns the partial remainder on tick/shutdown.
/// No event is ever dropped here — everything pushed is either emitted or drained.
struct PartitionBatcher {
    num_partitions: i32,
    by_part: BTreeMap<i32, Vec<OrderSentEvent>>,
}

impl PartitionBatcher {
    fn new(num_partitions: i32) -> Self {
        Self {
            num_partitions,
            by_part: BTreeMap::new(),
        }
    }

    /// Buffer one event; return a ready (partition, chunk) if it just filled one.
    fn push(&mut self, event: OrderSentEvent) -> Option<(i32, Vec<OrderSentEvent>)> {
        let part = partition_for(&event.order_id, self.num_partitions);
        let group = self.by_part.entry(part).or_default();
        group.push(event);
        if group.len() >= MAX_EVENTS_PER_BATCH {
            Some((part, std::mem::take(group)))
        } else {
            None
        }
    }

    /// Drain every buffered (partial) chunk — for the flush ticker and shutdown.
    fn drain_ready(&mut self) -> Vec<(i32, Vec<OrderSentEvent>)> {
        std::mem::take(&mut self.by_part)
            .into_iter()
            .filter(|(_, v)| !v.is_empty())
            .collect()
    }
}

pub(crate) const MAX_EVENTS_PER_BATCH: usize = 1000;

#[cfg(test)]
mod tests {
    use super::*;
    use iicpc_schemas_rust::{OrdType, OrderSentBatch, PayloadType, Side};

    /// test_event performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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

    // The batcher is the lossless core of the new aggregator: every pushed event is
    // either emitted in a full chunk or held for drain — nothing is dropped — and
    // emitted chunks are exactly MAX (so they stay under the broker message ceiling),
    // each event landing in its co-partition.
    #[test]
    fn partition_batcher_emits_and_drains_without_loss() {
        let n = 24;
        let total = 50_000usize;
        let mut b = PartitionBatcher::new(n);
        let mut emitted: Vec<(i32, Vec<OrderSentEvent>)> = Vec::new();
        for i in 0..total {
            if let Some(chunk) = b.push(test_event(i)) {
                emitted.push(chunk);
            }
        }
        let drained = b.drain_ready();

        let count: usize = emitted
            .iter()
            .chain(drained.iter())
            .map(|(_, v)| v.len())
            .sum();
        assert_eq!(count, total, "no events lost across emit + drain");

        for (_, chunk) in &emitted {
            assert_eq!(
                chunk.len(),
                MAX_EVENTS_PER_BATCH,
                "push emits only full chunks"
            );
        }
        for (_, chunk) in &drained {
            assert!(!chunk.is_empty() && chunk.len() < MAX_EVENTS_PER_BATCH);
        }
        for (part, chunk) in emitted.iter().chain(drained.iter()) {
            for e in chunk {
                assert_eq!(partition_for(&e.order_id, n), *part, "wrong partition");
            }
        }
    }

    #[test]
    fn partition_batcher_spreads_a_session_across_partitions() {
        let n = 24;
        let mut b = PartitionBatcher::new(n);
        for i in 0..2_000 {
            let _ = b.push(test_event(i));
        }
        let used = b.drain_ready().len();
        assert!(
            used >= n as usize / 2,
            "a busy session must use many partitions, used {used}"
        );
    }

    #[test]
    /// full_chunk_stays_under_broker_message_ceiling performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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
