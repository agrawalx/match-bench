use std::{
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

use iicpc_schemas_rust::OrderSentEvent;

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
                            if let Err(err) = flush(&producer, &topic, &session_id, &worker_id, &mut events).await {
                                error!(error = %err, "failed to flush telemetry batch");
                            }
                        }
                    }
                    None => {
                        if let Err(err) = flush(&producer, &topic, &session_id, &worker_id, &mut events).await {
                            error!(error = %err, "failed to flush final telemetry batch");
                            metrics::telemetry_dropped();
                            events.clear();
                        }
                        return Ok(());
                    }
                }
            }
            _ = ticker.tick() => {
                if let Err(err) = flush(&producer, &topic, &session_id, &worker_id, &mut events).await {
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

/// flush publishes the current telemetry batch and only clears what Kafka
/// accepted.
///
/// The batch is split into size-bounded chunks so a large accumulated buffer
/// cannot exceed the topic's max.message.bytes, and the chunk publishes are
/// PIPELINED: every delivery future is fired before any is awaited
/// (join_all), so one flush pays roughly one broker RTT instead of one per
/// chunk. The old one-await-per-chunk loop serialized those RTTs and capped
/// the sink at ~50-100k ev/s — silent drops exactly at the scoring waves.
///
/// Failure semantics: drain only what Kafka accepted. Events belonging to a
/// failed chunk stay queued (in order) for the next flush; the first error is
/// returned after the buffer is reconciled.
async fn flush(
    producer: &KafkaProducer,
    topic: &str,
    session_id: &str,
    worker_id: &str,
    events: &mut Vec<OrderSentEvent>,
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let payloads = encode_chunks(session_id, worker_id, events)?;
    let results = futures::future::join_all(
        payloads
            .iter()
            .map(|payload| kafka::publish_bytes(producer, topic, session_id, payload)),
    )
    .await;

    let failed: Vec<bool> = results.iter().map(Result::is_err).collect();
    let mut first_err = None;
    for (chunk, result) in events.chunks(MAX_EVENTS_PER_BATCH).zip(results) {
        match result {
            Ok(()) => metrics::telemetry_flushed(chunk.len()),
            Err(err) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }
    }
    retain_failed_chunks(events, &failed);
    match first_err {
        None => Ok(()),
        Some(err) => Err(err),
    }
}

/// encode_chunks splits the event buffer into MAX_EVENTS_PER_BATCH-sized
/// chunks and MessagePack-encodes each as one OrderSentBatch message.
/// msgpack-named events repeat field names (~360 B each at realistic
/// identifier sizes); a few thousand in one message overflow the topic's
/// 1 MiB max.message.bytes and the broker rejects the whole message.
fn encode_chunks(
    session_id: &str,
    worker_id: &str,
    events: &[OrderSentEvent],
) -> Result<Vec<Vec<u8>>> {
    events
        .chunks(MAX_EVENTS_PER_BATCH)
        .map(|chunk| {
            rmp_serde::to_vec_named(&OrderSentBatchRef {
                session_id,
                worker_id,
                events: chunk,
            })
            .context("encode orders.sent messagepack")
        })
        .collect()
}

/// retain_failed_chunks keeps only the events whose chunk publish failed
/// (failed[i] covers events[i*MAX_EVENTS_PER_BATCH ..]), in order, so the
/// next flush retries exactly what Kafka has not accepted.
fn retain_failed_chunks(events: &mut Vec<OrderSentEvent>, failed: &[bool]) {
    let mut index = 0;
    events.retain(|_| {
        let chunk = index / MAX_EVENTS_PER_BATCH;
        index += 1;
        failed.get(chunk).copied().unwrap_or(false)
    });
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
        }
    }

    // A flush larger than the per-message ceiling must split into multiple
    // size-bounded chunks, each decodable as a complete OrderSentBatch, with
    // event order preserved across chunk boundaries.
    #[test]
    fn oversized_flush_encodes_multiple_chunks() {
        let events: Vec<OrderSentEvent> = (0..2_500).map(test_event).collect();

        let payloads = encode_chunks("sess", "worker", &events).expect("encode chunks");

        assert_eq!(payloads.len(), 3, "2500 events must split into 3 chunks");
        let batches: Vec<OrderSentBatch> = payloads
            .iter()
            .map(|p| rmp_serde::from_slice(p).expect("decode chunk"))
            .collect();
        let counts: Vec<usize> = batches.iter().map(|b| b.events.len()).collect();
        assert_eq!(counts, vec![1000, 1000, 500]);
        // Order preserved: chunk 2 starts at event 2000.
        assert_eq!(batches[2].events[0].order_id, test_event(2_000).order_id);
        for payload in &payloads {
            assert!(
                payload.len() < 1_048_576,
                "chunk payload {} bytes breaches max.message.bytes",
                payload.len()
            );
        }
    }

    // Size math behind the 1000-event ceiling: msgpack-named events repeat
    // field names, ≈360 B/event at realistic identifier sizes, so a full
    // chunk is ≈360 KB — comfortably under the topic's 1 MiB
    // max.message.bytes even with headroom for larger identifiers.
    #[test]
    fn full_chunk_stays_under_broker_message_ceiling() {
        let events: Vec<OrderSentEvent> = (0..MAX_EVENTS_PER_BATCH).map(test_event).collect();

        let payloads = encode_chunks("sess", "worker", &events).expect("encode chunk");

        assert_eq!(payloads.len(), 1);
        assert!(
            payloads[0].len() < 1_048_576,
            "full chunk is {} bytes, must stay under 1 MiB",
            payloads[0].len()
        );
    }

    // Failure semantics: drain only what Kafka accepted. Events belonging to
    // a failed chunk stay queued (in order) for the next flush; accepted
    // chunks are dropped.
    #[test]
    fn retain_failed_chunks_keeps_only_rejected_events() {
        let mut events: Vec<OrderSentEvent> = (0..2_500).map(test_event).collect();

        // Chunk 0 (0..1000) accepted, chunk 1 (1000..2000) failed,
        // chunk 2 (2000..2500) accepted.
        retain_failed_chunks(&mut events, &[false, true, false]);

        assert_eq!(events.len(), 1_000);
        assert_eq!(events[0].order_id, test_event(1_000).order_id);
        assert_eq!(events[999].order_id, test_event(1_999).order_id);
    }

    // All chunks accepted → nothing retained (the steady-state path).
    #[test]
    fn retain_failed_chunks_drains_everything_on_success() {
        let mut events: Vec<OrderSentEvent> = (0..1_500).map(test_event).collect();

        retain_failed_chunks(&mut events, &[false, false]);

        assert!(events.is_empty());
    }
}
