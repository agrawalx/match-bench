//! This module implements telemetry behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

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

    /// record performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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

/// run_aggregator performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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

#[derive(Serialize)]
/// OrderSentBatchRef stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct OrderSentBatchRef<'a> {
    session_id: &'a str,
    worker_id: &'a str,
    events: &'a [OrderSentEvent],
}

/// flush performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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

/// encode_chunks performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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

/// retain_failed_chunks performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn retain_failed_chunks(events: &mut Vec<OrderSentEvent>, failed: &[bool]) {
    let mut index = 0;
    events.retain(|_| {
        let chunk = index / MAX_EVENTS_PER_BATCH;
        index += 1;
        failed.get(chunk).copied().unwrap_or(false)
    });
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
        }
    }

    #[test]
    /// oversized_flush_encodes_multiple_chunks performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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
        assert_eq!(batches[2].events[0].order_id, test_event(2_000).order_id);
        for payload in &payloads {
            assert!(
                payload.len() < 1_048_576,
                "chunk payload {} bytes breaches max.message.bytes",
                payload.len()
            );
        }
    }

    #[test]
    /// full_chunk_stays_under_broker_message_ceiling performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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

    #[test]
    /// retain_failed_chunks_keeps_only_rejected_events performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn retain_failed_chunks_keeps_only_rejected_events() {
        let mut events: Vec<OrderSentEvent> = (0..2_500).map(test_event).collect();

        retain_failed_chunks(&mut events, &[false, true, false]);

        assert_eq!(events.len(), 1_000);
        assert_eq!(events[0].order_id, test_event(1_000).order_id);
        assert_eq!(events[999].order_id, test_event(1_999).order_id);
    }

    #[test]
    /// retain_failed_chunks_drains_everything_on_success performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn retain_failed_chunks_drains_everything_on_success() {
        let mut events: Vec<OrderSentEvent> = (0..1_500).map(test_event).collect();

        retain_failed_chunks(&mut events, &[false, false]);

        assert!(events.is_empty());
    }
}
