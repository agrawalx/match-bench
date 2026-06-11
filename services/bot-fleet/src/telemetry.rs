//! This module implements telemetry behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

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
    num_partitions: i32,
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

#[derive(Serialize)]
/// OrderSentBatchRef stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct OrderSentBatchRef<'a> {
    session_id: &'a str,
    worker_id: &'a str,
    events: &'a [OrderSentEvent],
}

/// shard_events groups pending sent-order events by their target partition.
/// It drains the caller's buffer so each event is flushed exactly once.
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

/// flush publishes buffered telemetry events in partitioned msgpack batches.
/// It chunks oversized groups and records per-partition send metrics for
/// observability.
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

    let by_part = shard_events(events, num_partitions);

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

    let results = futures::future::join_all(msgs.iter().map(|(part, payload, _)| {
        kafka::publish_to_partition(producer, topic, *part, session_id, payload)
    }))
    .await;

    let mut first_err = None;
    for ((_, _, chunk), result) in msgs.into_iter().zip(results) {
        match result {
            Ok(()) => metrics::telemetry_flushed(chunk.len()),
            Err(err) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
                events.extend(chunk);
            }
        }
    }
    match first_err {
        None => Ok(()),
        Some(err) => Err(err),
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

    /// shard_events_groups_every_event_to_its_partition checks partition grouping.
    /// It verifies sharding drains the input and places every event on its hashed
    /// Kafka partition.
    #[test]
    fn shard_events_groups_every_event_to_its_partition() {
        let n = 24;
        let mut events: Vec<OrderSentEvent> = (0..5_000).map(test_event).collect();
        let total = events.len();

        let by_part = shard_events(&mut events, n);

        assert!(
            events.is_empty(),
            "shard_events must drain the input buffer"
        );
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

    /// shard_events_spreads_a_single_session_across_partitions checks hash spread.
    /// It ensures a busy session still uses many partitions through order ids.
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
