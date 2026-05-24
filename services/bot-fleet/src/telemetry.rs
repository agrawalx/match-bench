use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::{sync::mpsc, time};
use tracing::{error, warn};

use iicpc_schemas_rust::OrderSentEvent;

use crate::kafka::{self, KafkaProducer};

/// TelemetrySink accepts per-order telemetry from bot tasks and forwards it to
/// one background aggregator for batched Kafka publishing.
#[derive(Clone)]
pub struct TelemetrySink {
    tx: mpsc::Sender<OrderSentEvent>,
    handle: Arc<Mutex<Option<tokio::task::JoinHandle<Result<()>>>>>,
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
        if let Err(err) = self.tx.try_send(event) {
            warn!(error = %err, "dropping telemetry event because channel is full");
        }
    }

    /// close drops the final sender and waits for the aggregator to flush.
    /// Any final publish failure is returned to the workload caller.
    pub async fn close(self) -> Result<()> {
        drop(self.tx);

        let handle = self
            .handle
            .lock()
            .map_err(|err| crate::errors::BotFleetError::TelemetryError(format!("telemetry join handle mutex poisoned: {err}")))?
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
        tokio::select! {
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
                        flush(&producer, &topic, &session_id, &worker_id, &mut events)
                            .await
                            .context("flush final telemetry batch")?;
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

/// flush publishes the current telemetry batch and only clears it after Kafka
/// accepts the encoded payload.
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

    #[derive(Serialize)]
    struct OrderSentBatchRef<'a> {
        session_id: &'a str,
        worker_id: &'a str,
        events: &'a [OrderSentEvent],
    }

    let batch = OrderSentBatchRef {
        session_id,
        worker_id,
        events,
    };
    let payload = rmp_serde::to_vec_named(&batch).context("encode orders.sent messagepack")?;
    kafka::publish_bytes(producer, topic, session_id, &payload).await?;
    events.clear();
    Ok(())
}
