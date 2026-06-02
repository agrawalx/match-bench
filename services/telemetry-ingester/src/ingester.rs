//! The run loop: consume both telemetry streams, aggregate, and snapshot to
//! TimescaleDB + Redis once per interval.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use iicpc_schemas_rust::{OrderAckedBatch, OrderSentBatch, TOPIC_ORDERS_ACKED, TOPIC_ORDERS_SENT};
use rdkafka::message::Message;
use tokio::time;
use tracing::{info, warn};

use crate::aggregate::Aggregator;
use crate::config::Config;
use crate::kafka::build_consumer;
use crate::redis_sink::RedisSink;
use crate::store::Store;

pub async fn run(cfg: Config) -> Result<()> {
    let consumer = build_consumer(
        &cfg.kafka_brokers,
        &cfg.consumer_group,
        &[TOPIC_ORDERS_SENT, TOPIC_ORDERS_ACKED],
    )?;
    let store = Store::connect(&cfg.timescale_url).await?;
    store.init_schema().await.context("init timescale schema")?;
    let redis = RedisSink::connect(&cfg.redis_url).await?;

    let mut agg = Aggregator::new(cfg.wave_ns);
    let mut ticker = time::interval(Duration::from_millis(cfg.snapshot_interval_ms));
    let mut last_snapshot_ns = now_ns();

    info!(
        brokers = %cfg.kafka_brokers,
        group = %cfg.consumer_group,
        wave_ms = cfg.wave_ns / 1_000_000,
        snapshot_ms = cfg.snapshot_interval_ms,
        "telemetry ingester started"
    );

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                // Final flush so the last second of a run isn't lost.
                flush(&mut agg, &store, &redis, &mut last_snapshot_ns).await;
                return Ok(());
            }
            _ = ticker.tick() => {
                flush(&mut agg, &store, &redis, &mut last_snapshot_ns).await;
            }
            res = consumer.recv() => {
                match res {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            ingest(&mut agg, m.topic(), payload);
                        }
                    }
                    Err(e) => warn!(error = %e, "kafka recv error"),
                }
            }
        }
    }
}

fn ingest(agg: &mut Aggregator, topic: &str, payload: &[u8]) {
    match topic {
        TOPIC_ORDERS_SENT => match rmp_serde::from_slice::<OrderSentBatch>(payload) {
            Ok(b) => {
                for e in &b.events {
                    agg.observe_sent(e);
                }
            }
            Err(e) => warn!(error = %e, "decode orders.sent batch"),
        },
        TOPIC_ORDERS_ACKED => match rmp_serde::from_slice::<OrderAckedBatch>(payload) {
            Ok(b) => {
                for e in &b.events {
                    agg.observe_acked(e);
                }
            }
            Err(e) => warn!(error = %e, "decode orders.acked batch"),
        },
        _ => {}
    }
}

async fn flush(agg: &mut Aggregator, store: &Store, redis: &RedisSink, last_snapshot_ns: &mut u64) {
    let now = now_ns();
    let interval_secs = (now.saturating_sub(*last_snapshot_ns)) as f64 / 1e9;
    *last_snapshot_ns = now;
    let snaps = agg.snapshot(now, interval_secs);
    if snaps.is_empty() {
        return;
    }
    if let Err(e) = store.write(&snaps).await {
        warn!(error = %e, rows = snaps.len(), "timescale write failed");
    }
    if let Err(e) = redis.write(&snaps).await {
        warn!(error = %e, rows = snaps.len(), "redis write failed");
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
