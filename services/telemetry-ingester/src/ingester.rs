//! This module implements ingester behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use iicpc_schemas_rust::{OrderAckedBatch, OrderSentBatch, TOPIC_ORDERS_ACKED, TOPIC_ORDERS_SENT};
use rdkafka::message::Message;
use tokio::time;
use tracing::{info, warn};

use std::time::Instant;

use crate::aggregate::Aggregator;
use crate::config::Config;
use crate::kafka::build_consumer;
use crate::metrics;
use crate::redis_sink::RedisSink;
use crate::store::Store;

/// run performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub async fn run(cfg: Config) -> Result<()> {
    let consumer = build_consumer(
        &cfg.kafka_brokers,
        &cfg.consumer_group,
        &[TOPIC_ORDERS_SENT, TOPIC_ORDERS_ACKED],
    )?;
    let store = Store::connect(&cfg.timescale_url, cfg.shard.clone()).await?;
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
                    Err(e) => {
                        metrics::consume_error();
                        warn!(error = %e, "kafka recv error");
                    }
                }
            }
        }
    }
}

/// ingest performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn ingest(agg: &mut Aggregator, topic: &str, payload: &[u8]) {
    match topic {
        TOPIC_ORDERS_SENT => match rmp_serde::from_slice::<OrderSentBatch>(payload) {
            Ok(b) => {
                metrics::events_consumed(TOPIC_ORDERS_SENT, b.events.len() as u64);
                for e in &b.events {
                    agg.observe_sent(e);
                }
            }
            Err(e) => {
                metrics::decode_error(TOPIC_ORDERS_SENT);
                warn!(error = %e, "decode orders.sent batch");
            }
        },
        TOPIC_ORDERS_ACKED => match rmp_serde::from_slice::<OrderAckedBatch>(payload) {
            Ok(b) => {
                metrics::events_consumed(TOPIC_ORDERS_ACKED, b.events.len() as u64);
                for e in &b.events {
                    agg.observe_acked(e);
                }
            }
            Err(e) => {
                metrics::decode_error(TOPIC_ORDERS_ACKED);
                warn!(error = %e, "decode orders.acked batch");
            }
        },
        _ => {}
    }
}

/// flush performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn flush(agg: &mut Aggregator, store: &Store, redis: &RedisSink, last_snapshot_ns: &mut u64) {
    let now = now_ns();
    let interval_secs = (now.saturating_sub(*last_snapshot_ns)) as f64 / 1e9;
    *last_snapshot_ns = now;
    let snaps = agg.snapshot(now, interval_secs);
    metrics::set_join_buffer_size(agg.join_buffer_size());
    metrics::records_evicted(agg.last_evicted());
    // finalized = orders that got a recorded latency sample this interval (real
    // recorded-latency throughput), reported every tick regardless of snapshot rows.
    metrics::records_finalized(agg.take_finalized());
    if snaps.is_empty() {
        return;
    }

    let started = Instant::now();
    match store.write(&snaps).await {
        Ok(()) => metrics::timescale_write(started.elapsed()),
        Err(e) => {
            metrics::timescale_error();
            warn!(error = %e, rows = snaps.len(), "timescale write failed");
        }
    }
    match redis.write(&snaps).await {
        Ok(()) => metrics::redis_write(),
        Err(e) => {
            metrics::redis_error();
            warn!(error = %e, rows = snaps.len(), "redis write failed");
        }
    }
}

/// now_ns performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
