//! Prometheus metrics for the telemetry ingester.
//!
//! Mirrors the `iicpc-ebpf-latency` / `iicpc-bot-fleet` idiom: a single static
//! `Registry`, per-metric helper fns, and a `start_server()` that spawns a thread
//! serving `/metrics` over a hand-rolled HTTP responder. Metric names are
//! `iicpc_telemetry_*`, consistent with `iicpc_ebpf_*` / `iicpc_bot_*`.

use std::{
    env,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::LazyLock,
    thread,
    time::Duration,
};

use prometheus_client::{
    encoding::text::encode,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};

type TopicFamily = Family<[(&'static str, &'static str); 1], Counter>;

struct Metrics {
    registry: Registry,
    /// orders.sent / orders.acked telemetry events consumed, keyed by topic.
    events_consumed: TopicFamily,
    /// Kafka batches whose payload failed to decode, keyed by topic.
    decode_errors: TopicFamily,
    /// Kafka consume (recv) errors.
    consume_errors: Counter,
    /// Snapshot rows finalized (emitted to the sinks) across all flushes.
    records_finalized: Counter,
    /// First-response entries evicted without ever producing a scored sample
    /// (idle eviction of in-flight orders ≈ acks that never completed in time).
    records_evicted: Counter,
    /// Current first-response join-buffer size (in-flight tracked orders).
    join_buffer_size: Gauge,
    /// Snapshot batches written to TimescaleDB.
    timescale_writes: Counter,
    /// TimescaleDB write duration, in seconds.
    timescale_write_seconds: Histogram,
    /// TimescaleDB write errors.
    timescale_errors: Counter,
    /// Snapshot batches written to Redis.
    redis_writes: Counter,
    /// Redis write errors.
    redis_errors: Counter,
}

static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let mut registry = Registry::default();

    let events_consumed = TopicFamily::default();
    let decode_errors = TopicFamily::default();
    let consume_errors = Counter::default();
    let records_finalized = Counter::default();
    let records_evicted = Counter::default();
    let join_buffer_size = Gauge::default();
    let timescale_writes = Counter::default();
    // 100us .. ~6.5s, doubling each bucket — covers a healthy ms-scale batch
    // insert through a pathological multi-second stall.
    let timescale_write_seconds = Histogram::new(
        prometheus_client::metrics::histogram::exponential_buckets(0.0001, 2.0, 17),
    );
    let timescale_errors = Counter::default();
    let redis_writes = Counter::default();
    let redis_errors = Counter::default();

    registry.register(
        "iicpc_telemetry_events_consumed",
        "Telemetry events consumed from Kafka by topic.",
        events_consumed.clone(),
    );
    registry.register(
        "iicpc_telemetry_decode_errors",
        "Telemetry Kafka batches that failed to decode, by topic.",
        decode_errors.clone(),
    );
    registry.register(
        "iicpc_telemetry_consume_errors",
        "Kafka consume (recv) errors.",
        consume_errors.clone(),
    );
    registry.register(
        "iicpc_telemetry_records_finalized",
        "Metric snapshot rows finalized and emitted to the sinks.",
        records_finalized.clone(),
    );
    registry.register(
        "iicpc_telemetry_records_evicted",
        "In-flight orders evicted from the join buffer without a completed scored sample.",
        records_evicted.clone(),
    );
    registry.register(
        "iicpc_telemetry_join_buffer_size",
        "Current first-response join-buffer size (in-flight tracked orders).",
        join_buffer_size.clone(),
    );
    registry.register(
        "iicpc_telemetry_timescale_writes",
        "Snapshot batches written to TimescaleDB.",
        timescale_writes.clone(),
    );
    registry.register(
        "iicpc_telemetry_timescale_write_seconds",
        "TimescaleDB snapshot write duration in seconds.",
        timescale_write_seconds.clone(),
    );
    registry.register(
        "iicpc_telemetry_timescale_errors",
        "TimescaleDB write errors.",
        timescale_errors.clone(),
    );
    registry.register(
        "iicpc_telemetry_redis_writes",
        "Snapshot batches written to Redis.",
        redis_writes.clone(),
    );
    registry.register(
        "iicpc_telemetry_redis_errors",
        "Redis write errors.",
        redis_errors.clone(),
    );

    Metrics {
        registry,
        events_consumed,
        decode_errors,
        consume_errors,
        records_finalized,
        records_evicted,
        join_buffer_size,
        timescale_writes,
        timescale_write_seconds,
        timescale_errors,
        redis_writes,
        redis_errors,
    }
});

pub fn start_server() {
    let addr = env::var("METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9090".to_string());

    thread::spawn(move || {
        let Ok(listener) = TcpListener::bind(&addr) else {
            eprintln!("metrics server bind failed on {addr}");
            return;
        };
        LazyLock::force(&METRICS);
        for stream in listener.incoming() {
            let Ok(stream) = stream else {
                continue;
            };
            handle_client(stream);
        }
    });
}

/// `n` telemetry events were consumed from `topic` and handed to the aggregator.
pub fn events_consumed(topic: &'static str, n: u64) {
    METRICS
        .events_consumed
        .get_or_create(&[("topic", topic)])
        .inc_by(n);
}

/// A Kafka batch for `topic` failed to decode.
pub fn decode_error(topic: &'static str) {
    METRICS
        .decode_errors
        .get_or_create(&[("topic", topic)])
        .inc();
}

/// A Kafka recv() returned an error.
pub fn consume_error() {
    METRICS.consume_errors.inc();
}

/// `n` snapshot rows were finalized for this flush.
pub fn records_finalized(n: usize) {
    METRICS.records_finalized.inc_by(n as u64);
}

/// `n` in-flight orders were evicted from the join buffer without completing.
pub fn records_evicted(n: usize) {
    if n > 0 {
        METRICS.records_evicted.inc_by(n as u64);
    }
}

/// Report the current first-response join-buffer size.
pub fn set_join_buffer_size(n: usize) {
    METRICS.join_buffer_size.set(n as i64);
}

/// Record a successful TimescaleDB write of a snapshot batch and its duration.
pub fn timescale_write(elapsed: Duration) {
    METRICS.timescale_writes.inc();
    METRICS
        .timescale_write_seconds
        .observe(elapsed.as_secs_f64());
}

pub fn timescale_error() {
    METRICS.timescale_errors.inc();
}

/// Record a successful Redis write of a snapshot batch.
pub fn redis_write() {
    METRICS.redis_writes.inc();
}

pub fn redis_error() {
    METRICS.redis_errors.inc();
}

fn render() -> String {
    let mut out = String::new();
    let _ = encode(&mut out, &METRICS.registry);
    out
}

fn handle_client(mut stream: TcpStream) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut buf = [0_u8; 512];
    let Ok(n) = stream.read(&mut buf) else {
        return;
    };
    let request = String::from_utf8_lossy(&buf[..n]);
    let first_line = request.lines().next().unwrap_or("");
    if !first_line.starts_with("GET /metrics ") {
        let body = "not found\n";
        let response = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes());
        return;
    }

    let body = render();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
}
