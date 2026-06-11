//! This module implements metrics behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

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

/// Metrics stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct Metrics {
    registry: Registry,
    events_consumed: TopicFamily,
    decode_errors: TopicFamily,
    consume_errors: Counter,
    records_finalized: Counter,
    records_evicted: Counter,
    join_buffer_size: Gauge,
    timescale_writes: Counter,
    timescale_write_seconds: Histogram,
    timescale_errors: Counter,
    redis_writes: Counter,
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

/// start_server performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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

/// events_consumed performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn events_consumed(topic: &'static str, n: u64) {
    METRICS
        .events_consumed
        .get_or_create(&[("topic", topic)])
        .inc_by(n);
}

/// decode_error performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn decode_error(topic: &'static str) {
    METRICS
        .decode_errors
        .get_or_create(&[("topic", topic)])
        .inc();
}

/// consume_error performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn consume_error() {
    METRICS.consume_errors.inc();
}

/// records_finalized performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn records_finalized(n: usize) {
    METRICS.records_finalized.inc_by(n as u64);
}

/// records_evicted performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn records_evicted(n: usize) {
    if n > 0 {
        METRICS.records_evicted.inc_by(n as u64);
    }
}

/// set_join_buffer_size performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn set_join_buffer_size(n: usize) {
    METRICS.join_buffer_size.set(n as i64);
}

/// timescale_write performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn timescale_write(elapsed: Duration) {
    METRICS.timescale_writes.inc();
    METRICS
        .timescale_write_seconds
        .observe(elapsed.as_secs_f64());
}

/// timescale_error performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn timescale_error() {
    METRICS.timescale_errors.inc();
}

/// redis_write performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn redis_write() {
    METRICS.redis_writes.inc();
}

/// redis_error performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn redis_error() {
    METRICS.redis_errors.inc();
}

/// render performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn render() -> String {
    let mut out = String::new();
    let _ = encode(&mut out, &METRICS.registry);
    out
}

/// handle_client performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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
