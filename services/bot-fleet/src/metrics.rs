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
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::Registry,
};

type ResultFamily = Family<[(&'static str, &'static str); 1], Counter>;

/// Metrics stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct Metrics {
    registry: Registry,
    workloads: ResultFamily,
    tasks_assigned: Counter,
    tasks_connected: Counter,
    connect_failures: Counter,
    orders_sent: Counter,
    order_write_errors: Counter,
    telemetry_dropped: Counter,
    telemetry_batches: Counter,
    telemetry_events_flushed: Counter,
    // write_seconds: wall time spent inside write_all per order. This is the direct
    // backpressure probe — if the drain stops reading, its TCP window closes and this
    // grows. schedule_slip_seconds: send_ts - target_send_ts, i.e. how far behind the
    // paced schedule each order actually went out (CPU OR backpressure lateness).
    write_seconds: Histogram,
    schedule_slip_seconds: Histogram,
    // write_batch_size: number of orders coalesced into each write_all. With batching
    // off (BOT_WRITE_BATCH=1) this is always 1; under load it shows how effectively
    // catch-up batching amortises the per-write syscall.
    write_batch_size: Histogram,
}

/// batch_buckets returns bucket edges for the per-write batch-size histogram
/// (1 .. 4096 orders/write).
fn batch_buckets() -> impl Iterator<Item = f64> {
    [
        1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 4096.0,
    ]
    .into_iter()
}

/// send_buckets returns the shared bucket edges (seconds) for the per-order send
/// histograms: 1us .. 1s, fine enough to separate "write returns instantly"
/// (no backpressure) from "write blocks for ms" (drain not draining).
fn send_buckets() -> impl Iterator<Item = f64> {
    [
        1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1.0,
    ]
    .into_iter()
}

static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let mut registry = Registry::default();

    let workloads = ResultFamily::default();
    let tasks_assigned = Counter::default();
    let tasks_connected = Counter::default();
    let connect_failures = Counter::default();
    let orders_sent = Counter::default();
    let order_write_errors = Counter::default();
    let telemetry_dropped = Counter::default();
    let telemetry_batches = Counter::default();
    let telemetry_events_flushed = Counter::default();
    let write_seconds = Histogram::new(send_buckets());
    let schedule_slip_seconds = Histogram::new(send_buckets());
    let write_batch_size = Histogram::new(batch_buckets());

    registry.register(
        "iicpc_bot_workloads",
        "Bot workload messages by result.",
        workloads.clone(),
    );
    registry.register(
        "iicpc_bot_tasks_assigned",
        "Bot tasks assigned from workload messages.",
        tasks_assigned.clone(),
    );
    registry.register(
        "iicpc_bot_tasks_connected",
        "Bot task websocket connections established.",
        tasks_connected.clone(),
    );
    registry.register(
        "iicpc_bot_connect_failures",
        "Bot task websocket connection failures.",
        connect_failures.clone(),
    );
    registry.register(
        "iicpc_bot_orders_sent",
        "Bot orders sent to workload targets.",
        orders_sent.clone(),
    );
    registry.register(
        "iicpc_bot_order_write_errors",
        "Bot order websocket write errors.",
        order_write_errors.clone(),
    );
    registry.register(
        "iicpc_bot_telemetry_events_dropped",
        "Bot telemetry events dropped before flush.",
        telemetry_dropped.clone(),
    );
    registry.register(
        "iicpc_bot_telemetry_batches",
        "Bot telemetry batches flushed.",
        telemetry_batches.clone(),
    );
    registry.register(
        "iicpc_bot_telemetry_events_flushed",
        "Bot telemetry events flushed.",
        telemetry_events_flushed.clone(),
    );
    registry.register(
        "iicpc_bot_write_seconds",
        "Wall time spent inside write_all per order (direct drain backpressure probe).",
        write_seconds.clone(),
    );
    registry.register(
        "iicpc_bot_schedule_slip_seconds",
        "Per-order lateness: actual send_ts minus paced target_send_ts.",
        schedule_slip_seconds.clone(),
    );
    registry.register(
        "iicpc_bot_write_batch_size",
        "Orders coalesced into each write_all syscall.",
        write_batch_size.clone(),
    );

    Metrics {
        registry,
        workloads,
        tasks_assigned,
        tasks_connected,
        connect_failures,
        orders_sent,
        order_write_errors,
        telemetry_dropped,
        telemetry_batches,
        telemetry_events_flushed,
        write_seconds,
        schedule_slip_seconds,
        write_batch_size,
    }
});

/// start_server performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn start_server() {
    let port = env::var("METRICS_PORT").unwrap_or_else(|_| "9090".to_string());
    let addr = format!("0.0.0.0:{port}");

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

/// workload_ok performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn workload_ok() {
    METRICS.workloads.get_or_create(&[("result", "ok")]).inc();
}

/// workload_error performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn workload_error() {
    METRICS
        .workloads
        .get_or_create(&[("result", "error")])
        .inc();
}

/// tasks_assigned performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn tasks_assigned(n: usize) {
    METRICS.tasks_assigned.inc_by(n as u64);
}

/// tasks_connected performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn tasks_connected(n: usize) {
    METRICS.tasks_connected.inc_by(n as u64);
}

/// connect_failure performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn connect_failure() {
    METRICS.connect_failures.inc();
}

/// order_sent performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn order_sent() {
    METRICS.orders_sent.inc();
}

/// orders_sent_by records a whole batch of `n` orders sent in one write.
pub fn orders_sent_by(n: usize) {
    METRICS.orders_sent.inc_by(n as u64);
}

/// order_write_error performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn order_write_error() {
    METRICS.order_write_errors.inc();
}

/// telemetry_dropped performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn telemetry_dropped() {
    METRICS.telemetry_dropped.inc();
}

/// telemetry_dropped_n records that a whole batch of `events` was lost (e.g. a
/// batch that failed delivery after all retries). Lossless operation aims to keep
/// this at zero.
pub fn telemetry_dropped_n(events: usize) {
    METRICS.telemetry_dropped.inc_by(events as u64);
}

/// telemetry_flushed performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn telemetry_flushed(events: usize) {
    METRICS.telemetry_batches.inc();
    METRICS.telemetry_events_flushed.inc_by(events as u64);
}

/// observe_write records one write_all: its wall duration (ns) and how many orders
/// were coalesced into it. write_ns isolates drain backpressure; batch_size shows how
/// effectively catch-up batching amortises the per-write syscall.
pub fn observe_write(write_ns: u64, batch_size: usize) {
    METRICS.write_seconds.observe(write_ns as f64 / 1e9);
    METRICS.write_batch_size.observe(batch_size as f64);
}

/// observe_slip records one order's lateness vs its paced schedule (ns).
pub fn observe_slip(slip_ns: u64) {
    METRICS
        .schedule_slip_seconds
        .observe(slip_ns as f64 / 1e9);
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
