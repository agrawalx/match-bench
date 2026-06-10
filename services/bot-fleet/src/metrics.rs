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
    metrics::{counter::Counter, family::Family},
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

/// telemetry_flushed performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn telemetry_flushed(events: usize) {
    METRICS.telemetry_batches.inc();
    METRICS.telemetry_events_flushed.inc_by(events as u64);
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
