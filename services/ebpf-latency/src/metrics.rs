//! This module implements metrics behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::{
    env,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicU64, Ordering},
        LazyLock,
    },
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
    events_decoded: Counter,
    events_decode_errors: Counter,
    ringbuf_dropped: Counter,
    truncated_captures: Counter,
    flushes: Counter,
    events_flushed: Counter,
    acked_dropped: Counter,
    reordering: Counter,
    retransmissions: Counter,
    attach: ResultFamily,
}

static LAST_RINGBUF_DROPPED: AtomicU64 = AtomicU64::new(0);
static LAST_TRUNCATED: AtomicU64 = AtomicU64::new(0);

static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let mut registry = Registry::default();

    let events_decoded = Counter::default();
    let events_decode_errors = Counter::default();
    let ringbuf_dropped = Counter::default();
    let truncated_captures = Counter::default();
    let flushes = Counter::default();
    let events_flushed = Counter::default();
    let acked_dropped = Counter::default();
    let reordering = Counter::default();
    let retransmissions = Counter::default();
    let attach = ResultFamily::default();

    registry.register(
        "iicpc_ebpf_events_decoded",
        "eBPF latency events decoded.",
        events_decoded.clone(),
    );
    registry.register(
        "iicpc_ebpf_events_decode_errors",
        "eBPF latency event decode errors.",
        events_decode_errors.clone(),
    );
    registry.register(
        "iicpc_ebpf_ringbuf_dropped",
        "eBPF ring-buffer events dropped.",
        ringbuf_dropped.clone(),
    );
    registry.register(
        "iicpc_ebpf_truncated_captures",
        "Packets whose payload exceeded the BPF capture cap. Every FIX message past the cap in such a packet is LOST, so its order looks unanswered downstream; a non-zero rate invalidates correctness scoring and biases latency toward uncoalesced responses.",
        truncated_captures.clone(),
    );
    registry.register("iicpc_ebpf_flushes", "eBPF flushes.", flushes.clone());
    registry.register(
        "iicpc_ebpf_events_flushed",
        "eBPF events flushed to downstream sinks.",
        events_flushed.clone(),
    );
    registry.register(
        "iicpc_ebpf_acked_dropped",
        "orders.acked events dropped because the Kafka producer queue was full (non-blocking drain; graceful degradation).",
        acked_dropped.clone(),
    );
    registry.register(
        "iicpc_ebpf_reordering_detected",
        "eBPF observations where packet reordering was detected.",
        reordering.clone(),
    );
    registry.register(
        "iicpc_ebpf_retransmissions",
        "TCP retransmissions observed by the eBPF latency pipeline.",
        retransmissions.clone(),
    );
    registry.register(
        "iicpc_ebpf_attach",
        "eBPF attach attempts by result.",
        attach.clone(),
    );

    Metrics {
        registry,
        events_decoded,
        events_decode_errors,
        ringbuf_dropped,
        truncated_captures,
        flushes,
        events_flushed,
        acked_dropped,
        reordering,
        retransmissions,
        attach,
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

/// event_decoded performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn event_decoded(reordering: bool, retransmission_count: u32) {
    METRICS.events_decoded.inc();
    if reordering {
        METRICS.reordering.inc();
    }
    if retransmission_count > 0 {
        METRICS
            .retransmissions
            .inc_by(u64::from(retransmission_count));
    }
}

/// decode_error performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn decode_error() {
    METRICS.events_decode_errors.inc();
}

/// ringbuf_dropped performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn ringbuf_dropped(total: u64) {
    let previous = LAST_RINGBUF_DROPPED.swap(total, Ordering::Relaxed);
    if total > previous {
        METRICS.ringbuf_dropped.inc_by(total - previous);
    }
}

/// truncated_captures mirrors the kernel-side truncation counter (an absolute total) into
/// a monotonic Prometheus counter.
///
/// This was a log line only, which is why a defect that cost 25.6% of orders their entire
/// response record went unnoticed: nothing scraped it, nothing alerted on it, and the
/// downstream validator saw the result as contestants failing to answer.
pub fn truncated_captures(total: u64) {
    let previous = LAST_TRUNCATED.swap(total, Ordering::Relaxed);
    if total > previous {
        METRICS.truncated_captures.inc_by(total - previous);
    }
}

/// flushed performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn flushed(events: usize) {
    METRICS.flushes.inc();
    METRICS.events_flushed.inc_by(events as u64);
}

/// acked_dropped records orders.acked events dropped because the Kafka producer
/// queue was full (non-blocking drain — graceful degradation under broker pressure).
pub fn acked_dropped(events: usize) {
    METRICS.acked_dropped.inc_by(events as u64);
}

/// attach_ok performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn attach_ok() {
    METRICS.attach.get_or_create(&[("result", "ok")]).inc();
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
