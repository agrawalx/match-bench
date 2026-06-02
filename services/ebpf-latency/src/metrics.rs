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

struct Metrics {
    registry: Registry,
    events_decoded: Counter,
    events_decode_errors: Counter,
    ringbuf_dropped: Counter,
    flushes: Counter,
    events_flushed: Counter,
    reordering: Counter,
    retransmissions: Counter,
    attach: ResultFamily,
}

static LAST_RINGBUF_DROPPED: AtomicU64 = AtomicU64::new(0);

static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let mut registry = Registry::default();

    let events_decoded = Counter::default();
    let events_decode_errors = Counter::default();
    let ringbuf_dropped = Counter::default();
    let flushes = Counter::default();
    let events_flushed = Counter::default();
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
    registry.register("iicpc_ebpf_flushes", "eBPF flushes.", flushes.clone());
    registry.register(
        "iicpc_ebpf_events_flushed",
        "eBPF events flushed to downstream sinks.",
        events_flushed.clone(),
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
        flushes,
        events_flushed,
        reordering,
        retransmissions,
        attach,
    }
});

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

pub fn decode_error() {
    METRICS.events_decode_errors.inc();
}

pub fn ringbuf_dropped(total: u64) {
    let previous = LAST_RINGBUF_DROPPED.swap(total, Ordering::Relaxed);
    if total > previous {
        METRICS.ringbuf_dropped.inc_by(total - previous);
    }
}

pub fn flushed(events: usize) {
    METRICS.flushes.inc();
    METRICS.events_flushed.inc_by(events as u64);
}

pub fn attach_ok() {
    METRICS.attach.get_or_create(&[("result", "ok")]).inc();
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
