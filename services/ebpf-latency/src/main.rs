//! This module starts the src service.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

mod capture;
mod matcher;
mod mtu;
mod netns;
mod parse;
mod pipeline;
mod reassembly;

use std::fs::File;
use std::io::ErrorKind;
use std::os::fd::AsRawFd;
use std::{env, path::PathBuf, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use aya::{
    maps::{MapData, PerCpuArray, RingBuf},
    programs::{tc, SchedClassifier, TcAttachType, Xdp, XdpFlags},
    Ebpf,
};
use iicpc_bot_fleet::kafka::{self, KafkaProducer};
use iicpc_logger_rust::loki;
use iicpc_schemas_rust::{partition_for, OrderAckedBatchRef, OrderAckedEventRef, TOPIC_ORDERS_ACKED};
use std::collections::BTreeMap;
use tokio::signal::unix::{signal, Signal, SignalKind};
use tokio::time;
use tracing::{info, warn};

mod metrics;

use matcher::MatchedEvent;
use pipeline::Pipeline;

const DEFAULT_RINGBUF_MAP: &str = "EVENTS";
const DROPPED_EVENTS_MAP: &str = "DROPPED_EVENTS";
const TRUNCATED_CAPTURES_MAP: &str = "TRUNCATED_CAPTURES";
const DEFAULT_XDP_INGRESS_PROGRAM: &str = "iicpc_xdp_ingress";
const DEFAULT_TC_EGRESS_PROGRAM: &str = "iicpc_tc_egress";
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(5);
const DEFAULT_BATCH_SIZE: usize = 4096;
const EVICT_INTERVAL: Duration = Duration::from_secs(1);
const MAX_PENDING_EVENTS: usize = 100_000;
const MAX_EVENTS_PER_BATCH: usize = 1000;
const DEFAULT_CLAMP_MTU: usize = 1500;

#[derive(Debug, Clone)]
/// Config stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct Config {
    kafka_brokers: String,
    topic: String,
    session_id: String,
    contestant_id: String,
    iface: String,
    netns_path: Option<PathBuf>,
    object_path: PathBuf,
    xdp_ingress_program: String,
    tc_egress_program: String,
    ringbuf_map: String,
    flush_interval: Duration,
    batch_size: usize,
    clamp_mtu: usize,
    /// Partition count of orders.acked. The flush shards each batch by
    /// partition_for(order_id, this) — the SAME hash bot-fleet uses for
    /// orders.sent — so an order's acked event co-locates with its sent event on
    /// one partition (the co-partitioning the distributed ingester joins on).
    /// MUST equal the topic's real partition count (topic-init creates 24).
    orders_partitions: i32,
}

impl Config {
    /// from_env performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn from_env() -> Result<Self> {
        let object_path = env::var("EBPF_OBJECT_PATH")
            .or_else(|_| env::var("IICPC_EBPF_OBJECT"))
            .context("EBPF_OBJECT_PATH must point at the compiled eBPF object")?;

        let netns_path = match optional_path_env("EBPF_NETNS_PATH") {
            Some(p) => Some(p),
            None => match env::var("EBPF_ALGO_POD_UID")
                .ok()
                .filter(|v| !v.trim().is_empty())
            {
                Some(uid) => {
                    let cid = env::var("EBPF_ALGO_CONTAINER_ID").ok();
                    Some(
                        netns::resolve_netns_path(&uid, cid.as_deref())
                            .context("resolve algo pod network namespace from pod UID")?,
                    )
                }
                None => None,
            },
        };

        Ok(Self {
            kafka_brokers: env_or("KAFKA_BROKERS", "localhost:9092"),
            topic: env_or("ORDERS_ACKED_TOPIC", TOPIC_ORDERS_ACKED),
            session_id: required_env("SESSION_ID")?,
            contestant_id: required_env("CONTESTANT_ID")?,
            iface: required_env("EBPF_IFACE")?,
            netns_path,
            object_path: PathBuf::from(object_path),
            xdp_ingress_program: env_or("EBPF_XDP_INGRESS_PROGRAM", DEFAULT_XDP_INGRESS_PROGRAM),
            tc_egress_program: env_or("EBPF_TC_EGRESS_PROGRAM", DEFAULT_TC_EGRESS_PROGRAM),
            ringbuf_map: env_or("EBPF_RINGBUF_MAP", DEFAULT_RINGBUF_MAP),
            flush_interval: env_duration_ms("EBPF_FLUSH_INTERVAL_MS", DEFAULT_FLUSH_INTERVAL),
            batch_size: env_usize("EBPF_BATCH_SIZE", DEFAULT_BATCH_SIZE),
            clamp_mtu: env_usize("CAPTURE_CLAMP_MTU", DEFAULT_CLAMP_MTU),
            orders_partitions: env_usize("ORDERS_PARTITIONS", 24).max(1) as i32,
        })
    }

    /// validate performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn validate(&self) -> Result<()> {
        if self.batch_size == 0 {
            bail!("EBPF_BATCH_SIZE must be greater than zero");
        }
        if self.flush_interval.is_zero() {
            bail!("EBPF_FLUSH_INTERVAL_MS must be greater than zero");
        }
        if self.clamp_mtu != 0 && !(68..=65_535).contains(&self.clamp_mtu) {
            bail!("CAPTURE_CLAMP_MTU must be 0 (disabled) or between 68 and 65535");
        }
        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread")]
/// main performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn main() -> Result<()> {
    let _loki_guard = loki::init("ebpf-latency");
    metrics::start_server();

    let mut config = Config::from_env()?;
    config.validate()?;

    let producer = kafka::telemetry_producer(&config.kafka_brokers)?;
    // Authoritative N for order_id sharding: the orders.acked topic's real partition
    // count, falling back to ORDERS_PARTITIONS. Must match the count bot-fleet uses
    // for orders.sent so an order's sent + acked land on the same partition.
    if let Some(n) = kafka::topic_partition_count(&producer, &config.topic) {
        config.orders_partitions = n;
    }
    run(config, producer).await
}

/// run performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn run(config: Config, producer: KafkaProducer) -> Result<()> {
    let mut bpf = Ebpf::load_file(&config.object_path)
        .with_context(|| format!("load eBPF object {}", config.object_path.display()))?;

    attach_programs(&mut bpf, &config)?;
    let mut ringbuf = RingBuf::try_from(
        bpf.take_map(&config.ringbuf_map)
            .ok_or_else(|| anyhow!("ringbuf map {} not found", config.ringbuf_map))?,
    )
    .with_context(|| format!("open ringbuf map {}", config.ringbuf_map))?;
    let dropped_events = take_counter(&mut bpf, DROPPED_EVENTS_MAP);
    let truncated_captures = take_counter(&mut bpf, TRUNCATED_CAPTURES_MAP);

    info!(
        iface = %config.iface,
        session_id = %config.session_id,
        contestant_id = %config.contestant_id,
        topic = %config.topic,
        netns_path = config.netns_path.as_ref().map(|p| p.display().to_string()).as_deref().unwrap_or("current"),
        "eBPF capture publisher started (userspace parse + match)"
    );

    let mut pipeline = Pipeline::new();
    let mut events: Vec<MatchedEvent> = Vec::with_capacity(config.batch_size);
    let mut ticker = time::interval(config.flush_interval);
    let mut evict_ticker = time::interval(EVICT_INTERVAL);
    let mut last_dropped = 0u64;
    let mut last_truncated = 0u64;
    let mut shutdown = ShutdownSignal::new()?;

    loop {
        tokio::select! {
            sig = shutdown.recv() => {
                info!(signal = sig, "shutdown signal received; flushing buffered orders.acked tail");
                flush(&producer, &config, &mut events).await?;
                return Ok(());
            }
            _ = ticker.tick() => {
                drain_ringbuf(&mut ringbuf, &mut pipeline, &producer, &config, &mut events).await?;
                flush(&producer, &config, &mut events).await?;
                report_counter(&dropped_events, &mut last_dropped, "eBPF ring buffer dropped events");
                report_counter(&truncated_captures, &mut last_truncated, "eBPF truncated oversized captures (check GSO/TSO off)");
            }
            _ = evict_ticker.tick() => {
                pipeline.evict_idle();
            }
        }
    }
}

/// ShutdownSignal stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct ShutdownSignal {
    sigterm: Signal,
    sigint: Signal,
}

impl ShutdownSignal {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn new() -> Result<Self> {
        Ok(Self {
            sigterm: signal(SignalKind::terminate()).context("install SIGTERM handler")?,
            sigint: signal(SignalKind::interrupt()).context("install SIGINT handler")?,
        })
    }

    /// recv performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.sigterm.recv() => "SIGTERM",
            _ = self.sigint.recv() => "SIGINT",
        }
    }
}

/// drain_ringbuf performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn drain_ringbuf(
    ringbuf: &mut RingBuf<MapData>,
    pipeline: &mut Pipeline,
    producer: &KafkaProducer,
    config: &Config,
    events: &mut Vec<MatchedEvent>,
) -> Result<()> {
    while let Some(item) = ringbuf.next() {
        match capture::decode(&item) {
            Ok(cap) => pipeline.process(&cap, events),
            Err(err) => {
                metrics::decode_error();
                warn!(error = %err, "dropping malformed capture record");
                continue;
            }
        }
        if events.len() >= config.batch_size {
            flush(producer, config, events).await?;
        }
    }
    Ok(())
}

/// flush performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
async fn flush(
    producer: &KafkaProducer,
    config: &Config,
    events: &mut Vec<MatchedEvent>,
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    
    let mut by_part: BTreeMap<i32, Vec<MatchedEvent>> = BTreeMap::new();
    for e in events.drain(..) {
        by_part
            .entry(partition_for(&e.order_id, config.orders_partitions))
            .or_default()
            .push(e);
    }

    
    let mut msgs: Vec<(i32, Vec<u8>, Vec<MatchedEvent>)> = Vec::new();
    for (part, mut group) in by_part {
        while !group.is_empty() {
            let take = group.len().min(MAX_EVENTS_PER_BATCH);
            let chunk: Vec<MatchedEvent> = group.drain(..take).collect();
            let event_refs = chunk
                .iter()
                .map(|e| OrderAckedEventRef {
                    session_id: &config.session_id,
                    contestant_id: &config.contestant_id,
                    order_id: &e.order_id,
                    src_ip: e.src_ip,
                    src_port: e.src_port,
                    tcp_seq: e.tcp_seq,
                    t3_xdp_ingress_ns: e.t3_ns,
                    t7_xdp_egress_ns: e.t7_ns,
                    pod_service_time_ns: e.pod_service_time_ns,
                    exec_type: &e.exec_type,
                    fill_qty: e.fill_qty,
                    fill_price: e.fill_price,
                    orig_order_id: &e.orig_order_id,
                    reordering_detected: e.reordering_detected,
                    retransmission_count: e.retransmission_count,
                })
                .collect::<Vec<_>>();
            let batch = OrderAckedBatchRef {
                session_id: &config.session_id,
                contestant_id: &config.contestant_id,
                events: &event_refs,
            };
            let payload =
                rmp_serde::to_vec_named(&batch).context("encode orders.acked messagepack")?;
            msgs.push((part, payload, chunk));
        }
    }

    // Pipeline every publish to its explicit partition, then await together.
    let results = futures::future::join_all(msgs.iter().map(|(part, payload, _)| {
        kafka::publish_to_partition(producer, &config.topic, *part, &config.contestant_id, payload)
    }))
    .await;

    
    let mut publish_failed = false;
    for ((_, _, chunk), result) in msgs.into_iter().zip(results) {
        match result {
            Ok(()) => {
                for event in &chunk {
                    metrics::event_decoded(event.reordering_detected, event.retransmission_count);
                }
                metrics::flushed(chunk.len());
            }
            Err(err) => {
                publish_failed = true;
                warn!(error = %err, "publish orders.acked failed; retaining for retry");
                events.extend(chunk);
            }
        }
    }
    if events.len() > MAX_PENDING_EVENTS {
        let drop = events.len() - MAX_PENDING_EVENTS;
        events.drain(0..drop);
        warn!(
            dropped = drop,
            "dropped oldest pending orders.acked events (publish backlog)"
        );
    }
    let _ = publish_failed; // failures are logged + retained, never propagated (H16)
    Ok(())
}

/// take_counter performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn take_counter(bpf: &mut Ebpf, name: &str) -> Option<PerCpuArray<MapData, u64>> {
    bpf.take_map(name)
        .and_then(|m| PerCpuArray::try_from(m).ok())
}

/// read_counter performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn read_counter(map: &PerCpuArray<MapData, u64>) -> u64 {
    map.get(&0, 0)
        .map(|vals| vals.iter().copied().sum())
        .unwrap_or(0)
}

/// report_counter performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn report_counter(map: &Option<PerCpuArray<MapData, u64>>, last: &mut u64, msg: &str) {
    if let Some(map) = map {
        let total = read_counter(map);
        if total > *last {
            metrics::ringbuf_dropped(total);
            warn!(total, delta = total - *last, "{msg}");
        }
        *last = total;
    }
}

/// attach_programs performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn attach_programs(bpf: &mut Ebpf, config: &Config) -> Result<()> {
    let mut attach = || -> Result<()> {
        disable_offloads(&config.iface);
        mtu::clamp_to(&config.iface, config.clamp_mtu);
        attach_xdp_ingress(bpf, &config.xdp_ingress_program, &config.iface)?;
        attach_tc_egress(bpf, &config.tc_egress_program, &config.iface)
    };
    if let Some(netns_path) = &config.netns_path {
        return with_network_namespace(netns_path, attach);
    }
    attach()
}

/// disable_offloads performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn disable_offloads(iface: &str) {
    for feature in ["tso", "gso", "gro", "lro"] {
        match std::process::Command::new("ethtool")
            .args(["-K", iface, feature, "off"])
            .output()
        {
            Ok(out) if out.status.success() => {
                info!(iface, feature, "disabled network offload");
            }
            Ok(out) => {
                warn!(
                    iface,
                    feature,
                    detail = %String::from_utf8_lossy(&out.stderr).trim(),
                    "could not disable offload (likely fixed on this device); continuing"
                );
            }
            Err(err) => {
                warn!(iface, feature, error = %err, "failed to run ethtool; continuing without offload disable");
            }
        }
    }
}

/// with_network_namespace performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn with_network_namespace<T>(netns_path: &PathBuf, f: impl FnOnce() -> Result<T>) -> Result<T> {
    let original = File::open("/proc/self/ns/net").context("open current network namespace")?;
    let target = File::open(netns_path)
        .with_context(|| format!("open target network namespace {}", netns_path.display()))?;

    set_network_namespace(target.as_raw_fd())
        .with_context(|| format!("enter target network namespace {}", netns_path.display()))?;

    let result = f();
    let restore =
        set_network_namespace(original.as_raw_fd()).context("restore original network namespace");

    match (result, restore) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(err), Ok(())) => Err(err),
        (Ok(_), Err(err)) => Err(err),
        (Err(err), Err(restore_err)) => Err(err).context(format!(
            "also failed to restore original network namespace: {restore_err}"
        )),
    }
}

/// set_network_namespace performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn set_network_namespace(fd: i32) -> Result<()> {
    let rc = unsafe { libc::setns(fd, libc::CLONE_NEWNET) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()).context("setns(CLONE_NEWNET)")
    }
}

/// attach_xdp_ingress performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn attach_xdp_ingress(bpf: &mut Ebpf, program_name: &str, iface: &str) -> Result<()> {
    let program: &mut Xdp = bpf
        .program_mut(program_name)
        .ok_or_else(|| anyhow!("XDP program {program_name} not found"))?
        .try_into()
        .with_context(|| format!("{program_name} is not an XDP program"))?;
    program
        .load()
        .with_context(|| format!("load XDP program {program_name}"))?;
    match program.attach(iface, XdpFlags::DRV_MODE) {
        Ok(_) => {
            metrics::attach_ok();
            info!(
                iface,
                program = program_name,
                mode = "drv",
                "attached XDP program"
            );
            Ok(())
        }
        Err(err) => {
            warn!(iface, program = program_name, error = %err, "XDP driver mode attach failed; trying skb mode");
            program
                .attach(iface, XdpFlags::SKB_MODE)
                .with_context(|| format!("attach XDP program {program_name} to {iface}"))?;
            metrics::attach_ok();
            Ok(())
        }
    }
}

/// attach_tc_egress performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn attach_tc_egress(bpf: &mut Ebpf, program_name: &str, iface: &str) -> Result<()> {
    match tc::qdisc_add_clsact(iface) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {}
        Err(err) => return Err(err).with_context(|| format!("add clsact qdisc to {iface}")),
    }
    let program: &mut SchedClassifier = bpf
        .program_mut(program_name)
        .ok_or_else(|| anyhow!("tc egress program {program_name} not found"))?
        .try_into()
        .with_context(|| format!("{program_name} is not a tc classifier program"))?;
    program
        .load()
        .with_context(|| format!("load tc egress program {program_name}"))?;
    program
        .attach(iface, TcAttachType::Egress)
        .with_context(|| format!("attach tc egress program {program_name} to {iface}"))?;
    metrics::attach_ok();
    info!(iface, program = program_name, "attached tc egress program");
    Ok(())
}

/// required_env performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn required_env(key: &str) -> Result<String> {
    env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow!("{key} must be set"))
}

/// env_or performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn env_or(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// optional_path_env performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn optional_path_env(key: &str) -> Option<PathBuf> {
    env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
}

/// env_usize performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn env_usize(key: &str, default: usize) -> usize {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => match value.parse() {
            Ok(parsed) => parsed,
            Err(err) => {
                warn!(key, value, error = %err, default, "invalid numeric environment value; using default");
                default
            }
        },
        _ => default,
    }
}

/// env_duration_ms performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn env_duration_ms(key: &str, default: Duration) -> Duration {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => match value.parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms),
            Err(err) => {
                warn!(key, value, error = %err, default_ms = default.as_millis(), "invalid duration environment value; using default");
                default
            }
        },
        _ => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iicpc_schemas_rust::OrderAckedBatch;
    use std::sync::{Mutex, OnceLock};

    const ENV_KEYS: &[&str] = &[
        "KAFKA_BROKERS",
        "ORDERS_ACKED_TOPIC",
        "SESSION_ID",
        "CONTESTANT_ID",
        "EBPF_IFACE",
        "EBPF_NETNS_PATH",
        "EBPF_OBJECT_PATH",
        "IICPC_EBPF_OBJECT",
        "EBPF_XDP_INGRESS_PROGRAM",
        "EBPF_TC_EGRESS_PROGRAM",
        "EBPF_RINGBUF_MAP",
        "EBPF_FLUSH_INTERVAL_MS",
        "EBPF_BATCH_SIZE",
        "CAPTURE_CLAMP_MTU",
    ];

    /// env_lock performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    /// clear_test_env performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn clear_test_env() {
        for key in ENV_KEYS {
            unsafe {
                env::remove_var(key);
            }
        }
    }

    /// set_env performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn set_env(key: &str, value: &str) {
        unsafe {
            env::set_var(key, value);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// kafka_integration_flush_publishes_real_orders_acked_batch performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn kafka_integration_flush_publishes_real_orders_acked_batch() {
        let Ok(brokers) = env::var("KAFKA_BROKERS") else {
            eprintln!("skipping real Kafka integration test: KAFKA_BROKERS is not set");
            return;
        };
        if brokers.trim().is_empty() {
            eprintln!("skipping real Kafka integration test: KAFKA_BROKERS is empty");
            return;
        }

        kafka::ensure_topics(&brokers, &[TOPIC_ORDERS_ACKED])
            .await
            .expect("ensure orders.acked topic");
        let suffix = integration_suffix();
        let session_id = format!("itest-ebpf-session-{suffix}");
        let contestant_id = format!("itest-contestant-{suffix}");
        let consumer_group = format!("itest-ebpf-acked-{suffix}");
        let consumer = kafka::consumer(
            &brokers,
            &consumer_group,
            &[TOPIC_ORDERS_ACKED],
            std::time::Duration::from_secs(300),
        )
        .expect("create orders.acked consumer");
        let producer = kafka::telemetry_producer(&brokers).expect("create telemetry producer");
        let config = Config {
            kafka_brokers: brokers,
            topic: TOPIC_ORDERS_ACKED.to_string(),
            session_id: session_id.clone(),
            contestant_id: contestant_id.clone(),
            iface: "eth0".to_string(),
            netns_path: None,
            object_path: PathBuf::from("/tmp/unused-ebpf-object.o"),
            xdp_ingress_program: DEFAULT_XDP_INGRESS_PROGRAM.to_string(),
            tc_egress_program: DEFAULT_TC_EGRESS_PROGRAM.to_string(),
            ringbuf_map: DEFAULT_RINGBUF_MAP.to_string(),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            batch_size: DEFAULT_BATCH_SIZE,
            clamp_mtu: DEFAULT_CLAMP_MTU,
            orders_partitions: 24,
        };
        let mut events = vec![MatchedEvent {
            order_id: format!("order-{suffix}"),
            src_ip: 0x7f000001,
            src_port: 9898,
            tcp_seq: 42,
            t3_ns: 100,
            t7_ns: 175,
            pod_service_time_ns: 75,
            exec_type: "FILL".to_string(),
            fill_qty: 10,
            fill_price: 123_000_000_000,
            orig_order_id: format!("order-{suffix}"),
            reordering_detected: true,
            retransmission_count: 1,
        }];

        flush(&producer, &config, &mut events)
            .await
            .expect("flush orders.acked to Kafka");
        assert!(events.is_empty(), "flush should clear events after publish");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for orders.acked batch"
            );
            let msg = tokio::time::timeout(remaining, kafka::recv_message(&consumer))
                .await
                .expect("receive timeout")
                .expect("receive Kafka message");
            let Some(payload) = msg.payload.as_deref() else {
                kafka::commit_message(&consumer, &msg).expect("commit tombstone");
                continue;
            };
            let batch = match rmp_serde::from_slice::<OrderAckedBatch>(payload) {
                Ok(batch) => batch,
                Err(_) => {
                    kafka::commit_message(&consumer, &msg).expect("commit unrelated message");
                    continue;
                }
            };
            kafka::commit_message(&consumer, &msg).expect("commit orders.acked message");
            if batch.session_id != session_id {
                continue;
            }
            assert_eq!(batch.contestant_id, contestant_id);
            assert_eq!(batch.events.len(), 1);
            assert_eq!(batch.events[0].pod_service_time_ns, 75);
            assert!(batch.events[0].reordering_detected);
            break;
        }
    }

    /// integration_suffix performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn integration_suffix() -> String {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_string()
    }

    #[test]
    /// config_from_env_uses_required_values_and_defaults performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn config_from_env_uses_required_values_and_defaults() {
        let _guard = env_lock();
        clear_test_env();
        set_env("SESSION_ID", "session-a");
        set_env("CONTESTANT_ID", "contestant-a");
        set_env("EBPF_IFACE", "eth0");
        set_env("EBPF_OBJECT_PATH", "/opt/iicpc/ebpf/iicpc_latency.bpf.o");

        let config = Config::from_env().unwrap();
        assert_eq!(config.kafka_brokers, "localhost:9092");
        assert_eq!(config.topic, TOPIC_ORDERS_ACKED);
        assert_eq!(config.session_id, "session-a");
        assert_eq!(config.contestant_id, "contestant-a");
        assert_eq!(config.iface, "eth0");
        assert_eq!(config.netns_path, None);
        assert_eq!(config.flush_interval, DEFAULT_FLUSH_INTERVAL);
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(config.clamp_mtu, DEFAULT_CLAMP_MTU);
        config.validate().unwrap();
        clear_test_env();
    }

    #[test]
    /// config_from_env_parses_capture_clamp_mtu performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn config_from_env_parses_capture_clamp_mtu() {
        let _guard = env_lock();
        let cases: &[(&str, usize)] = &[
            ("9001", 9001),                      // explicit override
            ("0", 0),                            // explicit disable
            ("", DEFAULT_CLAMP_MTU),             // empty -> default
            ("not-a-number", DEFAULT_CLAMP_MTU), // invalid -> default (warn)
        ];
        for &(value, want) in cases {
            clear_test_env();
            set_env("SESSION_ID", "session-a");
            set_env("CONTESTANT_ID", "contestant-a");
            set_env("EBPF_IFACE", "eth0");
            set_env("EBPF_OBJECT_PATH", "/tmp/latency.o");
            set_env("CAPTURE_CLAMP_MTU", value);
            let config = Config::from_env().unwrap();
            assert_eq!(config.clamp_mtu, want, "CAPTURE_CLAMP_MTU={value:?}");
        }
        clear_test_env();
    }

    #[test]
    /// config_validate_rejects_out_of_range_clamp_mtu performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn config_validate_rejects_out_of_range_clamp_mtu() {
        let _guard = env_lock();
        let cases: &[(&str, bool)] = &[
            ("0", true), // disabled
            ("68", true),
            ("1500", true),
            ("65535", true),
            ("67", false),
            ("65536", false),
        ];
        for &(value, valid) in cases {
            clear_test_env();
            set_env("SESSION_ID", "session-a");
            set_env("CONTESTANT_ID", "contestant-a");
            set_env("EBPF_IFACE", "eth0");
            set_env("EBPF_OBJECT_PATH", "/tmp/latency.o");
            set_env("CAPTURE_CLAMP_MTU", value);
            let config = Config::from_env().unwrap();
            assert_eq!(
                config.validate().is_ok(),
                valid,
                "CAPTURE_CLAMP_MTU={value}"
            );
        }
        clear_test_env();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// shutdown_signal_resolves_on_sigterm_and_sigint performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    async fn shutdown_signal_resolves_on_sigterm_and_sigint() {
        let mut shutdown = ShutdownSignal::new().expect("install signal handlers");

        unsafe { libc::raise(libc::SIGTERM) };
        let name = time::timeout(Duration::from_secs(5), shutdown.recv())
            .await
            .expect("SIGTERM not observed within 5s");
        assert_eq!(name, "SIGTERM");

        unsafe { libc::raise(libc::SIGINT) };
        let name = time::timeout(Duration::from_secs(5), shutdown.recv())
            .await
            .expect("SIGINT not observed within 5s");
        assert_eq!(name, "SIGINT");
    }

    #[test]
    /// config_from_env_rejects_missing_required_values performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn config_from_env_rejects_missing_required_values() {
        let _guard = env_lock();
        clear_test_env();
        set_env("SESSION_ID", "session-a");
        set_env("CONTESTANT_ID", "contestant-a");
        set_env("EBPF_OBJECT_PATH", "/tmp/latency.o");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("EBPF_IFACE must be set"));
        clear_test_env();
    }
}
