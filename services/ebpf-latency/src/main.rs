use std::fs::File;
use std::io::ErrorKind;
use std::os::fd::AsRawFd;
use std::{env, mem, path::PathBuf, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use aya::{
    maps::{MapData, PerCpuArray, RingBuf},
    programs::{tc, SchedClassifier, TcAttachType, Xdp, XdpFlags},
    Ebpf,
};
use iicpc_bot_fleet::kafka::{self, KafkaProducer};
use iicpc_logger_rust::loki;
use iicpc_schemas_rust::{OrderAckedBatchRef, OrderAckedEventRef, TOPIC_ORDERS_ACKED};
use tokio::time;
use tracing::{info, warn};

const DEFAULT_RINGBUF_MAP: &str = "EVENTS";
const DROPPED_EVENTS_MAP: &str = "DROPPED_EVENTS";
const DEFAULT_XDP_INGRESS_PROGRAM: &str = "iicpc_xdp_ingress";
const DEFAULT_TC_EGRESS_PROGRAM: &str = "iicpc_tc_egress";
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(5);
const DEFAULT_BATCH_SIZE: usize = 4096;
const MAX_ORDER_ID_LEN: usize = 32;
const MAX_EXEC_TYPE_LEN: usize = 16;
const KERNEL_EVENT_SIZE: usize = 144;

/// KernelEvent is the fixed ABI written by the eBPF program into the ringbuf.
///
/// Keep this layout in sync with the eBPF-side event struct. The order id is
/// an inline, NUL-free byte slice to avoid kernel allocations and userspace
/// pointer chasing in the hot path.
#[repr(C)]
#[derive(Clone, Copy)]
struct KernelEvent {
    t3_xdp_ingress_ns: u64,
    t7_xdp_egress_ns: u64,
    pod_service_time_ns: u64,
    fill_qty: u64,
    fill_price: u64,
    src_ip: u32,
    tcp_seq: u32,
    retransmission_count: u32,
    src_port: u16,
    order_id_len: u16,
    exec_type_len: u16,
    orig_order_id_len: u16,
    flags: u16,
    _pad: u16,
    order_id: [u8; MAX_ORDER_ID_LEN],
    exec_type: [u8; MAX_EXEC_TYPE_LEN],
    orig_order_id: [u8; MAX_ORDER_ID_LEN],
}

const _: [(); KERNEL_EVENT_SIZE] = [(); mem::size_of::<KernelEvent>()];

#[derive(Debug)]
struct DecodedOrderAckedEvent {
    order_id: String,
    src_ip: u32,
    src_port: u16,
    tcp_seq: u32,
    t3_xdp_ingress_ns: u64,
    t7_xdp_egress_ns: u64,
    pod_service_time_ns: u64,
    exec_type: String,
    fill_qty: u64,
    fill_price: u64,
    orig_order_id: String,
    reordering_detected: bool,
    retransmission_count: u32,
}

impl DecodedOrderAckedEvent {
    fn as_schema_ref<'a>(
        &'a self,
        session_id: &'a str,
        contestant_id: &'a str,
    ) -> OrderAckedEventRef<'a> {
        OrderAckedEventRef {
            session_id,
            contestant_id,
            order_id: &self.order_id,
            src_ip: self.src_ip,
            src_port: self.src_port,
            tcp_seq: self.tcp_seq,
            t3_xdp_ingress_ns: self.t3_xdp_ingress_ns,
            t7_xdp_egress_ns: self.t7_xdp_egress_ns,
            pod_service_time_ns: self.pod_service_time_ns,
            exec_type: &self.exec_type,
            fill_qty: self.fill_qty,
            fill_price: self.fill_price,
            orig_order_id: &self.orig_order_id,
            reordering_detected: self.reordering_detected,
            retransmission_count: self.retransmission_count,
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    kafka_brokers: String,
    topic: String,
    session_id: String,
    contestant_id: String,
    iface: String, //eth0, eth3, etc.
    netns_path: Option<PathBuf>,
    object_path: PathBuf, // epbf object file compiled from ebpf/ directory
    xdp_ingress_program: String,
    tc_egress_program: String,
    ringbuf_map: String,
    flush_interval: Duration,
    batch_size: usize,
}

impl Config {
    fn from_env() -> Result<Self> {
        let object_path = env::var("EBPF_OBJECT_PATH")
            .or_else(|_| env::var("IICPC_EBPF_OBJECT"))
            .context("EBPF_OBJECT_PATH must point at the compiled eBPF object")?;

        Ok(Self {
            kafka_brokers: env_or("KAFKA_BROKERS", "localhost:9092"),
            topic: env_or("ORDERS_ACKED_TOPIC", TOPIC_ORDERS_ACKED),
            session_id: required_env("SESSION_ID")?,
            contestant_id: required_env("CONTESTANT_ID")?,
            iface: required_env("EBPF_IFACE")?,
            netns_path: optional_path_env("EBPF_NETNS_PATH"),
            object_path: PathBuf::from(object_path),
            xdp_ingress_program: env_or("EBPF_XDP_INGRESS_PROGRAM", DEFAULT_XDP_INGRESS_PROGRAM),
            tc_egress_program: env_or("EBPF_TC_EGRESS_PROGRAM", DEFAULT_TC_EGRESS_PROGRAM),
            ringbuf_map: env_or("EBPF_RINGBUF_MAP", DEFAULT_RINGBUF_MAP),
            flush_interval: env_duration_ms("EBPF_FLUSH_INTERVAL_MS", DEFAULT_FLUSH_INTERVAL),
            batch_size: env_usize("EBPF_BATCH_SIZE", DEFAULT_BATCH_SIZE),
        })
    }

    fn validate(&self) -> Result<()> {
        if self.batch_size == 0 {
            bail!("EBPF_BATCH_SIZE must be greater than zero");
        }
        if self.flush_interval.is_zero() {
            bail!("EBPF_FLUSH_INTERVAL_MS must be greater than zero");
        }
        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let _loki_guard = loki::init("ebpf-latency");

    let config = Config::from_env()?;
    config.validate()?;

    let producer = kafka::telemetry_producer(&config.kafka_brokers)?;
    run(config, producer).await
}

async fn run(config: Config, producer: KafkaProducer) -> Result<()> {
    let mut bpf = Ebpf::load_file(&config.object_path)
        .with_context(|| format!("load eBPF object {}", config.object_path.display()))?;

    attach_programs(&mut bpf, &config)?;
    let mut ringbuf = RingBuf::try_from(
        bpf.take_map(&config.ringbuf_map)
            .ok_or_else(|| anyhow!("ringbuf map {} not found", config.ringbuf_map))?,
    )
    .with_context(|| format!("open ringbuf map {}", config.ringbuf_map))?;
    let dropped_events = bpf
        .take_map(DROPPED_EVENTS_MAP)
        .map(PerCpuArray::<_, u64>::try_from)
        .transpose()
        .with_context(|| format!("open dropped-event counter map {DROPPED_EVENTS_MAP}"))?;

    info!(
        iface = %config.iface,
        session_id = %config.session_id,
        contestant_id = %config.contestant_id,
        topic = %config.topic,
        netns_path = config.netns_path.as_ref().map(|path| path.display().to_string()).as_deref().unwrap_or("current"),
        xdp_ingress_program = %config.xdp_ingress_program,
        tc_egress_program = %config.tc_egress_program,
        "eBPF wire-to-wire latency publisher started"
    );

    let mut ticker = time::interval(config.flush_interval);
    let mut events = Vec::with_capacity(config.batch_size);
    let mut last_dropped_events = 0u64;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                flush(&producer, &config, &mut events).await?;
                return Ok(());
            }
            _ = ticker.tick() => {
                drain_ringbuf(&mut ringbuf, &producer, &config, &mut events).await?;
                flush(&producer, &config, &mut events).await?;
                if let Some(dropped_events) = &dropped_events {
                    let dropped = total_dropped_events(dropped_events)
                        .context("read eBPF dropped-event counter")?;
                    if dropped > last_dropped_events {
                        warn!(
                            dropped_events = dropped,
                            dropped_since_last_poll = dropped - last_dropped_events,
                            "eBPF ring buffer dropped events"
                        );
                    }
                    last_dropped_events = dropped;
                }
            }
        }
    }
}

fn attach_programs(bpf: &mut Ebpf, config: &Config) -> Result<()> {
    let mut attach = || -> Result<()> {
        attach_xdp_ingress(bpf, &config.xdp_ingress_program, &config.iface)?;
        attach_tc_egress(bpf, &config.tc_egress_program, &config.iface)
    };

    if let Some(netns_path) = &config.netns_path {
        return with_network_namespace(netns_path, attach);
    }

    attach()
}

// switch namespace and execute the provided closure, then switch back to the original namespace. This is necessary to attach eBPF programs to interfaces in a different network namespace.
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

fn set_network_namespace(fd: i32) -> Result<()> {
    let rc = unsafe { libc::setns(fd, libc::CLONE_NEWNET) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()).context("setns(CLONE_NEWNET)")
    }
}

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
            info!(
                iface,
                program = program_name,
                mode = "drv",
                "attached XDP program"
            );
            Ok(())
        }
        Err(err) => {
            warn!(
                iface,
                program = program_name,
                error = %err,
                "XDP driver mode attach failed; trying skb mode"
            );
            program
                .attach(iface, XdpFlags::SKB_MODE)
                .with_context(|| format!("attach XDP program {program_name} to {iface}"))?;
            Ok(())
        }
    }
}

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

    info!(iface, program = program_name, "attached tc egress program");
    Ok(())
}

async fn drain_ringbuf(
    ringbuf: &mut RingBuf<MapData>,
    producer: &KafkaProducer,
    config: &Config,
    events: &mut Vec<DecodedOrderAckedEvent>,
) -> Result<()> {
    while let Some(item) = ringbuf.next() {
        match decode_kernel_event(&item) {
            Ok(event) => {
                info!(
                    order_id = %event.order_id,
                    src_ip = event.src_ip,
                    src_port = event.src_port,
                    tcp_seq = event.tcp_seq,
                    request_ingress_t3_ns = event.t3_xdp_ingress_ns,
                    response_egress_t7_ns = event.t7_xdp_egress_ns,
                    pod_service_time_ns = event.pod_service_time_ns,
                    exec_type = %event.exec_type,
                    fill_qty = event.fill_qty,
                    fill_price = event.fill_price,
                    retransmission_count = event.retransmission_count,
                    reordering_detected = event.reordering_detected,
                    "decoded eBPF request/response boundary timestamps"
                );
                events.push(event);
            }
            Err(err) => {
                warn!(error = %err, "dropping malformed eBPF event");
                continue;
            }
        }
        if events.len() >= config.batch_size {
            flush(producer, config, events).await?;
        }
    }
    Ok(())
}

fn total_dropped_events(map: &PerCpuArray<MapData, u64>) -> Result<u64> {
    let values = map.get(&0, 0).context("read DROPPED_EVENTS[0]")?;
    Ok(values.iter().copied().sum())
}

fn decode_kernel_event(bytes: &[u8]) -> Result<DecodedOrderAckedEvent> {
    if bytes.len() != mem::size_of::<KernelEvent>() {
        bail!(
            "unexpected eBPF event size: got {}, want {}",
            bytes.len(),
            mem::size_of::<KernelEvent>()
        );
    }

    // Ring buffer samples are byte slices and are not guaranteed to be aligned
    // for KernelEvent, so this must stay as an unaligned read.
    let raw = unsafe { (bytes.as_ptr() as *const KernelEvent).read_unaligned() };
    let order_id_len = usize::from(raw.order_id_len);
    if order_id_len == 0 || order_id_len > raw.order_id.len() {
        bail!("invalid order_id_len from eBPF event: {order_id_len}");
    }
    let order_id = std::str::from_utf8(&raw.order_id[..order_id_len])
        .context("eBPF event order_id is not UTF-8")?
        .to_string();
    let exec_type =
        decode_inline_utf8(&raw.exec_type, usize::from(raw.exec_type_len), "exec_type")?;
    let orig_order_id = decode_inline_utf8(
        &raw.orig_order_id,
        usize::from(raw.orig_order_id_len),
        "orig_order_id",
    )?;
    if raw.orig_order_id_len == 0 {
        warn!(
            order_id = %order_id,
            exec_type = %exec_type,
            "eBPF event has empty orig_order_id"
        );
    }

    Ok(DecodedOrderAckedEvent {
        order_id,
        src_ip: raw.src_ip,
        src_port: raw.src_port,
        tcp_seq: raw.tcp_seq,
        t3_xdp_ingress_ns: raw.t3_xdp_ingress_ns,
        t7_xdp_egress_ns: raw.t7_xdp_egress_ns,
        pod_service_time_ns: raw.pod_service_time_ns,
        exec_type,
        fill_qty: raw.fill_qty,
        fill_price: raw.fill_price,
        orig_order_id,
        reordering_detected: raw.flags & 0x1 != 0,
        retransmission_count: raw.retransmission_count,
    })
}

fn decode_inline_utf8<const N: usize>(bytes: &[u8; N], len: usize, field: &str) -> Result<String> {
    if len > N {
        bail!("invalid {field}_len from eBPF event: {len}");
    }
    std::str::from_utf8(&bytes[..len])
        .with_context(|| format!("eBPF event {field} is not UTF-8"))
        .map(str::to_string)
}

async fn flush(
    producer: &KafkaProducer,
    config: &Config,
    events: &mut Vec<DecodedOrderAckedEvent>,
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let event_refs = events
        .iter()
        .map(|event| event.as_schema_ref(&config.session_id, &config.contestant_id))
        .collect::<Vec<_>>();
    let batch = OrderAckedBatchRef {
        session_id: &config.session_id,
        contestant_id: &config.contestant_id,
        events: &event_refs,
    };
    let payload = rmp_serde::to_vec_named(&batch).context("encode orders.acked messagepack")?;
    events.clear();
    kafka::publish_bytes(producer, &config.topic, &config.contestant_id, &payload).await?;
    Ok(())
}

fn required_env(key: &str) -> Result<String> {
    env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow!("{key} must be set"))
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn optional_path_env(key: &str) -> Option<PathBuf> {
    env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
}

fn env_usize(key: &str, default: usize) -> usize {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => match value.parse() {
            Ok(parsed) => parsed,
            Err(err) => {
                warn!(
                    key,
                    value,
                    error = %err,
                    default,
                    "invalid numeric environment value; using default"
                );
                default
            }
        },
        _ => default,
    }
}

fn env_duration_ms(key: &str, default: Duration) -> Duration {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => match value.parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms),
            Err(err) => {
                warn!(
                    key,
                    value,
                    error = %err,
                    default_ms = default.as_millis(),
                    "invalid duration environment value; using default"
                );
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
    ];

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn clear_test_env() {
        for key in ENV_KEYS {
            unsafe {
                env::remove_var(key);
            }
        }
    }

    fn set_env(key: &str, value: &str) {
        unsafe {
            env::set_var(key, value);
        }
    }

    fn base_kernel_event() -> KernelEvent {
        let mut event = KernelEvent {
            t3_xdp_ingress_ns: 100,
            t7_xdp_egress_ns: 450,
            pod_service_time_ns: 350,
            fill_qty: 12,
            fill_price: 42_500_000_000,
            src_ip: 0x0a00_0001,
            tcp_seq: 123_456,
            retransmission_count: 2,
            src_port: 51_234,
            order_id_len: 0,
            exec_type_len: 0,
            orig_order_id_len: 0,
            flags: 0x1,
            _pad: 0,
            order_id: [0; MAX_ORDER_ID_LEN],
            exec_type: [0; MAX_EXEC_TYPE_LEN],
            orig_order_id: [0; MAX_ORDER_ID_LEN],
        };
        write_inline(&mut event.order_id, &mut event.order_id_len, b"order-123");
        write_inline(&mut event.exec_type, &mut event.exec_type_len, b"FILL");
        write_inline(
            &mut event.orig_order_id,
            &mut event.orig_order_id_len,
            b"orig-123",
        );
        event
    }

    fn write_inline<const N: usize>(dst: &mut [u8; N], len: &mut u16, value: &[u8]) {
        dst[..value.len()].copy_from_slice(value);
        *len = value.len().try_into().unwrap();
    }

    fn event_bytes(event: &KernelEvent) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (event as *const KernelEvent).cast::<u8>(),
                mem::size_of::<KernelEvent>(),
            )
        }
    }

    #[test]
    fn kernel_event_abi_size_is_stable() {
        assert_eq!(mem::size_of::<KernelEvent>(), KERNEL_EVENT_SIZE);
        assert_eq!(mem::size_of::<KernelEvent>(), 144);
    }

    #[test]
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
        assert_eq!(
            config.object_path,
            PathBuf::from("/opt/iicpc/ebpf/iicpc_latency.bpf.o")
        );
        assert_eq!(config.xdp_ingress_program, DEFAULT_XDP_INGRESS_PROGRAM);
        assert_eq!(config.tc_egress_program, DEFAULT_TC_EGRESS_PROGRAM);
        assert_eq!(config.ringbuf_map, DEFAULT_RINGBUF_MAP);
        assert_eq!(config.flush_interval, DEFAULT_FLUSH_INTERVAL);
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        config.validate().unwrap();

        clear_test_env();
    }

    #[test]
    fn config_from_env_honors_overrides_and_object_alias() {
        let _guard = env_lock();
        clear_test_env();
        set_env("SESSION_ID", "session-b");
        set_env("CONTESTANT_ID", "contestant-b");
        set_env("EBPF_IFACE", "veth0");
        set_env("IICPC_EBPF_OBJECT", "/tmp/latency.o");
        set_env("KAFKA_BROKERS", "kafka:9092");
        set_env("ORDERS_ACKED_TOPIC", "custom.acked");
        set_env("EBPF_NETNS_PATH", "/proc/123/ns/net");
        set_env("EBPF_XDP_INGRESS_PROGRAM", "xdp_custom");
        set_env("EBPF_TC_EGRESS_PROGRAM", "tc_custom");
        set_env("EBPF_RINGBUF_MAP", "CUSTOM_EVENTS");
        set_env("EBPF_FLUSH_INTERVAL_MS", "25");
        set_env("EBPF_BATCH_SIZE", "64");

        let config = Config::from_env().unwrap();

        assert_eq!(config.kafka_brokers, "kafka:9092");
        assert_eq!(config.topic, "custom.acked");
        assert_eq!(config.object_path, PathBuf::from("/tmp/latency.o"));
        assert_eq!(config.netns_path, Some(PathBuf::from("/proc/123/ns/net")));
        assert_eq!(config.xdp_ingress_program, "xdp_custom");
        assert_eq!(config.tc_egress_program, "tc_custom");
        assert_eq!(config.ringbuf_map, "CUSTOM_EVENTS");
        assert_eq!(config.flush_interval, Duration::from_millis(25));
        assert_eq!(config.batch_size, 64);
        config.validate().unwrap();

        clear_test_env();
    }

    #[test]
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

    #[test]
    fn config_validate_rejects_zero_batch_size_and_flush_interval() {
        let mut config = Config {
            kafka_brokers: "localhost:9092".to_string(),
            topic: TOPIC_ORDERS_ACKED.to_string(),
            session_id: "session-a".to_string(),
            contestant_id: "contestant-a".to_string(),
            iface: "eth0".to_string(),
            netns_path: None,
            object_path: PathBuf::from("/tmp/latency.o"),
            xdp_ingress_program: DEFAULT_XDP_INGRESS_PROGRAM.to_string(),
            tc_egress_program: DEFAULT_TC_EGRESS_PROGRAM.to_string(),
            ringbuf_map: DEFAULT_RINGBUF_MAP.to_string(),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            batch_size: 0,
        };

        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("EBPF_BATCH_SIZE"));

        config.batch_size = 1;
        config.flush_interval = Duration::ZERO;
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("EBPF_FLUSH_INTERVAL_MS"));
    }

    #[test]
    fn decode_kernel_event_maps_all_fields() {
        let event = base_kernel_event();

        let decoded = decode_kernel_event(event_bytes(&event)).unwrap();

        assert_eq!(decoded.order_id, "order-123");
        assert_eq!(decoded.src_ip, 0x0a00_0001);
        assert_eq!(decoded.src_port, 51_234);
        assert_eq!(decoded.tcp_seq, 123_456);
        assert_eq!(decoded.t3_xdp_ingress_ns, 100);
        assert_eq!(decoded.t7_xdp_egress_ns, 450);
        assert_eq!(decoded.pod_service_time_ns, 350);
        assert_eq!(decoded.exec_type, "FILL");
        assert_eq!(decoded.fill_qty, 12);
        assert_eq!(decoded.fill_price, 42_500_000_000);
        assert_eq!(decoded.orig_order_id, "orig-123");
        assert!(decoded.reordering_detected);
        assert_eq!(decoded.retransmission_count, 2);
    }

    #[test]
    fn decode_kernel_event_rejects_wrong_size_and_invalid_lengths() {
        assert!(decode_kernel_event(&[0; KERNEL_EVENT_SIZE - 1])
            .unwrap_err()
            .to_string()
            .contains("unexpected eBPF event size"));

        let mut event = base_kernel_event();
        event.order_id_len = 0;
        assert!(decode_kernel_event(event_bytes(&event))
            .unwrap_err()
            .to_string()
            .contains("invalid order_id_len"));

        let mut event = base_kernel_event();
        event.exec_type_len = (MAX_EXEC_TYPE_LEN + 1).try_into().unwrap();
        assert!(decode_kernel_event(event_bytes(&event))
            .unwrap_err()
            .to_string()
            .contains("invalid exec_type_len"));
    }

    #[test]
    fn decode_kernel_event_rejects_invalid_utf8() {
        let mut event = base_kernel_event();
        event.order_id[0] = 0xff;

        assert!(decode_kernel_event(event_bytes(&event))
            .unwrap_err()
            .to_string()
            .contains("order_id is not UTF-8"));
    }

    #[test]
    fn decoded_event_serializes_to_order_acked_batch_schema() {
        let event = decode_kernel_event(event_bytes(&base_kernel_event())).unwrap();
        let event_refs = [event.as_schema_ref("session-a", "contestant-a")];
        let batch = OrderAckedBatchRef {
            session_id: "session-a",
            contestant_id: "contestant-a",
            events: &event_refs,
        };

        let payload = rmp_serde::to_vec_named(&batch).unwrap();
        let decoded: OrderAckedBatch = rmp_serde::from_slice(&payload).unwrap();

        assert_eq!(decoded.session_id, "session-a");
        assert_eq!(decoded.contestant_id, "contestant-a");
        assert_eq!(decoded.events.len(), 1);
        let event = &decoded.events[0];
        assert_eq!(event.session_id, "session-a");
        assert_eq!(event.contestant_id, "contestant-a");
        assert_eq!(event.order_id, "order-123");
        assert_eq!(event.pod_service_time_ns, 350);
        assert_eq!(event.exec_type, "FILL");
        assert_eq!(event.retransmission_count, 2);
    }
}
