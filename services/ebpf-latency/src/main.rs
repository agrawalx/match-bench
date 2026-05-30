use std::fs::File;
use std::io::ErrorKind;
use std::os::fd::AsRawFd;
use std::{env, mem, path::PathBuf, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use aya::{
    maps::{MapData, RingBuf},
    programs::{tc, SchedClassifier, TcAttachType, Xdp, XdpFlags},
    Ebpf,
};
use iicpc_bot_fleet::kafka::{self, KafkaProducer};
use iicpc_schemas_rust::{OrderAckedBatchRef, OrderAckedEventRef, TOPIC_ORDERS_ACKED};
use tokio::time;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_RINGBUF_MAP: &str = "EVENTS";
const DEFAULT_XDP_INGRESS_PROGRAM: &str = "iicpc_xdp_ingress";
const DEFAULT_TC_EGRESS_PROGRAM: &str = "iicpc_tc_egress";
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(5);
const DEFAULT_BATCH_SIZE: usize = 4096;
const MAX_ORDER_ID_LEN: usize = 96;
const MAX_EXEC_TYPE_LEN: usize = 16;
const KERNEL_EVENT_SIZE: usize = 272;

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
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

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

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                flush(&producer, &config, &mut events).await?;
                return Ok(());
            }
            _ = ticker.tick() => {
                drain_ringbuf(&mut ringbuf, &producer, &config, &mut events).await?;
                flush(&producer, &config, &mut events).await?;
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
            Ok(event) => events.push(event),
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

fn decode_kernel_event(bytes: &[u8]) -> Result<DecodedOrderAckedEvent> {
    if bytes.len() != mem::size_of::<KernelEvent>() {
        bail!(
            "unexpected eBPF event size: got {}, want {}",
            bytes.len(),
            mem::size_of::<KernelEvent>()
        );
    }

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
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_duration_ms(key: &str, default: Duration) -> Duration {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(default)
}
