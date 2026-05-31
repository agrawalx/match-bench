use std::{
    fs::{self, File},
    io::{Read, Write},
    net::TcpStream,
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use aya::{
    maps::{MapData, PerCpuArray, RingBuf},
    programs::{tc, SchedClassifier, TcAttachType, Xdp, XdpFlags},
    Ebpf,
};

const MAX_ORDER_ID_LEN: usize = 32;
const MAX_EXEC_TYPE_LEN: usize = 16;
const KERNEL_EVENT_SIZE: usize = 144;
const DEBUG_COUNTER_LABELS: [&str; 14] = [
    "xdp_match",
    "xdp_insert",
    "xdp_existing",
    "tc_match",
    "tc_load_ok",
    "tc_parse_fix_ok",
    "tc_parse_http_ok",
    "tc_parse_ws_ok",
    "tc_lookup_miss",
    "tc_lookup_hit",
    "tc_event_submit",
    "tc_reserve_fail",
    "tc_entry",
    "tc_bounds_miss",
];

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

const _: [(); KERNEL_EVENT_SIZE] = [(); std::mem::size_of::<KernelEvent>()];

#[test]
#[ignore = "requires root, bpf-linker/nightly or EBPF_OBJECT_PATH, and local netns privileges"]
fn attaches_real_ebpf_to_netns_veth_and_observes_fix_rest_ws_roundtrips() -> Result<()> {
    log_step("starting real eBPF integration test");
    if !require_root()? || !require_command("ip")? || !require_command("python3")? {
        return Ok(());
    }

    let Some(object_path) = ebpf_object_path()? else {
        return Ok(());
    };
    log_step(format!("using eBPF object {}", object_path.display()));

    log_step("creating temporary network namespace and veth pair");
    let fixture = NetnsFixture::create()?;
    log_step(format!(
        "fixture ready: netns={}, netns_path={}, host_veth={}, pod_iface=eth0",
        fixture.ns_name,
        fixture.netns_path.display(),
        fixture.host_veth
    ));

    log_step("loading eBPF object with Aya");
    let mut bpf = Ebpf::load_file(&object_path)
        .with_context(|| format!("load eBPF object {}", object_path.display()))?;

    log_step("entering target netns and attaching XDP ingress + tc egress to eth0");
    with_network_namespace(&fixture.netns_path, || {
        attach_xdp_ingress(&mut bpf, "iicpc_xdp_ingress", "eth0")?;
        attach_tc_egress(&mut bpf, "iicpc_tc_egress", "eth0")
    })?;
    log_step("programs attached; opening EVENTS ringbuf");

    let mut ringbuf = RingBuf::try_from(
        bpf.take_map("EVENTS")
            .ok_or_else(|| anyhow!("ringbuf map EVENTS not found"))?,
    )
    .context("open EVENTS ringbuf")?;
    let dropped_events = bpf
        .take_map("DROPPED_EVENTS")
        .map(PerCpuArray::<_, u64>::try_from)
        .transpose()
        .context("open DROPPED_EVENTS map")?;
    let debug_counters = bpf
        .take_map("DEBUG_COUNTERS")
        .map(PerCpuArray::<_, u64>::try_from)
        .transpose()
        .context("open DEBUG_COUNTERS map")?;
    assert!(
        dropped_events.is_some(),
        "DROPPED_EVENTS map not found in eBPF object"
    );
    assert!(
        debug_counters.is_some(),
        "DEBUG_COUNTERS map not found in eBPF object"
    );
    log_step(
        "flow key contract: XDP ingress keys on (client_ip, client_port); tc egress keys on outbound (ip.daddr, tcp.dest), which is the same client tuple",
    );

    log_step("starting real TCP FIX server inside target network namespace");
    let mut server = fixture.spawn_fix_server()?;
    log_step("sending real FIX request from host namespace to netns server");
    send_fix_roundtrip()?;
    log_step("FIX response received; polling eBPF ringbuf for order-real-1");
    let event = read_matching_event(
        &mut ringbuf,
        dropped_events.as_ref(),
        debug_counters.as_ref(),
        "order-real-1",
        Duration::from_secs(3),
    )?;
    assert_event(&event, "order-real-1", "F", 12, 42_500_000_000);
    let _ = server.kill();
    let _ = server.wait();

    log_step("starting REST JSON server inside target network namespace");
    let mut server = fixture.spawn_rest_server()?;
    log_step("sending real REST JSON request from host namespace to netns server");
    send_rest_roundtrip()?;
    log_step("REST response received; polling eBPF ringbuf for order-rest-1");
    let event = read_matching_event(
        &mut ringbuf,
        dropped_events.as_ref(),
        debug_counters.as_ref(),
        "order-rest-1",
        Duration::from_secs(3),
    )?;
    assert_event(&event, "order-rest-1", "F", 7, 99_250_000_000);
    let _ = server.kill();
    let _ = server.wait();

    log_step("starting raw WebSocket frame server inside target network namespace");
    let mut server = fixture.spawn_ws_server()?;
    log_step("sending masked WebSocket order request from host namespace to netns server");
    send_ws_roundtrip()?;
    log_step("WebSocket response received; polling eBPF ringbuf for order-ws-1");
    let event = read_matching_event(
        &mut ringbuf,
        dropped_events.as_ref(),
        debug_counters.as_ref(),
        "order-ws-1",
        Duration::from_secs(3),
    )?;
    assert_event(&event, "order-ws-1", "F", 3, 11_750_000_000);
    let _ = server.kill();
    let _ = server.wait();

    log_step("real eBPF integration test completed");
    Ok(())
}

#[derive(Debug)]
struct DecodedEvent {
    order_id: String,
    exec_type: String,
    t3_xdp_ingress_ns: u64,
    t7_xdp_egress_ns: u64,
    pod_service_time_ns: u64,
    fill_qty: u64,
    fill_price: u64,
}

struct NetnsFixture {
    ns_name: String,
    netns_path: PathBuf,
    host_veth: String,
}

impl NetnsFixture {
    fn create() -> Result<Self> {
        let suffix = format!("{}-{}", std::process::id(), now_nanos());
        let ns_name = format!("iicpc-ebpf-{suffix}");
        let host_veth = format!("iicpc{}", &suffix[..suffix.len().min(8)]);
        let fixture = Self {
            netns_path: PathBuf::from(format!("/var/run/netns/{ns_name}")),
            ns_name,
            host_veth,
        };

        log_step(format!("ip netns add {}", fixture.ns_name));
        run_ip(&["netns", "add", &fixture.ns_name])?;
        log_step(format!(
            "ip link add {} type veth peer name eth0 netns {}",
            fixture.host_veth, fixture.ns_name
        ));
        run_ip(&[
            "link",
            "add",
            &fixture.host_veth,
            "type",
            "veth",
            "peer",
            "name",
            "eth0",
            "netns",
            &fixture.ns_name,
        ])?;
        log_step(format!(
            "assigning host veth address 10.241.0.1/24 on {}",
            fixture.host_veth
        ));
        run_ip(&["addr", "add", "10.241.0.1/24", "dev", &fixture.host_veth])?;
        run_ip(&["link", "set", &fixture.host_veth, "up"])?;
        log_step("assigning netns eth0 address 10.241.0.2/24 and bringing links up");
        run_ip(&[
            "netns",
            "exec",
            &fixture.ns_name,
            "ip",
            "addr",
            "add",
            "10.241.0.2/24",
            "dev",
            "eth0",
        ])?;
        run_ip(&[
            "netns",
            "exec",
            &fixture.ns_name,
            "ip",
            "link",
            "set",
            "lo",
            "up",
        ])?;
        run_ip(&[
            "netns",
            "exec",
            &fixture.ns_name,
            "ip",
            "link",
            "set",
            "eth0",
            "up",
        ])?;

        Ok(fixture)
    }

    fn spawn_fix_server(&self) -> Result<Child> {
        let script = r#"
import socket
response = b"8=FIX.4.2\x0135=8\x0111=order-real-1\x01150=F\x0139=2\x0132=12\x0131=42.5\x01"
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("10.241.0.2", 9898))
    s.listen(1)
    conn, _ = s.accept()
    with conn:
        conn.recv(4096)
        conn.sendall(response)
"#;
        let child = Command::new("ip")
            .args(["netns", "exec", &self.ns_name, "python3", "-c", script])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .context("spawn FIX server in netns")?;

        thread::sleep(Duration::from_millis(250));
        log_step("FIX server process spawned inside netns on 10.241.0.2:9898");
        Ok(child)
    }

    fn spawn_rest_server(&self) -> Result<Child> {
        let script = r#"
import socket
body = b'{"cl_ord_id":"order-rest-1","exec_type":"F","fill_qty":7,"fill_price":99.25}'
response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("10.241.0.2", 8080))
    s.listen(1)
    conn, _ = s.accept()
    with conn:
        conn.recv(4096)
        conn.sendall(response)
"#;
        let child = Command::new("ip")
            .args(["netns", "exec", &self.ns_name, "python3", "-c", script])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .context("spawn REST server in netns")?;

        thread::sleep(Duration::from_millis(250));
        log_step("REST server process spawned inside netns on 10.241.0.2:8080");
        Ok(child)
    }

    fn spawn_ws_server(&self) -> Result<Child> {
        let script = r#"
import socket
body = b'{"cl_ord_id":"order-ws-1","exec_type":"F","fill_qty":3,"fill_price":11.75}'
response = bytes([0x81, len(body)]) + body
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("10.241.0.2", 8080))
    s.listen(1)
    conn, _ = s.accept()
    with conn:
        conn.recv(4096)
        conn.sendall(response)
"#;
        let child = Command::new("ip")
            .args(["netns", "exec", &self.ns_name, "python3", "-c", script])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .context("spawn WebSocket server in netns")?;

        thread::sleep(Duration::from_millis(250));
        log_step("WebSocket server process spawned inside netns on 10.241.0.2:8080");
        Ok(child)
    }
}

impl Drop for NetnsFixture {
    fn drop(&mut self) {
        log_step(format!(
            "cleaning up fixture: deleting {} and netns {}",
            self.host_veth, self.ns_name
        ));
        let _ = Command::new("ip")
            .args(["link", "del", &self.host_veth])
            .status();
        let _ = Command::new("ip")
            .args(["netns", "del", &self.ns_name])
            .status();
    }
}

fn send_fix_roundtrip() -> Result<()> {
    let request =
        b"8=FIX.4.2\x0135=D\x0111=order-real-1\x0155=IICPC\x0138=12\x0144=42.5\x0154=1\x01";
    let start = Instant::now();
    let mut last_err = None;
    while start.elapsed() < Duration::from_secs(2) {
        match TcpStream::connect("10.241.0.2:9898") {
            Ok(mut stream) => {
                log_step("connected to netns FIX server over veth");
                stream.write_all(request).context("write FIX request")?;
                let mut response = [0; 256];
                let n = stream.read(&mut response).context("read FIX response")?;
                if n == 0 {
                    bail!("FIX server closed without response");
                }
                log_step(format!("received {n} bytes from netns FIX server"));
                return Ok(());
            }
            Err(err) => {
                last_err = Some(err);
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
    Err(last_err
        .map(anyhow::Error::from)
        .unwrap_or_else(|| anyhow!("timed out connecting to FIX server")))
    .context("connect to netns FIX server")
}

fn send_rest_roundtrip() -> Result<()> {
    let body = br#"{"cl_ord_id":"order-rest-1","qty":7,"price":99.25}"#;
    let request = format!(
        "POST /orders HTTP/1.1\r\nHost: 10.241.0.2\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    let mut stream = connect_with_retry("10.241.0.2:8080", "REST server")?;
    stream
        .write_all(request.as_bytes())
        .context("write REST headers")?;
    stream.write_all(body).context("write REST body")?;
    let mut response = [0; 512];
    let n = stream.read(&mut response).context("read REST response")?;
    if n == 0 {
        bail!("REST server closed without response");
    }
    log_step(format!("received {n} bytes from netns REST server"));
    Ok(())
}

fn send_ws_roundtrip() -> Result<()> {
    let body = br#"{"cl_ord_id":"order-ws-1","qty":3,"price":11.75}"#;
    let request = masked_ws_frame(body)?;
    let mut stream = connect_with_retry("10.241.0.2:8080", "WebSocket server")?;
    stream
        .write_all(&request)
        .context("write WebSocket frame")?;
    let mut response = [0; 256];
    let n = stream
        .read(&mut response)
        .context("read WebSocket response")?;
    if n == 0 {
        bail!("WebSocket server closed without response");
    }
    log_step(format!("received {n} bytes from netns WebSocket server"));
    Ok(())
}

fn connect_with_retry(addr: &str, label: &str) -> Result<TcpStream> {
    let start = Instant::now();
    let mut last_err = None;
    while start.elapsed() < Duration::from_secs(2) {
        match TcpStream::connect(addr) {
            Ok(stream) => {
                log_step(format!("connected to netns {label} over veth"));
                return Ok(stream);
            }
            Err(err) => {
                last_err = Some(err);
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
    Err(last_err
        .map(anyhow::Error::from)
        .unwrap_or_else(|| anyhow!("timed out connecting to {label}")))
    .with_context(|| format!("connect to netns {label}"))
}

fn masked_ws_frame(body: &[u8]) -> Result<Vec<u8>> {
    if body.len() >= 126 {
        bail!("test WebSocket frame helper only supports payloads shorter than 126 bytes");
    }
    let mask = [0x13, 0x37, 0xc0, 0xde];
    let mut frame = Vec::with_capacity(6 + body.len());
    frame.push(0x81);
    frame.push(0x80 | body.len() as u8);
    frame.extend_from_slice(&mask);
    for (i, byte) in body.iter().enumerate() {
        frame.push(byte ^ mask[i % mask.len()]);
    }
    Ok(frame)
}

fn read_matching_event(
    ringbuf: &mut RingBuf<MapData>,
    dropped_events: Option<&PerCpuArray<MapData, u64>>,
    debug_counters: Option<&PerCpuArray<MapData, u64>>,
    order_id: &str,
    timeout: Duration,
) -> Result<DecodedEvent> {
    let start = Instant::now();
    let mut malformed = Vec::new();
    let mut observed = Vec::new();
    while start.elapsed() < timeout {
        while let Some(item) = ringbuf.next() {
            log_step(format!("read {} bytes from EVENTS ringbuf", item.len()));
            match decode_event(&item) {
                Ok(event) if event.order_id == order_id => return Ok(event),
                Ok(event) => observed.push(event.order_id),
                Err(err) => malformed.push(err.to_string()),
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    let dropped = dropped_events
        .map(total_dropped_events)
        .transpose()?
        .unwrap_or(0);
    let debug = debug_counters
        .map(debug_counter_summary)
        .transpose()?
        .unwrap_or_else(|| "unavailable".to_string());
    bail!(
        "timed out after {:.1}s waiting for eBPF event for {order_id}; observed order ids: {:?}; malformed events: {:?}; dropped events: {}; debug counters: {}",
        start.elapsed().as_secs_f32(),
        observed,
        malformed,
        dropped,
        debug
    )
}

fn total_dropped_events(map: &PerCpuArray<MapData, u64>) -> Result<u64> {
    let values = map.get(&0, 0).context("read DROPPED_EVENTS[0]")?;
    Ok(values.iter().copied().sum())
}

fn debug_counter_summary(map: &PerCpuArray<MapData, u64>) -> Result<String> {
    let mut parts = Vec::new();
    for (index, label) in DEBUG_COUNTER_LABELS.iter().enumerate() {
        let values = map
            .get(&(index as u32), 0)
            .with_context(|| format!("read DEBUG_COUNTERS[{index}]"))?;
        let total: u64 = values.iter().copied().sum();
        if total > 0 {
            parts.push(format!("{label}={total}"));
        }
    }
    if parts.is_empty() {
        Ok("all zero".to_string())
    } else {
        Ok(parts.join(", "))
    }
}

fn decode_event(bytes: &[u8]) -> Result<DecodedEvent> {
    if bytes.len() != std::mem::size_of::<KernelEvent>() {
        bail!(
            "unexpected event size: got {}, want {}",
            bytes.len(),
            std::mem::size_of::<KernelEvent>()
        );
    }
    let raw = unsafe { (bytes.as_ptr() as *const KernelEvent).read_unaligned() };
    let order_id = inline_utf8(&raw.order_id, raw.order_id_len, "order_id")?;
    let exec_type = inline_utf8(&raw.exec_type, raw.exec_type_len, "exec_type")?;
    Ok(DecodedEvent {
        order_id,
        exec_type,
        t3_xdp_ingress_ns: raw.t3_xdp_ingress_ns,
        t7_xdp_egress_ns: raw.t7_xdp_egress_ns,
        pod_service_time_ns: raw.pod_service_time_ns,
        fill_qty: raw.fill_qty,
        fill_price: raw.fill_price,
    })
}

fn assert_event(
    event: &DecodedEvent,
    order_id: &str,
    exec_type: &str,
    fill_qty: u64,
    fill_price: u64,
) {
    log_step(format!(
        "observed real eBPF event: order_id={}, exec_type={}, fill_qty={}, fill_price={}, request_ingress_t3={}, response_egress_t7={}, service_time={}",
        event.order_id,
        event.exec_type,
        event.fill_qty,
        event.fill_price,
        event.t3_xdp_ingress_ns,
        event.t7_xdp_egress_ns,
        event.pod_service_time_ns
    ));

    assert_eq!(event.order_id, order_id);
    assert_eq!(event.exec_type, exec_type);
    assert_eq!(event.fill_qty, fill_qty);
    assert_eq!(event.fill_price, fill_price);
    assert!(event.t3_xdp_ingress_ns > 0);
    assert!(event.t7_xdp_egress_ns >= event.t3_xdp_ingress_ns);
    assert_eq!(
        event.pod_service_time_ns,
        event.t7_xdp_egress_ns - event.t3_xdp_ingress_ns
    );
}

fn inline_utf8<const N: usize>(bytes: &[u8; N], len: u16, field: &str) -> Result<String> {
    let len = usize::from(len);
    if len > N {
        bail!("{field} length {len} exceeds {N}");
    }
    std::str::from_utf8(&bytes[..len])
        .with_context(|| format!("{field} is not UTF-8"))
        .map(str::to_string)
}

fn attach_xdp_ingress(bpf: &mut Ebpf, program_name: &str, iface: &str) -> Result<()> {
    log_step(format!("loading XDP program {program_name}"));
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
            log_step(format!(
                "attached XDP program {program_name} to {iface} in driver mode"
            ));
            Ok(())
        }
        Err(err) => {
            log_step(format!(
                "XDP driver mode attach failed ({err}); trying skb/generic mode"
            ));
            program
                .attach(iface, XdpFlags::SKB_MODE)
                .map(|_| ())
                .with_context(|| format!("attach XDP program {program_name} to {iface}"))?;
            log_step(format!(
                "attached XDP program {program_name} to {iface} in skb mode"
            ));
            Ok(())
        }
    }
}

fn attach_tc_egress(bpf: &mut Ebpf, program_name: &str, iface: &str) -> Result<()> {
    log_step(format!("adding clsact qdisc on {iface}"));
    let _ = tc::qdisc_add_clsact(iface);
    log_step(format!("loading tc classifier program {program_name}"));
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
    log_step(format!(
        "attached tc egress program {program_name} to {iface}"
    ));
    Ok(())
}

fn with_network_namespace<T>(netns_path: &Path, f: impl FnOnce() -> Result<T>) -> Result<T> {
    log_step(format!(
        "opening current netns and target {}",
        netns_path.display()
    ));
    let original = File::open("/proc/self/ns/net").context("open current network namespace")?;
    let target = File::open(netns_path)
        .with_context(|| format!("open target network namespace {}", netns_path.display()))?;
    log_step(format!("setns into {}", netns_path.display()));
    set_network_namespace(target.as_raw_fd())
        .with_context(|| format!("enter target network namespace {}", netns_path.display()))?;

    let result = f();
    log_step("restoring original network namespace");
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

fn ebpf_object_path() -> Result<Option<PathBuf>> {
    if let Some(path) = std::env::var_os("EBPF_OBJECT_PATH").map(PathBuf::from) {
        log_step(format!("EBPF_OBJECT_PATH supplied: {}", path.display()));
        if path.exists() {
            return Ok(Some(path));
        }
        skip_or_fail(format!(
            "EBPF_OBJECT_PATH does not exist: {}",
            path.display()
        ))?;
        return Ok(None);
    }

    if command_exists("bpf-linker") {
        log_step("bpf-linker found; building optimized eBPF object with cargo +nightly");
        if !build_ebpf_object()? {
            return Ok(None);
        }
    } else {
        skip_or_fail(
            "bpf-linker is not installed; set EBPF_OBJECT_PATH to a prebuilt object or install bpf-linker",
        )?;
        return Ok(None);
    }

    Ok(Some(find_built_object().ok_or_else(|| {
        anyhow!(
            "could not find built optimized eBPF object under {}",
            workspace_root()
                .join("target/bpfel-unknown-none/release")
                .display()
        )
    })?))
}

fn build_ebpf_object() -> Result<bool> {
    log_step("running cargo +nightly build --release -Z build-std=core for bpfel-unknown-none");
    let status = Command::new("cargo")
        .args([
            "+nightly",
            "build",
            "--release",
            "-Z",
            "build-std=core",
            "-p",
            "iicpc-ebpf-latency",
            "--lib",
            "--target",
            "bpfel-unknown-none",
            "--features",
            "ebpf",
        ])
        .current_dir(workspace_root())
        .status()
        .context("run cargo nightly eBPF build")?;
    if !status.success() {
        return skip_or_fail("cargo nightly eBPF build failed");
    }
    Ok(true)
}

fn find_built_object() -> Option<PathBuf> {
    let dir = workspace_root().join("target/bpfel-unknown-none/release");
    let entries = fs::read_dir(dir).ok()?;
    entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| {
                    name.contains("iicpc_ebpf_latency")
                        && !name.ends_with(".d")
                        && !name.ends_with(".rlib")
                        && !name.ends_with(".rmeta")
                })
                .unwrap_or(false)
        })
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("package is under workspace/services")
        .to_path_buf()
}

fn require_root() -> Result<bool> {
    if unsafe { libc::geteuid() } == 0 {
        return Ok(true);
    }
    skip_or_fail("real eBPF test requires root privileges")
}

fn require_command(command: &str) -> Result<bool> {
    if command_exists(command) {
        Ok(true)
    } else {
        skip_or_fail(format!("{command} is required for real eBPF test"))
    }
}

fn command_exists(command: &str) -> bool {
    Command::new(command)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

fn skip_or_fail(reason: impl AsRef<str>) -> Result<bool> {
    let reason = reason.as_ref();
    if std::env::var_os("IICPC_REAL_EBPF_STRICT").is_some() {
        bail!("{reason}");
    }
    eprintln!("skipping real eBPF integration test: {reason}");
    Ok(false)
}

fn run_ip(args: &[&str]) -> Result<()> {
    log_step(format!("running: ip {}", args.join(" ")));
    let status = Command::new("ip")
        .args(args)
        .status()
        .with_context(|| format!("run ip {}", args.join(" ")))?;
    if status.success() {
        Ok(())
    } else {
        bail!("ip {} failed with {status}", args.join(" "))
    }
}

fn now_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn log_step(message: impl AsRef<str>) {
    eprintln!("[real-ebpf] {}", message.as_ref());
}
