//! Real eBPF integration test.
//!
//! Loads the actual capture-only kernel program (this is where the BPF verifier
//! runs), attaches XDP ingress + tc egress to a netns veth, drives real
//! FIX/REST/WS round-trips, and feeds the kernel's CaptureRecords through the
//! REAL userspace pipeline (capture -> reassembly -> parse -> matcher, included
//! below via #[path]) — asserting the emitted OrderAckedEvents. This exercises
//! the same code `main` runs, end to end.
//!
//! Run (needs root + bpf-linker/nightly or EBPF_OBJECT_PATH):
//!   IICPC_REAL_EBPF_STRICT=1 sudo -E env "PATH=$PATH" \
//!     cargo test -p iicpc-ebpf-latency --test real_ebpf -- --ignored --nocapture

#[path = "../src/capture.rs"]
mod capture;
#[path = "../src/matcher.rs"]
mod matcher;
#[path = "../src/parse.rs"]
mod parse;
#[path = "../src/pipeline.rs"]
mod pipeline;
#[path = "../src/reassembly.rs"]
mod reassembly;

use std::{
    fs::File,
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

use matcher::MatchedEvent;
use pipeline::Pipeline;

#[test]
#[ignore = "requires root, bpf-linker/nightly or EBPF_OBJECT_PATH, and local netns privileges"]
fn captures_and_matches_fix_rest_ws_including_partial_fills() -> Result<()> {
    log_step("starting real eBPF integration test");
    if !require_root()? || !require_command("ip")? || !require_command("python3")? {
        return Ok(());
    }
    let Some(object_path) = ebpf_object_path()? else {
        return Ok(());
    };
    log_step(format!("using eBPF object {}", object_path.display()));

    let fixture = NetnsFixture::create()?;
    log_step(format!("fixture ready: netns={}", fixture.ns_name));

    let mut bpf = Ebpf::load_file(&object_path)
        .with_context(|| format!("load eBPF object {}", object_path.display()))?;
    log_step("eBPF object loaded (verifier accepted); attaching programs");
    with_network_namespace(&fixture.netns_path, || {
        attach_xdp_ingress(&mut bpf, "iicpc_xdp_ingress", "eth0")?;
        attach_tc_egress(&mut bpf, "iicpc_tc_egress", "eth0")
    })?;

    let mut ringbuf = RingBuf::try_from(
        bpf.take_map("EVENTS")
            .ok_or_else(|| anyhow!("ringbuf map EVENTS not found"))?,
    )
    .context("open EVENTS ringbuf")?;
    let dropped = take_counter(&mut bpf, "DROPPED_EVENTS");
    let truncated = take_counter(&mut bpf, "TRUNCATED_CAPTURES");

    let mut pipeline = Pipeline::new();

    // 1) Single FIX round-trip → one event.
    {
        let mut server = fixture.spawn_python(
            9898,
            &fix_server_script(&[fix_exec("order-real-1", "F", 12, "42.5")]),
        )?;
        send_fix(&[fix_new_order("order-real-1")])?;
        let events = collect(&mut ringbuf, &mut pipeline, 1, Duration::from_secs(3))?;
        let e = find(&events, "order-real-1");
        assert_event(e, "order-real-1", "F", 12, 42_500_000_000);
        kill(&mut server);
    }

    // 2) Partial fills: one order, TWO ExecutionReports → TWO events sharing t3.
    {
        let mut server = fixture.spawn_python(
            9898,
            &fix_server_script(&[
                fix_exec("order-multi-1", "0", 0, "0"),
                fix_exec("order-multi-1", "2", 9, "10.5"),
            ]),
        )?;
        send_fix(&[fix_new_order("order-multi-1")])?;
        let events = collect(&mut ringbuf, &mut pipeline, 2, Duration::from_secs(3))?;
        let mine: Vec<&MatchedEvent> = events
            .iter()
            .filter(|e| e.order_id == "order-multi-1")
            .collect();
        assert_eq!(
            mine.len(),
            2,
            "expected ACK + FILL events, got {}",
            mine.len()
        );
        assert_eq!(mine[0].exec_type, "0");
        assert_eq!(mine[1].exec_type, "2");
        assert_eq!(mine[1].fill_qty, 9);
        assert_eq!(mine[1].fill_price, 10_500_000_000);
        assert_eq!(
            mine[0].t3_ns, mine[1].t3_ns,
            "both responses share the request's t3"
        );
        assert!(mine[1].t7_ns >= mine[0].t7_ns);
        kill(&mut server);
    }

    // 3) Two orders pipelined on ONE connection, responses out of order → matched
    //    per-ClOrdID (the EBPF-10 regression).
    {
        let mut server = fixture.spawn_python(
            9898,
            &fix_server_script(&[
                fix_exec("order-pipe-B", "2", 5, "2.0"),
                fix_exec("order-pipe-A", "2", 7, "1.0"),
            ]),
        )?;
        send_fix(&[fix_new_order("order-pipe-A"), fix_new_order("order-pipe-B")])?;
        let events = collect(&mut ringbuf, &mut pipeline, 2, Duration::from_secs(3))?;
        let a = find(&events, "order-pipe-A");
        let b = find(&events, "order-pipe-B");
        assert_eq!(a.fill_qty, 7);
        assert_eq!(b.fill_qty, 5);
        assert!(a.t3_ns > 0 && b.t3_ns > 0);
        kill(&mut server);
    }

    // 4) REST JSON round-trip.
    {
        let body =
            br#"{"cl_ord_id":"order-rest-1","exec_type":"F","fill_qty":7,"fill_price":99.25}"#;
        let mut server = fixture.spawn_python(8080, &http_server_script(body))?;
        send_rest("order-rest-1")?;
        let events = collect(&mut ringbuf, &mut pipeline, 1, Duration::from_secs(3))?;
        assert_event(
            find(&events, "order-rest-1"),
            "order-rest-1",
            "F",
            7,
            99_250_000_000,
        );
        kill(&mut server);
    }

    // 5) WebSocket JSON round-trip (server sends an unmasked text frame).
    {
        let body = br#"{"cl_ord_id":"order-ws-1","exec_type":"F","fill_qty":3,"fill_price":11.75}"#;
        let mut server = fixture.spawn_python(8080, &ws_server_script(body))?;
        send_ws("order-ws-1")?;
        let events = collect(&mut ringbuf, &mut pipeline, 1, Duration::from_secs(3))?;
        assert_event(
            find(&events, "order-ws-1"),
            "order-ws-1",
            "F",
            3,
            11_750_000_000,
        );
        kill(&mut server);
    }

    log_diagnostics(&dropped, &truncated);
    log_step("real eBPF integration test completed");
    Ok(())
}

// ---- pipeline driving -------------------------------------------------------

fn collect(
    ringbuf: &mut RingBuf<MapData>,
    pipeline: &mut Pipeline,
    want: usize,
    timeout: Duration,
) -> Result<Vec<MatchedEvent>> {
    let mut out = Vec::new();
    let start = Instant::now();
    while start.elapsed() < timeout {
        while let Some(item) = ringbuf.next() {
            match capture::decode(&item) {
                Ok(cap) => pipeline.process(&cap, &mut out),
                Err(err) => log_step(format!("malformed capture: {err}")),
            }
        }
        if out.len() >= want {
            return Ok(out);
        }
        thread::sleep(Duration::from_millis(10));
    }
    Ok(out)
}

fn find<'a>(events: &'a [MatchedEvent], order_id: &str) -> &'a MatchedEvent {
    events
        .iter()
        .find(|e| e.order_id == order_id)
        .unwrap_or_else(|| {
            let ids: Vec<&str> = events.iter().map(|e| e.order_id.as_str()).collect();
            panic!("no event for {order_id}; observed {ids:?}")
        })
}

fn assert_event(e: &MatchedEvent, order_id: &str, exec_type: &str, fill_qty: u64, fill_price: u64) {
    log_step(format!(
        "event: order_id={}, exec_type={}, fill_qty={}, fill_price={}, t3={}, t7={}, svc={}",
        e.order_id, e.exec_type, e.fill_qty, e.fill_price, e.t3_ns, e.t7_ns, e.pod_service_time_ns
    ));
    assert_eq!(e.order_id, order_id);
    assert_eq!(e.exec_type, exec_type);
    assert_eq!(e.fill_qty, fill_qty);
    assert_eq!(e.fill_price, fill_price);
    assert!(e.t3_ns > 0);
    assert!(e.t7_ns >= e.t3_ns);
    assert_eq!(e.pod_service_time_ns, e.t7_ns - e.t3_ns);
}

// ---- FIX/JSON wire builders -------------------------------------------------

/// Wrap a FIX body (`|`-separated, no 8=/9=/10=) into a full wire message.
fn fix(body: &str) -> Vec<u8> {
    let body = body.replace('|', "\x01");
    let head = format!("8=FIX.4.2\x019={}\x01", body.len());
    let mut bytes = format!("{head}{body}").into_bytes();
    let sum: u32 = bytes.iter().map(|&b| b as u32).sum::<u32>() % 256;
    bytes.extend_from_slice(format!("10={sum:03}\x01").as_bytes());
    bytes
}

fn fix_new_order(clordid: &str) -> Vec<u8> {
    // Every field, including the last, is SOH-terminated (trailing `|`).
    fix(&format!(
        "35=D|49=IICPC-BOT|56=CONTESTANT|34=1|52=19700101-00:00:00.000|11={clordid}|21=1|55=IICPC|54=1|38=12|40=2|44=42.5|59=0|"
    ))
}

fn fix_exec(clordid: &str, exec_type: &str, qty: u64, px: &str) -> Vec<u8> {
    fix(&format!(
        "35=8|49=CONTESTANT|56=IICPC-BOT|34=1|37=EXEC|11={clordid}|17=E|150={exec_type}|39={exec_type}|32={qty}|31={px}|"
    ))
}

/// Python that emits the given pre-built frames as one bytes literal.
fn py_bytes(frames: &[Vec<u8>]) -> String {
    let mut joined = Vec::new();
    for f in frames {
        joined.extend_from_slice(f);
    }
    let escaped: String = joined.iter().map(|b| format!("\\x{b:02x}")).collect();
    format!("b\"{escaped}\"")
}

fn fix_server_script(responses: &[Vec<u8>]) -> String {
    server_script(9898, &py_bytes(responses))
}

fn http_server_script(body: &[u8]) -> String {
    let body_lit = py_bytes(&[body.to_vec()]);
    let resp = format!(
        "b\"HTTP/1.1 200 OK\\r\\nContent-Type: application/json\\r\\nContent-Length: \" + str(len({body_lit})).encode() + b\"\\r\\n\\r\\n\" + {body_lit}"
    );
    server_script(8080, &resp)
}

fn ws_server_script(body: &[u8]) -> String {
    let body_lit = py_bytes(&[body.to_vec()]);
    let resp = format!("bytes([0x81, len({body_lit})]) + {body_lit}");
    server_script(8080, &resp)
}

fn server_script(port: u16, response_expr: &str) -> String {
    format!(
        r#"
import socket
response = {response_expr}
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("10.241.0.2", {port}))
    s.listen(1)
    conn, _ = s.accept()
    with conn:
        conn.recv(4096)
        conn.sendall(response)
"#
    )
}

// ---- clients ----------------------------------------------------------------

fn send_fix(requests: &[Vec<u8>]) -> Result<()> {
    let mut stream = connect_with_retry("10.241.0.2:9898", "FIX server")?;
    for r in requests {
        stream.write_all(r).context("write FIX request")?;
    }
    let mut buf = [0u8; 1024];
    let _ = stream.read(&mut buf);
    Ok(())
}

fn send_rest(_clordid: &str) -> Result<()> {
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
    let mut buf = [0u8; 512];
    let _ = stream.read(&mut buf);
    Ok(())
}

fn send_ws(_clordid: &str) -> Result<()> {
    let body = br#"{"cl_ord_id":"order-ws-1","qty":3,"price":11.75}"#;
    let mask = [0x13u8, 0x37, 0xc0, 0xde];
    let mut frame = vec![0x81u8, 0x80 | body.len() as u8];
    frame.extend_from_slice(&mask);
    for (i, &b) in body.iter().enumerate() {
        frame.push(b ^ mask[i % 4]);
    }
    let mut stream = connect_with_retry("10.241.0.2:8080", "WebSocket server")?;
    stream.write_all(&frame).context("write WS frame")?;
    let mut buf = [0u8; 256];
    let _ = stream.read(&mut buf);
    Ok(())
}

fn connect_with_retry(addr: &str, label: &str) -> Result<TcpStream> {
    let start = Instant::now();
    let mut last = None;
    while start.elapsed() < Duration::from_secs(2) {
        match TcpStream::connect(addr) {
            Ok(s) => return Ok(s),
            Err(e) => {
                last = Some(e);
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
    Err(last
        .map(anyhow::Error::from)
        .unwrap_or_else(|| anyhow!("timed out connecting to {label}")))
    .with_context(|| format!("connect to netns {label}"))
}

// ---- netns fixture / attach / build (unchanged structure) -------------------

struct NetnsFixture {
    ns_name: String,
    netns_path: PathBuf,
    host_veth: String,
}

impl NetnsFixture {
    fn create() -> Result<Self> {
        let suffix = format!("{}-{}", std::process::id(), monotonic_suffix());
        let ns_name = format!("iicpc-ebpf-{suffix}");
        let host_veth = format!("iicpc{}", &suffix[..suffix.len().min(8)]);
        let fixture = Self {
            netns_path: PathBuf::from(format!("/var/run/netns/{ns_name}")),
            ns_name,
            host_veth,
        };
        run_ip(&["netns", "add", &fixture.ns_name])?;
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
        run_ip(&["addr", "add", "10.241.0.1/24", "dev", &fixture.host_veth])?;
        run_ip(&["link", "set", &fixture.host_veth, "up"])?;
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
        // Disable segmentation offload so tc egress sees per-MTU segments.
        let _ = Command::new("ip")
            .args([
                "netns",
                "exec",
                &fixture.ns_name,
                "ethtool",
                "-K",
                "eth0",
                "tso",
                "off",
                "gso",
                "off",
                "gro",
                "off",
            ])
            .status();
        Ok(fixture)
    }

    fn spawn_python(&self, port: u16, script: &str) -> Result<Child> {
        let child = Command::new("ip")
            .args(["netns", "exec", &self.ns_name, "python3", "-c", script])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("spawn server on {port} in netns"))?;
        thread::sleep(Duration::from_millis(250));
        Ok(child)
    }
}

impl Drop for NetnsFixture {
    fn drop(&mut self) {
        let _ = Command::new("ip")
            .args(["link", "del", &self.host_veth])
            .status();
        let _ = Command::new("ip")
            .args(["netns", "del", &self.ns_name])
            .status();
    }
}

fn kill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
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
        Ok(_) => Ok(()),
        Err(_) => program
            .attach(iface, XdpFlags::SKB_MODE)
            .map(|_| ())
            .with_context(|| format!("attach XDP program {program_name} to {iface}")),
    }
}

fn attach_tc_egress(bpf: &mut Ebpf, program_name: &str, iface: &str) -> Result<()> {
    let _ = tc::qdisc_add_clsact(iface);
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
    Ok(())
}

fn with_network_namespace<T>(netns_path: &Path, f: impl FnOnce() -> Result<T>) -> Result<T> {
    let original = File::open("/proc/self/ns/net").context("open current network namespace")?;
    let target = File::open(netns_path)
        .with_context(|| format!("open target network namespace {}", netns_path.display()))?;
    set_network_namespace(target.as_raw_fd())
        .with_context(|| format!("enter target network namespace {}", netns_path.display()))?;
    let result = f();
    let restore =
        set_network_namespace(original.as_raw_fd()).context("restore original network namespace");
    match (result, restore) {
        (Ok(v), Ok(())) => Ok(v),
        (Err(e), Ok(())) => Err(e),
        (Ok(_), Err(e)) => Err(e),
        (Err(e), Err(re)) => Err(e).context(format!("also failed to restore netns: {re}")),
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

fn take_counter(bpf: &mut Ebpf, name: &str) -> Option<PerCpuArray<MapData, u64>> {
    bpf.take_map(name)
        .and_then(|m| PerCpuArray::try_from(m).ok())
}

fn log_diagnostics(
    dropped: &Option<PerCpuArray<MapData, u64>>,
    truncated: &Option<PerCpuArray<MapData, u64>>,
) {
    let sum = |m: &Option<PerCpuArray<MapData, u64>>| {
        m.as_ref()
            .and_then(|m| m.get(&0, 0).ok())
            .map(|v| v.iter().copied().sum::<u64>())
            .unwrap_or(0)
    };
    log_step(format!(
        "dropped_events={}, truncated_captures={}",
        sum(dropped),
        sum(truncated)
    ));
}

// ---- build / env ------------------------------------------------------------

fn ebpf_object_path() -> Result<Option<PathBuf>> {
    if let Some(path) = std::env::var_os("EBPF_OBJECT_PATH").map(PathBuf::from) {
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
        if !build_ebpf_object()? {
            return Ok(None);
        }
    } else {
        skip_or_fail("bpf-linker is not installed; set EBPF_OBJECT_PATH or install bpf-linker")?;
        return Ok(None);
    }
    Ok(Some(find_built_object().ok_or_else(|| {
        anyhow!("could not find built eBPF object under target/bpfel-unknown-none/release")
    })?))
}

fn build_ebpf_object() -> Result<bool> {
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
    std::fs::read_dir(dir)
        .ok()?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| {
                    n.contains("iicpc_ebpf_latency")
                        && !n.ends_with(".d")
                        && !n.ends_with(".rlib")
                        && !n.ends_with(".rmeta")
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

fn monotonic_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn log_step(message: impl AsRef<str>) {
    eprintln!("[real-ebpf] {}", message.as_ref());
}
