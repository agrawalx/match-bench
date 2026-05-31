#![cfg_attr(target_arch = "bpf", no_std)]
#![cfg_attr(target_arch = "bpf", no_main)]

//! Capture-only eBPF data plane.
//!
//! This program does the absolute minimum in the kernel: parse the L2-L4 headers
//! (fixed-size, verifier-trivial), stamp a timestamp, and copy the TCP payload of
//! every request/response segment into a ring buffer. ALL protocol parsing (FIX,
//! REST, WebSocket), TCP stream reassembly (coalescing + straddling), request to
//! response matching by ClOrdID, and per-response emission happen in userspace
//! (see src/capture.rs, reassembly.rs, parse.rs, matcher.rs).
//!
//! Rationale: keeping parsing out of the kernel removes the BPF verifier
//! complexity, lets the parser handle arbitrary (contestant-controlled) message
//! layouts, and makes per-order matching a normal HashMap instead of a flow-keyed
//! LRU map. XDP is kept for the ingress timestamp because it fires before the
//! kernel network stack, giving the most faithful t3.

#[cfg(not(target_arch = "bpf"))]
pub fn host_placeholder() {}

#[cfg(target_arch = "bpf")]
use aya_ebpf::{
    bindings::{__sk_buff, xdp_action::XDP_PASS, TC_ACT_PIPE},
    cty::c_void,
    helpers::{bpf_skb_load_bytes, bpf_xdp_load_bytes},
    macros::{map, xdp},
    maps::{PerCpuArray, RingBuf},
    programs::{TcContext, XdpContext},
};
#[cfg(target_arch = "bpf")]
use core::{mem, panic::PanicInfo, ptr};

#[cfg(target_arch = "bpf")]
const ETH_P_IP: u16 = 0x0800;
#[cfg(target_arch = "bpf")]
const IPPROTO_TCP: u8 = 6;
#[cfg(target_arch = "bpf")]
const FIX_PORT: u16 = 9898;
#[cfg(target_arch = "bpf")]
const HTTP_WS_PORT: u16 = 8080;

/// Maximum payload bytes copied per segment. Sized to cover a full MTU payload so
/// that, with GSO/TSO disabled on the veth (segments <= MTU), `captured_len`
/// always equals `payload_len` — i.e. coalesced multi-message segments are
/// captured whole and never truncated. `TRUNCATED_CAPTURES` counts any segment
/// that exceeds this (expected 0 with GSO off).
#[cfg(target_arch = "bpf")]
const CAPTURE_CAP: usize = 1536;
/// Byte offset of the inline payload within `CaptureRecord` (repr(C) layout).
/// Must match `CAPTURE_HEADER_LEN` in src/capture.rs.
#[cfg(target_arch = "bpf")]
const CAPTURE_HEADER_LEN: usize = 28;

#[cfg(target_arch = "bpf")]
const DIR_REQUEST: u8 = 0;
#[cfg(target_arch = "bpf")]
const DIR_RESPONSE: u8 = 1;

/// Fixed-layout capture record. The header is followed by `captured_len` payload
/// bytes; only `CAPTURE_HEADER_LEN + captured_len` bytes are emitted to the ring
/// buffer (variable-length record), so small messages do not pay for the full
/// `CAPTURE_CAP`. Keep this layout in sync with `CaptureRecord` in src/capture.rs.
#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
struct CaptureRecord {
    timestamp_ns: u64,
    client_ip: u32,
    tcp_seq: u32,
    payload_len: u32,
    client_port: u16,
    server_port: u16,
    captured_len: u16,
    direction: u8,
    _pad: u8,
    payload: [u8; CAPTURE_CAP],
}

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
struct EthHdr {
    dst: [u8; 6],
    src: [u8; 6],
    eth_proto: u16,
}

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
struct Ipv4Hdr {
    version_ihl: u8,
    tos: u8,
    tot_len: u16,
    id: u16,
    frag_off: u16,
    ttl: u8,
    protocol: u8,
    check: u16,
    saddr: u32,
    daddr: u32,
}

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
struct TcpHdr {
    source: u16,
    dest: u16,
    seq: u32,
    ack_seq: u32,
    doff_res_flags: u16,
}

/// Per-packet bounds + flow identity. `client_ip`/`client_port` are always the
/// BOT side (canonicalized across both directions) so a flow is identified
/// identically on ingress and egress. `server_port` is the FIX/HTTP port (a
/// transport hint for userspace). `payload_offset` is from the start of packet
/// data; `tcp_seq` is this segment's starting sequence number.
#[cfg(target_arch = "bpf")]
#[derive(Clone, Copy)]
struct PacketBounds {
    payload_offset: usize,
    server_port: u16,
    client_ip: u32,
    client_port: u16,
    tcp_seq: u32,
}

#[cfg(target_arch = "bpf")]
#[map]
static EVENTS: RingBuf = RingBuf::with_byte_size(64 * 1024 * 1024, 0);

// Per-CPU staging buffer for one capture record. Filled then emitted within a
// single program invocation; never holds a pointer across invocations.
#[cfg(target_arch = "bpf")]
#[map]
static SCRATCH: PerCpuArray<CaptureRecord> = PerCpuArray::with_max_entries(1, 0);

// Incremented when RingBuf::output fails (ring full) — the userspace reader logs
// the delta so backpressure is observable.
#[cfg(target_arch = "bpf")]
#[map]
static DROPPED_EVENTS: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);

// Incremented when a segment's payload exceeded CAPTURE_CAP and was truncated.
// Expected to stay 0 when GSO/TSO are disabled on the veth.
#[cfg(target_arch = "bpf")]
#[map]
static TRUNCATED_CAPTURES: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);

#[cfg(target_arch = "bpf")]
#[xdp]
pub fn iicpc_xdp_ingress(ctx: XdpContext) -> u32 {
    try_xdp_ingress(&ctx);
    XDP_PASS
}

#[cfg(target_arch = "bpf")]
#[no_mangle]
#[link_section = "classifier"]
pub extern "C" fn iicpc_tc_egress(ctx: *mut __sk_buff) -> i32 {
    try_tc_egress(TcContext::new(ctx));
    TC_ACT_PIPE
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn try_xdp_ingress(ctx: &XdpContext) {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let Some(bounds) = xdp_payload_bounds(data, data_end) else {
        return;
    };
    let payload_start = data + bounds.payload_offset;
    if payload_start >= data_end {
        return;
    }
    let payload_len = data_end - payload_start;

    let Some(rec) = SCRATCH.get_ptr_mut(0) else {
        return;
    };
    // The verifier loses "payload_len > 0" across the packet-pointer subtraction,
    // and the optimizer would fold a plain `cap == 0` guard away (it CAN prove
    // it). A volatile reload makes `cap` opaque so the guard survives into
    // codegen, giving bpf_xdp_load_bytes a provable 1..=CAPTURE_CAP length.
    let cap = unsafe { ptr::read_volatile(&clamp_cap(payload_len)) };
    if cap == 0 || cap > CAPTURE_CAP {
        return;
    }
    // Copy straight from the (linear) XDP buffer into the staging record.
    let dst = unsafe { ptr::addr_of_mut!((*rec).payload) as *mut c_void };
    let ret = unsafe { bpf_xdp_load_bytes(ctx.ctx, bounds.payload_offset as u32, dst, cap as u32) };
    if ret != 0 {
        return;
    }
    emit_capture(rec, &bounds, payload_len, cap, DIR_REQUEST);
}

#[cfg(target_arch = "bpf")]
#[inline(never)]
fn try_tc_egress(ctx: TcContext) {
    let Some(bounds) = tc_payload_bounds(&ctx) else {
        return;
    };
    let packet_len = ctx.len() as usize;
    let Some(payload_len) = packet_len.checked_sub(bounds.payload_offset) else {
        return;
    };
    if payload_len == 0 {
        return;
    }

    let Some(rec) = SCRATCH.get_ptr_mut(0) else {
        return;
    };
    // See try_xdp_ingress: volatile reload keeps the >0 guard for the verifier.
    let cap = unsafe { ptr::read_volatile(&clamp_cap(payload_len)) };
    if cap == 0 || cap > CAPTURE_CAP {
        return;
    }
    let dst = unsafe { ptr::addr_of_mut!((*rec).payload) as *mut c_void };
    let ret = unsafe {
        bpf_skb_load_bytes(
            ctx.skb.skb as *const c_void,
            bounds.payload_offset as u32,
            dst,
            cap as u32,
        )
    };
    if ret != 0 {
        return;
    }
    emit_capture(rec, &bounds, payload_len, cap, DIR_RESPONSE);
}

/// Clamp the copy length to CAPTURE_CAP. Returning a value the verifier can prove
/// is <= CAPTURE_CAP is what makes the variable-length ring output provably safe.
#[cfg(target_arch = "bpf")]
#[inline(always)]
fn clamp_cap(payload_len: usize) -> usize {
    if payload_len > CAPTURE_CAP {
        if let Some(c) = TRUNCATED_CAPTURES.get_ptr_mut(0) {
            unsafe { ptr::write(c, ptr::read(c).saturating_add(1)) };
        }
        CAPTURE_CAP
    } else {
        payload_len
    }
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn emit_capture(
    rec: *mut CaptureRecord,
    bounds: &PacketBounds,
    payload_len: usize,
    cap: usize,
    direction: u8,
) {
    // `cap` is proven <= CAPTURE_CAP by clamp_cap, so total <= size_of::<CaptureRecord>().
    let cap = if cap > CAPTURE_CAP { CAPTURE_CAP } else { cap };
    let total = CAPTURE_HEADER_LEN + cap;
    unsafe {
        ptr::addr_of_mut!((*rec).timestamp_ns).write(bpf_ktime_get_ns());
        ptr::addr_of_mut!((*rec).client_ip).write(bounds.client_ip);
        ptr::addr_of_mut!((*rec).tcp_seq).write(bounds.tcp_seq);
        ptr::addr_of_mut!((*rec).payload_len).write(payload_len as u32);
        ptr::addr_of_mut!((*rec).client_port).write(bounds.client_port);
        ptr::addr_of_mut!((*rec).server_port).write(bounds.server_port);
        ptr::addr_of_mut!((*rec).captured_len).write(cap as u16);
        ptr::addr_of_mut!((*rec).direction).write(direction);
        ptr::addr_of_mut!((*rec)._pad).write(0);

        let bytes = core::slice::from_raw_parts(rec as *const u8, total);
        if EVENTS.output(bytes, 0).is_err() {
            if let Some(c) = DROPPED_EVENTS.get_ptr_mut(0) {
                ptr::write(c, ptr::read(c).saturating_add(1));
            }
        }
    }
}

/// Parse eth/ip/tcp for an XDP ingress packet (request: client -> server).
/// The client tuple is the source; the server port is the destination.
#[cfg(target_arch = "bpf")]
#[inline(always)]
fn xdp_payload_bounds(data: usize, data_end: usize) -> Option<PacketBounds> {
    let eth: EthHdr = load(data, data_end, 0)?;
    if u16::from_be(eth.eth_proto) != ETH_P_IP {
        return None;
    }
    let ip_offset = mem::size_of::<EthHdr>();
    let ip: Ipv4Hdr = load(data, data_end, ip_offset)?;
    if ip.version_ihl >> 4 != 4 || ip.protocol != IPPROTO_TCP {
        return None;
    }
    let ihl = usize::from(ip.version_ihl & 0x0f) * 4;
    if ihl < mem::size_of::<Ipv4Hdr>() {
        return None;
    }
    let tcp_offset = ip_offset + ihl;
    let tcp: TcpHdr = load(data, data_end, tcp_offset)?;
    let source = u16::from_be(tcp.source);
    let dest = u16::from_be(tcp.dest);
    if dest != FIX_PORT && dest != HTTP_WS_PORT {
        return None;
    }
    let data_offset = usize::from(u16::from_be(tcp.doff_res_flags) >> 12) * 4;
    if data_offset < mem::size_of::<TcpHdr>() {
        return None;
    }
    Some(PacketBounds {
        payload_offset: tcp_offset + data_offset,
        server_port: dest,
        client_ip: u32::from_be(ip.saddr),
        client_port: source,
        tcp_seq: u32::from_be(tcp.seq),
    })
}

/// Parse eth/ip/tcp for a tc egress skb (response: server -> client). Tries with
/// and without an ethernet header (veth may present either). The client tuple is
/// the destination; the server port is the source.
#[cfg(target_arch = "bpf")]
#[inline(always)]
fn tc_payload_bounds(ctx: &TcContext) -> Option<PacketBounds> {
    parse_skb_ip_tcp_at(ctx, 0).or_else(|| parse_skb_ip_tcp_at(ctx, mem::size_of::<EthHdr>()))
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn parse_skb_ip_tcp_at(ctx: &TcContext, ip_offset: usize) -> Option<PacketBounds> {
    let ip: Ipv4Hdr = ctx.load(ip_offset).ok()?;
    if ip.version_ihl >> 4 != 4 || ip.protocol != IPPROTO_TCP {
        return None;
    }
    let ihl = usize::from(ip.version_ihl & 0x0f) * 4;
    if ihl < mem::size_of::<Ipv4Hdr>() {
        return None;
    }
    let tcp_offset = ip_offset + ihl;
    let tcp: TcpHdr = ctx.load(tcp_offset).ok()?;
    let source = u16::from_be(tcp.source);
    let dest = u16::from_be(tcp.dest);
    if source != FIX_PORT && source != HTTP_WS_PORT {
        return None;
    }
    let data_offset = usize::from(u16::from_be(tcp.doff_res_flags) >> 12) * 4;
    if data_offset < mem::size_of::<TcpHdr>() {
        return None;
    }
    Some(PacketBounds {
        payload_offset: tcp_offset + data_offset,
        server_port: source,
        client_ip: u32::from_be(ip.daddr),
        client_port: dest,
        tcp_seq: u32::from_be(tcp.seq),
    })
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn load<T: Copy>(data: usize, data_end: usize, offset: usize) -> Option<T> {
    let len = mem::size_of::<T>();
    if data + offset + len > data_end {
        return None;
    }
    let ptr = (data + offset) as *const T;
    Some(unsafe { ptr::read_unaligned(ptr) })
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
unsafe fn bpf_ktime_get_ns() -> u64 {
    // Helper id 5 = bpf_ktime_get_ns (CLOCK_MONOTONIC). Userspace converts to
    // CLOCK_REALTIME using a sampled offset so t3/t7 share the bot's clock domain.
    let helper: extern "C" fn() -> u64 = mem::transmute(5usize);
    helper()
}

#[cfg(target_arch = "bpf")]
#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}

#[cfg(target_arch = "bpf")]
#[link_section = "license"]
#[no_mangle]
static LICENSE: [u8; 4] = *b"GPL\0";
