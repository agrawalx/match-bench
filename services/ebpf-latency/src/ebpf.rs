//! This module implements ebpf behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

#![cfg_attr(target_arch = "bpf", no_std)]
#![cfg_attr(target_arch = "bpf", no_main)]

#[cfg(not(target_arch = "bpf"))]
/// host_placeholder performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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

#[cfg(target_arch = "bpf")]
const CAPTURE_CAP: usize = 1536;
#[cfg(target_arch = "bpf")]
const CAPTURE_HEADER_LEN: usize = 28;

#[cfg(target_arch = "bpf")]
const DIR_REQUEST: u8 = 0;
#[cfg(target_arch = "bpf")]
const DIR_RESPONSE: u8 = 1;

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
/// CaptureRecord stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
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
/// EthHdr stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct EthHdr {
    dst: [u8; 6],
    src: [u8; 6],
    eth_proto: u16,
}

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
/// Ipv4Hdr stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
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
/// TcpHdr stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct TcpHdr {
    source: u16,
    dest: u16,
    seq: u32,
    ack_seq: u32,
    doff_res_flags: u16,
}

#[cfg(target_arch = "bpf")]
#[derive(Clone, Copy)]
/// PacketBounds stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct PacketBounds {
    payload_offset: usize,
    payload_len: usize,
    server_port: u16,
    client_ip: u32,
    client_port: u16,
    tcp_seq: u32,
}

#[cfg(target_arch = "bpf")]
#[map]
static EVENTS: RingBuf = RingBuf::with_byte_size(64 * 1024 * 1024, 0);

#[cfg(target_arch = "bpf")]
#[map]
static SCRATCH: PerCpuArray<CaptureRecord> = PerCpuArray::with_max_entries(1, 0);

#[cfg(target_arch = "bpf")]
#[map]
static DROPPED_EVENTS: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);

#[cfg(target_arch = "bpf")]
#[map]
static TRUNCATED_CAPTURES: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);

#[cfg(target_arch = "bpf")]
#[xdp]
/// iicpc_xdp_ingress performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn iicpc_xdp_ingress(ctx: XdpContext) -> u32 {
    try_xdp_ingress(&ctx);
    XDP_PASS
}

#[cfg(target_arch = "bpf")]
#[no_mangle]
#[link_section = "classifier"]
/// iicpc_tc_egress performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub extern "C" fn iicpc_tc_egress(ctx: *mut __sk_buff) -> i32 {
    try_tc_egress(TcContext::new(ctx));
    TC_ACT_PIPE
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// try_xdp_ingress performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn try_xdp_ingress(ctx: &XdpContext) {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let Some(bounds) = xdp_payload_bounds(data, data_end) else {
        return;
    };
    if bounds.payload_len == 0 {
        return;
    }

    let Some(rec) = SCRATCH.get_ptr_mut(0) else {
        return;
    };
    let cap = unsafe { ptr::read_volatile(&clamp_cap(bounds.payload_len)) };
    if cap == 0 || cap > CAPTURE_CAP {
        return;
    }
    let dst = unsafe { ptr::addr_of_mut!((*rec).payload) as *mut c_void };
    let ret = unsafe { bpf_xdp_load_bytes(ctx.ctx, bounds.payload_offset as u32, dst, cap as u32) };
    if ret != 0 {
        return;
    }
    emit_capture(rec, &bounds, cap, DIR_REQUEST);
}

#[cfg(target_arch = "bpf")]
#[inline(never)]
/// try_tc_egress performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn try_tc_egress(ctx: TcContext) {
    let Some(bounds) = tc_payload_bounds(&ctx) else {
        return;
    };
    if bounds.payload_len == 0 {
        return;
    }

    let Some(rec) = SCRATCH.get_ptr_mut(0) else {
        return;
    };
    let cap = unsafe { ptr::read_volatile(&clamp_cap(bounds.payload_len)) };
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
    emit_capture(rec, &bounds, cap, DIR_RESPONSE);
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// clamp_cap performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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
/// emit_capture performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn emit_capture(rec: *mut CaptureRecord, bounds: &PacketBounds, cap: usize, direction: u8) {
    let cap = if cap > CAPTURE_CAP { CAPTURE_CAP } else { cap };
    let total = CAPTURE_HEADER_LEN + cap;
    unsafe {
        ptr::addr_of_mut!((*rec).timestamp_ns).write(bpf_ktime_get_ns());
        ptr::addr_of_mut!((*rec).client_ip).write(bounds.client_ip);
        ptr::addr_of_mut!((*rec).tcp_seq).write(bounds.tcp_seq);
        ptr::addr_of_mut!((*rec).payload_len).write(bounds.payload_len as u32);
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

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// xdp_payload_bounds performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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
    let ip_total = usize::from(u16::from_be(ip.tot_len));
    if ip_total < ihl {
        return None;
    }
    let ip_end = ip_offset.checked_add(ip_total)?;
    if ip_end > data_end.saturating_sub(data) {
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
    let payload_offset = tcp_offset.checked_add(data_offset)?;
    if payload_offset > ip_end {
        return None;
    }
    Some(PacketBounds {
        payload_offset,
        payload_len: ip_end - payload_offset,
        server_port: dest,
        client_ip: u32::from_be(ip.saddr),
        client_port: source,
        tcp_seq: u32::from_be(tcp.seq),
    })
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// tc_payload_bounds performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn tc_payload_bounds(ctx: &TcContext) -> Option<PacketBounds> {
    let packet_len = ctx.len() as usize;
    let framed = || -> Option<PacketBounds> {
        let eth: EthHdr = ctx.load(0).ok()?;
        if u16::from_be(eth.eth_proto) != ETH_P_IP {
            return None;
        }
        parse_skb_ip_tcp_at(ctx, mem::size_of::<EthHdr>(), packet_len)
    };
    framed().or_else(|| parse_skb_ip_tcp_at(ctx, 0, packet_len))
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// parse_skb_ip_tcp_at performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn parse_skb_ip_tcp_at(
    ctx: &TcContext,
    ip_offset: usize,
    packet_len: usize,
) -> Option<PacketBounds> {
    let ip: Ipv4Hdr = ctx.load(ip_offset).ok()?;
    if ip.version_ihl >> 4 != 4 || ip.protocol != IPPROTO_TCP {
        return None;
    }
    let ihl = usize::from(ip.version_ihl & 0x0f) * 4;
    if ihl < mem::size_of::<Ipv4Hdr>() {
        return None;
    }
    let ip_total = usize::from(u16::from_be(ip.tot_len));
    if ip_total < ihl {
        return None;
    }
    let ip_end = ip_offset.checked_add(ip_total)?;
    if ip_end > packet_len {
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
    let payload_offset = tcp_offset.checked_add(data_offset)?;
    if payload_offset > ip_end {
        return None;
    }
    Some(PacketBounds {
        payload_offset,
        payload_len: ip_end - payload_offset,
        server_port: source,
        client_ip: u32::from_be(ip.daddr),
        client_port: dest,
        tcp_seq: u32::from_be(tcp.seq),
    })
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
/// load performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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
/// bpf_ktime_get_ns performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
unsafe fn bpf_ktime_get_ns() -> u64 {
    let helper: extern "C" fn() -> u64 = mem::transmute(5usize);
    helper()
}

#[cfg(target_arch = "bpf")]
#[panic_handler]
/// panic performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}

#[cfg(target_arch = "bpf")]
#[link_section = "license"]
#[no_mangle]
static LICENSE: [u8; 4] = *b"GPL\0";
