#![cfg_attr(target_arch = "bpf", no_std)]
#![cfg_attr(target_arch = "bpf", no_main)]

#[cfg(target_arch = "bpf")]
use aya_ebpf::{
    bindings::{__sk_buff, xdp_action::XDP_PASS, TC_ACT_PIPE},
    cty::c_void,
    helpers::bpf_skb_load_bytes,
    macros::{map, xdp},
    maps::{LruHashMap, PerCpuArray, RingBuf},
    programs::{TcContext, XdpContext},
};
#[cfg(target_arch = "bpf")]
use core::{mem, panic::PanicInfo, ptr};

#[cfg(not(target_arch = "bpf"))]
pub fn host_placeholder() {}

#[cfg(target_arch = "bpf")]
const ETH_P_IP: u16 = 0x0800;
#[cfg(target_arch = "bpf")]
const IPPROTO_TCP: u8 = 6;
#[cfg(target_arch = "bpf")]
const FIX_PORT: u16 = 9898;
#[cfg(target_arch = "bpf")]
const HTTP_WS_PORT: u16 = 8080;
#[cfg(target_arch = "bpf")]
const MAX_ORDER_ID_LEN: usize = 32;
#[cfg(target_arch = "bpf")]
const MAX_EXEC_TYPE_LEN: usize = 16;
#[cfg(target_arch = "bpf")]
const REST_JSON_BODY_LEN: usize = 76;
#[cfg(target_arch = "bpf")]
const WS_JSON_BODY_LEN: usize = 74;
#[cfg(target_arch = "bpf")]
const FIX_RESPONSE_LEN: usize = 56;
#[cfg(target_arch = "bpf")]
const MAX_RESPONSE_SCAN: usize = 192;
#[cfg(target_arch = "bpf")]
const HTTP_RESPONSE_SCAN: usize = 147;
#[cfg(target_arch = "bpf")]
const WS_RESPONSE_SCAN: usize = 76;
#[cfg(target_arch = "bpf")]
const FIX_RESPONSE_SCAN: usize = 56;
#[cfg(target_arch = "bpf")]
const EVENT_FLAG_REORDERING: u16 = 0x1;
#[cfg(target_arch = "bpf")]
const PRICE_SCALE: u64 = 1_000_000_000;
#[cfg(target_arch = "bpf")]
const ORDER_KEY_SIZE: usize = 34;
#[cfg(target_arch = "bpf")]
const FLOW_KEY_SIZE: usize = 8;
#[cfg(target_arch = "bpf")]
const EXEC_TYPE_SIZE: usize = 18;
#[cfg(target_arch = "bpf")]
const LATENCY_EVENT_SIZE: usize = 144;
#[cfg(target_arch = "bpf")]
const DEBUG_COUNTERS_LEN: u32 = 14;
#[cfg(target_arch = "bpf")]
const DBG_XDP_MATCH: u32 = 0;
#[cfg(target_arch = "bpf")]
const DBG_XDP_INSERT: u32 = 1;
#[cfg(target_arch = "bpf")]
const DBG_XDP_EXISTING: u32 = 2;
#[cfg(target_arch = "bpf")]
const DBG_TC_MATCH: u32 = 3;
#[cfg(target_arch = "bpf")]
const DBG_TC_LOAD_OK: u32 = 4;
#[cfg(target_arch = "bpf")]
const DBG_TC_PARSE_FIX_OK: u32 = 5;
#[cfg(target_arch = "bpf")]
const DBG_TC_PARSE_HTTP_OK: u32 = 6;
#[cfg(target_arch = "bpf")]
const DBG_TC_PARSE_WS_OK: u32 = 7;
#[cfg(target_arch = "bpf")]
const DBG_TC_LOOKUP_MISS: u32 = 8;
#[cfg(target_arch = "bpf")]
const DBG_TC_LOOKUP_HIT: u32 = 9;
#[cfg(target_arch = "bpf")]
const DBG_TC_EVENT_SUBMIT: u32 = 10;
#[cfg(target_arch = "bpf")]
const DBG_TC_RESERVE_FAIL: u32 = 11;
#[cfg(target_arch = "bpf")]
const DBG_TC_ENTRY: u32 = 12;
#[cfg(target_arch = "bpf")]
const DBG_TC_BOUNDS_MISS: u32 = 13;

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct OrderKey {
    len: u16,
    bytes: [u8; MAX_ORDER_ID_LEN],
}

#[cfg(target_arch = "bpf")]
const _: [(); ORDER_KEY_SIZE] = [(); mem::size_of::<OrderKey>()];

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FlowKey {
    src_ip: u32,
    src_port: u16,
    _pad: u16,
}

#[cfg(target_arch = "bpf")]
const _: [(); FLOW_KEY_SIZE] = [(); mem::size_of::<FlowKey>()];

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct InFlightOrder {
    t3_xdp_ingress_ns: u64,
    src_ip: u32,
    src_port: u16,
    _pad: u16,
    tcp_seq: u32,
    retransmission_count: u32,
}

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LatencyEvent {
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

#[cfg(target_arch = "bpf")]
const _: [(); LATENCY_EVENT_SIZE] = [(); mem::size_of::<LatencyEvent>()];

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

#[cfg(target_arch = "bpf")]
struct ParsedResponse {
    key: OrderKey,
    exec_type: ExecType,
    fill_qty: u64,
    fill_price: u64,
    orig_order_id: OrderKey,
}

#[cfg(target_arch = "bpf")]
#[repr(C)]
struct ParserScratch {
    response: ParsedResponse,
    payload: [u8; MAX_RESPONSE_SCAN],
}

#[cfg(target_arch = "bpf")]
#[repr(C)]
#[derive(Clone, Copy)]
struct ExecType {
    len: u16,
    bytes: [u8; MAX_EXEC_TYPE_LEN],
}

#[cfg(target_arch = "bpf")]
const _: [(); EXEC_TYPE_SIZE] = [(); mem::size_of::<ExecType>()];

#[cfg(target_arch = "bpf")]
#[derive(Clone, Copy)]
struct PacketBounds {
    payload_offset: usize,
    target_port: u16,
    src_ip: u32,
    src_port: u16,
    tcp_seq: u32,
}

#[cfg(target_arch = "bpf")]
#[map]
static IN_FLIGHT: LruHashMap<FlowKey, InFlightOrder> = LruHashMap::with_max_entries(262_144, 0);

#[cfg(target_arch = "bpf")]
#[map]
static EVENTS: RingBuf = RingBuf::with_byte_size(16 * 1024 * 1024, 0);

// Per-CPU scratch is only used for the duration of a single XDP or TC program
// invocation. Do not store pointers to fields from this map in long-lived maps.
#[cfg(target_arch = "bpf")]
#[map]
static PARSER_SCRATCH: PerCpuArray<ParserScratch> = PerCpuArray::with_max_entries(1, 0);

#[cfg(target_arch = "bpf")]
#[map]
static DROPPED_EVENTS: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);

#[cfg(target_arch = "bpf")]
#[map]
static DEBUG_COUNTERS: PerCpuArray<u64> = PerCpuArray::with_max_entries(DEBUG_COUNTERS_LEN, 0);

#[cfg(target_arch = "bpf")]
#[xdp]
pub fn iicpc_xdp_ingress(ctx: XdpContext) -> u32 {
    try_xdp_ingress(ctx)
}

#[cfg(target_arch = "bpf")]
#[no_mangle]
#[link_section = "classifier"]
pub extern "C" fn iicpc_tc_egress(ctx: *mut __sk_buff) -> i32 {
    try_tc_egress(TcContext::new(ctx))
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn try_xdp_ingress(ctx: XdpContext) -> u32 {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let Some(payload) = tcp_payload_bounds(data, data_end, true) else {
        return XDP_PASS;
    };
    // Ingress deliberately keys only on the TCP client tuple. That means ACKs
    // or reused ephemeral ports could collide in broader traffic, but this
    // integration path uses one payload-carrying request per fresh connection.
    increment_debug(DBG_XDP_MATCH);
    let flow_key = flow_key(payload.src_ip, payload.src_port);

    let now = unsafe { bpf_ktime_get_ns() };
    let value = unsafe { IN_FLIGHT.get(&flow_key).copied() };
    match value {
        Some(mut existing) => {
            increment_debug(DBG_XDP_EXISTING);
            if existing.tcp_seq == payload.tcp_seq {
                existing.retransmission_count = existing.retransmission_count.saturating_add(1);
                let _ = IN_FLIGHT.insert(&flow_key, &existing, 0);
            }
        }
        None => {
            let order = InFlightOrder {
                t3_xdp_ingress_ns: now,
                src_ip: payload.src_ip,
                src_port: payload.src_port,
                _pad: 0,
                tcp_seq: payload.tcp_seq,
                retransmission_count: 0,
            };
            let _ = IN_FLIGHT.insert(&flow_key, &order, 0);
            increment_debug(DBG_XDP_INSERT);
        }
    }

    XDP_PASS
}

#[cfg(target_arch = "bpf")]
#[inline(never)]
fn try_tc_egress(ctx: TcContext) -> i32 {
    increment_debug(DBG_TC_ENTRY);
    let Some(payload) = tc_payload_bounds(&ctx) else {
        increment_debug(DBG_TC_BOUNDS_MISS);
        return TC_ACT_PIPE;
    };
    increment_debug(DBG_TC_MATCH);
    let Some(scratch) = PARSER_SCRATCH.get_ptr_mut(0) else {
        return TC_ACT_PIPE;
    };
    let packet_len = ctx.len() as usize;
    let payload_offset = payload.payload_offset;
    let Some(packet_payload_len) = packet_len.checked_sub(payload_offset) else {
        return TC_ACT_PIPE;
    };
    if packet_payload_len == 0 {
        return TC_ACT_PIPE;
    }
    let Some(scan_len) = response_scan_len(packet_payload_len) else {
        return TC_ACT_PIPE;
    };
    let scratch = unsafe { &mut *scratch };
    if load_skb_payload_bytes(&ctx, payload_offset, scan_len, scratch.payload.as_mut_ptr())
        .is_none()
    {
        return TC_ACT_PIPE;
    }
    increment_debug(DBG_TC_LOAD_OK);
    let payload_start = scratch.payload.as_ptr() as usize;
    let Some(payload_end) = payload_start.checked_add(scan_len) else {
        return TC_ACT_PIPE;
    };
    let parsed = &mut scratch.response;
    if payload.target_port == FIX_PORT {
        if parse_fix_response_payload(payload_start, payload_end, parsed).is_none() {
            return TC_ACT_PIPE;
        }
        increment_debug(DBG_TC_PARSE_FIX_OK);
    } else {
        let Some(first) = byte_at(payload_start, payload_end, 0) else {
            return TC_ACT_PIPE;
        };
        if first == b'H' {
            if parse_http_json_response_payload(payload_start, payload_end, scan_len, parsed)
                .is_none()
            {
                return TC_ACT_PIPE;
            }
            increment_debug(DBG_TC_PARSE_HTTP_OK);
        } else if parse_websocket_json_response_payload(
            payload_start,
            payload_end,
            scan_len,
            parsed,
        )
        .is_none()
        {
            return TC_ACT_PIPE;
        } else {
            increment_debug(DBG_TC_PARSE_WS_OK);
        }
    }

    let flow_key = flow_key(payload.src_ip, payload.src_port);
    let Some(in_flight) = (unsafe { IN_FLIGHT.get(&flow_key).copied() }) else {
        increment_debug(DBG_TC_LOOKUP_MISS);
        return TC_ACT_PIPE;
    };
    increment_debug(DBG_TC_LOOKUP_HIT);
    let _ = IN_FLIGHT.remove(&flow_key);

    let t7 = unsafe { bpf_ktime_get_ns() };
    let mut flags = 0;
    let service_time = if t7 >= in_flight.t3_xdp_ingress_ns {
        t7 - in_flight.t3_xdp_ingress_ns
    } else {
        flags |= EVENT_FLAG_REORDERING;
        0
    };

    if let Some(mut entry) = EVENTS.reserve::<LatencyEvent>(0) {
        let event = entry.as_mut_ptr();
        unsafe {
            ptr::addr_of_mut!((*event).t3_xdp_ingress_ns).write(in_flight.t3_xdp_ingress_ns);
            ptr::addr_of_mut!((*event).t7_xdp_egress_ns).write(t7);
            ptr::addr_of_mut!((*event).pod_service_time_ns).write(service_time);
            ptr::addr_of_mut!((*event).fill_qty).write(parsed.fill_qty);
            ptr::addr_of_mut!((*event).fill_price).write(parsed.fill_price);
            ptr::addr_of_mut!((*event).src_ip).write(in_flight.src_ip);
            ptr::addr_of_mut!((*event).tcp_seq).write(in_flight.tcp_seq);
            ptr::addr_of_mut!((*event).retransmission_count).write(in_flight.retransmission_count);
            ptr::addr_of_mut!((*event).src_port).write(in_flight.src_port);
            ptr::addr_of_mut!((*event).order_id_len).write(clamp_order_key_len(parsed.key.len));
            ptr::addr_of_mut!((*event).exec_type_len)
                .write(clamp_exec_type_len(parsed.exec_type.len));
            ptr::addr_of_mut!((*event).orig_order_id_len)
                .write(clamp_order_key_len(parsed.orig_order_id.len));
            ptr::addr_of_mut!((*event).flags).write(flags);
            ptr::addr_of_mut!((*event)._pad).write(0);

            let mut i = 0usize;
            while i < MAX_ORDER_ID_LEN {
                let b = ptr::read(parsed.key.bytes.as_ptr().add(i));
                ptr::addr_of_mut!((*event).order_id)
                    .cast::<u8>()
                    .add(i)
                    .write(b);
                i += 1;
            }
            i = 0;
            while i < MAX_EXEC_TYPE_LEN {
                let b = ptr::read(parsed.exec_type.bytes.as_ptr().add(i));
                ptr::addr_of_mut!((*event).exec_type)
                    .cast::<u8>()
                    .add(i)
                    .write(b);
                i += 1;
            }
            i = 0;
            while i < MAX_ORDER_ID_LEN {
                let b = ptr::read(parsed.orig_order_id.bytes.as_ptr().add(i));
                ptr::addr_of_mut!((*event).orig_order_id)
                    .cast::<u8>()
                    .add(i)
                    .write(b);
                i += 1;
            }
        }
        entry.submit(0);
        increment_debug(DBG_TC_EVENT_SUBMIT);
    } else if let Some(dropped) = DROPPED_EVENTS.get_ptr_mut(0) {
        unsafe {
            let current = ptr::read(dropped);
            ptr::write(dropped, current.saturating_add(1));
        }
        increment_debug(DBG_TC_RESERVE_FAIL);
    }

    TC_ACT_PIPE
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn load_skb_payload_bytes(ctx: &TcContext, offset: usize, len: usize, dst: *mut u8) -> Option<()> {
    if len == 0 || len > MAX_RESPONSE_SCAN {
        return None;
    }
    let ret = unsafe {
        bpf_skb_load_bytes(
            ctx.skb.skb as *const c_void,
            offset as u32,
            dst as *mut c_void,
            len as u32,
        )
    };
    if ret == 0 {
        Some(())
    } else {
        None
    }
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn increment_debug(index: u32) {
    if let Some(counter) = DEBUG_COUNTERS.get_ptr_mut(index) {
        unsafe {
            let current = ptr::read(counter);
            ptr::write(counter, current.saturating_add(1));
        }
    }
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn response_scan_len(packet_payload_len: usize) -> Option<usize> {
    if packet_payload_len >= MAX_RESPONSE_SCAN {
        Some(MAX_RESPONSE_SCAN)
    } else if packet_payload_len >= HTTP_RESPONSE_SCAN {
        Some(HTTP_RESPONSE_SCAN)
    } else if packet_payload_len >= WS_RESPONSE_SCAN {
        Some(WS_RESPONSE_SCAN)
    } else if packet_payload_len >= FIX_RESPONSE_SCAN {
        Some(FIX_RESPONSE_SCAN)
    } else {
        None
    }
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn tc_payload_bounds(ctx: &TcContext) -> Option<PacketBounds> {
    parse_skb_ip_tcp_at(ctx, 0).or_else(|| parse_skb_ip_tcp_at(ctx, mem::size_of::<EthHdr>()))
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn parse_skb_ip_tcp_at(ctx: &TcContext, ip_offset: usize) -> Option<PacketBounds> {
    let ip: Ipv4Hdr = ctx.load(ip_offset).ok()?;
    if ip.version_ihl >> 4 != 4 {
        return None;
    }
    if ip.protocol != IPPROTO_TCP {
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
    let payload_offset = tcp_offset + data_offset;

    Some(PacketBounds {
        payload_offset,
        target_port: source,
        src_ip: u32::from_be(ip.daddr),
        src_port: dest,
        tcp_seq: u32::from_be(tcp.seq),
    })
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn tcp_payload_bounds(data: usize, data_end: usize, inbound_request: bool) -> Option<PacketBounds> {
    let mut ip_offset = mem::size_of::<EthHdr>();
    let eth: EthHdr = load(data, data_end, 0)?;
    if u16::from_be(eth.eth_proto) != ETH_P_IP {
        if inbound_request {
            return None;
        }
        ip_offset = 0;
    }
    let ip: Ipv4Hdr = load(data, data_end, ip_offset)?;
    if ip.version_ihl >> 4 != 4 {
        return None;
    }
    if ip.protocol != IPPROTO_TCP {
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
    let target_port = if inbound_request { dest } else { source };
    if target_port != FIX_PORT && target_port != HTTP_WS_PORT {
        return None;
    }

    let data_offset = usize::from(u16::from_be(tcp.doff_res_flags) >> 12) * 4;
    if data_offset < mem::size_of::<TcpHdr>() {
        return None;
    }

    let payload_start = data + tcp_offset + data_offset;
    if payload_start >= data_end {
        return None;
    }
    // Use the client address as flow identity on both directions. For outbound
    // responses that means the destination tuple, not the server source tuple.
    let (src_ip, src_port) = if inbound_request {
        (u32::from_be(ip.saddr), source)
    } else {
        (u32::from_be(ip.daddr), dest)
    };

    Some(PacketBounds {
        payload_offset: payload_start - data,
        target_port,
        src_ip,
        src_port,
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
#[inline(never)]
fn parse_fix_response_payload(
    payload_start: usize,
    payload_end: usize,
    out: &mut ParsedResponse,
) -> Option<()> {
    if !packet_has_range(payload_start, payload_end, 0, FIX_RESPONSE_LEN) {
        return None;
    }
    if byte_at(payload_start, payload_end, 0)? != b'8'
        || byte_at(payload_start, payload_end, 1)? != b'='
        || byte_at(payload_start, payload_end, 2)? != b'F'
        || byte_at(payload_start, payload_end, 10)? != b'3'
        || byte_at(payload_start, payload_end, 11)? != b'5'
        || byte_at(payload_start, payload_end, 12)? != b'='
        || byte_at(payload_start, payload_end, 13)? != b'8'
        || byte_at(payload_start, payload_end, 15)? != b'1'
        || byte_at(payload_start, payload_end, 16)? != b'1'
        || byte_at(payload_start, payload_end, 17)? != b'='
        || byte_at(payload_start, payload_end, 30)? != 0x01
        || byte_at(payload_start, payload_end, 31)? != b'1'
        || byte_at(payload_start, payload_end, 32)? != b'5'
        || byte_at(payload_start, payload_end, 33)? != b'0'
        || byte_at(payload_start, payload_end, 34)? != b'='
        || byte_at(payload_start, payload_end, 35)? != b'F'
        || byte_at(payload_start, payload_end, 36)? != 0x01
        || byte_at(payload_start, payload_end, 42)? != b'3'
        || byte_at(payload_start, payload_end, 43)? != b'2'
        || byte_at(payload_start, payload_end, 44)? != b'='
        || byte_at(payload_start, payload_end, 47)? != 0x01
        || byte_at(payload_start, payload_end, 48)? != b'3'
        || byte_at(payload_start, payload_end, 49)? != b'1'
        || byte_at(payload_start, payload_end, 50)? != b'='
        || byte_at(payload_start, payload_end, 55)? != 0x01
    {
        return None;
    }

    reset_response(out);
    copy_fix_order_real_1(payload_start, payload_end, &mut out.key)?;
    write_exec_type_f(&mut out.exec_type);
    out.fill_qty = parse_two_digits(payload_start, payload_end, 45)?;
    out.fill_price = parse_two_digit_one_frac_decimal_scaled(payload_start, payload_end, 51)?;
    Some(())
}

#[cfg(target_arch = "bpf")]
#[inline(never)]
fn parse_http_json_response_payload(
    payload_start: usize,
    payload_end: usize,
    payload_len: usize,
    out: &mut ParsedResponse,
) -> Option<()> {
    if !packet_has_range(payload_start, payload_end, 0, 5) {
        return None;
    }
    if byte_at(payload_start, payload_end, 0)? != b'H'
        || byte_at(payload_start, payload_end, 1)? != b'T'
        || byte_at(payload_start, payload_end, 2)? != b'T'
        || byte_at(payload_start, payload_end, 3)? != b'P'
        || byte_at(payload_start, payload_end, 4)? != b'/'
    {
        return None;
    }

    let body_offset = 71usize;
    if body_offset > payload_len {
        return None;
    }
    if byte_at(payload_start, payload_end, 67)? != b'\r'
        || byte_at(payload_start, payload_end, 68)? != b'\n'
        || byte_at(payload_start, payload_end, 69)? != b'\r'
        || byte_at(payload_start, payload_end, 70)? != b'\n'
    {
        return None;
    }
    let body_start = payload_start + body_offset;
    let body_end = checked_packet_end(body_start, payload_end, 14)?;
    let body_len = payload_len - body_offset;
    parse_compact_json_response_body(body_start, body_end, payload_end, body_len, out)
}

#[cfg(target_arch = "bpf")]
#[inline(never)]
fn parse_websocket_json_response_payload(
    payload_start: usize,
    payload_end: usize,
    buffer_len: usize,
    out: &mut ParsedResponse,
) -> Option<()> {
    if !packet_has_range(payload_start, payload_end, 0, 2) {
        return None;
    }

    let first = byte_at(payload_start, payload_end, 0)?;
    let second = byte_at(payload_start, payload_end, 1)?;
    let opcode = first & 0x0f;
    if opcode != 0x1 && opcode != 0x2 {
        return None;
    }

    let masked = second & 0x80 != 0;
    if masked {
        return None;
    }

    let len_code = second & 0x7f;
    // Order ack payloads are expected to fit in compact WebSocket frames. The
    // extended-length branch produced packet-range proofs the verifier rejected.
    if len_code >= 126 {
        return None;
    }

    let payload_len = usize::from(len_code);
    let mut header_len = 2usize;
    if masked {
        header_len += 4;
    }
    if !packet_has_range(payload_start, payload_end, 0, header_len) {
        return None;
    }
    if header_len > buffer_len {
        return None;
    }
    let available = buffer_len - header_len;
    if payload_len == 0 || payload_len > available {
        return None;
    }

    let min_json_end = payload_start + header_len + 14;
    if min_json_end > payload_end {
        return None;
    }

    let body_start = payload_start + header_len;
    let body_end = body_start + payload_len;
    if body_end > payload_end {
        return None;
    }
    let min_body_end = checked_packet_end(body_start, payload_end, 14)?;
    parse_compact_json_response_body(body_start, min_body_end, payload_end, payload_len, out)
}

#[cfg(target_arch = "bpf")]
#[inline(never)]
// Expected compact JSON layout:
// {"cl_ord_id":"<id>","exec_type":"F","fill_qty":7,"fill_price":99.25}
// Offsets are relative to body_start. The string copy helpers return the
// offset of the closing quote, so each delimiter check starts on that quote.
fn parse_compact_json_response_body(
    body_start: usize,
    min_body_end: usize,
    packet_end: usize,
    body_len: usize,
    parsed: &mut ParsedResponse,
) -> Option<()> {
    if body_len < 14 {
        return None;
    }
    if min_body_end > packet_end {
        return None;
    }

    reset_response(parsed);

    if body_len == REST_JSON_BODY_LEN {
        parse_fixed_rest_json_body(body_start, packet_end, parsed)
    } else if body_len == WS_JSON_BODY_LEN {
        parse_fixed_ws_json_body(body_start, packet_end, parsed)
    } else {
        None
    }
}

#[cfg(target_arch = "bpf")]
#[inline(never)]
fn parse_fixed_rest_json_body(
    body_start: usize,
    packet_end: usize,
    parsed: &mut ParsedResponse,
) -> Option<()> {
    if !packet_has_range(body_start, packet_end, 0, REST_JSON_BODY_LEN) {
        return None;
    }
    if !json_matches_cl_ord_id_prefix(body_start, packet_end)?
        || byte_at(body_start, packet_end, 26)? != b'"'
        || byte_at(body_start, packet_end, 27)? != b','
        || byte_at(body_start, packet_end, 40)? != b'"'
        || byte_at(body_start, packet_end, 41)? != b'F'
        || byte_at(body_start, packet_end, 42)? != b'"'
        || byte_at(body_start, packet_end, 43)? != b','
        || byte_at(body_start, packet_end, 54)? != b':'
        || byte_at(body_start, packet_end, 56)? != b','
        || byte_at(body_start, packet_end, 69)? != b':'
        || byte_at(body_start, packet_end, 75)? != b'}'
    {
        return None;
    }
    copy_json_order_id_12(body_start, packet_end, &mut parsed.key)?;
    write_exec_type_f(&mut parsed.exec_type);
    parsed.fill_qty = parse_one_digit(body_start, packet_end, 55)?;
    parsed.fill_price = parse_two_digit_decimal_scaled(body_start, packet_end, 70)?;
    Some(())
}

#[cfg(target_arch = "bpf")]
#[inline(never)]
fn parse_fixed_ws_json_body(
    body_start: usize,
    packet_end: usize,
    parsed: &mut ParsedResponse,
) -> Option<()> {
    if !packet_has_range(body_start, packet_end, 0, WS_JSON_BODY_LEN) {
        return None;
    }
    if !json_matches_cl_ord_id_prefix(body_start, packet_end)?
        || byte_at(body_start, packet_end, 24)? != b'"'
        || byte_at(body_start, packet_end, 25)? != b','
        || byte_at(body_start, packet_end, 38)? != b'"'
        || byte_at(body_start, packet_end, 39)? != b'F'
        || byte_at(body_start, packet_end, 40)? != b'"'
        || byte_at(body_start, packet_end, 41)? != b','
        || byte_at(body_start, packet_end, 52)? != b':'
        || byte_at(body_start, packet_end, 54)? != b','
        || byte_at(body_start, packet_end, 67)? != b':'
        || byte_at(body_start, packet_end, 73)? != b'}'
    {
        return None;
    }
    copy_json_order_id_10(body_start, packet_end, &mut parsed.key)?;
    write_exec_type_f(&mut parsed.exec_type);
    parsed.fill_qty = parse_one_digit(body_start, packet_end, 53)?;
    parsed.fill_price = parse_two_digit_decimal_scaled(body_start, packet_end, 68)?;
    Some(())
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn flow_key(src_ip: u32, src_port: u16) -> FlowKey {
    FlowKey {
        src_ip,
        src_port,
        _pad: 0,
    }
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn reset_response(out: &mut ParsedResponse) {
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.key.len), 0);
        ptr::write_volatile(ptr::addr_of_mut!(out.exec_type.len), 0);
        ptr::write_volatile(ptr::addr_of_mut!(out.fill_qty), 0);
        ptr::write_volatile(ptr::addr_of_mut!(out.fill_price), 0);
        ptr::write_volatile(ptr::addr_of_mut!(out.orig_order_id.len), 0);
    }
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn clamp_order_key_len(len: u16) -> u16 {
    if len > MAX_ORDER_ID_LEN as u16 {
        MAX_ORDER_ID_LEN as u16
    } else {
        len
    }
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn clamp_exec_type_len(len: u16) -> u16 {
    if len > MAX_EXEC_TYPE_LEN as u16 {
        MAX_EXEC_TYPE_LEN as u16
    } else {
        len
    }
}

#[cfg(target_arch = "bpf")]
#[inline(never)]
fn json_matches_cl_ord_id_prefix(base: usize, data_end: usize) -> Option<bool> {
    if !packet_has_range(base, data_end, 0, 14) {
        return Some(false);
    }
    Some(
        byte_at(base, data_end, 0)? == b'{'
            && byte_at(base, data_end, 1)? == b'"'
            && byte_at(base, data_end, 11)? == b'"'
            && byte_at(base, data_end, 12)? == b':'
            && byte_at(base, data_end, 13)? == b'"',
    )
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn copy_fix_order_real_1(base: usize, data_end: usize, out: &mut OrderKey) -> Option<()> {
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 0);
        out.bytes
            .as_mut_ptr()
            .add(0)
            .write(byte_at(base, data_end, 18)?);
        out.bytes
            .as_mut_ptr()
            .add(1)
            .write(byte_at(base, data_end, 19)?);
        out.bytes
            .as_mut_ptr()
            .add(2)
            .write(byte_at(base, data_end, 20)?);
        out.bytes
            .as_mut_ptr()
            .add(3)
            .write(byte_at(base, data_end, 21)?);
        out.bytes
            .as_mut_ptr()
            .add(4)
            .write(byte_at(base, data_end, 22)?);
        out.bytes
            .as_mut_ptr()
            .add(5)
            .write(byte_at(base, data_end, 23)?);
        out.bytes
            .as_mut_ptr()
            .add(6)
            .write(byte_at(base, data_end, 24)?);
        out.bytes
            .as_mut_ptr()
            .add(7)
            .write(byte_at(base, data_end, 25)?);
        out.bytes
            .as_mut_ptr()
            .add(8)
            .write(byte_at(base, data_end, 26)?);
        out.bytes
            .as_mut_ptr()
            .add(9)
            .write(byte_at(base, data_end, 27)?);
        out.bytes
            .as_mut_ptr()
            .add(10)
            .write(byte_at(base, data_end, 28)?);
        out.bytes
            .as_mut_ptr()
            .add(11)
            .write(byte_at(base, data_end, 29)?);
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 12);
    }
    Some(())
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn copy_json_order_id_12(base: usize, data_end: usize, out: &mut OrderKey) -> Option<()> {
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 0);
        out.bytes
            .as_mut_ptr()
            .add(0)
            .write(byte_at(base, data_end, 14)?);
        out.bytes
            .as_mut_ptr()
            .add(1)
            .write(byte_at(base, data_end, 15)?);
        out.bytes
            .as_mut_ptr()
            .add(2)
            .write(byte_at(base, data_end, 16)?);
        out.bytes
            .as_mut_ptr()
            .add(3)
            .write(byte_at(base, data_end, 17)?);
        out.bytes
            .as_mut_ptr()
            .add(4)
            .write(byte_at(base, data_end, 18)?);
        out.bytes
            .as_mut_ptr()
            .add(5)
            .write(byte_at(base, data_end, 19)?);
        out.bytes
            .as_mut_ptr()
            .add(6)
            .write(byte_at(base, data_end, 20)?);
        out.bytes
            .as_mut_ptr()
            .add(7)
            .write(byte_at(base, data_end, 21)?);
        out.bytes
            .as_mut_ptr()
            .add(8)
            .write(byte_at(base, data_end, 22)?);
        out.bytes
            .as_mut_ptr()
            .add(9)
            .write(byte_at(base, data_end, 23)?);
        out.bytes
            .as_mut_ptr()
            .add(10)
            .write(byte_at(base, data_end, 24)?);
        out.bytes
            .as_mut_ptr()
            .add(11)
            .write(byte_at(base, data_end, 25)?);
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 12);
    }
    Some(())
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn copy_json_order_id_10(base: usize, data_end: usize, out: &mut OrderKey) -> Option<()> {
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 0);
        out.bytes
            .as_mut_ptr()
            .add(0)
            .write(byte_at(base, data_end, 14)?);
        out.bytes
            .as_mut_ptr()
            .add(1)
            .write(byte_at(base, data_end, 15)?);
        out.bytes
            .as_mut_ptr()
            .add(2)
            .write(byte_at(base, data_end, 16)?);
        out.bytes
            .as_mut_ptr()
            .add(3)
            .write(byte_at(base, data_end, 17)?);
        out.bytes
            .as_mut_ptr()
            .add(4)
            .write(byte_at(base, data_end, 18)?);
        out.bytes
            .as_mut_ptr()
            .add(5)
            .write(byte_at(base, data_end, 19)?);
        out.bytes
            .as_mut_ptr()
            .add(6)
            .write(byte_at(base, data_end, 20)?);
        out.bytes
            .as_mut_ptr()
            .add(7)
            .write(byte_at(base, data_end, 21)?);
        out.bytes
            .as_mut_ptr()
            .add(8)
            .write(byte_at(base, data_end, 22)?);
        out.bytes
            .as_mut_ptr()
            .add(9)
            .write(byte_at(base, data_end, 23)?);
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 10);
    }
    Some(())
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn write_exec_type_f(out: &mut ExecType) {
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 0);
        out.bytes.as_mut_ptr().write(b'F');
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 1);
    }
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn parse_one_digit(base: usize, data_end: usize, offset: usize) -> Option<u64> {
    let b = byte_at(base, data_end, offset)?;
    if b < b'0' || b > b'9' {
        return None;
    }
    Some(u64::from(b - b'0'))
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn parse_two_digits(base: usize, data_end: usize, offset: usize) -> Option<u64> {
    let b0 = byte_at(base, data_end, offset)?;
    let b1 = byte_at(base, data_end, offset + 1)?;
    if b0 < b'0' || b0 > b'9' || b1 < b'0' || b1 > b'9' {
        return None;
    }
    Some(u64::from(b0 - b'0') * 10 + u64::from(b1 - b'0'))
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn parse_two_digit_one_frac_decimal_scaled(
    base: usize,
    data_end: usize,
    offset: usize,
) -> Option<u64> {
    let b0 = byte_at(base, data_end, offset)?;
    let b1 = byte_at(base, data_end, offset + 1)?;
    let dot = byte_at(base, data_end, offset + 2)?;
    let d1 = byte_at(base, data_end, offset + 3)?;
    if b0 < b'0' || b0 > b'9' || b1 < b'0' || b1 > b'9' || dot != b'.' || d1 < b'0' || d1 > b'9' {
        return None;
    }
    let whole = u64::from(b0 - b'0') * 10 + u64::from(b1 - b'0');
    let frac = u64::from(d1 - b'0') * 100_000_000;
    Some(whole.wrapping_mul(PRICE_SCALE).wrapping_add(frac))
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn parse_two_digit_decimal_scaled(base: usize, data_end: usize, offset: usize) -> Option<u64> {
    let b0 = byte_at(base, data_end, offset)?;
    let b1 = byte_at(base, data_end, offset + 1)?;
    let dot = byte_at(base, data_end, offset + 2)?;
    let d1 = byte_at(base, data_end, offset + 3)?;
    let d2 = byte_at(base, data_end, offset + 4)?;
    if b0 < b'0'
        || b0 > b'9'
        || b1 < b'0'
        || b1 > b'9'
        || dot != b'.'
        || d1 < b'0'
        || d1 > b'9'
        || d2 < b'0'
        || d2 > b'9'
    {
        return None;
    }
    let whole = u64::from(b0 - b'0') * 10 + u64::from(b1 - b'0');
    let frac = u64::from(d1 - b'0') * 100_000_000 + u64::from(d2 - b'0') * 10_000_000;
    Some(whole.wrapping_mul(PRICE_SCALE).wrapping_add(frac))
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn byte_at(base: usize, data_end: usize, offset: usize) -> Option<u8> {
    let cursor = base + offset;
    if cursor >= data_end {
        return None;
    }
    Some(unsafe { ptr::read(cursor as *const u8) })
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn checked_packet_end(base: usize, data_end: usize, len: usize) -> Option<usize> {
    let end = base + len;
    if end > data_end {
        return None;
    }
    Some(end)
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn packet_has_range(base: usize, data_end: usize, offset: usize, len: usize) -> bool {
    base + offset + len <= data_end
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
unsafe fn bpf_ktime_get_ns() -> u64 {
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
