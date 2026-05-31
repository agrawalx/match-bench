#![cfg_attr(target_arch = "bpf", no_std)]
#![cfg_attr(target_arch = "bpf", no_main)]

#[cfg(target_arch = "bpf")]
use aya_ebpf::{
    bindings::{__sk_buff, xdp_action::XDP_PASS, TC_ACT_PIPE},
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
const MAX_JSON_ORDER_ID_SCAN: usize = 16;
#[cfg(target_arch = "bpf")]
// Keep parser bounds small enough for the kernel verifier to finish. The hot
// fields used by supported order/ack payloads are expected near the front.
const MAX_FIX_SCAN: usize = 96;
#[cfg(target_arch = "bpf")]
const MAX_FIX_FIELDS: usize = 12;
#[cfg(target_arch = "bpf")]
const MAX_HTTP_HEADER_SCAN: usize = 80;
#[cfg(target_arch = "bpf")]
const MAX_RESPONSE_SCAN: usize = 192;
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
    payload_start: usize,
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
    let flow_key = flow_key(payload.src_ip, payload.src_port);

    let now = unsafe { bpf_ktime_get_ns() };
    let value = unsafe { IN_FLIGHT.get(&flow_key).copied() };
    match value {
        Some(mut existing) => {
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
        }
    }

    XDP_PASS
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn try_tc_egress(ctx: TcContext) -> i32 {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let Some(payload) = tcp_payload_bounds(data, data_end, false) else {
        return TC_ACT_PIPE;
    };
    let Some(scratch) = PARSER_SCRATCH.get_ptr_mut(0) else {
        return TC_ACT_PIPE;
    };
    let Some(payload_offset) = payload.payload_start.checked_sub(data) else {
        return TC_ACT_PIPE;
    };
    let scratch = unsafe { &mut *scratch };
    let Ok(scan_len) = ctx.load_bytes(payload_offset, &mut scratch.payload) else {
        return TC_ACT_PIPE;
    };
    if scan_len == 0 {
        return TC_ACT_PIPE;
    }
    let payload_start = scratch.payload.as_ptr() as usize;
    let Some(payload_end) = payload_start.checked_add(scan_len) else {
        return TC_ACT_PIPE;
    };
    let parsed = &mut scratch.response;
    if payload.target_port == FIX_PORT {
        if parse_fix_response_payload(payload_start, payload_end, parsed).is_none() {
            return TC_ACT_PIPE;
        }
    } else {
        let Some(first) = byte_at(payload_start, payload_end, 0) else {
            return TC_ACT_PIPE;
        };
        if first == b'H' {
            if parse_http_json_response_payload(payload_start, payload_end, parsed).is_none()
            {
                return TC_ACT_PIPE;
            }
        } else if parse_websocket_json_response_payload(
            payload_start,
            payload_end,
            false,
            parsed,
        )
        .is_none()
        {
            return TC_ACT_PIPE;
        }
    }

    let flow_key = flow_key(payload.src_ip, payload.src_port);
    let Some(in_flight) = (unsafe { IN_FLIGHT.get(&flow_key).copied() }) else {
        return TC_ACT_PIPE;
    };
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
    } else if let Some(dropped) = DROPPED_EVENTS.get_ptr_mut(0) {
        unsafe {
            let current = ptr::read(dropped);
            ptr::write(dropped, current.saturating_add(1));
        }
    }

    TC_ACT_PIPE
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn tcp_payload_bounds(data: usize, data_end: usize, inbound_request: bool) -> Option<PacketBounds> {
    let eth: EthHdr = load(data, data_end, 0)?;
    if u16::from_be(eth.eth_proto) != ETH_P_IP {
        return None;
    }

    let ip_offset = mem::size_of::<EthHdr>();
    let ip: Ipv4Hdr = load(data, data_end, ip_offset)?;
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
        payload_start,
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
fn parse_fix_response_payload(
    payload_start: usize,
    payload_end: usize,
    out: &mut ParsedResponse,
) -> Option<()> {
    let scan_len = core::cmp::min(payload_end - payload_start, MAX_FIX_SCAN);
    if !starts_with_fix(payload_start, payload_end, scan_len)? {
        return None;
    }

    let mut msg_type = 0u8;
    reset_response(out);

    let mut i = 0usize;
    let mut fields = 0usize;
    while i < scan_len && fields < MAX_FIX_FIELDS {
        if i + 3 < scan_len {
            let t0 = byte_at(payload_start, payload_end, i)?;
            let t1 = byte_at(payload_start, payload_end, i + 1)?;
            let t2 = byte_at(payload_start, payload_end, i + 2)?;
            if t2 == b'=' {
                if t0 == b'3' && t1 == b'5' {
                    msg_type = byte_at(payload_start, payload_end, i + 3)?;
                } else if t0 == b'1' && t1 == b'1' {
                    copy_fix_value_to_order_key(
                        payload_start,
                        payload_end,
                        scan_len,
                        i + 3,
                        &mut out.key,
                    )?;
                } else if t0 == b'3' && t1 == b'2' {
                    out.fill_qty = parse_fix_u64(payload_start, payload_end, scan_len, i + 3)?;
                } else if t0 == b'3' && t1 == b'1' {
                    out.fill_price =
                        parse_fix_decimal_scaled(payload_start, payload_end, scan_len, i + 3)?;
                } else if t0 == b'3' && t1 == b'9' && out.exec_type.len == 0 {
                    copy_fix_value_to_exec_type(
                        payload_start,
                        payload_end,
                        scan_len,
                        i + 3,
                        &mut out.exec_type,
                    )?;
                } else if t0 == b'4' && t1 == b'1' {
                    copy_fix_value_to_order_key(
                        payload_start,
                        payload_end,
                        scan_len,
                        i + 3,
                        &mut out.orig_order_id,
                    )?;
                }
            }
        }

        if i + 4 < scan_len
            && byte_at(payload_start, payload_end, i)? == b'1'
            && byte_at(payload_start, payload_end, i + 1)? == b'5'
            && byte_at(payload_start, payload_end, i + 2)? == b'0'
            && byte_at(payload_start, payload_end, i + 3)? == b'='
        {
            copy_fix_value_to_exec_type(
                payload_start,
                payload_end,
                scan_len,
                i + 4,
                &mut out.exec_type,
            )?;
        }

        while i < scan_len {
            let b = byte_at(payload_start, payload_end, i)?;
            i += 1;
            if b == 0x01 {
                break;
            }
        }
        fields += 1;
    }

    if out.key.len == 0 || msg_type != b'8' {
        return None;
    }

    Some(())
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn parse_http_json_response_payload(
    payload_start: usize,
    payload_end: usize,
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

    let mut header_offset = 0usize;
    let mut crlf_state = 0u8;
    let mut body_offset = 0usize;
    while header_offset < MAX_HTTP_HEADER_SCAN {
        let cursor = payload_start.checked_add(header_offset)?;
        if cursor >= payload_end {
            return None;
        }
        let b = unsafe { ptr::read(cursor as *const u8) };
        if crlf_state == 0 {
            if b == b'\r' {
                crlf_state = 1;
            }
        } else if crlf_state == 1 {
            if b == b'\n' {
                crlf_state = 2;
            } else if b != b'\r' {
                crlf_state = 0;
            }
        } else if crlf_state == 2 {
            if b == b'\r' {
                crlf_state = 3;
            } else {
                crlf_state = 0;
            }
        } else if b == b'\n' {
            body_offset = header_offset.checked_add(1)?;
            break;
        } else if b == b'\r' {
            crlf_state = 1;
        } else {
            crlf_state = 0;
        }
        header_offset += 1;
    }
    if body_offset == 0 {
        return None;
    }
    let body_start = payload_start.checked_add(body_offset)?;
    let body_end = checked_packet_end(body_start, payload_end, 14)?;
    let body_len = payload_end.checked_sub(body_start)?;
    parse_compact_json_response_body(body_start, body_end, payload_end, body_len, out)
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn parse_websocket_json_response_payload(
    payload_start: usize,
    payload_end: usize,
    client_to_server: bool,
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
    if client_to_server && !masked {
        return None;
    }
    if !client_to_server && masked {
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
        header_len = header_len.checked_add(4)?;
    }
    if !packet_has_range(payload_start, payload_end, 0, header_len) {
        return None;
    }
    let available = payload_end
        .checked_sub(payload_start)?
        .checked_sub(header_len)?;
    if payload_len == 0 || payload_len > available {
        return None;
    }

    let min_json_end = payload_start.checked_add(header_len)?.checked_add(14)?;
    if min_json_end > payload_end {
        return None;
    }

    let body_start = payload_start.checked_add(header_len)?;
    let body_end = body_start.checked_add(payload_len)?;
    if body_end > payload_end {
        return None;
    }
    let min_body_end = checked_packet_end(body_start, payload_end, 14)?;
    parse_compact_json_response_body(body_start, min_body_end, payload_end, payload_len, out)
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
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

    if !json_matches_cl_ord_id_prefix(body_start, packet_end)? {
        return None;
    }
    let mut offset = 14usize;
    offset =
        copy_json_string_to_order_key(body_start, packet_end, body_len, offset, &mut parsed.key)?;

    if offset.checked_add(15)? > body_len
        || !packet_has_range(body_start, packet_end, offset, 15)
    {
        return None;
    }
    if byte_at(body_start, packet_end, offset)? != b'"'
        || byte_at(body_start, packet_end, offset + 1)? != b','
        || byte_at(body_start, packet_end, offset + 14)? != b'"'
    {
        return None;
    }
    offset += 15;
    offset = copy_json_exec_type(body_start, packet_end, body_len, offset, &mut parsed.exec_type)?;

    if offset.checked_add(13)? > body_len
        || !packet_has_range(body_start, packet_end, offset, 13)
    {
        return None;
    }
    if byte_at(body_start, packet_end, offset)? != b'"'
        || byte_at(body_start, packet_end, offset + 1)? != b','
        || byte_at(body_start, packet_end, offset + 12)? != b':'
    {
        return None;
    }
    offset += 13;
    offset = parse_json_u64(body_start, packet_end, body_len, offset, &mut parsed.fill_qty)?;

    if offset.checked_add(14)? > body_len
        || !packet_has_range(body_start, packet_end, offset, 14)
    {
        return None;
    }
    if byte_at(body_start, packet_end, offset)? != b','
        || byte_at(body_start, packet_end, offset + 1)? != b'"'
        || byte_at(body_start, packet_end, offset + 13)? != b':'
    {
        return None;
    }
    offset += 14;
    let _ = parse_json_decimal_scaled(
        body_start,
        packet_end,
        body_len,
        offset,
        &mut parsed.fill_price,
    )?;

    if parsed.key.len == 0 {
        return None;
    }
    Some(())
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn starts_with_fix(base: usize, data_end: usize, scan_len: usize) -> Option<bool> {
    if scan_len < 5 {
        return Some(false);
    }
    Some(
        byte_at(base, data_end, 0)? == b'8'
            && byte_at(base, data_end, 1)? == b'='
            && byte_at(base, data_end, 2)? == b'F'
            && byte_at(base, data_end, 3)? == b'I'
            && byte_at(base, data_end, 4)? == b'X',
    )
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
#[inline(always)]
fn copy_fix_value_to_order_key(
    base: usize,
    data_end: usize,
    scan_len: usize,
    value_start: usize,
    out: &mut OrderKey,
) -> Option<()> {
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 0);
    }
    let mut j = value_start;
    while j < scan_len && j - value_start < MAX_ORDER_ID_LEN {
        let b = byte_at(base, data_end, j)?;
        if b == 0x01 {
            break;
        }
        unsafe {
            out.bytes.as_mut_ptr().add(j - value_start).write(b);
        }
        j += 1;
    }
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.len), (j - value_start) as u16);
    }
    Some(())
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn copy_fix_value_to_exec_type(
    base: usize,
    data_end: usize,
    scan_len: usize,
    value_start: usize,
    out: &mut ExecType,
) -> Option<()> {
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 0);
    }
    let mut j = value_start;
    while j < scan_len && j - value_start < MAX_EXEC_TYPE_LEN {
        let b = byte_at(base, data_end, j)?;
        if b == 0x01 {
            break;
        }
        unsafe {
            out.bytes.as_mut_ptr().add(j - value_start).write(b);
        }
        j += 1;
    }
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.len), (j - value_start) as u16);
    }
    Some(())
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn parse_fix_u64(base: usize, data_end: usize, scan_len: usize, value_start: usize) -> Option<u64> {
    let mut value = 0u64;
    let mut j = value_start;
    while j < scan_len {
        let b = byte_at(base, data_end, j)?;
        if b == 0x01 || b == b'.' {
            break;
        }
        if b < b'0' || b > b'9' {
            return Some(value);
        }
        value = value.wrapping_mul(10).wrapping_add(u64::from(b - b'0'));
        j += 1;
    }
    Some(value)
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn parse_fix_decimal_scaled(
    base: usize,
    data_end: usize,
    scan_len: usize,
    value_start: usize,
) -> Option<u64> {
    let mut whole = 0u64;
    let mut frac = 0u64;
    let mut frac_digits = 0usize;
    let mut seen_dot = false;
    let mut j = value_start;
    while j < scan_len {
        let b = byte_at(base, data_end, j)?;
        if b == 0x01 {
            break;
        }
        if b == b'.' {
            seen_dot = true;
            j += 1;
            continue;
        }
        if b < b'0' || b > b'9' {
            break;
        }
        if seen_dot {
            if frac_digits < 9 {
                frac = frac.wrapping_mul(10).wrapping_add(u64::from(b - b'0'));
                frac_digits += 1;
            }
        } else {
            whole = whole.wrapping_mul(10).wrapping_add(u64::from(b - b'0'));
        }
        j += 1;
    }
    while frac_digits < 9 {
        frac = frac.wrapping_mul(10);
        frac_digits += 1;
    }
    Some(whole.wrapping_mul(PRICE_SCALE).wrapping_add(frac))
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
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
fn copy_json_string_to_order_key(
    base: usize,
    data_end: usize,
    body_len: usize,
    value_start: usize,
    out: &mut OrderKey,
) -> Option<usize> {
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 0);
    }

    let mut j = value_start;
    let mut idx = 0usize;
    while idx < MAX_JSON_ORDER_ID_SCAN {
        if j >= body_len {
            break;
        }
        let b = byte_at(base, data_end, j)?;
        if b == b'"' || b == b'\\' {
            break;
        }
        unsafe {
            out.bytes.as_mut_ptr().add(idx).write(b);
        }
        j += 1;
        idx += 1;
    }
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.len), idx as u16);
    }
    Some(j)
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn copy_json_exec_type(
    base: usize,
    data_end: usize,
    body_len: usize,
    value_start: usize,
    out: &mut ExecType,
) -> Option<usize> {
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 0);
    }
    if value_start.checked_add(2)? > body_len {
        return Some(value_start);
    }
    let b = byte_at(base, data_end, value_start)?;
    if b == b'"' || b == b'\\' {
        return Some(value_start);
    }
    if byte_at(base, data_end, value_start + 1)? != b'"' {
        return Some(value_start);
    }
    unsafe {
        out.bytes.as_mut_ptr().write(b);
    }
    unsafe {
        ptr::write_volatile(ptr::addr_of_mut!(out.len), 1);
    }
    Some(value_start + 1)
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn parse_json_u64(
    base: usize,
    data_end: usize,
    body_len: usize,
    value_start: usize,
    out: &mut u64,
) -> Option<usize> {
    *out = 0;
    if value_start.checked_add(2)? > body_len {
        return Some(value_start);
    }
    let b0 = byte_at(base, data_end, value_start)?;
    if b0 < b'0' || b0 > b'9' {
        return Some(value_start);
    }
    let mut value = u64::from(b0 - b'0');
    let mut j = value_start + 1;
    let b1 = byte_at(base, data_end, j)?;
    if b1 >= b'0' && b1 <= b'9' {
        value = value.wrapping_mul(10).wrapping_add(u64::from(b1 - b'0'));
        j += 1;
    }
    *out = value;
    Some(j)
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn parse_json_decimal_scaled(
    base: usize,
    data_end: usize,
    body_len: usize,
    value_start: usize,
    out: &mut u64,
) -> Option<usize> {
    *out = 0;
    if value_start.checked_add(2)? > body_len {
        return Some(value_start);
    }
    let b0 = byte_at(base, data_end, value_start)?;
    if b0 < b'0' || b0 > b'9' {
        return Some(value_start);
    }
    let mut whole = u64::from(b0 - b'0');
    let mut frac = 0u64;
    let mut j = value_start + 1;

    let b1 = byte_at(base, data_end, j)?;
    if b1 >= b'0' && b1 <= b'9' {
        whole = whole.wrapping_mul(10).wrapping_add(u64::from(b1 - b'0'));
        j += 1;
    }

    if byte_at(base, data_end, j)? == b'.' {
        j += 1;
        if j >= body_len {
            *out = whole.wrapping_mul(PRICE_SCALE);
            return Some(j);
        }
        let d1 = byte_at(base, data_end, j)?;
        if d1 >= b'0' && d1 <= b'9' {
            frac = frac.wrapping_add(u64::from(d1 - b'0') * 100_000_000);
            j += 1;
        }
        if j >= body_len {
            *out = whole.wrapping_mul(PRICE_SCALE).wrapping_add(frac);
            return Some(j);
        }
        let d2 = byte_at(base, data_end, j)?;
        if d2 >= b'0' && d2 <= b'9' {
            frac = frac.wrapping_add(u64::from(d2 - b'0') * 10_000_000);
            j += 1;
        }
    }
    *out = whole.wrapping_mul(PRICE_SCALE).wrapping_add(frac);
    Some(j)
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn byte_at(base: usize, data_end: usize, offset: usize) -> Option<u8> {
    let cursor = base.checked_add(offset)?;
    let end = unsafe { ptr::read_volatile(&data_end) };
    if cursor >= end {
        return None;
    }
    Some(unsafe { ptr::read_volatile(cursor as *const u8) })
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn checked_packet_end(base: usize, data_end: usize, len: usize) -> Option<usize> {
    let end = base.checked_add(len)?;
    if end > data_end {
        return None;
    }
    Some(end)
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn packet_has_range(base: usize, data_end: usize, offset: usize, len: usize) -> bool {
    match base
        .checked_add(offset)
        .and_then(|start| start.checked_add(len))
    {
        Some(end) => end <= data_end,
        None => false,
    }
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
