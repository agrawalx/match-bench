#![cfg_attr(target_arch = "bpf", no_std)]
#![cfg_attr(target_arch = "bpf", no_main)]

#[cfg(target_arch = "bpf")]
use aya_ebpf::{
    bindings::{xdp_action::XDP_PASS, TC_ACT_PIPE},
    macros::{classifier, map, xdp},
    maps::{LruHashMap, RingBuf},
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
const MAX_ORDER_ID_LEN: usize = 96;
#[cfg(target_arch = "bpf")]
const MAX_EXEC_TYPE_LEN: usize = 16;
#[cfg(target_arch = "bpf")]
const MAX_FIX_SCAN: usize = 768;
#[cfg(target_arch = "bpf")]
const MAX_JSON_SCAN: usize = 1024;
#[cfg(target_arch = "bpf")]
const EVENT_FLAG_REORDERING: u16 = 0x1;
#[cfg(target_arch = "bpf")]
const PRICE_SCALE: u64 = 1_000_000_000;
#[cfg(target_arch = "bpf")]
const ORDER_KEY_SIZE: usize = 98;
#[cfg(target_arch = "bpf")]
const EXEC_TYPE_SIZE: usize = 18;
#[cfg(target_arch = "bpf")]
const LATENCY_EVENT_SIZE: usize = 272;

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
struct ParsedFix {
    key: OrderKey,
    exec_type: ExecType,
    fill_qty: u64,
    fill_price: u64,
    orig_order_id: OrderKey,
    is_request: bool,
    is_execution_report: bool,
    is_retransmission_candidate: bool,
}

#[cfg(target_arch = "bpf")]
struct ParsedPacket {
    key: OrderKey,
    exec_type: ExecType,
    fill_qty: u64,
    fill_price: u64,
    orig_order_id: OrderKey,
    is_request: bool,
    is_response: bool,
    is_retransmission_candidate: bool,
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
    payload_end: usize,
    target_port: u16,
    src_ip: u32,
    src_port: u16,
    tcp_seq: u32,
}

#[cfg(target_arch = "bpf")]
#[map]
static IN_FLIGHT: LruHashMap<OrderKey, InFlightOrder> = LruHashMap::with_max_entries(262_144, 0);

#[cfg(target_arch = "bpf")]
#[map]
static EVENTS: RingBuf = RingBuf::with_byte_size(16 * 1024 * 1024, 0);

#[cfg(target_arch = "bpf")]
#[xdp]
pub fn iicpc_xdp_ingress(ctx: XdpContext) -> u32 {
    match try_xdp_ingress(ctx) {
        Ok(action) => action,
        Err(action) => action,
    }
}

#[cfg(target_arch = "bpf")]
#[classifier]
pub fn iicpc_tc_egress(ctx: TcContext) -> i32 {
    match try_tc_egress(ctx) {
        Ok(action) => action,
        Err(action) => action,
    }
}

#[cfg(target_arch = "bpf")]
fn try_xdp_ingress(ctx: XdpContext) -> Result<u32, u32> {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let payload = tcp_payload_bounds(data, data_end, true).ok_or(XDP_PASS)?;
    let parsed = parse_payload(
        payload.payload_start,
        payload.payload_end,
        payload.target_port,
        true,
    )
    .ok_or(XDP_PASS)?;
    if !parsed.is_request {
        return Ok(XDP_PASS);
    }

    let now = unsafe { bpf_ktime_get_real_ns() };
    let value = unsafe { IN_FLIGHT.get(&parsed.key).copied() };
    match value {
        Some(mut existing) => {
            if parsed.is_retransmission_candidate {
                existing.retransmission_count = existing.retransmission_count.saturating_add(1);
                let _ = IN_FLIGHT.insert(&parsed.key, &existing, 0);
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
            let _ = IN_FLIGHT.insert(&parsed.key, &order, 0);
        }
    }

    Ok(XDP_PASS)
}

#[cfg(target_arch = "bpf")]
fn try_tc_egress(ctx: TcContext) -> Result<i32, i32> {
    let data = ctx.data();
    let data_end = ctx.data_end();
    let payload = tcp_payload_bounds(data, data_end, false).ok_or(TC_ACT_PIPE)?;
    let parsed = parse_payload(
        payload.payload_start,
        payload.payload_end,
        payload.target_port,
        false,
    )
    .ok_or(TC_ACT_PIPE)?;
    if !parsed.is_response {
        return Ok(TC_ACT_PIPE);
    }

    let Some(in_flight) = (unsafe { IN_FLIGHT.get(&parsed.key).copied() }) else {
        return Ok(TC_ACT_PIPE);
    };
    let _ = IN_FLIGHT.remove(&parsed.key);

    let t7 = unsafe { bpf_ktime_get_real_ns() };
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
            ptr::write(
                event,
                LatencyEvent {
                    t3_xdp_ingress_ns: in_flight.t3_xdp_ingress_ns,
                    t7_xdp_egress_ns: t7,
                    pod_service_time_ns: service_time,
                    fill_qty: parsed.fill_qty,
                    fill_price: parsed.fill_price,
                    src_ip: in_flight.src_ip,
                    tcp_seq: in_flight.tcp_seq,
                    retransmission_count: in_flight.retransmission_count,
                    src_port: in_flight.src_port,
                    order_id_len: parsed.key.len,
                    exec_type_len: parsed.exec_type.len,
                    orig_order_id_len: parsed.orig_order_id.len,
                    flags,
                    _pad: 0,
                    order_id: parsed.key.bytes,
                    exec_type: parsed.exec_type.bytes,
                    orig_order_id: parsed.orig_order_id.bytes,
                },
            );
        }
        entry.submit(0);
    }

    Ok(TC_ACT_PIPE)
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

    let payload_start = tcp_offset + data_offset;
    if payload_start >= data_end {
        return None;
    }
    Some(PacketBounds {
        payload_start,
        payload_end: data_end,
        target_port,
        src_ip: u32::from_be(ip.saddr),
        src_port: source,
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
fn parse_payload(
    payload_start: usize,
    payload_end: usize,
    target_port: u16,
    inbound_request: bool,
) -> Option<ParsedPacket> {
    if target_port == FIX_PORT {
        let parsed = parse_fix_payload(payload_start, payload_end)?;
        return Some(ParsedPacket {
            key: parsed.key,
            exec_type: parsed.exec_type,
            fill_qty: parsed.fill_qty,
            fill_price: parsed.fill_price,
            orig_order_id: parsed.orig_order_id,
            is_request: parsed.is_request,
            is_response: parsed.is_execution_report,
            is_retransmission_candidate: parsed.is_retransmission_candidate,
        });
    }

    let parsed = parse_json_payload(payload_start, payload_end)
        .or_else(|| parse_websocket_json_payload(payload_start, payload_end, inbound_request))?;
    Some(ParsedPacket {
        key: parsed.key,
        exec_type: parsed.exec_type,
        fill_qty: parsed.fill_qty,
        fill_price: parsed.fill_price,
        orig_order_id: parsed.orig_order_id,
        is_request: inbound_request,
        is_response: !inbound_request,
        is_retransmission_candidate: true,
    })
}

#[cfg(target_arch = "bpf")]
fn parse_fix_payload(payload_start: usize, payload_end: usize) -> Option<ParsedFix> {
    let scan_len = core::cmp::min(payload_end - payload_start, MAX_FIX_SCAN);
    if !starts_with_fix(payload_start, payload_end, scan_len)? {
        return None;
    }

    let mut msg_type = 0u8;
    let mut key = empty_order_key();
    let mut exec_type = empty_exec_type();
    let mut ord_status = empty_exec_type();
    let mut orig_order_id = empty_order_key();
    let mut fill_qty = 0;
    let mut fill_price = 0;

    let mut i = 0usize;
    while i < scan_len {
        let field_start = payload_start + i;
        let soh_prefixed =
            i == 0 || byte_at(payload_start, payload_end, i.wrapping_sub(1))? == 0x01;

        if soh_prefixed && i + 3 < scan_len {
            if matches_tag(payload_start, payload_end, i, b'3', b'5', b'=')? {
                msg_type = byte_at(payload_start, payload_end, i + 3)?;
            }
            if matches_tag(payload_start, payload_end, i, b'1', b'1', b'=')? {
                let value_start = i + 3;
                copy_fix_value_to_order_key(
                    payload_start,
                    payload_end,
                    scan_len,
                    value_start,
                    &mut key,
                )?;
            }
            if matches_tag(payload_start, payload_end, i, b'3', b'2', b'=')? {
                fill_qty = parse_fix_u64(payload_start, payload_end, scan_len, i + 3)?;
            }
            if matches_tag(payload_start, payload_end, i, b'3', b'1', b'=')? {
                fill_price = parse_fix_decimal_scaled(payload_start, payload_end, scan_len, i + 3)?;
            }
            if matches_tag(payload_start, payload_end, i, b'3', b'9', b'=')? {
                copy_fix_value_to_exec_type(
                    payload_start,
                    payload_end,
                    scan_len,
                    i + 3,
                    &mut ord_status,
                )?;
            }
        }

        if soh_prefixed && i + 4 < scan_len {
            if matches_tag4(payload_start, payload_end, i, b'1', b'5', b'0', b'=')? {
                copy_fix_value_to_exec_type(
                    payload_start,
                    payload_end,
                    scan_len,
                    i + 4,
                    &mut exec_type,
                )?;
            }
        }

        if soh_prefixed && i + 3 < scan_len {
            if matches_tag(payload_start, payload_end, i, b'4', b'1', b'=')? {
                copy_fix_value_to_order_key(
                    payload_start,
                    payload_end,
                    scan_len,
                    i + 3,
                    &mut orig_order_id,
                )?;
            }
        }

        let mut next = i + 1;
        while next < scan_len {
            if byte_at(payload_start, payload_end, next)? == 0x01 {
                break;
            }
            next += 1;
        }
        i = if next == field_start { i + 1 } else { next + 1 };
    }

    if key.len == 0 {
        return None;
    }
    if exec_type.len == 0 {
        exec_type = ord_status;
    }

    Some(ParsedFix {
        key,
        exec_type,
        fill_qty,
        fill_price,
        orig_order_id,
        is_request: msg_type == b'D' || msg_type == b'F' || msg_type == b'G',
        is_execution_report: msg_type == b'8',
        is_retransmission_candidate: msg_type == b'D',
    })
}

#[cfg(target_arch = "bpf")]
#[derive(Clone, Copy)]
struct ParsedJson {
    key: OrderKey,
    exec_type: ExecType,
    fill_qty: u64,
    fill_price: u64,
    orig_order_id: OrderKey,
}

#[cfg(target_arch = "bpf")]
fn parse_json_payload(payload_start: usize, payload_end: usize) -> Option<ParsedJson> {
    parse_json_fields(payload_start, payload_end, 0, 0, false)
}

#[cfg(target_arch = "bpf")]
fn parse_websocket_json_payload(
    payload_start: usize,
    payload_end: usize,
    client_to_server: bool,
) -> Option<ParsedJson> {
    let available = payload_end.checked_sub(payload_start)?;
    if available < 2 {
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

    let len_code = second & 0x7f;
    let mut header_len = 2usize;
    let payload_len = if len_code < 126 {
        usize::from(len_code)
    } else if len_code == 126 {
        if available < 4 {
            return None;
        }
        header_len = 4;
        let hi = usize::from(byte_at(payload_start, payload_end, 2)?);
        let lo = usize::from(byte_at(payload_start, payload_end, 3)?);
        (hi << 8) | lo
    } else {
        return None;
    };

    let mask_offset = header_len;
    if masked {
        header_len = header_len.checked_add(4)?;
    }
    if available < header_len {
        return None;
    }

    let scan_len = core::cmp::min(payload_len, MAX_JSON_SCAN);
    if available < header_len + scan_len {
        return None;
    }

    let mask = if masked { mask_offset } else { 0 };
    parse_json_fields(payload_start, payload_end, header_len, mask, masked)
}

#[cfg(target_arch = "bpf")]
fn parse_json_fields(
    base: usize,
    data_end: usize,
    payload_offset: usize,
    mask_offset: usize,
    masked: bool,
) -> Option<ParsedJson> {
    let available = data_end.checked_sub(base)?.checked_sub(payload_offset)?;
    let scan_len = core::cmp::min(available, MAX_JSON_SCAN);
    if scan_len == 0 {
        return None;
    }

    let mut parsed = ParsedJson {
        key: empty_order_key(),
        exec_type: empty_exec_type(),
        fill_qty: 0,
        fill_price: 0,
        orig_order_id: empty_order_key(),
    };

    let mut i = 0usize;
    while i < scan_len {
        if json_key_matches(
            base,
            data_end,
            payload_offset,
            mask_offset,
            masked,
            i,
            b"cl_ord_id",
        )? {
            let value = i + 13;
            copy_json_string_to_order_key(
                base,
                data_end,
                payload_offset,
                mask_offset,
                masked,
                scan_len,
                value,
                &mut parsed.key,
            )?;
        }
        if json_key_matches(
            base,
            data_end,
            payload_offset,
            mask_offset,
            masked,
            i,
            b"orig_cl_ord_id",
        )? {
            let value = i + 18;
            copy_json_string_to_order_key(
                base,
                data_end,
                payload_offset,
                mask_offset,
                masked,
                scan_len,
                value,
                &mut parsed.orig_order_id,
            )?;
        }
        if json_key_matches(
            base,
            data_end,
            payload_offset,
            mask_offset,
            masked,
            i,
            b"exec_type",
        )? {
            let value = i + 13;
            copy_json_string_to_exec_type(
                base,
                data_end,
                payload_offset,
                mask_offset,
                masked,
                scan_len,
                value,
                &mut parsed.exec_type,
            )?;
        }
        if json_key_matches(
            base,
            data_end,
            payload_offset,
            mask_offset,
            masked,
            i,
            b"ord_status",
        )? && parsed.exec_type.len == 0
        {
            let value = i + 14;
            copy_json_string_to_exec_type(
                base,
                data_end,
                payload_offset,
                mask_offset,
                masked,
                scan_len,
                value,
                &mut parsed.exec_type,
            )?;
        }
        if json_key_matches(
            base,
            data_end,
            payload_offset,
            mask_offset,
            masked,
            i,
            b"fill_qty",
        )? {
            parsed.fill_qty = parse_json_u64(
                base,
                data_end,
                payload_offset,
                mask_offset,
                masked,
                scan_len,
                i + 11,
            )?;
        }
        if json_key_matches(
            base,
            data_end,
            payload_offset,
            mask_offset,
            masked,
            i,
            b"fill_price",
        )? {
            parsed.fill_price = parse_json_decimal_scaled(
                base,
                data_end,
                payload_offset,
                mask_offset,
                masked,
                scan_len,
                i + 13,
            )?;
        }

        i += 1;
    }

    if parsed.key.len == 0 {
        return None;
    }
    Some(parsed)
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
fn empty_order_key() -> OrderKey {
    OrderKey {
        len: 0,
        bytes: [0; MAX_ORDER_ID_LEN],
    }
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn empty_exec_type() -> ExecType {
    ExecType {
        len: 0,
        bytes: [0; MAX_EXEC_TYPE_LEN],
    }
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn matches_tag(base: usize, data_end: usize, offset: usize, a: u8, b: u8, c: u8) -> Option<bool> {
    Some(
        byte_at(base, data_end, offset)? == a
            && byte_at(base, data_end, offset + 1)? == b
            && byte_at(base, data_end, offset + 2)? == c,
    )
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn matches_tag4(
    base: usize,
    data_end: usize,
    offset: usize,
    a: u8,
    b: u8,
    c: u8,
    d: u8,
) -> Option<bool> {
    Some(
        byte_at(base, data_end, offset)? == a
            && byte_at(base, data_end, offset + 1)? == b
            && byte_at(base, data_end, offset + 2)? == c
            && byte_at(base, data_end, offset + 3)? == d,
    )
}

#[cfg(target_arch = "bpf")]
fn copy_fix_value_to_order_key(
    base: usize,
    data_end: usize,
    scan_len: usize,
    value_start: usize,
    out: &mut OrderKey,
) -> Option<()> {
    *out = empty_order_key();
    let mut j = value_start;
    while j < scan_len && j - value_start < MAX_ORDER_ID_LEN {
        let b = byte_at(base, data_end, j)?;
        if b == 0x01 {
            break;
        }
        out.bytes[j - value_start] = b;
        j += 1;
    }
    out.len = (j - value_start) as u16;
    Some(())
}

#[cfg(target_arch = "bpf")]
fn copy_fix_value_to_exec_type(
    base: usize,
    data_end: usize,
    scan_len: usize,
    value_start: usize,
    out: &mut ExecType,
) -> Option<()> {
    *out = empty_exec_type();
    let mut j = value_start;
    while j < scan_len && j - value_start < MAX_EXEC_TYPE_LEN {
        let b = byte_at(base, data_end, j)?;
        if b == 0x01 {
            break;
        }
        out.bytes[j - value_start] = b;
        j += 1;
    }
    out.len = (j - value_start) as u16;
    Some(())
}

#[cfg(target_arch = "bpf")]
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
        value = value.saturating_mul(10).saturating_add(u64::from(b - b'0'));
        j += 1;
    }
    Some(value)
}

#[cfg(target_arch = "bpf")]
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
                frac = frac.saturating_mul(10).saturating_add(u64::from(b - b'0'));
                frac_digits += 1;
            }
        } else {
            whole = whole.saturating_mul(10).saturating_add(u64::from(b - b'0'));
        }
        j += 1;
    }
    while frac_digits < 9 {
        frac = frac.saturating_mul(10);
        frac_digits += 1;
    }
    Some(whole.saturating_mul(PRICE_SCALE).saturating_add(frac))
}

#[cfg(target_arch = "bpf")]
fn json_key_matches(
    base: usize,
    data_end: usize,
    payload_offset: usize,
    mask_offset: usize,
    masked: bool,
    offset: usize,
    key: &[u8],
) -> Option<bool> {
    if payload_byte_at(base, data_end, payload_offset, mask_offset, masked, offset)? != b'"' {
        return Some(false);
    }
    let mut i = 0usize;
    while i < key.len() {
        if payload_byte_at(
            base,
            data_end,
            payload_offset,
            mask_offset,
            masked,
            offset + 1 + i,
        )? != key[i]
        {
            return Some(false);
        }
        i += 1;
    }
    let end_quote = offset + 1 + key.len();
    Some(
        payload_byte_at(
            base,
            data_end,
            payload_offset,
            mask_offset,
            masked,
            end_quote,
        )? == b'"'
            && payload_byte_at(
                base,
                data_end,
                payload_offset,
                mask_offset,
                masked,
                end_quote + 1,
            )? == b':',
    )
}

#[cfg(target_arch = "bpf")]
fn copy_json_string_to_order_key(
    base: usize,
    data_end: usize,
    payload_offset: usize,
    mask_offset: usize,
    masked: bool,
    scan_len: usize,
    value_start: usize,
    out: &mut OrderKey,
) -> Option<()> {
    *out = empty_order_key();
    let mut j = value_start;
    while j < scan_len && j - value_start < MAX_ORDER_ID_LEN {
        let b = payload_byte_at(base, data_end, payload_offset, mask_offset, masked, j)?;
        if b == b'"' || b == b'\\' {
            break;
        }
        out.bytes[j - value_start] = b;
        j += 1;
    }
    out.len = (j - value_start) as u16;
    Some(())
}

#[cfg(target_arch = "bpf")]
fn copy_json_string_to_exec_type(
    base: usize,
    data_end: usize,
    payload_offset: usize,
    mask_offset: usize,
    masked: bool,
    scan_len: usize,
    value_start: usize,
    out: &mut ExecType,
) -> Option<()> {
    *out = empty_exec_type();
    let mut j = value_start;
    while j < scan_len && j - value_start < MAX_EXEC_TYPE_LEN {
        let b = payload_byte_at(base, data_end, payload_offset, mask_offset, masked, j)?;
        if b == b'"' || b == b'\\' {
            break;
        }
        out.bytes[j - value_start] = b;
        j += 1;
    }
    out.len = (j - value_start) as u16;
    Some(())
}

#[cfg(target_arch = "bpf")]
fn parse_json_u64(
    base: usize,
    data_end: usize,
    payload_offset: usize,
    mask_offset: usize,
    masked: bool,
    scan_len: usize,
    value_start: usize,
) -> Option<u64> {
    let mut value = 0u64;
    let mut j = value_start;
    while j < scan_len {
        let b = payload_byte_at(base, data_end, payload_offset, mask_offset, masked, j)?;
        if b == b'"' || b == b',' || b == b'}' || b == b'.' {
            break;
        }
        if b >= b'0' && b <= b'9' {
            value = value.saturating_mul(10).saturating_add(u64::from(b - b'0'));
        } else if b != b' ' {
            return Some(value);
        }
        j += 1;
    }
    Some(value)
}

#[cfg(target_arch = "bpf")]
fn parse_json_decimal_scaled(
    base: usize,
    data_end: usize,
    payload_offset: usize,
    mask_offset: usize,
    masked: bool,
    scan_len: usize,
    value_start: usize,
) -> Option<u64> {
    let mut whole = 0u64;
    let mut frac = 0u64;
    let mut frac_digits = 0usize;
    let mut seen_dot = false;
    let mut j = value_start;
    while j < scan_len {
        let b = payload_byte_at(base, data_end, payload_offset, mask_offset, masked, j)?;
        if b == b'"' || b == b',' || b == b'}' {
            break;
        }
        if b == b'.' {
            seen_dot = true;
            j += 1;
            continue;
        }
        if b >= b'0' && b <= b'9' {
            if seen_dot {
                if frac_digits < 9 {
                    frac = frac.saturating_mul(10).saturating_add(u64::from(b - b'0'));
                    frac_digits += 1;
                }
            } else {
                whole = whole.saturating_mul(10).saturating_add(u64::from(b - b'0'));
            }
        } else if b != b' ' {
            break;
        }
        j += 1;
    }
    while frac_digits < 9 {
        frac = frac.saturating_mul(10);
        frac_digits += 1;
    }
    Some(whole.saturating_mul(PRICE_SCALE).saturating_add(frac))
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn payload_byte_at(
    base: usize,
    data_end: usize,
    payload_offset: usize,
    mask_offset: usize,
    masked: bool,
    offset: usize,
) -> Option<u8> {
    let mut b = byte_at(base + payload_offset, data_end, offset)?;
    if masked {
        let mask = byte_at(base, data_end, mask_offset + (offset & 3))?;
        b ^= mask;
    }
    Some(b)
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
fn byte_at(base: usize, data_end: usize, offset: usize) -> Option<u8> {
    if base + offset + 1 > data_end {
        return None;
    }
    Some(unsafe { ptr::read((base + offset) as *const u8) })
}

#[cfg(target_arch = "bpf")]
#[inline(always)]
unsafe fn bpf_ktime_get_real_ns() -> u64 {
    let helper: extern "C" fn() -> u64 = mem::transmute(212usize);
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
