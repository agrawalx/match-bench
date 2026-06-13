//! This module implements fix behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use iicpc_schemas_rust::{OrdType, PayloadType, Side};

use crate::time;

pub const FIX_TIMESTAMP_LEN: usize = 21;
const FIX_TIMESTAMP_PLACEHOLDER: &[u8; FIX_TIMESTAMP_LEN] = b"19700101-00:00:00.000";

const SOH: u8 = 0x01;

/// new_limit_order_id performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn new_limit_order_id(session_id: &str, bot_id: u64, seq: u64) -> String {
    format!("{session_id}_{bot_id}_{seq}_O")
}

/// replace_order_id performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn replace_order_id(session_id: &str, bot_id: u64, seq: u64) -> String {
    format!("{session_id}_{bot_id}_{seq}_R")
}

#[derive(Debug, Clone)]
/// OrderFrame stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct OrderFrame {
    pub order_id: String,
    pub orig_order_id: String,
    pub price: u64,
    pub qty: u64,
    pub side: Side,
    pub fix: Vec<u8>,
    pub rest: Vec<u8>,
    pub ws_bytes: Vec<u8>,
    pub tag52_offset: Option<usize>, // where timestamp bytes start in `fix` for in-place patching
    pub payload_type: PayloadType,
    pub ord_type: OrdType,
}

impl OrderFrame {
    /// patch_timestamp performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn patch_timestamp(&mut self, now_ns: u64) {
        let Some(offset) = self.tag52_offset else {
            return;
        };

        let new_ts = time::format_fix_timestamp(now_ns);

        let mut old_sum = 0u32;
        let mut new_sum = 0u32;
        for i in 0..FIX_TIMESTAMP_LEN {
            old_sum += u32::from(self.fix[offset + i]);
            new_sum += u32::from(new_ts[i]);
            self.fix[offset + i] = new_ts[i];
        }

        let chk_offset = self.fix.len() - 4;
        let old_chk_digit1 = self.fix[chk_offset] - b'0';
        let old_chk_digit2 = self.fix[chk_offset + 1] - b'0';
        let old_chk_digit3 = self.fix[chk_offset + 2] - b'0';
        let old_checksum = u32::from(old_chk_digit1) * 100
            + u32::from(old_chk_digit2) * 10
            + u32::from(old_chk_digit3);

        let new_checksum = (old_checksum + 256 + (new_sum % 256) - (old_sum % 256)) % 256;

        self.fix[chk_offset] = b'0' + (new_checksum / 100) as u8;
        self.fix[chk_offset + 1] = b'0' + ((new_checksum / 10) % 10) as u8;
        self.fix[chk_offset + 2] = b'0' + (new_checksum % 10) as u8;
    }
}

/// logon_frame performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn logon_frame(fix_version: &str, seq: u64) -> Vec<u8> {
    let body = format!("35=A\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={seq}\x0198=0\x01108=30\x01");
    finalize_fix(fix_version, &body)
}

/// find_tag52_offset performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn find_tag52_offset(fix: &[u8]) -> Option<usize> {
    fix.windows(3 + FIX_TIMESTAMP_LEN)
        .position(|window| window.starts_with(b"52=") && &window[3..] == FIX_TIMESTAMP_PLACEHOLDER)
        .map(|pos| pos + 3)
}

#[derive(Clone, Copy)]
/// FrameKind enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
enum FrameKind {
    New,
    Market,
    Cancel,
    Replace,
}

impl FrameKind {
    /// payload_type performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn payload_type(self) -> PayloadType {
        match self {
            Self::New | Self::Market => PayloadType::New,
            Self::Cancel => PayloadType::Cancel,
            Self::Replace => PayloadType::Replace,
        }
    }

    /// ord_type performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn ord_type(self) -> OrdType {
        match self {
            Self::Market => OrdType::Market,
            Self::New | Self::Cancel | Self::Replace => OrdType::Limit,
        }
    }

    /// order_id performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn order_id(self, session_id: &str, bot_id: u64, seq: u64) -> String {
        match self {
            Self::New => new_limit_order_id(session_id, bot_id, seq),
            Self::Market => format!("{session_id}_{bot_id}_{seq}_M"),
            Self::Cancel => format!("{session_id}_{bot_id}_{seq}_C"),
            Self::Replace => replace_order_id(session_id, bot_id, seq),
        }
    }

    /// rest_method performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn rest_method(self) -> &'static str {
        match self {
            Self::New | Self::Market => "POST",
            Self::Cancel => "DELETE",
            Self::Replace => "PUT",
        }
    }

    /// rest_path performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn rest_path(self, orig_order_id: &str) -> String {
        match self {
            Self::New | Self::Market => "/orders".to_string(),
            Self::Cancel | Self::Replace => format!("/orders/{orig_order_id}"),
        }
    }
}

/// build_fix_body performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn build_fix_body(
    kind: FrameKind,
    seq: u64,
    order_id: &str,
    orig_order_id: Option<&str>,
    price: u64,
    qty: u64,
    side: Side,
) -> String {
    let side_tag = match side {
        Side::Buy => "1",
        Side::Sell => "2",
    };

    let msg_seq_num = seq + 1;

    match kind {
        FrameKind::New => format!(
            "35=D\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={msg_seq_num}\x0152=19700101-00:00:00.000\x0111={order_id}\x0121=1\x0155=IICPC\x0154={side_tag}\x0138={qty}\x0140=2\x0144={price}\x0159=0\x01"
        ),
        FrameKind::Market => format!(
            "35=D\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={msg_seq_num}\x0152=19700101-00:00:00.000\x0111={order_id}\x0121=1\x0155=IICPC\x0154={side_tag}\x0138={qty}\x0140=1\x0159=0\x01"
        ),
        FrameKind::Cancel => {
            let orig_order_id = orig_order_id.expect("cancel requires orig_order_id");
            format!(
                "35=F\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={msg_seq_num}\x0152=19700101-00:00:00.000\x0111={order_id}\x0141={orig_order_id}\x0155=IICPC\x0154={side_tag}\x0138={qty}\x01"
            )
        }
        FrameKind::Replace => {
            let orig_order_id = orig_order_id.expect("replace requires orig_order_id");
            format!(
                "35=G\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={msg_seq_num}\x0152=19700101-00:00:00.000\x0111={order_id}\x0141={orig_order_id}\x0121=1\x0155=IICPC\x0154={side_tag}\x0138={qty}\x0140=2\x0144={price}\x01"
            )
        }
    }
}

/// build_json_payload performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn build_json_payload(
    kind: FrameKind,
    order_id: &str,
    orig_order_id: Option<&str>,
    price: u64,
    qty: u64,
    side: Side,
) -> String {
    match kind {
        FrameKind::New => {
            let side_name = match side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            };
            let encoded_order_id =
                serde_json::to_string(order_id).expect("serializing String cannot fail");
            format!("{{\"cl_ord_id\":{encoded_order_id},\"symbol\":\"IICPC\",\"side\":\"{side_name}\",\"qty\":{qty},\"price\":{price}}}")
        }
        FrameKind::Market => {
            let side_name = match side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            };
            let encoded_order_id =
                serde_json::to_string(order_id).expect("serializing String cannot fail");
            format!("{{\"cl_ord_id\":{encoded_order_id},\"symbol\":\"IICPC\",\"side\":\"{side_name}\",\"qty\":{qty},\"ord_type\":\"MARKET\"}}")
        }
        FrameKind::Cancel => {
            let orig_order_id = orig_order_id.expect("cancel requires orig_order_id");
            format!("{{\"action\":\"CANCEL\",\"cl_ord_id\":\"{order_id}\",\"orig_cl_ord_id\":\"{orig_order_id}\",\"symbol\":\"IICPC\"}}")
        }
        FrameKind::Replace => {
            let orig_order_id = orig_order_id.expect("replace requires orig_order_id");
            let side_name = match side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            };
            format!("{{\"action\":\"REPLACE\",\"cl_ord_id\":\"{order_id}\",\"orig_cl_ord_id\":\"{orig_order_id}\",\"symbol\":\"IICPC\",\"side\":\"{side_name}\",\"qty\":{qty},\"price\":{price}}}")
        }
    }
}

/// build_rest_request performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn build_rest_request(method: &str, target_host: &str, path: &str, json: &str) -> Vec<u8> {
    let mut rest = String::with_capacity(96 + json.len());
    use std::fmt::Write;
    write!(
        &mut rest,
        "{method} {path} HTTP/1.1\r\nHost: {target_host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
        json.len(),
        json
    )
    .expect("writing to String cannot fail");
    rest.into_bytes()
}

/// build_frame performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn build_frame(
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    orig_order_id: Option<&str>,
    price: u64,
    qty: u64,
    side: Side,
    kind: FrameKind,
) -> OrderFrame {
    let order_id = kind.order_id(session_id, bot_id, seq);

    let body = build_fix_body(kind, seq, &order_id, orig_order_id, price, qty, side);
    let fix = finalize_fix(fix_version, &body);

    let json = build_json_payload(kind, &order_id, orig_order_id, price, qty, side);
    let rest = build_rest_request(
        kind.rest_method(),
        target_host,
        &kind.rest_path(orig_order_id.unwrap_or("")),
        &json,
    );
    let tag52_offset = find_tag52_offset(&fix);

    OrderFrame {
        order_id,
        orig_order_id: orig_order_id.unwrap_or("").to_string(),
        price,
        qty,
        side,
        fix,
        rest,
        ws_bytes: json.into_bytes(),
        tag52_offset,
        payload_type: kind.payload_type(),
        ord_type: kind.ord_type(),
    }
}

/// order_frame performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn order_frame(
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    price: u64,
    qty: u64,
    side: Side,
) -> OrderFrame {
    build_frame(
        fix_version,
        session_id,
        target_host,
        bot_id,
        seq,
        None,
        price,
        qty,
        side,
        FrameKind::New,
    )
}

/// market_frame performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn market_frame(
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    qty: u64,
    side: Side,
) -> OrderFrame {
    build_frame(
        fix_version,
        session_id,
        target_host,
        bot_id,
        seq,
        None,
        0, // market orders carry no price
        qty,
        side,
        FrameKind::Market,
    )
}

/// cancel_frame performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn cancel_frame(
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    orig_order_id: &str,
    price: u64,
    qty: u64,
    side: Side,
) -> OrderFrame {
    build_frame(
        fix_version,
        session_id,
        target_host,
        bot_id,
        seq,
        Some(orig_order_id),
        price,
        qty,
        side,
        FrameKind::Cancel,
    )
}

/// replace_frame performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn replace_frame(
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    orig_order_id: &str,
    price: u64,
    qty: u64,
    side: Side,
) -> OrderFrame {
    build_frame(
        fix_version,
        session_id,
        target_host,
        bot_id,
        seq,
        Some(orig_order_id),
        price,
        qty,
        side,
        FrameKind::Replace,
    )
}

/// finalize_fix performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn finalize_fix(fix_version: &str, body: &str) -> Vec<u8> {
    let mut frame = format!("8={fix_version}\x019={}\x01{body}", body.len()).into_bytes();
    let checksum = frame
        .iter()
        .fold(0u32, |sum, b| sum.wrapping_add(u32::from(*b)))
        % 256;
    frame.extend_from_slice(format!("10={checksum:03}\x01").as_bytes());
    frame
}

#[derive(Debug, Clone, Copy)]
/// MessageRef stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct MessageRef<'a> {
    pub msg_type: &'a [u8],
    pub clord_id: Option<&'a [u8]>,
}

/// parse_messages performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn parse_messages(buf: &[u8]) -> (Vec<MessageRef<'_>>, usize) {
    let mut out = Vec::new();
    let mut cursor = 0;

    while cursor < buf.len() {
        let Some(start_off) = find_subslice(&buf[cursor..], b"8=FIX") else {
            cursor += partial_marker_start(&buf[cursor..]);
            break;
        };
        let abs_start = cursor + start_off;

        let Some(end) = find_message_end(&buf[abs_start..]) else {
            cursor = abs_start;
            break;
        };
        let abs_end = abs_start + end;
        let msg = &buf[abs_start..abs_end];

        out.push(parse_single_message(msg));
        cursor = abs_end;
    }

    (out, cursor)
}

/// find_message_end performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn find_message_end(buf: &[u8]) -> Option<usize> {
    let needle = b"\x0110=";
    let i = find_subslice(buf, needle)?;
    let after_eq = i + needle.len();
    let soh = buf[after_eq..].iter().position(|&b| b == SOH)?;
    Some(after_eq + soh + 1)
}

/// parse_single_message performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn parse_single_message(msg: &[u8]) -> MessageRef<'_> {
    MessageRef {
        msg_type: extract_tag(msg, b"35").unwrap_or(b""),
        clord_id: extract_tag(msg, b"11"),
    }
}

/// extract_tag performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn extract_tag<'a>(msg: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut needle = Vec::with_capacity(tag.len() + 2);
    needle.push(SOH);
    needle.extend_from_slice(tag);
    needle.push(b'=');

    let pos = find_subslice(msg, &needle)?;
    let value_start = pos + needle.len();
    let value_end = msg[value_start..].iter().position(|&b| b == SOH)?;
    Some(&msg[value_start..value_start + value_end])
}

/// partial_marker_start performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn partial_marker_start(buf: &[u8]) -> usize {
    let marker = b"8=FIX";
    let lo = buf.len().saturating_sub(marker.len() - 1);
    for p in lo..buf.len() {
        if marker.starts_with(&buf[p..]) {
            return p;
        }
    }
    buf.len()
}

/// find_subslice performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// execution_report_frame performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn execution_report_frame(fix_version: &str, seq: u64, clord_id: &str) -> Vec<u8> {
    let body = format!(
        "35=8\x0149=CONTESTANT\x0156=IICPC-BOT\x0134={seq}\x0152=19700101-00:00:00.000\x0137=EXEC_{seq}\x0111={clord_id}\x0117=EXECID_{seq}\x01150=0\x0139=0\x0155=IICPC\x0154=1\x0138=0\x0114=0\x016=0\x01"
    );
    finalize_fix(fix_version, &body)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Offline serialization-cost breakdown for the raw-send bottleneck. build_frame
    // currently serializes FIX + JSON + REST(HTTP) for EVERY order but only one is
    // sent — this quantifies that waste and the FIX-vs-REST split. Run with:
    //   cargo test -p iicpc-bot-fleet --lib serialization_cost -- --ignored --nocapture
    #[test]
    #[ignore]
    fn serialization_cost_breakdown() {
        use std::time::Instant;
        let n = 1_000_000u64;
        let (fv, sid, host) = ("FIX.4.2", "01890dd2-71f3-7abc-9def-0123456789ab", "10.0.0.5:9898");

        // all three (what build_frame does today, per order)
        let t = Instant::now();
        let mut sink = 0usize;
        for seq in 0..n {
            let f = order_frame(fv, sid, host, 42, seq, 10_000, 25, Side::Buy);
            sink += f.fix.len() + f.rest.len() + f.ws_bytes.len();
        }
        let all3 = t.elapsed().as_nanos() / n as u128;

        // FIX only
        let t = Instant::now();
        for seq in 0..n {
            let oid = FrameKind::New.order_id(sid, 42, seq);
            let body = build_fix_body(FrameKind::New, seq, &oid, None, 10_000, 25, Side::Buy);
            sink += finalize_fix(fv, &body).len();
        }
        let fix_only = t.elapsed().as_nanos() / n as u128;

        // REST only (JSON + HTTP request)
        let t = Instant::now();
        for seq in 0..n {
            let oid = FrameKind::New.order_id(sid, 42, seq);
            let json = build_json_payload(FrameKind::New, &oid, None, 10_000, 25, Side::Buy);
            sink += build_rest_request("POST", host, "/orders", &json).len();
        }
        let rest_only = t.elapsed().as_nanos() / n as u128;

        eprintln!("SERTIME ns/order  all3={all3}  fix_only={fix_only}  rest_only={rest_only}  (sink={sink})");
        eprintln!("SERTIME implied max ser-only throughput/core: all3={}/s fix={}/s rest={}/s",
            1_000_000_000 / all3.max(1), 1_000_000_000 / fix_only.max(1), 1_000_000_000 / rest_only.max(1));
    }

    #[test]
    /// logon_and_first_orders_have_monotonic_seq_nums performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn logon_and_first_orders_have_monotonic_seq_nums() {
        let logon = logon_frame("FIX.4.2", 1);
        assert_eq!(extract_tag(&logon, b"34"), Some(b"1".as_ref()));

        let first = order_frame("FIX.4.2", "sess1", "host", 7, 1, 10_000, 5, Side::Buy);
        let second = order_frame("FIX.4.2", "sess1", "host", 7, 2, 10_000, 5, Side::Buy);

        assert_eq!(extract_tag(&first.fix, b"34"), Some(b"2".as_ref()));
        assert_eq!(extract_tag(&second.fix, b"34"), Some(b"3".as_ref()));

        assert_eq!(first.order_id, "sess1_7_1_O");
        assert_eq!(
            extract_tag(&first.fix, b"11"),
            Some(b"sess1_7_1_O".as_ref())
        );
        assert_eq!(second.order_id, "sess1_7_2_O");
    }

    #[test]
    /// parses_one_complete_message performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn parses_one_complete_message() {
        let msg = execution_report_frame("FIX.4.2", 1, "ORD_42");
        let (msgs, consumed) = parse_messages(&msg);
        assert_eq!(consumed, msg.len(), "should consume the full buffer");
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].msg_type, b"8");
        assert_eq!(msgs[0].clord_id, Some(b"ORD_42".as_ref()));
    }

    #[test]
    /// parses_back_to_back_messages performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn parses_back_to_back_messages() {
        let mut buf = execution_report_frame("FIX.4.2", 1, "A");
        buf.extend(execution_report_frame("FIX.4.2", 2, "B"));
        buf.extend(execution_report_frame("FIX.4.2", 3, "C"));
        let (msgs, consumed) = parse_messages(&buf);
        assert_eq!(consumed, buf.len());
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0].clord_id, Some(b"A".as_ref()));
        assert_eq!(msgs[1].clord_id, Some(b"B".as_ref()));
        assert_eq!(msgs[2].clord_id, Some(b"C".as_ref()));
    }

    #[test]
    /// truncated_tail_is_carry_over performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn truncated_tail_is_carry_over() {
        let m1 = execution_report_frame("FIX.4.2", 1, "FIRST");
        let m2 = execution_report_frame("FIX.4.2", 2, "SECOND");
        let half_second = &m2[..m2.len() / 2];
        let mut buf = m1.clone();
        buf.extend_from_slice(half_second);

        let (msgs, consumed) = parse_messages(&buf);
        assert_eq!(msgs.len(), 1, "only the complete first message is parsed");
        assert_eq!(msgs[0].clord_id, Some(b"FIRST".as_ref()));
        assert_eq!(
            consumed,
            m1.len(),
            "consumed = end of first message; rest is carry-over"
        );
    }

    #[test]
    /// ignores_non_execution_reports performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn ignores_non_execution_reports() {
        let body = "35=D\x0149=X\x0156=Y\x0134=1\x0111=ORDER_X\x01";
        let frame = finalize_fix("FIX.4.2", body);
        let (msgs, _) = parse_messages(&frame);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].msg_type, b"D");
        assert_eq!(msgs[0].clord_id, Some(b"ORDER_X".as_ref()));
    }

    /// embedded_checksum_is_valid performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn embedded_checksum_is_valid(fix: &[u8]) -> bool {
        let len = fix.len();
        let computed: u32 = fix[..len - 7].iter().map(|&b| u32::from(b)).sum::<u32>() % 256;
        let embedded = u32::from(fix[len - 4] - b'0') * 100
            + u32::from(fix[len - 3] - b'0') * 10
            + u32::from(fix[len - 2] - b'0');
        computed == embedded
    }

    #[test]
    /// patch_timestamp_sets_sending_time_and_keeps_checksum_valid performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn patch_timestamp_sets_sending_time_and_keeps_checksum_valid() {
        let mut frame = order_frame("FIX.4.2", "sess1", "host", 7, 42, 10_000, 5, Side::Buy);
        let off = frame.tag52_offset.expect("tag 52 offset must be located");

        assert_eq!(
            &frame.fix[off..off + FIX_TIMESTAMP_LEN],
            &FIX_TIMESTAMP_PLACEHOLDER[..]
        );
        assert!(embedded_checksum_is_valid(&frame.fix));

        let ns = 1_716_023_400_123_000_000_u64; // 2024-05-18T08:30:00.123Z
        frame.patch_timestamp(ns);

        let expected = time::format_fix_timestamp(ns);
        assert_eq!(&frame.fix[off..off + FIX_TIMESTAMP_LEN], &expected[..]);
        assert_ne!(
            &frame.fix[off..off + FIX_TIMESTAMP_LEN],
            &FIX_TIMESTAMP_PLACEHOLDER[..]
        );

        assert!(embedded_checksum_is_valid(&frame.fix));
    }

    #[test]
    /// echo_read_loop_answers_every_order_under_arbitrary_chunking performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn echo_read_loop_answers_every_order_under_arbitrary_chunking() {
        const N: u64 = 200;
        let mut wire: Vec<u8> = Vec::new();
        let mut expected: Vec<String> = Vec::new();
        for seq in 1..=N {
            let f = order_frame(
                "FIX.4.2",
                "sess1",
                "host",
                7,
                seq,
                10_000 + seq,
                5,
                Side::Buy,
            );
            expected.push(f.order_id.clone());
            wire.extend_from_slice(&f.fix);
        }

        for &chunk in &[1usize, 64, 256, 512, 1024, 4096, wire.len()] {
            let mut buf: Vec<u8> = Vec::new();
            let mut answered: Vec<String> = Vec::new();
            for piece in wire.chunks(chunk) {
                buf.extend_from_slice(piece);
                let (messages, consumed) = parse_messages(&buf);
                for msg in &messages {
                    if msg.msg_type == b"D" {
                        if let Some(c) = msg.clord_id {
                            answered.push(String::from_utf8(c.to_vec()).unwrap());
                        }
                    }
                }
                if consumed > 0 {
                    buf.drain(..consumed);
                }
            }
            assert_eq!(
                answered, expected,
                "chunk={chunk}: every order must be answered exactly once (BF-PARSE-1)"
            );
        }
    }
}
