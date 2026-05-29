use iicpc_schemas_rust::{PayloadType, Side};

use crate::time;

/// The exact character length of standard FIX YYYYMMDD-HH:MM:SS.mmm timestamps.
/// Must match `time::format_fix_timestamp`'s stack-allocated array size.
pub const FIX_TIMESTAMP_LEN: usize = 21;
const FIX_TIMESTAMP_PLACEHOLDER: &[u8; FIX_TIMESTAMP_LEN] = b"19700101-00:00:00.000";

const SOH: u8 = 0x01;

/// OrderFrame holds the pre-rendered payloads for each supported transport.
/// Bots reuse these bytes during the live workload to avoid hot-path encoding.
///
/// - `fix`: A complete FIX 4.x message ready for raw TCP send.
/// - `rest`: A complete HTTP/1.1 POST request including headers and body.
/// - `ws_bytes`: The raw JSON payload body only. The WebSocket client library
///   (tokio-tungstenite) handles framing (FIN bit, opcode, masking) automatically
///   when sending via `SinkExt::send(Message::Binary(...))`.
#[derive(Debug, Clone)]
pub struct OrderFrame {
    pub order_id: String,
    pub price: u64,
    pub qty: u64,
    pub side: Side,
    pub fix: Vec<u8>,
    pub rest: Vec<u8>,
    pub ws_bytes: Vec<u8>,
    pub tag52_offset: Option<usize>, // where timestamp bytes start in `fix` for in-place patching
    pub payload_type: PayloadType,
}

impl OrderFrame {
    /// patch_timestamp updates the FIX SendingTime (Tag 52) and re-calculates
    /// the checksum in-place using O(1) delta arithmetic in the hot path.
    pub fn patch_timestamp(&mut self, now_ns: u64) {
        let Some(offset) = self.tag52_offset else {
            return;
        };

        // Generate actual timestamp bytes
        let new_ts = time::format_fix_timestamp(now_ns);

        // Compute the delta sum
        let mut old_sum = 0u32;
        let mut new_sum = 0u32;
        for i in 0..FIX_TIMESTAMP_LEN {
            old_sum += u32::from(self.fix[offset + i]);
            new_sum += u32::from(new_ts[i]);
            self.fix[offset + i] = new_ts[i];
        }

        // Read current checksum from bytes (Tag 10 is at the end: "10=XXX\x01")
        let chk_offset = self.fix.len() - 4;
        let old_chk_digit1 = self.fix[chk_offset] - b'0';
        let old_chk_digit2 = self.fix[chk_offset + 1] - b'0';
        let old_chk_digit3 = self.fix[chk_offset + 2] - b'0';
        let old_checksum = u32::from(old_chk_digit1) * 100
            + u32::from(old_chk_digit2) * 10
            + u32::from(old_chk_digit3);

        // Compute new checksum modulo 256
        let new_checksum = (old_checksum + 256 + (new_sum % 256) - (old_sum % 256)) % 256;

        // Overwrite the checksum digits in-place
        self.fix[chk_offset] = b'0' + (new_checksum / 100) as u8;
        self.fix[chk_offset + 1] = b'0' + ((new_checksum / 10) % 10) as u8;
        self.fix[chk_offset + 2] = b'0' + (new_checksum % 10) as u8;
    }
}

/// logon_frame builds a FIX Logon message for the contestant endpoint.
/// The sequence number is supplied by the caller so sessions stay deterministic.
pub fn logon_frame(fix_version: &str, seq: u64) -> Vec<u8> {
    let body = format!("35=A\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={seq}\x0198=0\x01108=30\x01");
    finalize_fix(fix_version, &body)
}

/// find_tag52_offset scans the precomputed FIX bytes to locate the exact start index
/// of the 21-byte placeholder timestamp (directly after '52=').
fn find_tag52_offset(fix: &[u8]) -> Option<usize> {
    fix.windows(3 + FIX_TIMESTAMP_LEN)
        .position(|window| window.starts_with(b"52=") && &window[3..] == FIX_TIMESTAMP_PLACEHOLDER)
        .map(|pos| pos + 3)
}

#[derive(Clone, Copy)]
enum FrameKind {
    New,
    Cancel,
    Replace,
}

impl FrameKind {
    fn payload_type(self) -> PayloadType {
        match self {
            Self::New => PayloadType::New,
            Self::Cancel => PayloadType::Cancel,
            Self::Replace => PayloadType::Replace,
        }
    }

    fn order_id(self, session_id: &str, bot_id: u64, seq: u64) -> String {
        match self {
            Self::New => format!("{session_id}_{bot_id}_{seq}_O"),
            Self::Cancel => format!("{session_id}_{bot_id}_{seq}_C"),
            Self::Replace => format!("{session_id}_{bot_id}_{seq}_R"),
        }
    }

    fn rest_method(self) -> &'static str {
        match self {
            Self::New => "POST",
            Self::Cancel => "DELETE",
            Self::Replace => "PUT",
        }
    }

    fn rest_path(self, orig_order_id: &str) -> String {
        match self {
            Self::New => "/orders".to_string(),
            Self::Cancel | Self::Replace => format!("/orders/{orig_order_id}"),
        }
    }
}

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

    match kind {
        FrameKind::New => format!(
            "35=D\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={seq}\x0152=19700101-00:00:00.000\x0111={order_id}\x0121=1\x0155=IICPC\x0154={side_tag}\x0138={qty}\x0140=2\x0144={price}\x0159=0\x01"
        ),
        FrameKind::Cancel => {
            let orig_order_id = orig_order_id.expect("cancel requires orig_order_id");
            format!(
                "35=F\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={seq}\x0152=19700101-00:00:00.000\x0111={order_id}\x0141={orig_order_id}\x0155=IICPC\x0154={side_tag}\x0138={qty}\x01"
            )
        }
        FrameKind::Replace => {
            let orig_order_id = orig_order_id.expect("replace requires orig_order_id");
            format!(
                "35=G\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={seq}\x0152=19700101-00:00:00.000\x0111={order_id}\x0141={orig_order_id}\x0121=1\x0155=IICPC\x0154={side_tag}\x0138={qty}\x0140=2\x0144={price}\x01"
            )
        }
    }
}

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

fn build_frame(
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    orig_seq: Option<u64>,
    price: u64,
    qty: u64,
    side: Side,
    kind: FrameKind,
) -> OrderFrame {
    let order_id = kind.order_id(session_id, bot_id, seq);
    let orig_order_id = orig_seq.map(|orig_seq| format!("{session_id}_{bot_id}_{orig_seq}"));

    let body = build_fix_body(
        kind,
        seq,
        &order_id,
        orig_order_id.as_deref(),
        price,
        qty,
        side,
    );
    let fix = finalize_fix(fix_version, &body);

    let json = build_json_payload(kind, &order_id, orig_order_id.as_deref(), price, qty, side);
    let rest = build_rest_request(
        kind.rest_method(),
        target_host,
        &kind.rest_path(orig_order_id.as_deref().unwrap_or("")),
        &json,
    );
    let tag52_offset = find_tag52_offset(&fix);

    OrderFrame {
        order_id,
        price,
        qty,
        side,
        fix,
        rest,
        ws_bytes: json.into_bytes(),
        tag52_offset,
        payload_type: kind.payload_type(),
    }
}

/// order_frame builds one logical order as FIX, REST, and WebSocket payloads.
/// The caller chooses the transport later based on the workload protocol.
///
/// `target_host` is threaded through to set the HTTP Host header for the REST
/// transport. FIX and WebSocket payloads do not use it.
///
/// ## Order ID format
///
/// order_id is `{session_id}_{bot_id}_{seq}`. This format is stable and
/// unambiguous: session_id is validated to contain only `[a-zA-Z0-9._-]` (see
/// `validate_identifier` in worker.rs), so underscores in session_id cannot
/// collide with the delimiter because bot_id and seq are always numeric.
/// Downstream consumers may rely on this format for indexing.
///
/// ## FIX SendingTime (tag 52)
///
/// Tag 52 is precomputed to a placeholder `19700101-00:00:00.000` and updated
/// in-place on the hot path via `patch_timestamp()`. This satisfies strict Tag 52
/// freshness validation checks on the matching engine side without incurring any
/// heap allocations during active benchmark sends.
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

/// cancel_frame builds a FIX Order Cancel Request (35=F) and REST/WS equivalents.
///
/// The REST / WS cancel JSON payload omits redundant price/qty/side fields to comply
/// with strict REST validator standards.
pub fn cancel_frame(
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    orig_seq: u64,
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
        Some(orig_seq),
        price,
        qty,
        side,
        FrameKind::Cancel,
    )
}

/// replace_frame builds a FIX Order Cancel/Replace Request (35=G) and REST/WS equivalents.
#[allow(dead_code)]
pub fn replace_frame(
    fix_version: &str,
    session_id: &str,
    target_host: &str,
    bot_id: u64,
    seq: u64,
    orig_seq: u64,
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
        Some(orig_seq),
        price,
        qty,
        side,
        FrameKind::Replace,
    )
}

/// finalize_fix prefixes the FIX body with BeginString/BodyLength and appends
/// the standard modulo-256 checksum trailer.
pub fn finalize_fix(fix_version: &str, body: &str) -> Vec<u8> {
    let mut frame = format!("8={fix_version}\x019={}\x01{body}", body.len()).into_bytes();
    let checksum = frame
        .iter()
        .fold(0u32, |sum, b| sum.wrapping_add(u32::from(*b)))
        % 256;
    frame.extend_from_slice(format!("10={checksum:03}\x01").as_bytes());
    frame
}

/// MessageRef borrows ClOrdID + MsgType out of one parsed FIX message. The
/// bot-fleet's read path only needs these two fields — everything else in
/// the response is ignored. Use parse_messages() to drive iteration over a
/// byte buffer with carry-over support.
#[derive(Debug, Clone, Copy)]
pub struct MessageRef<'a> {
    pub msg_type: &'a [u8],
    pub clord_id: Option<&'a [u8]>,
}

/// parse_messages walks `buf` and returns every complete FIX message plus the
/// number of bytes consumed. A trailing partial message (no checksum yet) is
/// left intact for the caller; advance the read buffer by `consumed` bytes
/// and append the next chunk on the next call.
///
/// Robustness:
///   - Does NOT validate BodyLength (tag 9) or checksum (tag 10) — we only
///     need ClOrdID matching, not strict FIX conformance. A malformed
///     message slips through as MessageRef { msg_type: b"", clord_id: None }
///     and is ignored by the caller.
///   - "Complete message" is detected by finding the checksum trailer
///     `\x0110=XXX\x01`. Any bytes between the previous message end and the
///     next `8=FIX` start marker are skipped.
pub fn parse_messages(buf: &[u8]) -> (Vec<MessageRef<'_>>, usize) {
    let mut out = Vec::new();
    let mut cursor = 0;

    while cursor < buf.len() {
        // Each FIX message starts with "8=FIX". If we don't find one from
        // the cursor onward, we're done — everything past the cursor is
        // either junk (which we drop) or a partial start (carry-over).
        let Some(start_off) = find_subslice(&buf[cursor..], b"8=FIX") else {
            // No more messages and no partial start; consume the whole tail.
            // (If a partial "8=F" appeared near the end the carry-over scan
            // below catches it.)
            cursor = buf.len();
            break;
        };
        let abs_start = cursor + start_off;

        // From this start, look for the checksum trailer that closes the
        // message: `\x0110=XXX\x01`. If absent, the message is truncated —
        // stop here and let the caller carry over from abs_start.
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

/// find_message_end locates the closing `\x0110=XXX\x01` of a FIX message
/// that begins at byte 0 of `buf`. Returns the absolute offset (exclusive)
/// past the trailing SOH, or None if the message is truncated.
fn find_message_end(buf: &[u8]) -> Option<usize> {
    let needle = b"\x0110=";
    let i = find_subslice(buf, needle)?;
    let after_eq = i + needle.len();
    // Next SOH after the checksum value closes the message.
    let soh = buf[after_eq..].iter().position(|&b| b == SOH)?;
    Some(after_eq + soh + 1)
}

/// parse_single_message extracts MsgType (tag 35) and ClOrdID (tag 11) from
/// one complete FIX message. Both are returned as borrowed slices into the
/// input — no allocation.
fn parse_single_message(msg: &[u8]) -> MessageRef<'_> {
    MessageRef {
        msg_type: extract_tag(msg, b"35").unwrap_or(b""),
        clord_id: extract_tag(msg, b"11"),
    }
}

/// extract_tag finds `\x01TAG=value\x01` inside `msg` and returns the value
/// slice. The leading SOH anchors the match so that tag names embedded in
/// other field values are not confused with real tags.
fn extract_tag<'a>(msg: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    // The first field after BeginString is preceded by SOH, so the first
    // legitimate tag in the message also follows an SOH. By only searching
    // for `\x01TAG=`, we avoid false positives like `54=` matching inside
    // a `554=` tag (which doesn't exist in FIX, but the principle holds).
    let mut needle = Vec::with_capacity(tag.len() + 2);
    needle.push(SOH);
    needle.extend_from_slice(tag);
    needle.push(b'=');

    let pos = find_subslice(msg, &needle)?;
    let value_start = pos + needle.len();
    let value_end = msg[value_start..].iter().position(|&b| b == SOH)?;
    Some(&msg[value_start..value_start + value_end])
}

/// find_subslice is a naive substring search. FIX messages are small
/// (~150 bytes) so the O(n·m) cost is acceptable and avoids pulling in a
/// dedicated SIMD search crate.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// execution_report_frame builds a minimal ExecutionReport (MsgType=8) with
/// OrdStatus=New (39=0) that the bot-fleet's integration test fixture echoes
/// back. Real exchanges send richer reports; the bot only cares about
/// MsgType + ClOrdID for r9 capture, so this lean variant is sufficient.
pub fn execution_report_frame(fix_version: &str, seq: u64, clord_id: &str) -> Vec<u8> {
    let body = format!(
        "35=8\x0149=CONTESTANT\x0156=IICPC-BOT\x0134={seq}\x0152=19700101-00:00:00.000\x0137=EXEC_{seq}\x0111={clord_id}\x0117=EXECID_{seq}\x01150=0\x0139=0\x0155=IICPC\x0154=1\x0138=0\x0114=0\x016=0\x01"
    );
    finalize_fix(fix_version, &body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_one_complete_message() {
        let msg = execution_report_frame("FIX.4.2", 1, "ORD_42");
        let (msgs, consumed) = parse_messages(&msg);
        assert_eq!(consumed, msg.len(), "should consume the full buffer");
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].msg_type, b"8");
        assert_eq!(msgs[0].clord_id, Some(b"ORD_42".as_ref()));
    }

    #[test]
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
    fn truncated_tail_is_carry_over() {
        let m1 = execution_report_frame("FIX.4.2", 1, "FIRST");
        let m2 = execution_report_frame("FIX.4.2", 2, "SECOND");
        // Take the full first message + a prefix of the second; the
        // truncated second message must be returned as carry-over.
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
    fn ignores_non_execution_reports() {
        // A New-Order-Single (35=D) message — the reader should still parse
        // it (as a message with msg_type=b"D"); the caller filters on
        // msg_type == b"8".
        let body = "35=D\x0149=X\x0156=Y\x0134=1\x0111=ORDER_X\x01";
        let frame = finalize_fix("FIX.4.2", body);
        let (msgs, _) = parse_messages(&frame);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].msg_type, b"D");
        assert_eq!(msgs[0].clord_id, Some(b"ORDER_X".as_ref()));
    }

    /// Recompute the FIX modulo-256 checksum over everything before the
    /// "10=XXX\x01" trailer (the last 7 bytes) and compare to the embedded value.
    fn embedded_checksum_is_valid(fix: &[u8]) -> bool {
        let len = fix.len();
        let computed: u32 = fix[..len - 7].iter().map(|&b| u32::from(b)).sum::<u32>() % 256;
        let embedded = u32::from(fix[len - 4] - b'0') * 100
            + u32::from(fix[len - 3] - b'0') * 10
            + u32::from(fix[len - 2] - b'0');
        computed == embedded
    }

    #[test]
    fn patch_timestamp_sets_sending_time_and_keeps_checksum_valid() {
        let mut frame = order_frame("FIX.4.2", "sess1", "host", 7, 42, 10_000, 5, Side::Buy);
        let off = frame.tag52_offset.expect("tag 52 offset must be located");

        // order_frame emits the epoch placeholder, and that frame's checksum is valid.
        assert_eq!(
            &frame.fix[off..off + FIX_TIMESTAMP_LEN],
            &FIX_TIMESTAMP_PLACEHOLDER[..]
        );
        assert!(embedded_checksum_is_valid(&frame.fix));

        let ns = 1_716_023_400_123_000_000_u64; // 2024-05-18T08:30:00.123Z
        frame.patch_timestamp(ns);

        // Tag 52 now reflects the real send time, not the placeholder.
        let expected = time::format_fix_timestamp(ns);
        assert_eq!(&frame.fix[off..off + FIX_TIMESTAMP_LEN], &expected[..]);
        assert_ne!(
            &frame.fix[off..off + FIX_TIMESTAMP_LEN],
            &FIX_TIMESTAMP_PLACEHOLDER[..]
        );

        // The delta-checksum rewrite kept the trailer consistent.
        assert!(embedded_checksum_is_valid(&frame.fix));
    }
}
