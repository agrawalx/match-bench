use iicpc_schemas_rust::Side;

const SOH: u8 = 0x01;

/// OrderFrame holds the pre-rendered payloads for each supported transport.
/// Bots reuse these bytes during the live workload to avoid hot-path encoding.
#[derive(Debug, Clone)]
pub struct OrderFrame {
    pub order_id: String,
    pub price: u64,
    pub qty: u64,
    pub side: Side,
    pub fix: Vec<u8>,
    pub rest: Vec<u8>,
    pub ws: String,
}

/// logon_frame builds a FIX Logon message for the contestant endpoint.
/// The sequence number is supplied by the caller so sessions stay deterministic.
pub fn logon_frame(fix_version: &str, seq: u64) -> Vec<u8> {
    let body = format!("35=A\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={seq}\x0198=0\x01108=30\x01");
    finalize_fix(fix_version, &body)
}

/// order_frame builds one logical order as FIX, REST, and WebSocket payloads.
/// The caller chooses the transport later based on the workload protocol.
pub fn order_frame(
    fix_version: &str,
    session_id: &str,
    bot_id: u64,
    seq: u64,
    price: u64,
    qty: u64,
    side: Side,
) -> OrderFrame {
    let order_id = format!("{session_id}_{bot_id}_{seq}");
    let side_tag = match side {
        Side::Buy => "1",
        Side::Sell => "2",
    };

    let body = format!(
        "35=D\x0149=IICPC-BOT\x0156=CONTESTANT\x0134={seq}\x0152=19700101-00:00:00.000\x0111={order_id}\x0121=1\x0155=IICPC\x0154={side_tag}\x0138={qty}\x0140=2\x0144={price}\x0159=0\x01"
    );
    let fix = finalize_fix(fix_version, &body);
    let side_name = match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };
    let encoded_order_id =
        serde_json::to_string(&order_id).expect("serializing String cannot fail");
    let json = format!(
        "{{\"cl_ord_id\":{encoded_order_id},\"symbol\":\"IICPC\",\"side\":\"{side_name}\",\"qty\":{qty},\"price\":{price}}}",
    );

    let mut rest = String::with_capacity(96 + json.len());
    use std::fmt::Write;
    write!(
        &mut rest,
        "POST /orders HTTP/1.1\r\nHost: contestant\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
        json.len(),
        json
    )
    .expect("writing to String cannot fail");
    let rest = rest.into_bytes();

    OrderFrame {
        order_id,
        price,
        qty,
        side,
        fix,
        rest,
        ws: json,
    }
}

/// finalize_fix prefixes the FIX body with BeginString/BodyLength and appends
/// the standard modulo-256 checksum trailer.
pub fn finalize_fix(fix_version: &str, body: &str) -> Vec<u8> {
    let mut frame = format!("8={fix_version}\x019={}\x01{body}", body.len()).into_bytes();
    // use wrapping_add to make u32 overflow defined;
    // messages are small so overflow is unlikely, but correctness matters
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
        assert_eq!(consumed, m1.len(), "consumed = end of first message; rest is carry-over");
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
}
