//! Message framing and field extraction for the reassembled byte streams.
//!
//! Framing splits a contiguous stream into whole messages; extraction pulls the
//! join key (ClOrdID) and response metadata out of each message. Unlike the old
//! in-kernel parser this uses NO fixed offsets — FIX is framed by BodyLength
//! (tag 9) and fields are found by scanning SOH-delimited `tag=value` pairs, so
//! it handles real bot requests (where tag 11 drifts) and arbitrary
//! contestant-generated responses.

use crate::capture::{Direction, Transport};

const SOH: u8 = 0x01;
const PRICE_SCALE: u64 = 1_000_000_000;
/// Guard against a corrupt BodyLength framing a runaway message.
const MAX_FIX_MESSAGE: usize = 64 * 1024;
const MAX_HTTP_MESSAGE: usize = 64 * 1024;

/// Result of trying to frame one message off the front of a stream.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    /// A complete message occupies the first `len` bytes.
    Message(usize),
    /// Not enough bytes yet; wait for more.
    Incomplete,
    /// The front `skip` bytes cannot start a message; drop them and retry.
    Resync(usize),
}

/// How a framed message should be treated by the matcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Classified {
    /// A new-order / cancel / replace request — carries t3, keyed by ClOrdID.
    Request,
    /// An execution report / order response — carries t7, joined by ClOrdID.
    Response,
    /// Admin/session traffic (logon, heartbeat, …) — no timestamp applies.
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ParsedMessage {
    pub class: Classified,
    pub clordid: String,
    pub orig_clordid: String,
    pub exec_type: String,
    pub fill_qty: u64,
    pub fill_price: u64,
}

impl Default for Classified {
    fn default() -> Self {
        Classified::Ignore
    }
}

/// Frame one message off the front of `buf` for the given transport/direction.
pub fn frame(transport: Transport, direction: Direction, buf: &[u8]) -> Frame {
    match transport {
        Transport::Fix => frame_fix(buf),
        Transport::HttpWs => frame_http_ws(direction, buf),
    }
}

/// Parse a single framed message (exactly the bytes `frame` reported).
pub fn parse(transport: Transport, direction: Direction, msg: &[u8]) -> ParsedMessage {
    match transport {
        Transport::Fix => parse_fix(direction, msg),
        Transport::HttpWs => parse_http_ws(direction, msg),
    }
}

// ---- FIX --------------------------------------------------------------------

fn frame_fix(buf: &[u8]) -> Frame {
    // A FIX message begins with "8=". Resync past any leading garbage.
    if buf.len() < 2 {
        return Frame::Incomplete;
    }
    if &buf[0..2] != b"8=" {
        return match find(buf, b"8=") {
            Some(i) => Frame::Resync(i),
            None => Frame::Resync(buf.len().saturating_sub(1)),
        };
    }
    // First SOH ends the BeginString (tag 8).
    let Some(soh1) = find_byte(buf, SOH, 0) else {
        return Frame::Incomplete;
    };
    // BodyLength (tag 9) must follow immediately.
    if buf.len() < soh1 + 3 || &buf[soh1 + 1..soh1 + 3] != b"9=" {
        return Frame::Resync(soh1 + 1);
    }
    let Some(soh2) = find_byte(buf, SOH, soh1 + 3) else {
        return Frame::Incomplete;
    };
    let Some(body_len) = parse_uint(&buf[soh1 + 3..soh2]) else {
        return Frame::Resync(soh2 + 1);
    };
    let body_len = body_len as usize;
    // CheckSum field "10=NNN\x01" is a fixed 7 bytes.
    let total = (soh2 + 1) + body_len + 7;
    if body_len > MAX_FIX_MESSAGE {
        return Frame::Resync(soh1 + 1);
    }
    if buf.len() < total {
        return Frame::Incomplete;
    }
    Frame::Message(total)
}

fn parse_fix(direction: Direction, msg: &[u8]) -> ParsedMessage {
    let mut msg_type: &[u8] = b"";
    let mut clordid = String::new();
    let mut orig = String::new();
    let mut exec150 = String::new();
    let mut ordstatus39 = String::new();
    let mut fill_qty = 0u64;
    let mut fill_price = 0u64;

    for field in msg.split(|&b| b == SOH) {
        let Some(eq) = field.iter().position(|&b| b == b'=') else {
            continue;
        };
        let (tag, val) = (&field[..eq], &field[eq + 1..]);
        match tag {
            b"35" => msg_type = val,
            b"11" => clordid = string(val),
            b"41" => orig = string(val),
            b"150" => exec150 = string(val),
            b"39" => ordstatus39 = string(val),
            b"32" => fill_qty = parse_uint(val).unwrap_or(0),
            b"31" => fill_price = parse_decimal_scaled(val),
            _ => {}
        }
    }

    let class = match (direction, msg_type) {
        (Direction::Request, b"D") | (Direction::Request, b"F") | (Direction::Request, b"G") => {
            Classified::Request
        }
        (Direction::Response, b"8") => Classified::Response,
        _ => Classified::Ignore,
    };

    let exec_type = if !exec150.is_empty() {
        exec150
    } else {
        ordstatus39
    };

    ParsedMessage {
        class,
        clordid,
        orig_clordid: orig,
        exec_type,
        fill_qty,
        fill_price,
    }
}

// ---- HTTP / WebSocket -------------------------------------------------------

fn frame_http_ws(direction: Direction, buf: &[u8]) -> Frame {
    if buf.is_empty() {
        return Frame::Incomplete;
    }
    if looks_like_http(buf) {
        frame_http(buf)
    } else {
        frame_ws(direction, buf)
    }
}

fn looks_like_http(buf: &[u8]) -> bool {
    const PREFIXES: [&[u8]; 5] = [b"POST", b"GET ", b"PUT ", b"DELE", b"HTTP"];
    PREFIXES.iter().any(|p| buf.starts_with(p))
}

fn frame_http(buf: &[u8]) -> Frame {
    let Some(hdr_end) = find(buf, b"\r\n\r\n") else {
        if buf.len() > MAX_HTTP_MESSAGE {
            return Frame::Resync(buf.len());
        }
        return Frame::Incomplete;
    };
    let body_start = hdr_end + 4;
    // M32: a chunked response carries no Content-Length. The old code defaulted
    // content_len to 0, framing only the headers and leaving the chunked body to be
    // mis-parsed as a fresh message — derailing framing for the whole connection.
    // Frame through the terminating zero-size chunk ("0\r\n\r\n") so the connection
    // stays in sync; field extraction from a chunked body is best-effort.
    if header_is_chunked(&buf[..hdr_end]) {
        return match find(&buf[body_start..], b"0\r\n\r\n") {
            Some(rel) => {
                let total = body_start + rel + 5; // include the "0\r\n\r\n" terminator
                if total > MAX_HTTP_MESSAGE {
                    Frame::Resync(body_start)
                } else {
                    Frame::Message(total)
                }
            }
            None if buf.len() > MAX_HTTP_MESSAGE => Frame::Resync(body_start),
            None => Frame::Incomplete,
        };
    }
    let content_len = header_content_length(&buf[..hdr_end]).unwrap_or(0);
    let total = body_start + content_len;
    if total > MAX_HTTP_MESSAGE {
        return Frame::Resync(body_start);
    }
    if buf.len() < total {
        return Frame::Incomplete;
    }
    Frame::Message(total)
}

/// True when the response headers declare Transfer-Encoding: chunked (no
/// Content-Length). Case-insensitive, mirrors header_content_length.
fn header_is_chunked(headers: &[u8]) -> bool {
    let lower: Vec<u8> = headers.iter().map(|b| b.to_ascii_lowercase()).collect();
    match find(&lower, b"transfer-encoding:") {
        Some(i) => {
            let rest = &lower[i + b"transfer-encoding:".len()..];
            let end = rest.iter().position(|&b| b == b'\r').unwrap_or(rest.len());
            find(&rest[..end], b"chunked").is_some()
        }
        None => false,
    }
}

fn frame_ws(direction: Direction, buf: &[u8]) -> Frame {
    if buf.len() < 2 {
        return Frame::Incomplete;
    }
    let masked = buf[1] & 0x80 != 0;
    let len7 = (buf[1] & 0x7f) as usize;
    let (mut header, payload_len) = if len7 < 126 {
        (2usize, len7)
    } else if len7 == 126 {
        if buf.len() < 4 {
            return Frame::Incomplete;
        }
        (4usize, ((buf[2] as usize) << 8) | buf[3] as usize)
    } else {
        // 64-bit length not expected for these small JSON frames.
        return Frame::Resync(1);
    };
    if masked {
        header += 4;
    }
    // Client->server frames must be masked; server->client must not be.
    let expect_masked = direction == Direction::Request;
    if masked != expect_masked {
        return Frame::Resync(1);
    }
    let total = header + payload_len;
    if buf.len() < total {
        return Frame::Incomplete;
    }
    Frame::Message(total)
}

fn parse_http_ws(direction: Direction, msg: &[u8]) -> ParsedMessage {
    let body: Vec<u8> = if looks_like_http(msg) {
        match find(msg, b"\r\n\r\n") {
            Some(i) => msg[i + 4..].to_vec(),
            None => Vec::new(),
        }
    } else {
        ws_unmasked_payload(direction, msg)
    };
    parse_json(direction, &body)
}

fn ws_unmasked_payload(direction: Direction, frame: &[u8]) -> Vec<u8> {
    if frame.len() < 2 {
        return Vec::new();
    }
    let masked = frame[1] & 0x80 != 0;
    let len7 = (frame[1] & 0x7f) as usize;
    let (mut off, payload_len) = if len7 < 126 {
        (2usize, len7)
    } else if len7 == 126 && frame.len() >= 4 {
        (4usize, ((frame[2] as usize) << 8) | frame[3] as usize)
    } else {
        return Vec::new();
    };
    let _ = direction;
    if masked {
        if frame.len() < off + 4 {
            return Vec::new();
        }
        let mask = [frame[off], frame[off + 1], frame[off + 2], frame[off + 3]];
        off += 4;
        let end = (off + payload_len).min(frame.len());
        frame[off..end]
            .iter()
            .enumerate()
            .map(|(i, &b)| b ^ mask[i % 4])
            .collect()
    } else {
        let end = (off + payload_len).min(frame.len());
        frame[off..end].to_vec()
    }
}

fn parse_json(direction: Direction, body: &[u8]) -> ParsedMessage {
    let clordid = json_string(body, "cl_ord_id").unwrap_or_default();
    let orig = json_string(body, "orig_cl_ord_id").unwrap_or_default();
    let exec_type = json_string(body, "exec_type").unwrap_or_default();
    let fill_qty = json_uint(body, "fill_qty").unwrap_or(0);
    let fill_price = json_decimal_scaled(body, "fill_price").unwrap_or(0);

    // The capture direction already tells request from response for JSON; there
    // is no admin traffic on this transport.
    let class = if clordid.is_empty() {
        Classified::Ignore
    } else if direction == Direction::Request {
        Classified::Request
    } else {
        Classified::Response
    };

    ParsedMessage {
        class,
        clordid,
        orig_clordid: orig,
        exec_type,
        fill_qty,
        fill_price,
    }
}

// ---- small byte helpers -----------------------------------------------------

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    (0..=hay.len() - needle.len()).find(|&i| &hay[i..i + needle.len()] == needle)
}

fn find_byte(hay: &[u8], b: u8, from: usize) -> Option<usize> {
    hay.get(from..)?
        .iter()
        .position(|&x| x == b)
        .map(|i| i + from)
}

fn string(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn parse_uint(b: &[u8]) -> Option<u64> {
    if b.is_empty() {
        return None;
    }
    let mut v = 0u64;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.saturating_mul(10).saturating_add((c - b'0') as u64);
    }
    Some(v)
}

/// Parse a decimal like "42.5" into a fixed-point integer scaled by 1e9.
fn parse_decimal_scaled(b: &[u8]) -> u64 {
    let mut whole = 0u64;
    let mut frac = 0u64;
    let mut frac_digits = 0u32;
    let mut seen_dot = false;
    for &c in b {
        match c {
            b'.' => seen_dot = true,
            b'0'..=b'9' => {
                if seen_dot {
                    if frac_digits < 9 {
                        frac = frac * 10 + (c - b'0') as u64;
                        frac_digits += 1;
                    }
                } else {
                    whole = whole.saturating_mul(10).saturating_add((c - b'0') as u64);
                }
            }
            _ => break,
        }
    }
    while frac_digits < 9 {
        frac *= 10;
        frac_digits += 1;
    }
    whole.saturating_mul(PRICE_SCALE).saturating_add(frac)
}

fn header_content_length(headers: &[u8]) -> Option<usize> {
    // Case-insensitive search for "content-length:".
    let lower: Vec<u8> = headers.iter().map(|b| b.to_ascii_lowercase()).collect();
    let i = find(&lower, b"content-length:")?;
    let rest = &headers[i + b"content-length:".len()..];
    let end = rest.iter().position(|&b| b == b'\r').unwrap_or(rest.len());
    let val: Vec<u8> = rest[..end]
        .iter()
        .copied()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    parse_uint(&val).map(|v| v as usize)
}

fn json_string(body: &[u8], key: &str) -> Option<String> {
    let pat = format!("\"{key}\"");
    let i = find(body, pat.as_bytes())?;
    let rest = &body[i + pat.len()..];
    let colon = rest.iter().position(|&b| b == b':')?;
    let after = &rest[colon + 1..];
    let q1 = after.iter().position(|&b| b == b'"')?;
    let after_q = &after[q1 + 1..];
    let q2 = after_q.iter().position(|&b| b == b'"')?;
    Some(string(&after_q[..q2]))
}

fn json_value_slice<'a>(body: &'a [u8], key: &str) -> Option<&'a [u8]> {
    let pat = format!("\"{key}\"");
    let i = find(body, pat.as_bytes())?;
    let rest = &body[i + pat.len()..];
    let colon = rest.iter().position(|&b| b == b':')?;
    let after = &rest[colon + 1..];
    let start = after.iter().position(|&b| !b.is_ascii_whitespace())?;
    let val = &after[start..];
    let end = val
        .iter()
        .position(|&b| b == b',' || b == b'}' || b == b'"' || b == b' ')
        .unwrap_or(val.len());
    Some(&val[..end])
}

fn json_uint(body: &[u8], key: &str) -> Option<u64> {
    parse_uint(json_value_slice(body, key)?)
}

fn json_decimal_scaled(body: &[u8], key: &str) -> Option<u64> {
    Some(parse_decimal_scaled(json_value_slice(body, key)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    // M32: a chunked HTTP response (no Content-Length) must be framed through its
    // terminating zero chunk, not treated as a headers-only message that derails
    // framing of everything after it.
    #[test]
    fn chunked_http_framed_through_terminator() {
        let resp =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
        match frame_http(resp) {
            Frame::Message(n) => assert_eq!(n, resp.len(), "frame whole chunked message"),
            other => panic!("expected Message, got {:?}", other),
        }
        // Without the terminating "0\r\n\r\n" yet, the message is incomplete (NOT
        // mis-framed as a 0-length-body message).
        let partial = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhel";
        assert_eq!(frame_http(partial), Frame::Incomplete);
    }

    /// Build a wire-format FIX message: prepend 8=/9= and append 10=NNN.
    fn fix(body: &str) -> Vec<u8> {
        let body = body.replace('|', "\x01");
        let head = format!("8=FIX.4.2\x019={}\x01", body.len());
        let mut bytes = format!("{head}{body}").into_bytes();
        let sum: u32 = bytes.iter().map(|&b| b as u32).sum::<u32>() % 256;
        bytes.extend_from_slice(format!("10={sum:03}\x01").as_bytes());
        bytes
    }

    #[test]
    fn frames_one_fix_message_by_body_length() {
        let m = fix("35=D|49=IICPC-BOT|56=CONTESTANT|34=7|11=sess_1_7_O|55=IICPC|54=1|38=12|40=2|44=42.5|59=0|");
        assert_eq!(
            frame(Transport::Fix, Direction::Request, &m),
            Frame::Message(m.len())
        );
    }

    #[test]
    fn frames_coalesced_then_consumes() {
        let a = fix("35=D|49=IICPC-BOT|56=CONTESTANT|34=8|11=sess_1_8_O|38=1|40=2|44=1.0|");
        let b = fix("35=D|49=IICPC-BOT|56=CONTESTANT|34=9|11=sess_1_9_O|38=1|40=2|44=2.0|");
        let mut stream = a.clone();
        stream.extend_from_slice(&b);
        let Frame::Message(n) = frame(Transport::Fix, Direction::Request, &stream) else {
            panic!("expected first message");
        };
        assert_eq!(n, a.len());
        let Frame::Message(n2) = frame(Transport::Fix, Direction::Request, &stream[n..]) else {
            panic!("expected second message");
        };
        assert_eq!(n2, b.len());
    }

    #[test]
    fn incomplete_fix_waits_for_more() {
        let m = fix("35=D|49=IICPC-BOT|34=7|11=sess_1_7_O|38=1|40=2|44=42.5|");
        assert_eq!(
            frame(Transport::Fix, Direction::Request, &m[..m.len() - 3]),
            Frame::Incomplete
        );
    }

    #[test]
    fn extracts_clordid_regardless_of_offset() {
        // Realistic header: 9= and 34= make tag 11 land far from any fixed offset.
        let m = fix("35=D|49=IICPC-BOT|56=CONTESTANT|34=123|52=19700101-00:00:00.000|11=sess_42_123_O|21=1|55=IICPC|54=1|38=12|40=2|44=42.5|59=0|");
        let p = parse(Transport::Fix, Direction::Request, &m);
        assert_eq!(p.class, Classified::Request);
        assert_eq!(p.clordid, "sess_42_123_O");
    }

    #[test]
    fn cancel_request_carries_orig_clordid() {
        let m = fix(
            "35=F|49=IICPC-BOT|56=CONTESTANT|34=5|11=sess_1_5_C|41=sess_1_2_O|55=IICPC|54=1|38=3|",
        );
        let p = parse(Transport::Fix, Direction::Request, &m);
        assert_eq!(p.class, Classified::Request);
        assert_eq!(p.clordid, "sess_1_5_C");
        assert_eq!(p.orig_clordid, "sess_1_2_O");
    }

    #[test]
    fn execution_report_is_a_response_with_fill() {
        let m = fix("35=8|49=CONTESTANT|56=IICPC-BOT|34=2|37=EXEC_2|11=order-real-1|17=E2|150=F|39=2|32=12|31=42.5|");
        let p = parse(Transport::Fix, Direction::Response, &m);
        assert_eq!(p.class, Classified::Response);
        assert_eq!(p.clordid, "order-real-1");
        assert_eq!(p.exec_type, "F");
        assert_eq!(p.fill_qty, 12);
        assert_eq!(p.fill_price, 42_500_000_000);
    }

    #[test]
    fn logon_is_ignored() {
        let m = fix("35=A|49=IICPC-BOT|56=CONTESTANT|34=1|98=0|108=30|");
        let p = parse(Transport::Fix, Direction::Request, &m);
        assert_eq!(p.class, Classified::Ignore);
    }

    #[test]
    fn http_response_framed_by_content_length_and_parsed() {
        let body =
            br#"{"cl_ord_id":"order-rest-1","exec_type":"F","fill_qty":7,"fill_price":99.25}"#;
        let mut msg = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        msg.extend_from_slice(body);
        assert_eq!(
            frame(Transport::HttpWs, Direction::Response, &msg),
            Frame::Message(msg.len())
        );
        let p = parse(Transport::HttpWs, Direction::Response, &msg);
        assert_eq!(p.class, Classified::Response);
        assert_eq!(p.clordid, "order-rest-1");
        assert_eq!(p.exec_type, "F");
        assert_eq!(p.fill_qty, 7);
        assert_eq!(p.fill_price, 99_250_000_000);
    }

    #[test]
    fn masked_ws_request_is_unmasked_and_parsed() {
        let body = br#"{"cl_ord_id":"order-ws-1","qty":3,"price":11.75}"#;
        let mask = [0x13u8, 0x37, 0xc0, 0xde];
        let mut frame_bytes = vec![0x81u8, 0x80 | body.len() as u8];
        frame_bytes.extend_from_slice(&mask);
        for (i, &b) in body.iter().enumerate() {
            frame_bytes.push(b ^ mask[i % 4]);
        }
        assert_eq!(
            frame(Transport::HttpWs, Direction::Request, &frame_bytes),
            Frame::Message(frame_bytes.len())
        );
        let p = parse(Transport::HttpWs, Direction::Request, &frame_bytes);
        assert_eq!(p.class, Classified::Request);
        assert_eq!(p.clordid, "order-ws-1");
    }
}
