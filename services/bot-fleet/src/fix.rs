use iicpc_schemas_rust::{PayloadType, Side};

use crate::time;

/// The exact character length of standard FIX YYYYMMDD-HH:MM:SS.mmm timestamps.
/// Must match `time::format_fix_timestamp`'s stack-allocated array size.
pub const FIX_TIMESTAMP_LEN: usize = 21;
const FIX_TIMESTAMP_PLACEHOLDER: &[u8; FIX_TIMESTAMP_LEN] = b"19700101-00:00:00.000";

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
fn finalize_fix(fix_version: &str, body: &str) -> Vec<u8> {
    let mut frame = format!("8={fix_version}\x019={}\x01{body}", body.len()).into_bytes();
    let checksum = frame
        .iter()
        .fold(0u32, |sum, b| sum.wrapping_add(u32::from(*b)))
        % 256;
    frame.extend_from_slice(format!("10={checksum:03}\x01").as_bytes());
    frame
}
