use iicpc_schemas_rust::Side;

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
fn finalize_fix(fix_version: &str, body: &str) -> Vec<u8> {
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
