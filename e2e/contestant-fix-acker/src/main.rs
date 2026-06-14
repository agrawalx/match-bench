//! Minimal FIX 4.2 acking contestant for the IICPC e2e suite.
//!
//! Listens on 0.0.0.0:9898 (override with BIND or PORT), reads NewOrderSingle
//! (35=D) messages off each connection, and replies to every one with an
//! ExecutionReport (35=8, OrdStatus=New) echoing the order's ClOrdID (tag 11).
//! That 35=8 reply is what the eBPF latency capture classifies as a response and
//! matches against the request by ClOrdID — a plain byte-echo (35=D back) would
//! be ignored by the matcher. The wire format mirrors
//! bot-fleet/src/fix.rs::{execution_report_frame, finalize_fix}.
//!
//! std-only (no external crates) so it builds offline in the build-worker.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() -> std::io::Result<()> {
    let bind = std::env::var("BIND").unwrap_or_else(|_| {
        let port = std::env::var("PORT").unwrap_or_else(|_| "9898".to_string());
        format!("0.0.0.0:{port}")
    });
    let listener = TcpListener::bind(&bind)?;
    eprintln!("fix_acker listening on {bind}");
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                thread::spawn(move || {
                    if let Err(e) = handle(s) {
                        eprintln!("connection error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
    Ok(())
}

fn handle(mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true).ok();
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut chunk = [0u8; 8192];
    let mut seq: u64 = 1;
    loop {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(()); // peer closed
        }
        buf.extend_from_slice(&chunk[..n]);

        let mut out: Vec<u8> = Vec::new();
        let consumed = ack_complete_messages(&buf, &mut seq, &mut out);
        if !out.is_empty() {
            stream.write_all(&out)?;
        }
        if consumed > 0 {
            buf.drain(..consumed);
        }
    }
}

/// Scan `buf` for complete FIX messages (`8=FIX` … `\x0110=NNN\x01`). For each
/// NewOrderSingle, append an ExecutionReport to `out`. Returns the number of
/// leading bytes fully consumed (a trailing partial message is left in `buf`).
fn ack_complete_messages(buf: &[u8], seq: &mut u64, out: &mut Vec<u8>) -> usize {
    let mut cursor = 0usize;
    loop {
        let Some(start) = find(buf, b"8=FIX", cursor) else {
            break;
        };
        // The terminating field is "\x0110=NNN\x01" (SOH + "10=" + 3-digit checksum + SOH).
        let Some(tail) = find(buf, b"\x0110=", start) else {
            break; // checksum field not yet received
        };
        let end = tail + 4 + 3 + 1; // "\x0110=" (4) + 3 checksum digits + trailing SOH (1)
        if end > buf.len() {
            break; // message not fully received yet
        }
        let msg = &buf[start..end];
        if extract_tag(msg, b"35") == Some(b"D".as_ref()) {
            if let Some(clord) = extract_tag(msg, b"11") {
                let clord = std::str::from_utf8(clord).unwrap_or("UNKNOWN");
                out.extend_from_slice(&execution_report_frame(*seq, clord));
                *seq += 1;
            }
        }
        cursor = end;
    }
    cursor
}

fn find(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || from >= hay.len() {
        return None;
    }
    hay[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| p + from)
}

/// Return the value of a SOH-delimited `tag=value` field, exact-matching the tag.
fn extract_tag<'a>(msg: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    for field in msg.split(|&b| b == 0x01) {
        if field.len() > tag.len() && &field[..tag.len()] == tag && field[tag.len()] == b'=' {
            return Some(&field[tag.len() + 1..]);
        }
    }
    None
}

/// Byte-for-byte equivalent of bot-fleet's execution_report_frame (FIX 4.2).
fn execution_report_frame(seq: u64, clord_id: &str) -> Vec<u8> {
    let body = format!(
        "35=8\x0149=CONTESTANT\x0156=IICPC-BOT\x0134={seq}\x0152=19700101-00:00:00.000\x0137=EXEC_{seq}\x0111={clord_id}\x0117=EXECID_{seq}\x01150=0\x0139=0\x0155=IICPC\x0154=1\x0138=0\x0114=0\x016=0\x01"
    );
    finalize_fix("FIX.4.2", &body)
}

fn finalize_fix(version: &str, body: &str) -> Vec<u8> {
    let mut frame = format!("8={version}\x019={}\x01{body}", body.len()).into_bytes();
    let checksum = frame.iter().fold(0u32, |s, b| s.wrapping_add(u32::from(*b))) % 256;
    frame.extend_from_slice(format!("10={checksum:03}\x01").as_bytes());
    frame
}
