//! Decoding of the kernel `CaptureRecord` ring-buffer ABI.
//!
//! The kernel (src/ebpf.rs) emits a fixed 28-byte header followed by
//! `captured_len` payload bytes. All integer fields are written on the
//! little-endian `bpfel` target as already-host-order values (the kernel applied
//! `from_be` to the network fields), so userspace reads them as plain
//! little-endian integers. Keep this in sync with `CaptureRecord` in ebpf.rs.

/// Byte offset of the payload within the record (matches CAPTURE_HEADER_LEN).
pub const CAPTURE_HEADER_LEN: usize = 28;
/// Maximum payload bytes the kernel copies per segment (matches CAPTURE_CAP).
pub const CAPTURE_CAP: usize = 1536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// XDP ingress — a request entering the algo pod (carries t3).
    Request,
    /// tc egress — a response leaving the algo pod (carries t7).
    Response,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// FIX on TCP port 9898.
    Fix,
    /// REST/WebSocket on TCP port 8080.
    HttpWs,
}

/// Identifies one TCP connection by its client (bot) side, which the kernel
/// reports identically on both directions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlowKey {
    pub client_ip: u32,
    pub client_port: u16,
}

/// A decoded capture: one TCP segment's worth of payload plus metadata.
#[derive(Debug, Clone)]
pub struct Capture<'a> {
    pub timestamp_ns: u64,
    pub flow: FlowKey,
    /// FIX/HTTP port — informational; the transport hint is precomputed.
    #[allow(dead_code)]
    pub server_port: u16,
    pub tcp_seq: u32,
    /// Full on-wire payload length (may exceed `payload.len()` if truncated).
    #[allow(dead_code)]
    pub payload_len: u32,
    pub direction: Direction,
    pub transport: Transport,
    pub payload: &'a [u8],
}

#[derive(Debug, PartialEq, Eq)]
pub enum CaptureError {
    TooShort { got: usize },
    BadCapturedLen { captured: usize, available: usize },
    UnknownDirection(u8),
}

impl core::fmt::Display for CaptureError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            CaptureError::TooShort { got } => {
                write!(f, "capture record shorter than header: {got} bytes")
            }
            CaptureError::BadCapturedLen {
                captured,
                available,
            } => write!(
                f,
                "captured_len {captured} exceeds available payload {available}"
            ),
            CaptureError::UnknownDirection(d) => write!(f, "unknown direction byte {d}"),
        }
    }
}

impl std::error::Error for CaptureError {}

fn read_u16(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([b[off], b[off + 1]])
}

fn read_u32(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

fn read_u64(b: &[u8], off: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[off..off + 8]);
    u64::from_le_bytes(a)
}

/// Decode one ring-buffer record. The returned `payload` borrows `bytes`.
pub fn decode(bytes: &[u8]) -> Result<Capture<'_>, CaptureError> {
    if bytes.len() < CAPTURE_HEADER_LEN {
        return Err(CaptureError::TooShort { got: bytes.len() });
    }
    let timestamp_ns = read_u64(bytes, 0);
    let client_ip = read_u32(bytes, 8);
    let tcp_seq = read_u32(bytes, 12);
    let payload_len = read_u32(bytes, 16);
    let client_port = read_u16(bytes, 20);
    let server_port = read_u16(bytes, 22);
    let captured_len = read_u16(bytes, 24) as usize;
    let direction = match bytes[26] {
        0 => Direction::Request,
        1 => Direction::Response,
        other => return Err(CaptureError::UnknownDirection(other)),
    };

    let available = bytes.len() - CAPTURE_HEADER_LEN;
    if captured_len > available || captured_len > CAPTURE_CAP {
        return Err(CaptureError::BadCapturedLen {
            captured: captured_len,
            available,
        });
    }
    let payload = &bytes[CAPTURE_HEADER_LEN..CAPTURE_HEADER_LEN + captured_len];

    let transport = if server_port == 9898 {
        Transport::Fix
    } else {
        Transport::HttpWs
    };

    Ok(Capture {
        timestamp_ns,
        flow: FlowKey {
            client_ip,
            client_port,
        },
        server_port,
        tcp_seq,
        payload_len,
        direction,
        transport,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(
        ts: u64,
        client_ip: u32,
        tcp_seq: u32,
        payload_len: u32,
        client_port: u16,
        server_port: u16,
        direction: u8,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&ts.to_le_bytes());
        v.extend_from_slice(&client_ip.to_le_bytes());
        v.extend_from_slice(&tcp_seq.to_le_bytes());
        v.extend_from_slice(&payload_len.to_le_bytes());
        v.extend_from_slice(&client_port.to_le_bytes());
        v.extend_from_slice(&server_port.to_le_bytes());
        v.extend_from_slice(&(payload.len() as u16).to_le_bytes());
        v.push(direction);
        v.push(0); // _pad
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn decodes_a_fix_request_record() {
        let rec = encode(42, 0x0a00_0001, 1000, 7, 51234, 9898, 0, b"8=FIX.4");
        let cap = decode(&rec).unwrap();
        assert_eq!(cap.timestamp_ns, 42);
        assert_eq!(cap.flow.client_ip, 0x0a00_0001);
        assert_eq!(cap.flow.client_port, 51234);
        assert_eq!(cap.server_port, 9898);
        assert_eq!(cap.tcp_seq, 1000);
        assert_eq!(cap.payload_len, 7);
        assert_eq!(cap.direction, Direction::Request);
        assert_eq!(cap.transport, Transport::Fix);
        assert_eq!(cap.payload, b"8=FIX.4");
    }

    #[test]
    fn decodes_an_http_response_record() {
        let rec = encode(99, 0x0a00_0002, 5, 4, 40000, 8080, 1, b"HTTP");
        let cap = decode(&rec).unwrap();
        assert_eq!(cap.direction, Direction::Response);
        assert_eq!(cap.transport, Transport::HttpWs);
        assert_eq!(cap.payload, b"HTTP");
    }

    #[test]
    fn rejects_short_record() {
        assert!(matches!(
            decode(&[0u8; 10]),
            Err(CaptureError::TooShort { got: 10 })
        ));
    }

    #[test]
    fn rejects_overlong_captured_len() {
        let mut rec = encode(1, 2, 3, 4, 5, 9898, 0, b"abc");
        // claim 200 captured bytes while only 3 are present
        rec[24] = 200;
        rec[25] = 0;
        assert!(matches!(
            decode(&rec),
            Err(CaptureError::BadCapturedLen { .. })
        ));
    }
}
