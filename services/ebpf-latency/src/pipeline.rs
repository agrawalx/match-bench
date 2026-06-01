//! The userspace processing pipeline: capture -> reassembly -> framing/parse ->
//! ClOrdID matching -> emitted measurements. Factored into its own module so the
//! integration test can drive the exact same code that `main` runs.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::capture::{Capture, Direction, FlowKey};
use crate::matcher::{MatchedEvent, Matcher, DEFAULT_IDLE_NS};
use crate::parse::{self, Classified, Frame};
use crate::reassembly::Reassembler;

pub struct Pipeline {
    reassemblers: HashMap<(FlowKey, Direction), Reassembler>,
    matcher: Matcher,
    /// Added to every kernel CLOCK_MONOTONIC stamp to get CLOCK_REALTIME ns.
    clock_offset_ns: u64,
}

impl Pipeline {
    /// Sample the monotonic->realtime offset once (so t7 - t3 stays exact).
    pub fn new() -> Self {
        Self::with_offset(realtime_minus_monotonic_ns())
    }

    pub fn with_offset(clock_offset_ns: u64) -> Self {
        Self {
            reassemblers: HashMap::new(),
            matcher: Matcher::new(),
            clock_offset_ns,
        }
    }

    pub fn to_realtime(&self, monotonic_ns: u64) -> u64 {
        monotonic_ns.wrapping_add(self.clock_offset_ns)
    }

    #[allow(dead_code)] // used for diagnostics; not exercised by every consumer
    pub fn unmatched_responses(&self) -> u64 {
        self.matcher.unmatched_responses
    }

    /// Feed one capture through reassembly + framing + matching, pushing any
    /// emitted measurements into `out`.
    pub fn process(&mut self, cap: &Capture, out: &mut Vec<MatchedEvent>) {
        let ts = self.to_realtime(cap.timestamp_ns);

        // Stage 1: reassemble + frame whole messages (reassembler borrow only).
        let mut framed: Vec<(u64, u32, bool, parse::ParsedMessage)> = Vec::new();
        {
            let re = self
                .reassemblers
                .entry((cap.flow, cap.direction))
                .or_default();
            let reordered = re.push(cap.tcp_seq, ts, cap.payload).reordered;
            loop {
                match parse::frame(cap.transport, cap.direction, re.available()) {
                    Frame::Message(n) => {
                        let msg_ts = re.timestamp_at(0);
                        let msg_seq = re.seq_at(0);
                        let parsed =
                            parse::parse(cap.transport, cap.direction, &re.available()[..n]);
                        re.consume(n);
                        framed.push((msg_ts, msg_seq, reordered, parsed));
                    }
                    Frame::Incomplete => break,
                    Frame::Resync(skip) => re.consume(skip.max(1)),
                }
            }
        }

        // Stage 2: apply framed messages to the matcher.
        for (msg_ts, msg_seq, reordered, p) in framed {
            match p.class {
                Classified::Request => self.matcher.on_request(
                    &p.clordid,
                    msg_ts,
                    cap.flow.client_ip,
                    cap.flow.client_port,
                    msg_seq,
                    reordered,
                ),
                Classified::Response => {
                    if let Some(ev) = self.matcher.on_response(
                        &p.clordid,
                        msg_ts,
                        &p.exec_type,
                        p.fill_qty,
                        p.fill_price,
                        &p.orig_clordid,
                        reordered,
                    ) {
                        out.push(ev);
                    }
                }
                Classified::Ignore => {}
            }
        }
    }

    /// Drop idle orders and reassemblers to bound memory between runs.
    #[allow(dead_code)] // driven by main's evict ticker; not by the integration test
    pub fn evict_idle(&mut self) {
        let now = self.to_realtime(monotonic_now_ns());
        self.matcher.evict_idle(now, DEFAULT_IDLE_NS);
        self.reassemblers
            .retain(|_, r| r.idle_ns(now) < DEFAULT_IDLE_NS);
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::with_offset(0)
    }
}

pub fn monotonic_now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime with a valid pointer; CLOCK_MONOTONIC matches the
    // kernel's bpf_ktime_get_ns domain.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    (ts.tv_sec as u64) * 1_000_000_000 + ts.tv_nsec as u64
}

/// Offset to add to a CLOCK_MONOTONIC stamp to get CLOCK_REALTIME nanoseconds.
pub fn realtime_minus_monotonic_ns() -> u64 {
    let real = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    real.wrapping_sub(monotonic_now_ns())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::{Capture, FlowKey, Transport};

    fn fix(body: &str) -> Vec<u8> {
        let body = body.replace('|', "\x01");
        let head = format!("8=FIX.4.2\x019={}\x01", body.len());
        let mut bytes = format!("{head}{body}").into_bytes();
        let sum: u32 = bytes.iter().map(|&b| b as u32).sum::<u32>() % 256;
        bytes.extend_from_slice(format!("10={sum:03}\x01").as_bytes());
        bytes
    }

    fn cap(dir: Direction, seq: u32, ts: u64, payload: &[u8]) -> Capture<'_> {
        Capture {
            timestamp_ns: ts,
            flow: FlowKey {
                client_ip: 1,
                client_port: 5,
            },
            server_port: 9898,
            tcp_seq: seq,
            payload_len: payload.len() as u32,
            direction: dir,
            transport: Transport::Fix,
            payload,
        }
    }

    #[test]
    fn request_then_two_responses_emit_two_events_sharing_t3() {
        let mut p = Pipeline::with_offset(0);
        let mut out = Vec::new();

        let req = fix("35=D|49=IICPC-BOT|56=CONTESTANT|34=1|11=o-1|38=12|40=2|44=42.5|");
        p.process(&cap(Direction::Request, 1, 100, &req), &mut out);
        assert!(out.is_empty());

        let ack = fix("35=8|49=CONTESTANT|56=IICPC-BOT|34=1|11=o-1|150=0|39=0|32=0|31=0|");
        let fill = fix("35=8|49=CONTESTANT|56=IICPC-BOT|34=2|11=o-1|150=2|39=2|32=12|31=42.5|");
        p.process(&cap(Direction::Response, 1, 200, &ack), &mut out);
        p.process(
            &cap(Direction::Response, 1 + ack.len() as u32, 350, &fill),
            &mut out,
        );

        assert_eq!(out.len(), 2);
        assert_eq!(out[0].t3_ns, 100);
        assert_eq!(out[1].t3_ns, 100);
        assert_eq!(out[1].fill_qty, 12);
    }

    #[test]
    fn two_pipelined_orders_match_per_clordid() {
        let mut p = Pipeline::with_offset(0);
        let mut out = Vec::new();
        let a = fix("35=D|49=IICPC-BOT|34=1|11=A|38=1|40=2|44=1.0|");
        let b = fix("35=D|49=IICPC-BOT|34=2|11=B|38=1|40=2|44=2.0|");
        // both requests coalesced into one segment
        let mut both = a.clone();
        both.extend_from_slice(&b);
        p.process(&cap(Direction::Request, 1, 100, &both), &mut out);

        let rb = fix("35=8|49=CONTESTANT|34=1|11=B|150=2|39=2|32=1|31=2.0|");
        let ra = fix("35=8|49=CONTESTANT|34=2|11=A|150=2|39=2|32=1|31=1.0|");
        p.process(&cap(Direction::Response, 1, 300, &rb), &mut out);
        p.process(
            &cap(Direction::Response, 1 + rb.len() as u32, 320, &ra),
            &mut out,
        );

        assert_eq!(out.len(), 2);
        let by_id = |id: &str| out.iter().find(|e| e.order_id == id).unwrap();
        assert_eq!(by_id("B").t3_ns, 100);
        assert_eq!(by_id("A").t3_ns, 100);
        assert_eq!(by_id("B").t7_ns, 300);
        assert_eq!(by_id("A").t7_ns, 320);
    }
}
