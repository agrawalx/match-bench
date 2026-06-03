//! Per-(flow, direction) TCP byte-stream reassembly.
//!
//! The kernel captures individual TCP segments. Because TCP is a byte stream,
//! one segment may carry several pipelined messages (coalescing) and one message
//! may be split across two segments (straddling). This reassembler restores the
//! contiguous byte stream so the framer/parser can extract whole messages, and it
//! attributes each stream offset to the timestamp of the segment that delivered
//! its bytes — so a message's t3/t7 is the stamp of the segment carrying its
//! FIRST byte even when it straddles.
//!
//! Sequence numbers are 32-bit and wrap; gaps are computed with wrapping signed
//! arithmetic, which is correct as long as a single jump is < 2 GiB (always true
//! for TCP). Absolute stream positions are tracked in 64-bit so timestamp
//! attribution is wrap-free within a session (< 4 GiB per direction).

use std::collections::BTreeMap;

/// Reset the buffer if a gap never fills and it grows past this (parser stuck or
/// lost capture). Bounds per-flow memory.
const MAX_BUFFERED: usize = 1 << 20; // 1 MiB
/// Drop the out-of-order hold if it grows past this many segments.
const MAX_HOLD_SEGMENTS: usize = 64;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PushStats {
    /// A segment arrived ahead of the next expected sequence (gap / reorder).
    pub reordered: bool,
    /// Bytes that duplicated already-delivered data (retransmission).
    pub retransmitted_bytes: usize,
    /// The buffer was reset because it exceeded its bound (data loss).
    pub reset: bool,
}

#[derive(Debug)]
pub struct Reassembler {
    initialized: bool,
    next_seq: u32,
    /// Absolute stream position of `buf[0]`.
    front_abs: u64,
    buf: Vec<u8>,
    /// (abs_start, timestamp_ns, seq) for each delivered segment, ascending by abs_start.
    marks: Vec<(u64, u64, u32)>,
    /// Out-of-order segments awaiting their predecessor, keyed by sequence.
    hold: BTreeMap<u32, (u64, Vec<u8>)>,
    last_activity_ns: u64,
}

impl Default for Reassembler {
    fn default() -> Self {
        Self::new()
    }
}

impl Reassembler {
    pub fn new() -> Self {
        Self {
            initialized: false,
            next_seq: 0,
            front_abs: 0,
            buf: Vec::new(),
            marks: Vec::new(),
            hold: BTreeMap::new(),
            last_activity_ns: 0,
        }
    }

    fn delivered_abs(&self) -> u64 {
        self.front_abs + self.buf.len() as u64
    }

    /// Append a contiguous segment (its first byte == `next_seq`) and record its
    /// timestamp mark.
    fn append_contiguous(&mut self, ts: u64, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let abs = self.delivered_abs();
        self.marks.push((abs, ts, self.next_seq));
        self.buf.extend_from_slice(data);
        self.next_seq = self.next_seq.wrapping_add(data.len() as u32);
    }

    /// Drain any held segments that are now contiguous OR overlap the front.
    fn drain_hold(&mut self) {
        // A held segment is deliverable when its start is at/before next_seq and
        // its end is strictly after it. The exact-match case (start == next_seq)
        // delivers the whole segment; the overlap case (start < next_seq < end) —
        // which happens when a later segment advanced next_seq into the MIDDLE of
        // an already-held segment — delivers only the undelivered tail. The old
        // code matched start == next_seq ONLY, so an overlapping held segment's
        // tail was never delivered and the contiguous stream stalled until a reset.
        loop {
            let key = self.hold.iter().find_map(|(&seq, (_, d))| {
                let end = seq.wrapping_add(d.len() as u32);
                if seq_le(seq, self.next_seq) && seq_lt(self.next_seq, end) {
                    Some(seq)
                } else {
                    None
                }
            });
            match key {
                Some(seq) => {
                    let (ts, data) = self.hold.remove(&seq).expect("key just found");
                    let skip = self.next_seq.wrapping_sub(seq) as usize; // already-delivered prefix
                    self.append_contiguous(ts, &data[skip..]);
                }
                None => break,
            }
        }
        // Discard stale held segments fully behind next_seq (already delivered).
        let next = self.next_seq;
        self.hold
            .retain(|&seq, (_, d)| seq_ge(seq.wrapping_add(d.len() as u32), next));
    }

    pub fn push(&mut self, seq: u32, ts: u64, data: &[u8]) -> PushStats {
        let mut stats = PushStats::default();
        if data.is_empty() {
            return stats;
        }
        self.last_activity_ns = self.last_activity_ns.max(ts);

        if !self.initialized {
            self.initialized = true;
            self.next_seq = seq;
            self.append_contiguous(ts, data);
            self.drain_hold();
            return stats;
        }

        // M31: if segments are currently held, the bytes this push makes contiguous
        // were delivered out of order. The push that FRAMES those bytes (the gap
        // filler) must report reordered=true — not just the earlier push that held a
        // segment and framed nothing. Propagate the hold state onto the delivering
        // push so the framed messages are correctly tagged reordering_detected.
        let had_hold = !self.hold.is_empty();
        let gap = seq.wrapping_sub(self.next_seq) as i32;
        if gap == 0 {
            self.append_contiguous(ts, data);
            self.drain_hold();
            if had_hold {
                stats.reordered = true;
            }
        } else if gap < 0 {
            // Overlaps already-delivered data (retransmission, possibly with new tail).
            let overlap = (-(gap as i64)) as usize;
            if overlap >= data.len() {
                stats.retransmitted_bytes = data.len();
            } else {
                stats.retransmitted_bytes = overlap;
                self.append_contiguous(ts, &data[overlap..]);
                self.drain_hold();
                if had_hold {
                    stats.reordered = true;
                }
            }
        } else {
            // Future segment — hold until the gap fills.
            stats.reordered = true;
            self.hold.insert(seq, (ts, data.to_vec()));
            if self.hold.len() > MAX_HOLD_SEGMENTS {
                self.hold.clear();
                stats.reset = true;
            }
        }

        if self.buf.len() > MAX_BUFFERED {
            self.reset_buffer();
            stats.reset = true;
        }
        stats
    }

    fn reset_buffer(&mut self) {
        // Keep `next_seq` so future contiguous data still lines up; drop buffered
        // bytes and marks that the parser failed to consume.
        self.front_abs = self.delivered_abs();
        self.buf.clear();
        self.marks.clear();
        self.hold.clear();
    }

    /// H14: a captured segment was TRUNCATED (its on-wire payload exceeded the
    /// capture cap — a GSO/TSO super-frame). Its missing tail is unrecoverable, and
    /// advancing `next_seq` by the short captured length would make every later
    /// segment look like a permanent forward gap and stall the flow. Drop all state
    /// and re-anchor at the NEXT segment instead: the truncated message is lost, but
    /// subsequent messages on the flow stay measurable. (Disabling GSO/TSO on the
    /// algo veth — see deployment — prevents truncation entirely.)
    pub fn reset_for_truncation(&mut self) {
        self.initialized = false;
        self.front_abs = 0;
        self.buf.clear();
        self.marks.clear();
        self.hold.clear();
    }

    /// Contiguous bytes available to the framer/parser, starting at the stream front.
    pub fn available(&self) -> &[u8] {
        &self.buf
    }

    /// Timestamp of the segment that delivered the byte at `offset` within
    /// `available()`. Used to stamp a message with the time its first byte arrived.
    pub fn timestamp_at(&self, offset: usize) -> u64 {
        let abs = self.front_abs + offset as u64;
        let mut ts = 0;
        for &(start, mark_ts, _) in &self.marks {
            if start <= abs {
                ts = mark_ts;
            } else {
                break;
            }
        }
        ts
    }

    /// TCP sequence number of the byte at `offset` within `available()` (the
    /// sequence of a message's first byte, for replay-ordering metadata).
    pub fn seq_at(&self, offset: usize) -> u32 {
        let abs = self.front_abs + offset as u64;
        let mut base: Option<(u64, u32)> = None;
        for &(start, _, seq) in &self.marks {
            if start <= abs {
                base = Some((start, seq));
            } else {
                break;
            }
        }
        match base {
            Some((start, seq)) => seq.wrapping_add((abs - start) as u32),
            None => 0,
        }
    }

    /// Drop the first `n` bytes after the parser framed and handled a message.
    pub fn consume(&mut self, n: usize) {
        let n = n.min(self.buf.len());
        if n == 0 {
            return;
        }
        self.buf.drain(0..n);
        self.front_abs += n as u64;
        // Keep only the mark covering the new front plus everything after it.
        while self.marks.len() >= 2 && self.marks[1].0 <= self.front_abs {
            self.marks.remove(0);
        }
    }

    /// Number of seconds (ns) since the most recent segment; for idle eviction.
    pub fn idle_ns(&self, now_ns: u64) -> u64 {
        now_ns.saturating_sub(self.last_activity_ns)
    }
}

/// Sequence-space `>=` (handles wrap): is `a` at or after `b`?
fn seq_ge(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) >= 0
}

/// Sequence-space `<=` (handles wrap).
fn seq_le(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) <= 0
}

/// Sequence-space `<` (handles wrap).
fn seq_lt(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_order_single_segment() {
        let mut r = Reassembler::new();
        r.push(1000, 50, b"hello");
        assert_eq!(r.available(), b"hello");
        assert_eq!(r.timestamp_at(0), 50);
        r.consume(5);
        assert_eq!(r.available(), b"");
    }

    #[test]
    fn coalesced_multiple_messages_in_one_segment() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAABBBCCC");
        assert_eq!(r.available(), b"AAABBBCCC");
        // all three messages came from the same segment → same timestamp
        assert_eq!(r.timestamp_at(0), 10);
        assert_eq!(r.timestamp_at(3), 10);
        assert_eq!(r.timestamp_at(6), 10);
        r.consume(3);
        assert_eq!(r.available(), b"BBBCCC");
        assert_eq!(r.timestamp_at(0), 10);
    }

    #[test]
    fn straddled_message_across_two_segments_keeps_first_byte_timestamp() {
        let mut r = Reassembler::new();
        // "MESSAGE" split: "MES" in segment @100, "SAGE" in next segment @200
        r.push(0, 100, b"MES");
        r.push(3, 200, b"SAGE");
        assert_eq!(r.available(), b"MESSAGE");
        // first byte came from the @100 segment
        assert_eq!(r.timestamp_at(0), 100);
        // a byte in the tail came from the @200 segment
        assert_eq!(r.timestamp_at(4), 200);
    }

    #[test]
    fn out_of_order_segments_are_reordered() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAA");
        let s = r.push(6, 30, b"CCC"); // arrives before BBB
        assert!(s.reordered);
        assert_eq!(r.available(), b"AAA"); // CCC held back
        r.push(3, 20, b"BBB");
        assert_eq!(r.available(), b"AAABBBCCC");
        assert_eq!(r.timestamp_at(0), 10);
        assert_eq!(r.timestamp_at(3), 20);
        assert_eq!(r.timestamp_at(6), 30);
    }

    // H10: a held out-of-order segment whose start later falls BEHIND next_seq but
    // whose tail extends beyond it (a gap-filling segment with a new tail arrives
    // overlapping the held one) must have its tail delivered — otherwise the stream
    // stalls forever and all subsequent messages on the flow are silently dropped.
    #[test]
    fn overlapping_held_segment_tail_is_drained() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAA"); // next_seq = 3
                               // Hold a future segment at seq 5 ("XYZZ", covers 5..9), leaving a gap 3..5.
        r.push(5, 30, b"XYZZ");
        assert_eq!(r.available(), b"AAA");
        // A segment fills 3..7 ("BBCC"), advancing next_seq to 7 — into the MIDDLE
        // of the held [5,9) segment. Its tail (bytes 7..9 = "ZZ") must still drain.
        r.push(3, 20, b"BBCC");
        assert_eq!(
            r.available(),
            b"AAABBCCZZ",
            "H10: overlapping held segment's tail must be delivered, not stranded"
        );
    }

    // M31: the push that DELIVERS previously-held (out-of-order) bytes must report
    // reordered=true, because it is the push from which those messages get framed.
    #[test]
    fn gap_fill_push_reports_reorder() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAA"); // next_seq = 3
        let held = r.push(6, 30, b"CCC"); // gap 3..6 -> held, frames nothing
        assert!(held.reordered, "the holding push flags reorder");
        let fill = r.push(3, 20, b"BBB"); // fills gap and drains CCC
        assert!(
            fill.reordered,
            "M31: the gap-filling push that delivers held bytes must flag reorder"
        );
        assert_eq!(r.available(), b"AAABBBCCC");
    }

    // H14: a truncated capture must reset the flow so it re-anchors at the next
    // segment instead of advancing next_seq by the short length and stalling.
    #[test]
    fn truncation_reset_resyncs_without_stall() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAA");
        r.reset_for_truncation(); // a GSO/TSO super-frame was truncated and dropped
        r.push(100, 20, b"BBB"); // far-ahead segment re-anchors cleanly (no gap stall)
        assert_eq!(r.available(), b"BBB");
        assert_eq!(r.timestamp_at(0), 20);
    }

    #[test]
    fn duplicate_retransmit_is_detected_and_ignored() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAA");
        let s = r.push(0, 11, b"AAA"); // full duplicate
        assert_eq!(s.retransmitted_bytes, 3);
        assert_eq!(r.available(), b"AAA");
    }

    #[test]
    fn partial_overlap_retransmit_appends_only_new_tail() {
        let mut r = Reassembler::new();
        r.push(0, 10, b"AAA");
        // resend last byte of AAA plus new data
        let s = r.push(2, 12, b"AXYZ");
        assert_eq!(s.retransmitted_bytes, 1);
        assert_eq!(r.available(), b"AAAXYZ");
    }
}
