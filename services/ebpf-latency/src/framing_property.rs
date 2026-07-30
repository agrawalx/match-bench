//! Property tests for the byte-stream half of the capture: Reassembler + frame_fix.
//!
//! Two silent data-loss bugs have now shipped in this pair, and neither was visible to the
//! existing tests, because those feed well-formed whole messages — the one shape the real
//! world never guarantees. What the real world does is split a TCP stream at arbitrary
//! byte offsets, deliver segments out of order, retransmit them, and occasionally never
//! show one to the capture at all.
//!
//! The property is total and cheap to check:
//!
//!   Given a FIX byte stream and ANY segmentation of it, every message in the stream must
//!   be framed exactly once — except messages overlapping a segment that was never
//!   delivered, which may be lost, and which must not prevent the rest from being framed.
//!
//! That last clause is the one that matters: a hole must cost its own bytes, not the
//! remainder of the session. Both shipped bugs were violations of this property, and both
//! reproduce here in milliseconds rather than in a ten-minute cluster run.

use crate::capture::{Direction, Transport};
use crate::parse::{frame, Frame};
use crate::reassembly::Reassembler;

/// Deterministic LCG. Tests must fail identically on every run — a flaky property test
/// gets muted, and a muted test is worse than none.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: usize) -> usize {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % bound.max(1)
    }
}

/// A realistic FIX NewOrderSingle, including the fields whose byte patterns previously
/// confused the framer: tag 38 (contains "8=") and a rotating self-match-prevention id.
fn fix_message(seq: usize) -> Vec<u8> {
    let clordid = format!("sess_7_{seq}_O");
    let body = format!(
        "35=D\u{1}49=IICPC-BOT\u{1}56=CONTESTANT\u{1}34={seq}\u{1}11={clordid}\u{1}\
         54=1\u{1}38=100\u{1}44=1000\u{1}40=2\u{1}7928={:03}\u{1}",
        seq % 8
    );
    let head = format!("8=FIX.4.2\u{1}9={}\u{1}", body.len());
    let mut msg = format!("{head}{body}").into_bytes();
    let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
    msg.extend_from_slice(format!("10={:03}\u{1}", sum % 256).as_bytes());
    msg
}

/// Builds a stream of `n` messages and the ClOrdIDs it should yield.
fn build_stream(n: usize) -> (Vec<u8>, Vec<String>) {
    let mut bytes = Vec::new();
    let mut ids = Vec::new();
    for seq in 1..=n {
        bytes.extend_from_slice(&fix_message(seq));
        ids.push(format!("sess_7_{seq}_O"));
    }
    (bytes, ids)
}

/// Cuts a stream into (seq, data) segments of the given sizes, cycling through them.
fn segment(bytes: &[u8], sizes: &[usize]) -> Vec<(u32, Vec<u8>)> {
    let mut out = Vec::new();
    let mut off = 0usize;
    let mut i = 0usize;
    while off < bytes.len() {
        let take = sizes[i % sizes.len()].max(1).min(bytes.len() - off);
        out.push((off as u32, bytes[off..off + take].to_vec()));
        off += take;
        i += 1;
    }
    out
}

/// Feeds segments through the reassembler and frames everything available, returning the
/// ClOrdID of every message recovered.
///
/// `guard` bounds the framing loop: a stalled framer would otherwise spin forever, and a
/// hanging test reports as an infrastructure problem rather than as the defect it is.
fn replay(segments: &[(u32, Vec<u8>)]) -> Vec<String> {
    let mut re = Reassembler::new();
    let mut ids = Vec::new();
    for (seq, data) in segments {
        re.push(*seq, 1_000, data);
        let mut guard = 0;
        loop {
            guard += 1;
            assert!(guard < 100_000, "framing loop failed to terminate");
            match frame(Transport::Fix, Direction::Request, re.available()) {
                Frame::Message(n) => {
                    let msg = &re.available()[..n];
                    if let Some(id) = clordid_of(msg) {
                        ids.push(id);
                    }
                    re.consume(n);
                }
                Frame::Incomplete => break,
                Frame::Resync(skip) => re.consume(skip.max(1)),
            }
        }
    }
    ids
}

fn clordid_of(msg: &[u8]) -> Option<String> {
    msg.split(|&b| b == 0x01)
        .find(|f| f.starts_with(b"11="))
        .map(|f| String::from_utf8_lossy(&f[3..]).into_owned())
}

/// Every segmentation must recover every message. Segment sizes are swept across the
/// awkward range — smaller than a header, straddling BodyLength, straddling the checksum —
/// because the shipped bug fired only when a boundary landed within a couple of bytes of a
/// BeginString.
#[test]
fn every_segmentation_recovers_every_message() {
    let (bytes, expected) = build_stream(200);
    for size in [1usize, 2, 3, 5, 7, 11, 13, 64, 97, 128, 200, 512, 1448] {
        let segs = segment(&bytes, &[size]);
        let got = replay(&segs);
        assert_eq!(
            got, expected,
            "segment size {size}: recovered {} of {} messages",
            got.len(),
            expected.len()
        );
    }
}

/// Irregular segment sizes, deterministically randomised — the real pattern, where a
/// write's tail and the next write's head share a segment.
#[test]
fn randomised_segmentation_recovers_every_message() {
    let (bytes, expected) = build_stream(300);
    let mut rng = Lcg(0x5eed);
    for round in 0..25 {
        let sizes: Vec<usize> = (0..16).map(|_| 1 + rng.next(300)).collect();
        let segs = segment(&bytes, &sizes);
        let got = replay(&segs);
        assert_eq!(got, expected, "round {round} with sizes {sizes:?}");
    }
}

/// Out-of-order delivery: adjacent segments swapped. TCP reorders, and the reassembler
/// exists precisely to absorb that without losing anything.
#[test]
fn out_of_order_segments_recover_every_message() {
    let (bytes, expected) = build_stream(200);
    let mut segs = segment(&bytes, &[137]);
    // Interior pairs only. The reassembler initialises next_seq from the FIRST segment it
    // sees, so a reordered stream start makes it adopt the wrong origin and silently drop
    // the true beginning — a separate pre-existing defect, recorded in
    // reordering_at_stream_start_loses_the_head rather than conflated with this property.
    for i in (1..segs.len().saturating_sub(1)).step_by(2) {
        segs.swap(i, i + 1);
    }
    let got = replay(&segs);
    assert_eq!(got, expected, "recovered {} of {}", got.len(), expected.len());
}

/// Retransmissions: every segment delivered twice. Duplicates must not produce duplicate
/// messages — a doubled execution report would read downstream as an overfill.
#[test]
fn duplicate_segments_do_not_duplicate_messages() {
    let (bytes, expected) = build_stream(150);
    let segs = segment(&bytes, &[211]);
    let mut doubled = Vec::new();
    for s in &segs {
        doubled.push(s.clone());
        doubled.push(s.clone());
    }
    let got = replay(&doubled);
    assert_eq!(got, expected, "recovered {} of {}", got.len(), expected.len());
}

/// THE regression test: one segment is never delivered.
///
/// A hole must cost only the messages that overlap it. The capture never sees a
/// retransmission for a segment the kernel delivered to the application, so this hole is
/// permanent — and the reassembler advances `next_seq` only through contiguous data, so
/// without explicit gap recovery the stream stalls here forever. That stall is what turned
/// a 0.2% loss into 93.8% on a live run.
#[test]
fn a_permanently_missing_segment_costs_only_its_own_messages() {
    let (bytes, expected) = build_stream(200);
    let segs = segment(&bytes, &[300]);
    let drop_at = segs.len() / 2;
    let (hole_start, hole_len) = (segs[drop_at].0 as usize, segs[drop_at].1.len());

    let kept: Vec<(u32, Vec<u8>)> = segs
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != drop_at)
        .map(|(_, s)| s.clone())
        .collect();

    let got = replay(&kept);

    // Which messages genuinely overlap the hole? Only those may be missing.
    let mut off = 0usize;
    let mut unaffected = Vec::new();
    for (i, id) in expected.iter().enumerate() {
        let len = fix_message(i + 1).len();
        let overlaps = off < hole_start + hole_len && hole_start < off + len;
        if !overlaps {
            unaffected.push(id.clone());
        }
        off += len;
    }

    let recovered_after: Vec<&String> = got
        .iter()
        .filter(|id| unaffected.iter().any(|u| u == *id))
        .collect();
    assert_eq!(
        recovered_after.len(),
        unaffected.len(),
        "a single missing segment must not cost the rest of the stream: recovered {} of {} \
         unaffected messages (total recovered {})",
        recovered_after.len(),
        unaffected.len(),
        got.len()
    );
}

/// Documents the stream-start behaviour rather than asserting it is correct: the
/// reassembler adopts the first segment it sees as the stream origin, so if the very first
/// segments arrive out of order the head of the stream is lost. Harmless for a long-lived
/// FIX session (the capture attaches before the connection opens), but it is a real edge
/// and should not be discovered a third time by a production run.
#[test]
fn reordering_at_stream_start_loses_the_head() {
    let (bytes, expected) = build_stream(50);
    let mut segs = segment(&bytes, &[137]);
    segs.swap(0, 1);
    let got = replay(&segs);
    assert!(
        got.len() < expected.len(),
        "if this now recovers everything, the origin handling was fixed — update this test"
    );
    assert!(
        !got.is_empty(),
        "a reordered start must not cost the whole stream, only its head"
    );
}
