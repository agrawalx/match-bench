//! Per-`(session, wave)` metric aggregation.
//!
//! Two independent streams feed the same windows:
//!   - `orders.acked` (algo side): the scored `service_time = t7 - t3` (already
//!     computed as `pod_service_time_ns`), recorded ONCE per order (first
//!     response, via the dedup tracker); plus fill latency, accept/reject counts.
//!   - `orders.sent` (bot side): `response_time = r9 - t0`, `schedule_slip =
//!     t1 - t0`, offered count, and timeouts (`timed_out`).
//!
//! `wave_index = floor((t - session_start) / wave_ns)` where `session_start` is
//! the earliest timestamp seen for the session — a pure time bucket (the scoring
//! service interprets it as ramp waves and ignores it for constant/spike).
//! Service-time percentiles are cumulative per wave (rolling); tps and error_rate
//! are per snapshot interval.

use std::collections::HashMap;

use hdrhistogram::serialization::{Serializer, V2DeflateSerializer};
use hdrhistogram::Histogram;
use iicpc_schemas_rust::{OrderAckedEvent, OrderSentEvent};

use crate::join::FirstResponseTracker;

pub const DEFAULT_WAVE_NS: u64 = 20_000_000_000; // 20 s ramp wave
const HDR_MAX_NS: u64 = 60_000_000_000; // 60 s upper bound
const HDR_SIGFIG: u8 = 3;
/// Evict a first-response entry / a window after this much idle. The first-
/// response window matches the eBPF reader's 5 s eviction; windows live longer so
/// a whole wave's cumulative histogram survives a quiet second.
pub const FIRST_RESP_IDLE_NS: u64 = 5_000_000_000;
pub const WINDOW_IDLE_NS: u64 = 30_000_000_000;

/// One row to be written to TimescaleDB + Redis on a snapshot tick.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub time_ns: u64,
    pub session_id: String,
    pub contestant_id: String,
    pub wave_index: u32,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub tps_1s: f64,
    pub error_rate: f64,
    /// V2-deflate-serialized cumulative service-time histogram (offline analysis).
    pub hdr_encoded: Vec<u8>,
}

struct Window {
    contestant_id: String,
    service_time: Histogram<u64>,
    fill_latency: Histogram<u64>,
    response_time: Histogram<u64>,
    schedule_slip: Histogram<u64>,
    last_update_ns: u64,
    // Reset every snapshot interval:
    offered: u64,
    responded: u64,
    accepted: u64,
    rejected: u64,
    timed_out: u64,
    fills: u64,
}

impl Window {
    fn new(contestant_id: String, now_ns: u64) -> Self {
        Self {
            contestant_id,
            service_time: new_hist(),
            fill_latency: new_hist(),
            response_time: new_hist(),
            schedule_slip: new_hist(),
            last_update_ns: now_ns,
            offered: 0,
            responded: 0,
            accepted: 0,
            rejected: 0,
            timed_out: 0,
            fills: 0,
        }
    }

    fn interval_active(&self) -> bool {
        self.offered > 0 || self.responded > 0 || self.fills > 0
    }
}

fn new_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, HDR_MAX_NS, HDR_SIGFIG)
        .expect("valid HDR bounds (1..=60s, 3 sig figs)")
}

/// HDR's lower bound is 1; a genuine 0 ns measurement is clamped to 1.
fn record(hist: &mut Histogram<u64>, value_ns: u64) {
    let _ = hist.record(value_ns.max(1));
}

/// FIX ExecType (tag 150) "8" is Rejected.
fn is_reject(exec_type: &str) -> bool {
    exec_type == "8"
}

/// FIX ExecType: "1" PartialFill, "2" Filled, "F" Trade (4.4). A fill must carry qty.
fn is_fill(exec_type: &str, fill_qty: u64) -> bool {
    fill_qty > 0 && matches!(exec_type, "1" | "2" | "F")
}

pub struct Aggregator {
    windows: HashMap<(String, u32), Window>,
    session_start: HashMap<String, u64>,
    session_contestant: HashMap<String, String>,
    first_response: FirstResponseTracker,
    wave_ns: u64,
    last_evicted: usize,
}

impl Aggregator {
    pub fn new(wave_ns: u64) -> Self {
        Self {
            windows: HashMap::new(),
            session_start: HashMap::new(),
            session_contestant: HashMap::new(),
            first_response: FirstResponseTracker::new(),
            wave_ns: wave_ns.max(1),
            last_evicted: 0,
        }
    }

    fn wave_of(&mut self, session_id: &str, t_ns: u64) -> u32 {
        let start = self
            .session_start
            .entry(session_id.to_string())
            .or_insert(t_ns);
        if t_ns < *start {
            *start = t_ns;
        }
        ((t_ns.saturating_sub(*start)) / self.wave_ns) as u32
    }

    pub fn observe_sent(&mut self, e: &OrderSentEvent) {
        let t0 = e.target_send_ts_ns;
        let t1 = e.send_ts_ns;
        let r9 = e.recv_done_ts_ns;
        let wave = self.wave_of(&e.session_id, t0);
        let contestant = self
            .session_contestant
            .get(&e.session_id)
            .cloned()
            .unwrap_or_default();
        let w = self
            .windows
            .entry((e.session_id.clone(), wave))
            .or_insert_with(|| Window::new(contestant, t1.max(t0)));

        w.offered += 1;
        if e.timed_out {
            w.timed_out += 1;
        }
        if t1 >= t0 {
            record(&mut w.schedule_slip, t1 - t0);
        }
        if !e.timed_out && r9 > t0 {
            record(&mut w.response_time, r9 - t0);
        }
        w.last_update_ns = w.last_update_ns.max(t1.max(t0));
    }

    pub fn observe_acked(&mut self, e: &OrderAckedEvent) {
        let t3 = e.t3_xdp_ingress_ns;
        self.session_contestant
            .insert(e.session_id.clone(), e.contestant_id.clone());
        let wave = self.wave_of(&e.session_id, t3);
        let w = self
            .windows
            .entry((e.session_id.clone(), wave))
            .or_insert_with(|| Window::new(e.contestant_id.clone(), e.t7_xdp_egress_ns));
        if w.contestant_id.is_empty() {
            w.contestant_id = e.contestant_id.clone();
        }

        // First response of an order = the scored service-time sample. Track idle
        // by the response's ARRIVAL time (t7), not the fixed request t3 — otherwise
        // the idle clock never advances across an order's stream of fills, so a
        // resting order whose fills span >5 s gets evicted mid-stream and its next
        // fill is re-scored as a fresh first response, corrupting service_time/tps.
        if self.first_response.observe(&e.order_id, e.t7_xdp_egress_ns) {
            record(&mut w.service_time, e.pod_service_time_ns);
            w.responded += 1;
            if is_reject(&e.exec_type) {
                w.rejected += 1;
            } else {
                w.accepted += 1;
            }
        }
        if is_fill(&e.exec_type, e.fill_qty) {
            w.fills += 1;
            record(&mut w.fill_latency, e.pod_service_time_ns);
        }
        w.last_update_ns = w.last_update_ns.max(e.t7_xdp_egress_ns);
    }

    /// Produce a snapshot per active window, reset per-interval counters, and
    /// evict idle windows + first-response entries. `interval_secs` is the wall
    /// time since the previous snapshot (≈ 1.0).
    pub fn snapshot(&mut self, now_ns: u64, interval_secs: f64) -> Vec<Snapshot> {
        let interval = if interval_secs > 0.0 {
            interval_secs
        } else {
            1.0
        };
        let mut out = Vec::new();
        for ((session_id, wave), w) in self.windows.iter_mut() {
            if !w.interval_active() {
                continue;
            }
            // M34: backfill contestant_id for a sent-first window from the session
            // map (observe_sent defaults it to "" until the first ack arrives).
            if w.contestant_id.is_empty() {
                if let Some(c) = self.session_contestant.get(session_id) {
                    w.contestant_id = c.clone();
                }
            }
            // M33/M34: only emit a latency row once a scored service-time sample
            // exists AND the contestant is known. A sent-only window (e.g. a wave
            // whose orders all timed out) has an empty histogram, so emitting it
            // would write misleading 0-latency, unattributable rows that drag down
            // the metrics_10s p99 average. Counters still reset below regardless.
            if !w.service_time.is_empty() && !w.contestant_id.is_empty() {
                let error_rate = if w.offered > 0 {
                    (w.timed_out + w.rejected) as f64 / w.offered as f64
                } else {
                    0.0
                };
                out.push(Snapshot {
                    time_ns: now_ns,
                    session_id: session_id.clone(),
                    contestant_id: w.contestant_id.clone(),
                    wave_index: *wave,
                    p50_ns: w.service_time.value_at_quantile(0.50),
                    p90_ns: w.service_time.value_at_quantile(0.90),
                    p99_ns: w.service_time.value_at_quantile(0.99),
                    p999_ns: w.service_time.value_at_quantile(0.999),
                    tps_1s: w.responded as f64 / interval,
                    error_rate,
                    hdr_encoded: serialize_hist(&w.service_time),
                });
            }
            w.offered = 0;
            w.responded = 0;
            w.accepted = 0;
            w.rejected = 0;
            w.timed_out = 0;
            w.fills = 0;
        }

        self.windows
            .retain(|_, w| now_ns.saturating_sub(w.last_update_ns) < WINDOW_IDLE_NS);
        // M29: prune per-session state once no window for that session remains, so
        // session_start/session_contestant grow with IN-FLIGHT sessions rather than
        // every session ever seen (this is a single, long-lived replica).
        let live: std::collections::HashSet<&str> =
            self.windows.keys().map(|(s, _)| s.as_str()).collect();
        self.session_start.retain(|s, _| live.contains(s.as_str()));
        self.session_contestant
            .retain(|s, _| live.contains(s.as_str()));
        self.last_evicted = self.first_response.evict_idle(now_ns, FIRST_RESP_IDLE_NS);
        out
    }

    pub fn window_count(&self) -> usize {
        self.windows.len()
    }

    /// Current first-response join-buffer size (in-flight tracked orders).
    pub fn join_buffer_size(&self) -> usize {
        self.first_response.len()
    }

    /// First-response entries evicted by the most recent `snapshot` call (idle
    /// in-flight orders that never completed a scored sample).
    pub fn last_evicted(&self) -> usize {
        self.last_evicted
    }
}

fn serialize_hist(hist: &Histogram<u64>) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = V2DeflateSerializer::new().serialize(hist, &mut buf);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use iicpc_schemas_rust::{OrdType, PayloadType, Side};

    fn acked(
        session: &str,
        order: &str,
        t3: u64,
        t7: u64,
        exec: &str,
        fill_qty: u64,
    ) -> OrderAckedEvent {
        OrderAckedEvent {
            session_id: session.into(),
            contestant_id: "c-1".into(),
            order_id: order.into(),
            src_ip: 1,
            src_port: 5,
            tcp_seq: 1,
            t3_xdp_ingress_ns: t3,
            t7_xdp_egress_ns: t7,
            pod_service_time_ns: t7.saturating_sub(t3),
            exec_type: exec.into(),
            fill_qty,
            fill_price: 0,
            orig_order_id: String::new(),
            reordering_detected: false,
            retransmission_count: 0,
        }
    }

    fn sent(
        session: &str,
        order: &str,
        t0: u64,
        t1: u64,
        r9: u64,
        timed_out: bool,
    ) -> OrderSentEvent {
        OrderSentEvent {
            session_id: session.into(),
            submission_id: "s-1".into(),
            worker_id: "w-1".into(),
            task_id: 0,
            order_id: order.into(),
            target_send_ts_ns: t0,
            send_ts_ns: t1,
            recv_done_ts_ns: r9,
            timed_out,
            price: 100,
            qty: 10,
            side: Side::Buy,
            payload_type: PayloadType::New,
            ord_type: OrdType::Limit,
            orig_order_id: String::new(),
        }
    }

    #[test]
    fn service_time_recorded_once_per_order_first_response() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        // ACK at +100us, then a fill at +200us — only the ACK is the scored sample.
        a.observe_acked(&acked("S", "o1", 1_000, 101_000, "0", 0));
        a.observe_acked(&acked("S", "o1", 1_000, 201_000, "2", 12));
        let snaps = a.snapshot(1_000_000_000, 1.0);
        assert_eq!(snaps.len(), 1);
        let s = &snaps[0];
        // one service-time sample of 100_000 ns
        assert_eq!(s.p50_ns, hist_q(100_000));
        assert_eq!(s.tps_1s, 1.0, "one order responded");
        assert_eq!(s.contestant_id, "c-1");
        assert!(!s.hdr_encoded.is_empty());
    }

    // H11: a resting order's fills stream in over several seconds. The idle clock
    // must track each fill's arrival (t7), so the order stays deduped and a later
    // fill is NOT re-scored as a fresh first response. With the old t3-based clock
    // the order evicted mid-stream and the trailing fill re-counted as a response.
    #[test]
    fn late_streaming_fill_not_rescored() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_acked(&acked("S", "o1", 0, 1_000_000_000, "0", 0)); // ACK -> scored
        let _ = a.snapshot(1_500_000_000, 1.0); // o1 idle 0.5s -> kept
        a.observe_acked(&acked("S", "o1", 0, 4_000_000_000, "1", 5)); // fill1 advances idle clock
        let _ = a.snapshot(6_000_000_000, 1.0); // t7-based idle 2s -> kept (t3-based would evict)
        a.observe_acked(&acked("S", "o1", 0, 6_500_000_000, "2", 5)); // fill2: known order, not re-scored
        let snaps = a.snapshot(7_000_000_000, 1.0);
        let s = snaps
            .iter()
            .find(|s| s.session_id == "S")
            .expect("window snapshot present");
        assert_eq!(
            s.tps_1s, 0.0,
            "H11: a trailing fill of an already-scored order must not count as a new response"
        );
    }

    // M33/M34: a window that only ever saw sent events (a wave whose orders all
    // timed out) has an empty service-time histogram and no contestant, so it must
    // emit NO row rather than a misleading 0-latency, unattributable one.
    #[test]
    fn sent_only_window_emits_no_row() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_sent(&sent("S2", "o1", 0, 10, 0, true)); // timed out, never acked
        let snaps = a.snapshot(1_000_000_000, 1.0);
        assert!(
            snaps.iter().all(|s| s.session_id != "S2"),
            "M33: sent-only window must not emit a latency row"
        );
    }

    // M29: per-session maps must be pruned once a session's windows evict, so they
    // grow with in-flight sessions rather than every session ever seen.
    #[test]
    fn session_state_pruned_after_window_eviction() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_acked(&acked("S3", "o1", 0, 1_000, "0", 0));
        assert_eq!(a.session_start.len(), 1);
        // Advance past WINDOW_IDLE_NS so the window (and its session state) evict.
        let _ = a.snapshot(WINDOW_IDLE_NS + 2_000_000_000, 1.0);
        assert_eq!(a.window_count(), 0, "window evicted");
        assert_eq!(a.session_start.len(), 0, "M29: session_start pruned");
        assert_eq!(
            a.session_contestant.len(),
            0,
            "M29: session_contestant pruned"
        );
    }

    #[test]
    fn wave_bucketing_by_elapsed_since_session_start() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS); // 20s
        let start = 1_000_000_000;
        a.observe_acked(&acked("S", "o1", start, start + 1_000, "0", 0)); // wave 0
        a.observe_acked(&acked(
            "S",
            "o2",
            start + 19_000_000_000,
            start + 19_000_001_000,
            "0",
            0,
        )); // wave 0
        a.observe_acked(&acked(
            "S",
            "o3",
            start + 21_000_000_000,
            start + 21_000_001_000,
            "0",
            0,
        )); // wave 1
        let mut snaps = a.snapshot(start + 30_000_000_000, 1.0);
        snaps.sort_by_key(|s| s.wave_index);
        assert_eq!(snaps.len(), 2);
        assert_eq!(snaps[0].wave_index, 0);
        assert_eq!(snaps[0].tps_1s, 2.0); // o1 + o2
        assert_eq!(snaps[1].wave_index, 1);
        assert_eq!(snaps[1].tps_1s, 1.0); // o3
    }

    #[test]
    fn error_rate_from_timeouts_and_rejects() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        // 4 offered: 1 timed out (sent), 1 rejected (acked), 2 accepted (acked)
        a.observe_sent(&sent("S", "o1", 1000, 1100, 0, true)); // timeout
        a.observe_sent(&sent("S", "o2", 1000, 1100, 5000, false));
        a.observe_sent(&sent("S", "o3", 1000, 1100, 5000, false));
        a.observe_sent(&sent("S", "o4", 1000, 1100, 5000, false));
        a.observe_acked(&acked("S", "o2", 1000, 2000, "8", 0)); // reject
        a.observe_acked(&acked("S", "o3", 1000, 2000, "0", 0)); // accept
        a.observe_acked(&acked("S", "o4", 1000, 2000, "0", 0)); // accept
        let snaps = a.snapshot(1_000_000_000, 1.0);
        assert_eq!(snaps.len(), 1);
        // error_rate = (1 timeout + 1 reject) / 4 offered = 0.5
        assert!((snaps[0].error_rate - 0.5).abs() < 1e-9);
    }

    #[test]
    fn percentiles_are_cumulative_across_snapshots_counters_reset() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_acked(&acked("S", "o1", 0, 100_000, "0", 0));
        let s1 = a.snapshot(1_000_000_000, 1.0);
        assert_eq!(s1[0].tps_1s, 1.0);
        let p99_after_one = s1[0].p99_ns;
        // second interval: another sample; tps resets to per-interval, histogram stays cumulative
        a.observe_acked(&acked("S", "o2", 0, 100_000, "0", 0));
        let s2 = a.snapshot(2_000_000_000, 1.0);
        assert_eq!(
            s2[0].tps_1s, 1.0,
            "tps counts only this interval's responses"
        );
        assert_eq!(
            s2[0].p99_ns, p99_after_one,
            "histogram is cumulative — same value"
        );
        assert_eq!(s2[0].p50_ns, p99_after_one);
    }

    #[test]
    fn idle_windows_are_evicted() {
        let mut a = Aggregator::new(DEFAULT_WAVE_NS);
        a.observe_acked(&acked("S", "o1", 1000, 2000, "0", 0));
        a.snapshot(3000, 1.0);
        assert_eq!(a.window_count(), 1);
        // far in the future, no activity -> window evicted on the next snapshot
        a.snapshot(3000 + WINDOW_IDLE_NS + 1, 1.0);
        assert_eq!(a.window_count(), 0);
    }

    // The exact bucket value HDR returns for a recorded latency (3 sig figs).
    fn hist_q(v: u64) -> u64 {
        let mut h = new_hist();
        let _ = h.record(v);
        h.value_at_quantile(0.5)
    }
}
