//! Request→response matching by ClOrdID, with per-response emission.
//!
//! Modeled on the bot's `PendingOrder` map (`bot-fleet/src/worker.rs`): each
//! request inserts its `t3`; each response looks the order up by ClOrdID and
//! emits an event. Unlike the bot — and unlike the old kernel matcher — this does
//! NOT drop the entry on the first response, so every ExecutionReport for an
//! order (ACK, then partial fills, then fill) produces its own event sharing the
//! request's `t3`. This is the component responsible for emitting one event per
//! response. Entries are evicted after an idle period to bound memory.

use std::collections::HashMap;

/// Drop an in-flight order this long after its last activity (request or any
/// response). Mirrors the telemetry ingester's 5 s join window.
pub const DEFAULT_IDLE_NS: u64 = 5_000_000_000;
/// Hard cap on tracked orders; oldest are evicted past this.
const MAX_INFLIGHT: usize = 1_000_000;

#[derive(Debug, Clone)]
struct Inflight {
    t3_ns: u64,
    client_ip: u32,
    client_port: u16,
    tcp_seq: u32,
    retransmission_count: u32,
    reordering_detected: bool,
    last_activity_ns: u64,
}

/// One emitted measurement, ready to map onto `OrderAckedEvent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedEvent {
    pub order_id: String,
    pub src_ip: u32,
    pub src_port: u16,
    pub tcp_seq: u32,
    pub t3_ns: u64,
    pub t7_ns: u64,
    pub pod_service_time_ns: u64,
    pub exec_type: String,
    pub fill_qty: u64,
    pub fill_price: u64,
    pub orig_order_id: String,
    pub reordering_detected: bool,
    pub retransmission_count: u32,
}

#[derive(Debug, Default)]
pub struct Matcher {
    inflight: HashMap<String, Inflight>,
    /// Responses that arrived with no matching request (capture gap / out of order).
    pub unmatched_responses: u64,
}

impl Matcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a request (`t3`). Keeps the first `t3` and bumps the retransmission
    /// count if the same ClOrdID is sent again.
    pub fn on_request(
        &mut self,
        clordid: &str,
        t3_ns: u64,
        client_ip: u32,
        client_port: u16,
        tcp_seq: u32,
        reordered: bool,
    ) {
        if clordid.is_empty() {
            return;
        }
        match self.inflight.get_mut(clordid) {
            Some(existing) => {
                existing.retransmission_count = existing.retransmission_count.saturating_add(1);
                existing.reordering_detected |= reordered;
                existing.last_activity_ns = existing.last_activity_ns.max(t3_ns);
            }
            None => {
                if self.inflight.len() >= MAX_INFLIGHT {
                    self.evict_oldest();
                }
                self.inflight.insert(
                    clordid.to_string(),
                    Inflight {
                        t3_ns,
                        client_ip,
                        client_port,
                        tcp_seq,
                        retransmission_count: 0,
                        reordering_detected: reordered,
                        last_activity_ns: t3_ns,
                    },
                );
            }
        }
    }

    /// Record a response (`t7`) and emit an event if its request was seen. The
    /// entry is retained so later responses for the same order also emit.
    #[allow(clippy::too_many_arguments)]
    pub fn on_response(
        &mut self,
        clordid: &str,
        t7_ns: u64,
        exec_type: &str,
        fill_qty: u64,
        fill_price: u64,
        orig_order_id: &str,
        reordered: bool,
    ) -> Option<MatchedEvent> {
        let Some(inflight) = self.inflight.get_mut(clordid) else {
            self.unmatched_responses = self.unmatched_responses.saturating_add(1);
            return None;
        };
        inflight.reordering_detected |= reordered;
        inflight.last_activity_ns = inflight.last_activity_ns.max(t7_ns);
        let pod_service_time_ns = t7_ns.saturating_sub(inflight.t3_ns);
        Some(MatchedEvent {
            order_id: clordid.to_string(),
            src_ip: inflight.client_ip,
            src_port: inflight.client_port,
            tcp_seq: inflight.tcp_seq,
            t3_ns: inflight.t3_ns,
            t7_ns,
            pod_service_time_ns,
            exec_type: exec_type.to_string(),
            fill_qty,
            fill_price,
            orig_order_id: orig_order_id.to_string(),
            reordering_detected: inflight.reordering_detected,
            retransmission_count: inflight.retransmission_count,
        })
    }

    /// Remove orders idle for `>= idle_ns` as of `now_ns`. Returns evicted count.
    pub fn evict_idle(&mut self, now_ns: u64, idle_ns: u64) -> usize {
        let before = self.inflight.len();
        self.inflight
            .retain(|_, v| now_ns.saturating_sub(v.last_activity_ns) < idle_ns);
        before - self.inflight.len()
    }

    fn evict_oldest(&mut self) {
        if let Some(key) = self
            .inflight
            .iter()
            .min_by_key(|(_, v)| v.last_activity_ns)
            .map(|(k, _)| k.clone())
        {
            self.inflight.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_responses_per_order_emit_two_events_sharing_t3() {
        let mut m = Matcher::new();
        m.on_request("o1", 100, 0x0a00_0001, 50000, 1, false);

        let ack = m.on_response("o1", 200, "0", 0, 0, "", false).unwrap();
        let fill = m
            .on_response("o1", 350, "2", 12, 42_500_000_000, "", false)
            .unwrap();

        assert_eq!(ack.t3_ns, 100);
        assert_eq!(fill.t3_ns, 100); // same t3
        assert_eq!(ack.t7_ns, 200);
        assert_eq!(fill.t7_ns, 350);
        assert_eq!(ack.exec_type, "0");
        assert_eq!(fill.exec_type, "2");
        assert_eq!(ack.pod_service_time_ns, 100);
        assert_eq!(fill.pod_service_time_ns, 250);
        assert_eq!(fill.fill_qty, 12);
    }

    #[test]
    fn per_clordid_isolation_across_pipelined_orders() {
        // The EBPF-10 case: two orders on one flow, responses matched per-ClOrdID.
        let mut m = Matcher::new();
        m.on_request("A", 100, 1, 5, 10, false);
        m.on_request("B", 130, 1, 5, 20, false); // same flow, later request

        let rb = m.on_response("B", 300, "2", 1, 0, "", false).unwrap();
        let ra = m.on_response("A", 320, "2", 1, 0, "", false).unwrap();

        assert_eq!(rb.t3_ns, 130); // B's own t3, not A's
        assert_eq!(ra.t3_ns, 100); // A's own t3
        assert_eq!(rb.pod_service_time_ns, 170);
        assert_eq!(ra.pod_service_time_ns, 220);
    }

    #[test]
    fn response_without_request_is_unmatched() {
        let mut m = Matcher::new();
        assert!(m.on_response("ghost", 200, "0", 0, 0, "", false).is_none());
        assert_eq!(m.unmatched_responses, 1);
    }

    #[test]
    fn duplicate_request_bumps_retransmission_and_keeps_first_t3() {
        let mut m = Matcher::new();
        m.on_request("o1", 100, 1, 5, 10, false);
        m.on_request("o1", 175, 1, 5, 10, false); // retransmit
        let e = m.on_response("o1", 200, "0", 0, 0, "", false).unwrap();
        assert_eq!(e.t3_ns, 100);
        assert_eq!(e.retransmission_count, 1);
    }

    #[test]
    fn idle_orders_are_evicted() {
        let mut m = Matcher::new();
        m.on_request("old", 100, 1, 5, 10, false);
        m.on_request("new", 9_000_000_000, 1, 5, 11, false);
        let evicted = m.evict_idle(10_000_000_000, DEFAULT_IDLE_NS);
        assert_eq!(evicted, 1);
        assert!(m
            .on_response("old", 10_000_000_100, "0", 0, 0, "", false)
            .is_none());
        assert!(m
            .on_response("new", 10_000_000_100, "0", 0, 0, "", false)
            .is_some());
    }
}
