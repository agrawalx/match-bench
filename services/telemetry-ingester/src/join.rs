//! First-response dedup for the scored service-time metric.
//!
//! `orders.acked` delivers SEVERAL events per order (the ACK, then each partial
//! fill, then the final fill), all sharing the request's `t3`. The leaderboard's
//! scored latency is the **first** response's `t7 - t3`, so each order must
//! contribute exactly one service-time sample even though we see N events for it.
//! This tracker records which order_ids have already contributed, and evicts
//! entries after an idle window so memory is bounded by in-flight orders rather
//! than the whole session.
//!
//! (The aggregate metrics otherwise decompose per stream — `pod_service_time_ns`
//! and `exec_type` come straight off each acked event, and `timed_out` /
//! `r9 - t0` / `t1 - t0` come straight off each sent event — so no full
//! cross-stream join buffer is needed.)

use std::collections::HashMap;

#[derive(Default)]
pub struct FirstResponseTracker {
    seen: HashMap<String, u64>, // order_id -> last activity ns (for idle eviction)
}

impl FirstResponseTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an order's response. Returns `true` only for the FIRST response of
    /// an order (the one whose latency is the scored service time); `false` for
    /// later responses (fills) of an already-seen order.
    pub fn observe(&mut self, order_id: &str, now_ns: u64) -> bool {
        match self.seen.get_mut(order_id) {
            Some(last) => {
                *last = (*last).max(now_ns);
                false
            }
            None => {
                self.seen.insert(order_id.to_string(), now_ns);
                true
            }
        }
    }

    /// Drop orders idle for `>= idle_ns` as of `now_ns`. Returns evicted count.
    pub fn evict_idle(&mut self, now_ns: u64, idle_ns: u64) -> usize {
        let before = self.seen.len();
        self.seen
            .retain(|_, &mut last| now_ns.saturating_sub(last) < idle_ns);
        before - self.seen.len()
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_response_is_true_then_false() {
        let mut t = FirstResponseTracker::new();
        assert!(
            t.observe("o1", 100),
            "first response must be the scored sample"
        );
        assert!(
            !t.observe("o1", 150),
            "a later fill must not be scored again"
        );
        assert!(!t.observe("o1", 200));
        assert!(
            t.observe("o2", 100),
            "a different order is its own first response"
        );
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn idle_orders_are_evicted_and_can_be_seen_again() {
        let mut t = FirstResponseTracker::new();
        t.observe("old", 100);
        t.observe("fresh", 9_000_000_000);
        let evicted = t.evict_idle(10_000_000_000, 5_000_000_000);
        assert_eq!(evicted, 1, "only the idle order is evicted");
        assert_eq!(t.len(), 1);
        // 'old' was evicted, so a late straggler is treated as a fresh first response
        assert!(t.observe("old", 10_000_000_100));
        // 'fresh' is still tracked
        assert!(!t.observe("fresh", 10_000_000_100));
    }

    #[test]
    fn observe_keeps_max_last_activity() {
        let mut t = FirstResponseTracker::new();
        t.observe("o1", 500);
        t.observe("o1", 100); // out-of-order/older timestamp must not lower last-seen
                              // idle window of 300 as of 750: last activity is 500, idle=250 < 300 -> kept
        assert_eq!(t.evict_idle(750, 300), 0);
    }
}
