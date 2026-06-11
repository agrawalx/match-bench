//! This module implements join behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::collections::HashMap;

#[derive(Default)]
/// FirstResponseTracker stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct FirstResponseTracker {
    seen: HashMap<String, u64>, // order_id -> last activity ns (for idle eviction)
}

impl FirstResponseTracker {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn new() -> Self {
        Self::default()
    }

    /// observe performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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

    /// evict_idle performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn evict_idle(&mut self, now_ns: u64, idle_ns: u64) -> usize {
        let before = self.seen.len();
        self.seen
            .retain(|_, &mut last| now_ns.saturating_sub(last) < idle_ns);
        before - self.seen.len()
    }

    /// len performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// is_empty performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// first_response_is_true_then_false performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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
    /// idle_orders_are_evicted_and_can_be_seen_again performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn idle_orders_are_evicted_and_can_be_seen_again() {
        let mut t = FirstResponseTracker::new();
        t.observe("old", 100);
        t.observe("fresh", 9_000_000_000);
        let evicted = t.evict_idle(10_000_000_000, 5_000_000_000);
        assert_eq!(evicted, 1, "only the idle order is evicted");
        assert_eq!(t.len(), 1);
        assert!(t.observe("old", 10_000_000_100));
        assert!(!t.observe("fresh", 10_000_000_100));
    }

    #[test]
    /// observe_keeps_max_last_activity performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn observe_keeps_max_last_activity() {
        let mut t = FirstResponseTracker::new();
        t.observe("o1", 500);
        t.observe("o1", 100); // out-of-order/older timestamp must not lower last-seen
        assert_eq!(t.evict_idle(750, 300), 0);
    }
}
