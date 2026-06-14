//! This module implements content behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::collections::VecDeque;

use iicpc_schemas_rust::{BotProfile, PayloadType, Side};
use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::fix;
use crate::worker::order_shape;

const MAX_RESTING_ORDERS: usize = 1024;

#[derive(Debug, Clone, Copy)]
/// OrderMix stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct OrderMix {
    pub market_pct: u8,
    pub cancel_pct: u8,
    pub replace_pct: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Kind enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
enum Kind {
    Limit,
    Market,
    Cancel,
    Replace,
}

#[derive(Debug, Clone)]
/// Resting stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct Resting {
    order_id: String,
    price: u64,
    qty: u64,
    side: Side,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Action enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum Action {
    NewLimit {
        seq: u32,
        price: u64,
        qty: u64,
        side: Side,
    },
    NewMarket {
        seq: u32,
        qty: u64,
        side: Side,
    },
    Cancel {
        seq: u32,
        orig_order_id: String,
        price: u64,
        qty: u64,
        side: Side,
    },
    Replace {
        seq: u32,
        orig_order_id: String,
        price: u64,
        qty: u64,
        side: Side,
    },
}

impl Action {
    /// seq performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn seq(&self) -> u32 {
        match self {
            Action::NewLimit { seq, .. }
            | Action::NewMarket { seq, .. }
            | Action::Cancel { seq, .. }
            | Action::Replace { seq, .. } => *seq,
        }
    }

    /// payload_type performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn payload_type(&self) -> PayloadType {
        match self {
            Action::NewLimit { .. } | Action::NewMarket { .. } => PayloadType::New,
            Action::Cancel { .. } => PayloadType::Cancel,
            Action::Replace { .. } => PayloadType::Replace,
        }
    }
}

/// TaskGenerator stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct TaskGenerator {
    rng: SmallRng,
    profile: BotProfile,
    mix: OrderMix,
    session_id: String,
    task_id: u64,
    seq: u32,
    // Bounded ring of recently-rested orders, used to pick targets for cancel/replace.
    // VecDeque so the bound is enforced with O(1) pop_front instead of Vec::remove(0),
    // which memmoves the whole ledger on every push once full — at high order rates with
    // cancel_pct=0 that memmove dominated CPU (~87% in profiling).
    ledger: VecDeque<Resting>,
}

impl TaskGenerator {
    /// new performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn new(
        session_id: String,
        task_id: u64,
        profile: BotProfile,
        mix: OrderMix,
        seed: u64,
    ) -> Self {
        Self {
            rng: SmallRng::seed_from_u64(seed),
            profile,
            mix,
            session_id,
            task_id,
            seq: 0,
            ledger: VecDeque::with_capacity(MAX_RESTING_ORDERS),
        }
    }

    /// next performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub fn next(&mut self) -> Action {
        self.seq += 1;
        let seq = self.seq;

        let roll = self.rng.gen_range(0u8..100);
        let kind = self.classify(roll);
        let (price, qty, side) = order_shape(self.profile, seq, &mut self.rng);

        match kind {
            Kind::Limit => self.emit_limit(seq, price, qty, side),
            Kind::Market => Action::NewMarket { seq, qty, side },
            Kind::Cancel => match self.take_resting() {
                Some(orig) => Action::Cancel {
                    seq,
                    orig_order_id: orig.order_id,
                    price: orig.price,
                    qty: orig.qty,
                    side: orig.side,
                },
                None => self.emit_limit(seq, price, qty, side),
            },
            Kind::Replace => match self.take_resting() {
                Some(orig) => self.emit_replace(seq, orig, price),
                None => self.emit_limit(seq, price, qty, side),
            },
        }
    }

    /// classify performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn classify(&self, roll: u8) -> Kind {
        let market = self.mix.market_pct;
        let cancel_end = market.saturating_add(self.mix.cancel_pct);
        let replace_end = cancel_end.saturating_add(self.mix.replace_pct);
        if roll < market {
            Kind::Market
        } else if roll < cancel_end {
            Kind::Cancel
        } else if roll < replace_end {
            Kind::Replace
        } else {
            Kind::Limit
        }
    }

    /// emit_limit performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn emit_limit(&mut self, seq: u32, price: u64, qty: u64, side: Side) -> Action {
        let order_id = fix::new_limit_order_id(&self.session_id, self.task_id, u64::from(seq));
        self.push_resting(Resting {
            order_id,
            price,
            qty,
            side,
        });
        Action::NewLimit {
            seq,
            price,
            qty,
            side,
        }
    }

    /// emit_replace performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn emit_replace(&mut self, seq: u32, orig: Resting, repriced: u64) -> Action {
        let (price, qty) = if self.rng.gen_bool(0.5) {
            (repriced, orig.qty)
        } else {
            (orig.price, (orig.qty / 2).max(1))
        };
        let order_id = fix::replace_order_id(&self.session_id, self.task_id, u64::from(seq));
        self.push_resting(Resting {
            order_id,
            price,
            qty,
            side: orig.side,
        });
        Action::Replace {
            seq,
            orig_order_id: orig.order_id,
            price,
            qty,
            side: orig.side,
        }
    }

    /// push_resting performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn push_resting(&mut self, resting: Resting) {
        if self.ledger.len() >= MAX_RESTING_ORDERS {
            self.ledger.pop_front(); // O(1) drop oldest to bound memory
        }
        self.ledger.push_back(resting);
    }

    /// take_resting performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn take_resting(&mut self) -> Option<Resting> {
        if self.ledger.is_empty() {
            return None;
        }
        let idx = self.rng.gen_range(0..self.ledger.len());
        // swap_remove_back is O(1); ordering of the ledger doesn't matter for random picks.
        self.ledger.swap_remove_back(idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// gen performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn gen(mix: OrderMix, seed: u64) -> TaskGenerator {
        TaskGenerator::new("sess1".into(), 7, BotProfile::Hft, mix, seed)
    }

    const HFT_MIX: OrderMix = OrderMix {
        market_pct: 10,
        cancel_pct: 30,
        replace_pct: 10,
    };

    #[test]
    /// same_seed_produces_identical_stream performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn same_seed_produces_identical_stream() {
        let mut a = gen(HFT_MIX, 42);
        let mut b = gen(HFT_MIX, 42);
        for _ in 0..5000 {
            assert_eq!(a.next(), b.next());
        }
    }

    #[test]
    /// different_seed_diverges performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn different_seed_diverges() {
        let mut a = gen(HFT_MIX, 1);
        let mut b = gen(HFT_MIX, 2);
        let a_seq: Vec<Action> = (0..200).map(|_| a.next()).collect();
        let b_seq: Vec<Action> = (0..200).map(|_| b.next()).collect();
        assert_ne!(a_seq, b_seq);
    }

    #[test]
    /// mix_ratio_is_within_tolerance performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn mix_ratio_is_within_tolerance() {
        let mut g = gen(HFT_MIX, 99);
        let (mut market, mut cancel, mut replace, mut limit) = (0, 0, 0, 0);
        let n = 100_000;
        for _ in 0..n {
            match g.next() {
                Action::NewMarket { .. } => market += 1,
                Action::Cancel { .. } => cancel += 1,
                Action::Replace { .. } => replace += 1,
                Action::NewLimit { .. } => limit += 1,
            }
        }
        let pct = |c: i32| (c as f64) / (n as f64) * 100.0;
        assert!((pct(market) - 10.0).abs() < 1.5, "market {}", pct(market));
        assert!((pct(cancel) - 30.0).abs() < 1.5, "cancel {}", pct(cancel));
        assert!(
            (pct(replace) - 10.0).abs() < 1.5,
            "replace {}",
            pct(replace)
        );
        assert!((pct(limit) - 50.0).abs() < 1.5, "limit {}", pct(limit));
    }

    #[test]
    /// cancel_and_replace_reference_a_previously_emitted_order performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn cancel_and_replace_reference_a_previously_emitted_order() {
        let mut g = gen(HFT_MIX, 7);
        let mut live = std::collections::HashSet::new();
        for _ in 0..10_000 {
            match g.next() {
                Action::NewLimit { seq, .. } => {
                    live.insert(fix::new_limit_order_id("sess1", 7, u64::from(seq)));
                }
                Action::Replace {
                    seq, orig_order_id, ..
                } => {
                    assert!(
                        live.remove(&orig_order_id),
                        "replace references unknown/again-used order {orig_order_id}"
                    );
                    live.insert(fix::replace_order_id("sess1", 7, u64::from(seq)));
                }
                Action::Cancel { orig_order_id, .. } => {
                    assert!(
                        live.remove(&orig_order_id),
                        "cancel references unknown/again-used order {orig_order_id}"
                    );
                }
                Action::NewMarket { .. } => {}
            }
        }
    }

    #[test]
    /// all_limit_mix_never_cancels performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn all_limit_mix_never_cancels() {
        let mut g = gen(
            OrderMix {
                market_pct: 0,
                cancel_pct: 0,
                replace_pct: 0,
            },
            3,
        );
        for _ in 0..1000 {
            assert!(matches!(g.next(), Action::NewLimit { .. }));
        }
    }
}
