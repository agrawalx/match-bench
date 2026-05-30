//! Deterministic, open-loop order-content generation for one bot task.
//!
//! The *schedule* (when / how-fast / how-long a task fires) comes from the
//! `TaskSpec`. The *content* of each message — its type, price, qty, side, and
//! which resting order a cancel/replace targets — is generated here. Two
//! properties are load-bearing for cross-contestant fairness:
//!
//!   - **Deterministic**: every choice is drawn from a seeded `SmallRng`, so the
//!     same `(seed, mix)` reproduces the same stream for every contestant and on
//!     every replay. The DRAW ORDER inside [`TaskGenerator::next`] is part of
//!     this contract — reordering rng draws shifts the whole stream for a seed.
//!   - **Open-loop**: a cancel/replace target is chosen from this task's own
//!     self-accounted [`Resting`] ledger — orders it *sent*, never what the algo
//!     *did* with them. The bot must not react to fills (a fast and a slow algo
//!     would otherwise receive different streams). The ledger may therefore
//!     reference an order the algo already filled; that is intentional and tests
//!     the engine's cancel-of-filled handling.

use iicpc_schemas_rust::{BotProfile, PayloadType, Side};
use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::fix;
use crate::worker::order_shape;

/// Maximum resting orders tracked per task. The bot never observes fills, so it
/// only removes a resting order via cancel/replace — without a cap the ledger
/// would grow for the whole session. Capping bounds memory and models
/// "cancel a recent order"; cancels/replaces target this recent window.
const MAX_RESTING_ORDERS: usize = 1024;

/// OrderMix is the per-task message-type distribution as percentages. The limit
/// fraction is implied: `100 - market - cancel - replace`.
#[derive(Debug, Clone, Copy)]
pub struct OrderMix {
    pub market_pct: u8,
    pub cancel_pct: u8,
    pub replace_pct: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Limit,
    Market,
    Cancel,
    Replace,
}

/// Resting is an order this task believes is live, tracked from its own sends.
#[derive(Debug, Clone)]
struct Resting {
    order_id: String,
    price: u64,
    qty: u64,
    side: Side,
}

/// Action is the next message the task should render (via `fix`) and send. The
/// `orig_order_id` on a cancel/replace is the full ClOrdID of the targeted
/// resting order, so tag 41 (OrigClOrdID) matches its original tag 11.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// seq is this message's per-task sequence number (also its tag 34 and the
    /// numeric component of its ClOrdID). Useful for logging.
    pub fn seq(&self) -> u32 {
        match self {
            Action::NewLimit { seq, .. }
            | Action::NewMarket { seq, .. }
            | Action::Cancel { seq, .. }
            | Action::Replace { seq, .. } => *seq,
        }
    }

    pub fn payload_type(&self) -> PayloadType {
        match self {
            Action::NewLimit { .. } | Action::NewMarket { .. } => PayloadType::New,
            Action::Cancel { .. } => PayloadType::Cancel,
            Action::Replace { .. } => PayloadType::Replace,
        }
    }
}

/// TaskGenerator produces the deterministic, open-loop content stream for one
/// task. One per task; never shared. Seed it with `global_seed ^ task_id` so
/// each task has an independent but reproducible stream.
pub struct TaskGenerator {
    rng: SmallRng,
    profile: BotProfile,
    mix: OrderMix,
    session_id: String,
    task_id: u64,
    seq: u32,
    ledger: Vec<Resting>,
}

impl TaskGenerator {
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
            ledger: Vec::new(),
        }
    }

    /// next produces the next message to send.
    ///
    /// DRAW ORDER IS PART OF THE DETERMINISM CONTRACT: (1) type roll, (2) order
    /// shape, then (3) — for cancel/replace only — the ledger target index, and
    /// (4) — for replace only — the reprice-vs-shrink coin. Do not reorder.
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
                // Cancel echoes the resting order's own qty/side (35=F carries
                // tag 38/54); price is informational for telemetry.
                Some(orig) => Action::Cancel {
                    seq,
                    orig_order_id: orig.order_id,
                    price: orig.price,
                    qty: orig.qty,
                    side: orig.side,
                },
                // Empty ledger (e.g. start of run): deterministically fall back
                // to a new limit order rather than blocking.
                None => self.emit_limit(seq, price, qty, side),
            },
            Kind::Replace => match self.take_resting() {
                Some(orig) => self.emit_replace(seq, orig, price),
                None => self.emit_limit(seq, price, qty, side),
            },
        }
    }

    /// classify maps a 0..100 roll to a message kind. Ranges:
    /// `[0, market) → Market`, `[market, market+cancel) → Cancel`,
    /// `[.., +replace) → Replace`, remainder → `Limit`.
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

    /// emit_replace splits replaces between a price change (loses time priority)
    /// and a quantity-only decrease (keeps it) so both validator paths are
    /// exercised. Side is preserved across a replace. The replaced order
    /// re-rests under the replace's own (`_R`) ClOrdID.
    fn emit_replace(&mut self, seq: u32, orig: Resting, repriced: u64) -> Action {
        let (price, qty) = if self.rng.gen_bool(0.5) {
            (repriced, orig.qty) // reprice: new price, same qty
        } else {
            (orig.price, (orig.qty / 2).max(1)) // shrink qty, same price
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

    fn push_resting(&mut self, resting: Resting) {
        if self.ledger.len() >= MAX_RESTING_ORDERS {
            self.ledger.remove(0); // drop oldest to bound memory
        }
        self.ledger.push(resting);
    }

    fn take_resting(&mut self) -> Option<Resting> {
        if self.ledger.is_empty() {
            return None;
        }
        let idx = self.rng.gen_range(0..self.ledger.len());
        Some(self.ledger.remove(idx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gen(mix: OrderMix, seed: u64) -> TaskGenerator {
        TaskGenerator::new("sess1".into(), 7, BotProfile::Hft, mix, seed)
    }

    const HFT_MIX: OrderMix = OrderMix {
        market_pct: 10,
        cancel_pct: 30,
        replace_pct: 10,
    };

    #[test]
    fn same_seed_produces_identical_stream() {
        let mut a = gen(HFT_MIX, 42);
        let mut b = gen(HFT_MIX, 42);
        for _ in 0..5000 {
            assert_eq!(a.next(), b.next());
        }
    }

    #[test]
    fn different_seed_diverges() {
        let mut a = gen(HFT_MIX, 1);
        let mut b = gen(HFT_MIX, 2);
        let a_seq: Vec<Action> = (0..200).map(|_| a.next()).collect();
        let b_seq: Vec<Action> = (0..200).map(|_| b.next()).collect();
        assert_ne!(a_seq, b_seq);
    }

    #[test]
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
        // Cancels/replaces fall back to limit when the ledger is empty, which
        // only happens in the very first ticks, so the steady-state mix matches
        // the configured percentages within a small tolerance.
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
