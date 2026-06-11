//! This module implements redis sink behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use anyhow::{Context, Result};
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;

use crate::aggregate::Snapshot;

/// RedisSink stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
pub struct RedisSink {
    conn: MultiplexedConnection,
}

impl RedisSink {
    /// connect performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub async fn connect(url: &str) -> Result<Self> {
        let client = redis::Client::open(url).context("open redis client")?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .context("connect redis")?;
        Ok(Self { conn })
    }

    /// write performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    pub async fn write(&self, snaps: &[Snapshot]) -> Result<()> {
        let mut conn = self.conn.clone();
        for s in snaps {
            if s.contestant_id.is_empty() {
                continue;
            }
            let key = redis_key(s);
            let fields: &[(&str, String)] = &[
                ("p50_ns", s.p50_ns.to_string()),
                ("p99_ns", s.p99_ns.to_string()),
                ("p999_ns", s.p999_ns.to_string()),
                ("tps_1s", s.tps_1s.to_string()),
                ("error_rate", s.error_rate.to_string()),
                ("wave_index", s.wave_index.to_string()),
                ("session_id", s.session_id.clone()),
                ("updated_at_ns", s.time_ns.to_string()),
            ];
            conn.hset_multiple::<_, _, _, ()>(&key, fields)
                .await
                .context("redis HSET contestant hash")?;
        }
        Ok(())
    }
}

/// redis_key performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn redis_key(s: &Snapshot) -> String {
    format!(
        "contestant:{}:{}:{}",
        s.contestant_id, s.session_id, s.wave_index
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// snap performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn snap(contestant: &str, session: &str, wave: u32) -> Snapshot {
        Snapshot {
            time_ns: 1,
            session_id: session.into(),
            contestant_id: contestant.into(),
            wave_index: wave,
            p50_ns: 0,
            p90_ns: 0,
            p99_ns: 0,
            p999_ns: 0,
            rt_p50_ns: 0,
            rt_p90_ns: 0,
            rt_p99_ns: 0,
            tps_1s: 0.0,
            error_rate: 0.0,
            offered: 0,
            errors: 0,
            hdr_encoded: Vec::new(),
            rt_hdr_encoded: Vec::new(),
            slip_hdr_encoded: Vec::new(),
        }
    }

    #[test]
    /// redis_key_disambiguates_session_and_wave performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn redis_key_disambiguates_session_and_wave() {
        let a = redis_key(&snap("c1", "S", 0));
        let b = redis_key(&snap("c1", "S", 1));
        assert_ne!(a, b, "different waves must not share a key");
        assert_eq!(
            redis_key(&snap("c1", "S", 0)),
            a,
            "stable for the same window"
        );
    }
}
