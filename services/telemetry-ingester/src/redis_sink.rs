//! Redis sink — the hot per-contestant snapshot the leaderboard API reads
//! without a DB round-trip. One hash per contestant holds the latest values.

use anyhow::{Context, Result};
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;

use crate::aggregate::Snapshot;

pub struct RedisSink {
    conn: MultiplexedConnection,
}

impl RedisSink {
    pub async fn connect(url: &str) -> Result<Self> {
        let client = redis::Client::open(url).context("open redis client")?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .context("connect redis")?;
        Ok(Self { conn })
    }

    /// For each snapshot, HSET the contestant's hot hash with the latest p99/tps/
    /// error_rate/wave (keeps the newest write per contestant per tick).
    pub async fn write(&self, snaps: &[Snapshot]) -> Result<()> {
        let mut conn = self.conn.clone();
        for s in snaps {
            if s.contestant_id.is_empty() {
                continue;
            }
            let key = format!("contestant:{}", s.contestant_id);
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
