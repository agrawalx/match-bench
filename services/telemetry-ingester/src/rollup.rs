//! Stage-2 rollup: merges the per-shard partial aggregates written by the
//! (horizontally scaled) ingester replicas into the canonical `metrics` table the
//! readers consume.
//!
//! Why a rollup at all: once a session is sharded across ingester replicas (by
//! order_id partition), each replica only sees a SUBSET of the session's orders,
//! so each writes a PARTIAL per-(session,wave) aggregate to `metrics_partial`,
//! tagged with its shard id. A percentile cannot be averaged across shards, so we
//! merge the underlying HDR sketches (lossless bucket addition) — done here, in
//! the same crate that produced them, so the V2-deflate decode is native and
//! exact. tps_1s sums across shards; error_rate is recomputed from summed
//! offered/errors counts (a ratio can't be averaged).
//!
//! The merge runs over 1-second time buckets and is light: its input is the
//! compact per-second per-shard partials, not the raw order firehose — so a single
//! rollup keeps up, and if it ever needed to scale it shards cleanly by session_id.

use anyhow::{Context, Result};
use hdrhistogram::serialization::Deserializer;
use hdrhistogram::Histogram;

use crate::aggregate::{new_hist, serialize_hist};

/// One ingester replica's partial aggregate for a (session, wave, second).
/// `shard`/`bucket_ns` identify the replica and 1-second bucket; the histograms
/// are CUMULATIVE per wave (the bytes grow each second), while tps/offered/errors
/// are PER-INTERVAL (just this second). That split is why the rollup carries the
/// histograms forward per shard but sums the counts per bucket — see
/// [`roll_wave_buckets`].
#[derive(Debug, Clone, Default)]
pub struct PartialRow {
    pub shard: String,
    pub bucket_ns: i64,
    pub tps_1s: f64,
    pub offered: u64,
    pub errors: u64,
    pub hdr_encoded: Vec<u8>,
    pub rt_hdr_encoded: Vec<u8>,
    pub slip_hdr_encoded: Vec<u8>,
}

/// The merged result for a (session, wave, second) — shaped to the `metrics` row.
#[derive(Debug, Clone)]
pub struct Merged {
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub rt_p50_ns: u64,
    pub rt_p90_ns: u64,
    pub rt_p99_ns: u64,
    pub tps_1s: f64,
    pub error_rate: f64,
    pub hdr_encoded: Vec<u8>,
    pub rt_hdr_encoded: Vec<u8>,
    pub slip_hdr_encoded: Vec<u8>,
}

/// decode_into adds a V2-deflate-serialized HDR blob into `acc`. Empty/garbage
/// blobs are skipped (a shard may have an empty response-time/slip histogram).
/// All blobs share new_hist()'s bounds, so add() never fails on range.
fn decode_into(acc: &mut Histogram<u64>, blob: &[u8]) {
    if blob.is_empty() {
        return;
    }
    if let Ok(h) = Deserializer::new().deserialize::<u64, _>(&mut std::io::Cursor::new(blob)) {
        let _ = acc.add(&h);
    }
}

fn merge_one<'a>(blobs: impl Iterator<Item = &'a [u8]>) -> Histogram<u64> {
    let mut acc = new_hist();
    for blob in blobs {
        decode_into(&mut acc, blob);
    }
    acc
}

/// build_merged assembles one `metrics` row: the three histograms are merged from
/// the supplied per-shard blobs (lossless HDR add → true merged percentiles), the
/// counts are taken as given. Callers decide WHICH blobs/counts to pass (a single
/// bucket's partials, or carry-forward state — see [`roll_wave_buckets`]).
fn build_merged(svc_blobs: &[&[u8]], rt_blobs: &[&[u8]], slip_blobs: &[&[u8]], tps_1s: f64, offered: u64, errors: u64) -> Merged {
    let svc = merge_one(svc_blobs.iter().copied());
    let rt = merge_one(rt_blobs.iter().copied());
    let slip = merge_one(slip_blobs.iter().copied());
    let error_rate = if offered > 0 {
        errors as f64 / offered as f64
    } else {
        0.0
    };
    Merged {
        p50_ns: svc.value_at_quantile(0.50),
        p90_ns: svc.value_at_quantile(0.90),
        p99_ns: svc.value_at_quantile(0.99),
        p999_ns: svc.value_at_quantile(0.999),
        rt_p50_ns: rt.value_at_quantile(0.50),
        rt_p90_ns: rt.value_at_quantile(0.90),
        rt_p99_ns: rt.value_at_quantile(0.99),
        tps_1s,
        error_rate,
        hdr_encoded: serialize_hist(&svc),
        rt_hdr_encoded: serialize_hist(&rt),
        slip_hdr_encoded: serialize_hist(&slip),
    }
}

/// merge_partials merges N per-shard partials for the same (session, wave, second)
/// into one canonical metric row: HDR sketches are added (true merged percentiles),
/// tps_1s is summed, error_rate is recomputed from summed offered/errors. Used when
/// every shard's partial for the bucket is present (the all-present case + tests).
pub fn merge_partials(rows: &[PartialRow]) -> Merged {
    let svc: Vec<&[u8]> = rows.iter().map(|r| r.hdr_encoded.as_slice()).collect();
    let rt: Vec<&[u8]> = rows.iter().map(|r| r.rt_hdr_encoded.as_slice()).collect();
    let slip: Vec<&[u8]> = rows.iter().map(|r| r.slip_hdr_encoded.as_slice()).collect();
    let tps_1s: f64 = rows.iter().map(|r| r.tps_1s).sum();
    let offered: u64 = rows.iter().map(|r| r.offered).sum();
    let errors: u64 = rows.iter().map(|r| r.errors).sum();
    build_merged(&svc, &rt, &slip, tps_1s, offered, errors)
}

/// roll_wave_buckets turns ONE wave's full partial history (all shards, all
/// seconds, sorted ascending by bucket_ns) into the per-second `metrics` rows.
///
/// The histograms are CUMULATIVE per wave but each shard flushes on its own phase,
/// so a given second may carry a partial from only SOME shards — and the shards
/// even FINISH the wave in different seconds. Summing only the partials present in
/// a bucket would therefore drop the absent shards' cumulative contribution (the
/// last row of a wave could end up holding a single shard — the bug this fixes).
/// Instead we carry each shard's LATEST cumulative blob forward (LOCF): a bucket's
/// histograms = the merge of every shard's most-recent cumulative blob at-or-before
/// that bucket. The final bucket therefore always reconstructs the whole wave
/// across all shards. Counts (tps/offered/errors) are PER-INTERVAL, so they are
/// summed over only the partials actually in that bucket (an absent shard
/// contributed nothing that second).
pub fn roll_wave_buckets(parts: &[PartialRow]) -> Vec<(i64, Merged)> {
    use std::collections::BTreeMap;
    // shard -> its latest cumulative (svc, rt, slip) blobs seen so far.
    let mut latest: BTreeMap<String, (Vec<u8>, Vec<u8>, Vec<u8>)> = BTreeMap::new();
    let mut out = Vec::new();
    let mut i = 0;
    while i < parts.len() {
        let bucket_ns = parts[i].bucket_ns;
        let (mut tps, mut offered, mut errors) = (0.0f64, 0u64, 0u64);
        // Fold every partial in this bucket: update its shard's carry-forward
        // blobs, and sum its per-interval counts.
        while i < parts.len() && parts[i].bucket_ns == bucket_ns {
            let r = &parts[i];
            tps += r.tps_1s;
            offered += r.offered;
            errors += r.errors;
            latest.insert(
                r.shard.clone(),
                (r.hdr_encoded.clone(), r.rt_hdr_encoded.clone(), r.slip_hdr_encoded.clone()),
            );
            i += 1;
        }
        let svc: Vec<&[u8]> = latest.values().map(|b| b.0.as_slice()).collect();
        let rt: Vec<&[u8]> = latest.values().map(|b| b.1.as_slice()).collect();
        let slip: Vec<&[u8]> = latest.values().map(|b| b.2.as_slice()).collect();
        out.push((bucket_ns, build_merged(&svc, &rt, &slip, tps, offered, errors)));
    }
    out
}

// ---- DB runner --------------------------------------------------------------

use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;

/// Lag (ns) behind `now` before a 1s bucket is considered sealed for rollup — long
/// enough that every shard's partial for that second has landed in metrics_partial.
const ROLLUP_LAG_NS: u64 = 3_000_000_000;

/// UPSERT into the canonical metrics table, keyed on (time, session, wave) so a
/// re-merge (e.g. after a rollup restart) is idempotent rather than duplicating.
const UPSERT_METRICS: &str = "\
INSERT INTO metrics
    (time, session_id, contestant_id, wave_index, p50_ns, p90_ns, p99_ns, p999_ns, rt_p50_ns, rt_p90_ns, rt_p99_ns, tps_1s, error_rate, hdr_encoded, rt_hdr_encoded, slip_hdr_encoded)
VALUES (to_timestamp($1::double precision / 1e9), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT (time, session_id, wave_index) DO UPDATE SET
    contestant_id=EXCLUDED.contestant_id, p50_ns=EXCLUDED.p50_ns, p90_ns=EXCLUDED.p90_ns,
    p99_ns=EXCLUDED.p99_ns, p999_ns=EXCLUDED.p999_ns, rt_p50_ns=EXCLUDED.rt_p50_ns,
    rt_p90_ns=EXCLUDED.rt_p90_ns, rt_p99_ns=EXCLUDED.rt_p99_ns, tps_1s=EXCLUDED.tps_1s,
    error_rate=EXCLUDED.error_rate, hdr_encoded=EXCLUDED.hdr_encoded,
    rt_hdr_encoded=EXCLUDED.rt_hdr_encoded, slip_hdr_encoded=EXCLUDED.slip_hdr_encoded";

/// The unique index ON CONFLICT needs. Includes `time` (the hypertable dimension).
const METRICS_UNIQUE_INDEX: &str =
    "CREATE UNIQUE INDEX IF NOT EXISTS metrics_session_wave_time ON metrics (time, session_id, wave_index);";

pub struct Rollup {
    pool: Pool,
}

impl Rollup {
    pub async fn connect(url: &str) -> Result<Self> {
        let pg_config: tokio_postgres::Config = url.parse().context("parse TIMESCALE_URL")?;
        let mgr = deadpool_postgres::Manager::from_config(
            pg_config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr)
            .max_size(4)
            .runtime(Runtime::Tokio1)
            .build()
            .context("build rollup pool")?;
        Ok(Self { pool })
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        let client = self.pool.get().await.context("get rollup conn")?;
        client
            .batch_execute(METRICS_UNIQUE_INDEX)
            .await
            .context("create metrics unique index")?;
        Ok(())
    }

    /// Roll up every (session, wave) that has a new partial in (from_ns, to_ns].
    /// Because the histograms are CUMULATIVE per wave and shards flush on different
    /// phases (and finish in different seconds), a correct bucket needs each shard's
    /// latest cumulative blob carried forward — which needs the wave's FULL partial
    /// history, not just this window's slice. So we use the window only to find
    /// which waves changed, then reload each changed wave end-to-end and recompute
    /// all its buckets via [`roll_wave_buckets`]. The upsert is idempotent (keyed on
    /// (time, session, wave)), so recomputing a wave every tick while it streams is
    /// safe; at benchmark scale a wave is ~20s of compact per-second partials.
    /// Returns the number of merged rows written.
    pub async fn roll_window(&self, from_ns: u64, to_ns: u64) -> Result<usize> {
        let client = self.pool.get().await.context("get rollup conn")?;
        // Which (session, wave) changed in this window?
        let touched = client
            .query(
                "SELECT DISTINCT session_id, wave_index FROM metrics_partial \
                 WHERE time > to_timestamp($1::double precision / 1e9) \
                   AND time <= to_timestamp($2::double precision / 1e9)",
                &[&(from_ns as f64), &(to_ns as f64)],
            )
            .await
            .context("read changed waves")?;
        if touched.is_empty() {
            return Ok(0);
        }

        let stmt = client
            .prepare_cached(UPSERT_METRICS)
            .await
            .context("prepare upsert")?;
        let mut written = 0usize;
        for t in &touched {
            let session_id: String = t.get(0);
            let wave_index: i32 = t.get(1);
            // Full partial history for this wave, ordered so roll_wave_buckets sees
            // buckets ascending (and rows within a bucket in flush order).
            let rows = client
                .query(
                    "SELECT (EXTRACT(EPOCH FROM date_trunc('second', time)) * 1e9)::bigint AS bucket_ns, \
                            contestant_id, shard, COALESCE(tps_1s,0), COALESCE(offered,0), COALESCE(errors,0), \
                            hdr_encoded, rt_hdr_encoded, slip_hdr_encoded \
                     FROM metrics_partial \
                     WHERE session_id = $1 AND wave_index = $2 \
                     ORDER BY bucket_ns ASC, time ASC",
                    &[&session_id, &wave_index],
                )
                .await
                .context("read wave partials")?;
            if rows.is_empty() {
                continue;
            }
            let contestant_id: String = rows[0].get(1);
            let parts: Vec<PartialRow> = rows
                .iter()
                .map(|r| PartialRow {
                    bucket_ns: r.get(0),
                    shard: r.get(2),
                    tps_1s: r.get(3),
                    offered: r.get::<_, i64>(4) as u64,
                    errors: r.get::<_, i64>(5) as u64,
                    hdr_encoded: r.get::<_, Option<Vec<u8>>>(6).unwrap_or_default(),
                    rt_hdr_encoded: r.get::<_, Option<Vec<u8>>>(7).unwrap_or_default(),
                    slip_hdr_encoded: r.get::<_, Option<Vec<u8>>>(8).unwrap_or_default(),
                })
                .collect();

            for (bucket_ns, m) in roll_wave_buckets(&parts) {
                client
                    .execute(
                        &stmt,
                        &[
                            &(bucket_ns as f64),
                            &session_id,
                            &contestant_id,
                            &wave_index,
                            &(m.p50_ns as i64),
                            &(m.p90_ns as i64),
                            &(m.p99_ns as i64),
                            &(m.p999_ns as i64),
                            &(m.rt_p50_ns as i64),
                            &(m.rt_p90_ns as i64),
                            &(m.rt_p99_ns as i64),
                            &m.tps_1s,
                            &m.error_rate,
                            &m.hdr_encoded,
                            &m.rt_hdr_encoded,
                            &m.slip_hdr_encoded,
                        ],
                    )
                    .await
                    .context("upsert merged metrics row")?;
                written += 1;
            }
        }
        Ok(written)
    }
}

/// run is the rollup service loop: every `interval`, merge the newly-sealed buckets.
pub async fn run(timescale_url: &str, interval: std::time::Duration) -> Result<()> {
    let rollup = Rollup::connect(timescale_url).await?;
    rollup.ensure_schema().await?;
    // Start a little in the past so a fresh rollup picks up an in-flight run.
    let mut watermark_ns = floor_to_second(now_ns().saturating_sub(60_000_000_000));
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        // Floor the window edge to a whole second. A bucket's per-shard partials
        // are written at DIFFERENT sub-second offsets within their second (each
        // replica flushes on its own phase), and roll_window buckets by
        // date_trunc('second'). If a window edge fell mid-second it would split
        // one shard into this tick and the other into the next — and the
        // ON CONFLICT upsert REPLACES the row, so the bucket would keep only the
        // last shard (silently dropping the others' orders). Second-aligned edges
        // guarantee every shard for a given second lands in the same window, so
        // merge_partials sees them all. (ROLLUP_LAG_NS still ensures the second is
        // sealed before we cross its boundary.)
        let sealed_to = floor_to_second(now_ns().saturating_sub(ROLLUP_LAG_NS));
        if sealed_to <= watermark_ns {
            continue;
        }
        match rollup.roll_window(watermark_ns, sealed_to).await {
            Ok(n) => {
                tracing::debug!(merged_rows = n, "rollup tick");
                watermark_ns = sealed_to;
            }
            Err(e) => tracing::warn!(error = %e, "rollup window failed; will retry next tick"),
        }
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Round a nanosecond instant DOWN to the start of its second. Window edges must
/// be second-aligned so a bucket's per-shard partials are never split across two
/// rollup ticks (see run()).
const NS_PER_SEC: u64 = 1_000_000_000;
fn floor_to_second(ns: u64) -> u64 {
    (ns / NS_PER_SEC) * NS_PER_SEC
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::{new_hist, serialize_hist};

    fn blob_from(samples: &[u64]) -> Vec<u8> {
        let mut h = new_hist();
        for &s in samples {
            let _ = h.record(s.max(1));
        }
        serialize_hist(&h)
    }

    fn partial(tps: f64, offered: u64, errors: u64, svc: &[u64]) -> PartialRow {
        PartialRow {
            shard: String::new(),
            bucket_ns: 0,
            tps_1s: tps,
            offered,
            errors,
            hdr_encoded: blob_from(svc),
            rt_hdr_encoded: Vec::new(),
            slip_hdr_encoded: Vec::new(),
        }
    }

    // Build a per-shard cumulative partial for a given bucket. svc is the shard's
    // CUMULATIVE sample set as of this bucket (mirrors the real ingester, whose
    // per-wave histogram only grows).
    fn shard_partial(shard: &str, bucket_ns: i64, tps: f64, offered: u64, svc_cumulative: &[u64]) -> PartialRow {
        PartialRow {
            shard: shard.to_string(),
            bucket_ns,
            tps_1s: tps,
            offered,
            errors: 0,
            hdr_encoded: blob_from(svc_cumulative),
            rt_hdr_encoded: Vec::new(),
            slip_hdr_encoded: Vec::new(),
        }
    }

    // The merged percentile must equal the percentile of the COMBINED raw samples
    // — i.e. merging the per-shard HDR sketches is lossless. We compare against a
    // single histogram built from all samples (HDR p99 is bucketed, so compare the
    // sketch values, not raw quantiles).
    #[test]
    fn merge_partials_equals_combined_histogram() {
        let shard_a = vec![100u64, 200, 300, 400, 500];
        let shard_b = vec![600u64, 700, 800, 900, 1000];

        let merged = merge_partials(&[
            partial(5.0, 5, 1, &shard_a),
            partial(5.0, 5, 0, &shard_b),
        ]);

        // Reference: one histogram with ALL samples.
        let mut all = new_hist();
        for &s in shard_a.iter().chain(shard_b.iter()) {
            let _ = all.record(s);
        }
        assert_eq!(merged.p50_ns, all.value_at_quantile(0.50));
        assert_eq!(merged.p99_ns, all.value_at_quantile(0.99));
        assert_eq!(merged.p999_ns, all.value_at_quantile(0.999));

        // tps_1s sums; error_rate = Σerrors/Σoffered = 1/10.
        assert_eq!(merged.tps_1s, 10.0);
        assert!((merged.error_rate - 0.1).abs() < 1e-9);

        // The merged blob re-decodes to the same distribution.
        let mut acc = new_hist();
        decode_into(&mut acc, &merged.hdr_encoded);
        assert_eq!(acc.value_at_quantile(0.99), all.value_at_quantile(0.99));
        assert_eq!(acc.len(), 10);
    }

    // No offered → error_rate is 0, not NaN; empty/missing HDR blobs are tolerated.
    #[test]
    fn merge_partials_handles_empty_and_zero_offered() {
        let merged = merge_partials(&[
            PartialRow::default(),
            partial(0.0, 0, 0, &[]),
        ]);
        assert_eq!(merged.error_rate, 0.0);
        assert_eq!(merged.tps_1s, 0.0);
        // Empty histogram percentile is 0, not a panic.
        assert_eq!(merged.p99_ns, 0);
    }

    // A single shard (the single-replica case) must pass through unchanged.
    #[test]
    fn merge_partials_single_shard_is_identity() {
        let samples = vec![10u64, 20, 30, 40, 50, 999];
        let merged = merge_partials(&[partial(3.0, 6, 2, &samples)]);
        let mut ref_h = new_hist();
        for &s in &samples {
            let _ = ref_h.record(s);
        }
        assert_eq!(merged.p99_ns, ref_h.value_at_quantile(0.99));
        assert_eq!(merged.tps_1s, 3.0);
        assert!((merged.error_rate - (2.0 / 6.0)).abs() < 1e-9);
    }

    // decoded count of a merged blob (proxy for "how many orders this row covers").
    fn blob_count(blob: &[u8]) -> u64 {
        let mut h = new_hist();
        decode_into(&mut h, blob);
        h.len()
    }
    fn blob_max(blob: &[u8]) -> u64 {
        let mut h = new_hist();
        decode_into(&mut h, blob);
        h.max()
    }

    // THE bug this fixes: two shards flush a wave on different phases and FINISH in
    // different seconds. Shard Y's last partial is at bucket 1; shard X keeps going
    // to bucket 2. A naive per-bucket sum would make bucket 2 hold only X (Y
    // dropped) — so the wave's final row (what the frontend shows) loses half the
    // orders. LOCF carries Y's last cumulative forward, so the final bucket
    // reconstructs the whole wave across both shards.
    #[test]
    fn roll_wave_buckets_carries_forward_a_shard_that_finished_earlier() {
        // Cumulative-per-shard: X grows 2→3 samples; Y stops at 2 samples (bucket 1).
        let parts = vec![
            shard_partial("Y", 1, 2.0, 2, &[400, 500]), // Y's first & LAST flush
            shard_partial("X", 1, 2.0, 2, &[100, 200]),
            shard_partial("X", 2, 1.0, 1, &[100, 200, 300]), // X continues; Y absent here
        ];
        let rows = roll_wave_buckets(&parts);
        assert_eq!(rows.len(), 2, "one row per bucket");

        // Bucket 1: both shards present → 4 orders.
        let (b1, m1) = &rows[0];
        assert_eq!(*b1, 1);
        assert_eq!(blob_count(&m1.hdr_encoded), 4);

        // Bucket 2 (the wave's FINAL row): X's 3 + Y's carried-forward 2 = 5, and Y's
        // tail (500) must still be present. The pre-fix code returned 3 here.
        let (b2, m2) = &rows[1];
        assert_eq!(*b2, 2);
        assert_eq!(blob_count(&m2.hdr_encoded), 5, "absent shard Y must be carried forward");
        assert_eq!(blob_max(&m2.hdr_encoded), 500, "Y's tail must survive into the final bucket");

        // Counts are PER-INTERVAL (not carried): bucket 2 had only X's interval.
        assert_eq!(m2.tps_1s, 1.0);
    }

    // Window edges must snap to whole seconds so a second's per-shard partials
    // (written at differing sub-second offsets) never split across two ticks.
    #[test]
    fn floor_to_second_snaps_down_to_second_boundary() {
        assert_eq!(floor_to_second(0), 0);
        assert_eq!(floor_to_second(NS_PER_SEC - 1), 0);
        assert_eq!(floor_to_second(NS_PER_SEC), NS_PER_SEC);
        // 22:58:56.516806 → 22:58:56.000000 and 22:58:56.106774 → same bucket.
        let s = 56 * NS_PER_SEC;
        assert_eq!(floor_to_second(s + 106_774_000), s);
        assert_eq!(floor_to_second(s + 516_806_000), s);
    }

    fn unhex(s: &str) -> Vec<u8> {
        let s = s.trim();
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn pcts(label: &str, h: &Histogram<u64>) {
        eprintln!(
            "{label}: count={} max={:.3}ms p50={:.3} p90={:.3} p99={:.3} p99.9={:.3} p99.99={:.3} (ms)",
            h.len(),
            h.max() as f64 / 1e6,
            h.value_at_quantile(0.50) as f64 / 1e6,
            h.value_at_quantile(0.90) as f64 / 1e6,
            h.value_at_quantile(0.99) as f64 / 1e6,
            h.value_at_quantile(0.999) as f64 / 1e6,
            h.value_at_quantile(0.9999) as f64 / 1e6,
        );
    }

    // Real-data tail check (run with the production blobs in env). Proves the
    // rollup's cross-shard merge preserves the response-time tail (p99/p99.9/p99.99)
    // bit-for-bit vs an independent merge of the same per-shard partials, and that
    // nothing is clipped at the 60s HDR ceiling. Ignored by default; invoke with:
    //   HDR_MERGED_HEX=<metrics.rt_hdr_encoded hex>
    //   HDR_SHARDS_HEX=<shardA hex>,<shardB hex>
    //   cargo test -p iicpc-telemetry-ingester rt_tail_matches_shard_merge -- --ignored --nocapture
    #[test]
    #[ignore]
    fn rt_tail_matches_shard_merge() {
        let merged_hex = std::env::var("HDR_MERGED_HEX").expect("HDR_MERGED_HEX");
        let shards_hex = std::env::var("HDR_SHARDS_HEX").expect("HDR_SHARDS_HEX");

        // The blob the rollup actually stored in `metrics` (what the frontend decodes).
        let mut from_metrics = new_hist();
        decode_into(&mut from_metrics, &unhex(&merged_hex));

        // Independent re-merge of the per-shard partials (ground truth).
        let mut from_shards = new_hist();
        for h in shards_hex.split(',') {
            decode_into(&mut from_shards, &unhex(h));
        }

        pcts("metrics(rollup) ", &from_metrics);
        pcts("shards(reference)", &from_shards);

        // Lossless: every quantile of the stored blob equals the reference merge.
        for q in [0.50, 0.90, 0.99, 0.999, 0.9999, 1.0] {
            assert_eq!(
                from_metrics.value_at_quantile(q),
                from_shards.value_at_quantile(q),
                "quantile {q} differs between rollup-stored blob and shard re-merge"
            );
        }
        assert_eq!(from_metrics.len(), from_shards.len(), "total count differs");
        // Heavy tail must actually be present (p99 < p99.9 < p99.99), i.e. not flattened.
        assert!(
            from_metrics.value_at_quantile(0.99) <= from_metrics.value_at_quantile(0.999)
                && from_metrics.value_at_quantile(0.999) <= from_metrics.value_at_quantile(0.9999),
            "tail not monotonic — distribution flattened"
        );
        // Nothing parked at the 60s ceiling (would mean clipped/dropped samples).
        const HDR_CEIL_NS: u64 = 60_000_000_000;
        assert!(
            from_metrics.max() < HDR_CEIL_NS,
            "max {} == HDR ceiling {HDR_CEIL_NS}: tail is being clipped",
            from_metrics.max()
        );
    }
}
