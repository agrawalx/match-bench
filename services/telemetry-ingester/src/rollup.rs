//! This module implements rollup behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

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
    pub match_hdr_encoded: Vec<u8>,
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
    pub match_hdr_encoded: Vec<u8>,
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

/// merge_one combines serialized HDR histograms into one accumulator.
/// It skips invalid or empty blobs through decode_into and returns the merged
/// histogram for percentile extraction.
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
#[allow(clippy::too_many_arguments)]
fn build_merged(
    svc_blobs: &[&[u8]],
    rt_blobs: &[&[u8]],
    slip_blobs: &[&[u8]],
    match_blobs: &[&[u8]],
    tps_1s: f64,
    offered: u64,
    errors: u64,
) -> Merged {
    let svc = merge_one(svc_blobs.iter().copied());
    let rt = merge_one(rt_blobs.iter().copied());
    let slip = merge_one(slip_blobs.iter().copied());
    let matched = merge_one(match_blobs.iter().copied());
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
        match_hdr_encoded: serialize_hist(&matched),
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
    let matched: Vec<&[u8]> = rows.iter().map(|r| r.match_hdr_encoded.as_slice()).collect();
    let tps_1s: f64 = rows.iter().map(|r| r.tps_1s).sum();
    let offered: u64 = rows.iter().map(|r| r.offered).sum();
    let errors: u64 = rows.iter().map(|r| r.errors).sum();
    build_merged(&svc, &rt, &slip, &matched, tps_1s, offered, errors)
}

/// decode_blob deserializes a single HDR blob into a fresh histogram.
/// It is the delta path's counterpart to decode_into for standalone decodes.
fn decode_blob(blob: &[u8]) -> Histogram<u64> {
    let mut h = new_hist();
    decode_into(&mut h, blob);
    h
}

/// shard_delta returns cur − prev for one shard's cumulative histogram, i.e. the
/// samples recorded in just this 1-second bucket. prev is the shard's cumulative
/// state at the end of the previous bucket (None at wave start / first appearance,
/// where the whole cumulative IS the slice). Because the per-wave histogram only
/// grows, cur is always a superset of prev, so subtract cannot underflow; on the
/// impossible error case we fall back to the full cumulative rather than panic.
fn shard_delta(cur: &Histogram<u64>, prev: Option<&Histogram<u64>>) -> Histogram<u64> {
    let mut d = cur.clone();
    if let Some(p) = prev {
        if d.subtract(p).is_err() {
            return cur.clone();
        }
    }
    d
}

/// roll_wave_buckets converts a wave's partial history into metrics rows.
/// It carries each shard's latest cumulative histograms forward while summing
/// per-bucket interval counters. The scalar percentiles (p*/rt_p*) are computed
/// from the PER-SECOND delta (cum(t) − cum(t−1)) merged across shards, so a 1s
/// latency spike shows sharply on the timeline instead of being smeared by the
/// cumulative-within-wave volume. The stored HDR blobs stay cumulative — the
/// aggregate percentile chart still merges whole-wave distributions.
pub fn roll_wave_buckets(parts: &[PartialRow]) -> Vec<(i64, Merged)> {
    use std::collections::BTreeMap;
    let mut latest: BTreeMap<String, (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)> = BTreeMap::new();
    // Per-shard cumulative decode at the end of the previous bucket, for deltas.
    let mut prev_svc: BTreeMap<String, Histogram<u64>> = BTreeMap::new();
    let mut prev_rt: BTreeMap<String, Histogram<u64>> = BTreeMap::new();
    let mut out = Vec::new();
    let mut i = 0;
    while i < parts.len() {
        let bucket_ns = parts[i].bucket_ns;
        let (mut tps, mut offered, mut errors) = (0.0f64, 0u64, 0u64);
        while i < parts.len() && parts[i].bucket_ns == bucket_ns {
            let r = &parts[i];
            tps += r.tps_1s;
            offered += r.offered;
            errors += r.errors;
            latest.insert(
                r.shard.clone(),
                (
                    r.hdr_encoded.clone(),
                    r.rt_hdr_encoded.clone(),
                    r.slip_hdr_encoded.clone(),
                    r.match_hdr_encoded.clone(),
                ),
            );
            i += 1;
        }
        let svc: Vec<&[u8]> = latest.values().map(|b| b.0.as_slice()).collect();
        let rt: Vec<&[u8]> = latest.values().map(|b| b.1.as_slice()).collect();
        let slip: Vec<&[u8]> = latest.values().map(|b| b.2.as_slice()).collect();
        let matched: Vec<&[u8]> = latest.values().map(|b| b.3.as_slice()).collect();
        let mut m = build_merged(&svc, &rt, &slip, &matched, tps, offered, errors);

        // Recompute the scalar percentiles from the 1s slice. Silent shards have
        // cur == prev → empty delta → contribute nothing this second.
        let mut delta_svc = new_hist();
        let mut delta_rt = new_hist();
        for (shard, blobs) in latest.iter() {
            let cur_svc = decode_blob(&blobs.0);
            let cur_rt = decode_blob(&blobs.1);
            let _ = delta_svc.add(&shard_delta(&cur_svc, prev_svc.get(shard)));
            let _ = delta_rt.add(&shard_delta(&cur_rt, prev_rt.get(shard)));
            prev_svc.insert(shard.clone(), cur_svc);
            prev_rt.insert(shard.clone(), cur_rt);
        }
        m.p50_ns = delta_svc.value_at_quantile(0.50);
        m.p90_ns = delta_svc.value_at_quantile(0.90);
        m.p99_ns = delta_svc.value_at_quantile(0.99);
        m.p999_ns = delta_svc.value_at_quantile(0.999);
        m.rt_p50_ns = delta_rt.value_at_quantile(0.50);
        m.rt_p90_ns = delta_rt.value_at_quantile(0.90);
        m.rt_p99_ns = delta_rt.value_at_quantile(0.99);

        out.push((bucket_ns, m));
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
    (time, session_id, contestant_id, wave_index, p50_ns, p90_ns, p99_ns, p999_ns, rt_p50_ns, rt_p90_ns, rt_p99_ns, tps_1s, error_rate, hdr_encoded, rt_hdr_encoded, slip_hdr_encoded, match_hdr_encoded)
VALUES (to_timestamp($1::double precision / 1e9), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
ON CONFLICT (time, session_id, wave_index) DO UPDATE SET
    contestant_id=EXCLUDED.contestant_id, p50_ns=EXCLUDED.p50_ns, p90_ns=EXCLUDED.p90_ns,
    p99_ns=EXCLUDED.p99_ns, p999_ns=EXCLUDED.p999_ns, rt_p50_ns=EXCLUDED.rt_p50_ns,
    rt_p90_ns=EXCLUDED.rt_p90_ns, rt_p99_ns=EXCLUDED.rt_p99_ns, tps_1s=EXCLUDED.tps_1s,
    error_rate=EXCLUDED.error_rate, hdr_encoded=EXCLUDED.hdr_encoded,
    rt_hdr_encoded=EXCLUDED.rt_hdr_encoded, slip_hdr_encoded=EXCLUDED.slip_hdr_encoded,
    match_hdr_encoded=EXCLUDED.match_hdr_encoded";

/// The unique index ON CONFLICT needs. Includes `time` (the hypertable dimension).
const METRICS_UNIQUE_INDEX: &str =
    "CREATE UNIQUE INDEX IF NOT EXISTS metrics_session_wave_time ON metrics (time, session_id, wave_index);";

/// Rollup stores the database pool used to merge partial telemetry rows.
/// It owns schema setup and periodic aggregation from metrics_partial into the
/// canonical metrics table.
pub struct Rollup {
    pool: Pool,
}

impl Rollup {
    /// connect opens the database pool used by rollup processing.
    /// It parses TIMESCALE_URL-compatible connection strings and configures a
    /// small async pool for periodic aggregation.
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

    /// ensure_schema creates indexes required by idempotent rollup writes.
    /// It prepares the metrics table conflict target before rollup cycles run.
    pub async fn ensure_schema(&self) -> Result<()> {
        let client = self.pool.get().await.context("get rollup conn")?;
        client
            .batch_execute(METRICS_UNIQUE_INDEX)
            .await
            .context("create metrics unique index")?;
        Ok(())
    }

    /// Roll up every (session, wave) that has a new partial in (from_ns, to_ns].
    /// roll_window recomputes changed waves and writes merged metrics rows.
    /// It uses the time window to find touched waves, then reloads each wave's
    /// partial history so roll_wave_buckets can preserve shard carry-forward state.
    pub async fn roll_window(&self, from_ns: u64, to_ns: u64) -> Result<usize> {
        let client = self.pool.get().await.context("get rollup conn")?;
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
            let rows = client
                .query(
                    "SELECT (EXTRACT(EPOCH FROM date_trunc('second', time)) * 1e9)::bigint AS bucket_ns, \
                            contestant_id, shard, COALESCE(tps_1s,0), COALESCE(offered,0), COALESCE(errors,0), \
                            hdr_encoded, rt_hdr_encoded, slip_hdr_encoded, match_hdr_encoded \
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
                    match_hdr_encoded: r.get::<_, Option<Vec<u8>>>(9).unwrap_or_default(),
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
                            &m.match_hdr_encoded,
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

/// run drives the rollup service loop on a fixed interval.
/// It merges newly sealed buckets and advances the watermark only after a
/// successful database write.
pub async fn run(timescale_url: &str, interval: std::time::Duration) -> Result<()> {
    let rollup = Rollup::connect(timescale_url).await?;
    rollup.ensure_schema().await?;
    let mut watermark_ns = floor_to_second(now_ns().saturating_sub(60_000_000_000));
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
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

/// now_ns returns the current Unix time in nanoseconds.
/// It falls back to zero if the system clock cannot be represented as a
/// positive duration since the Unix epoch.
fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// NS_PER_SEC is the nanosecond scale used for rollup bucket alignment.
/// Keeping it as a constant makes floor_to_second's boundary math explicit.
const NS_PER_SEC: u64 = 1_000_000_000;

/// floor_to_second rounds a nanosecond instant down to the start of its second.
/// It keeps rollup windows second-aligned so per-shard partials stay in the
/// same bucket.
fn floor_to_second(ns: u64) -> u64 {
    (ns / NS_PER_SEC) * NS_PER_SEC
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::{new_hist, serialize_hist};

    /// blob_from serializes sample values into the HDR format used by rows.
    /// It builds realistic test blobs for merge and rollup assertions.
    fn blob_from(samples: &[u64]) -> Vec<u8> {
        let mut h = new_hist();
        for &s in samples {
            let _ = h.record(s.max(1));
        }
        serialize_hist(&h)
    }

    /// partial creates a minimal PartialRow for merge unit tests.
    /// It fills service-time histograms while leaving response-time and slip
    /// histograms empty.
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
            match_hdr_encoded: Vec::new(),
        }
    }

    /// shard_partial creates a cumulative per-shard partial for a bucket.
    /// It mirrors the ingester behavior where each shard's per-wave histogram
    /// only grows across flushes.
    fn shard_partial(
        shard: &str,
        bucket_ns: i64,
        tps: f64,
        offered: u64,
        svc_cumulative: &[u64],
    ) -> PartialRow {
        PartialRow {
            shard: shard.to_string(),
            bucket_ns,
            tps_1s: tps,
            offered,
            errors: 0,
            hdr_encoded: blob_from(svc_cumulative),
            rt_hdr_encoded: Vec::new(),
            slip_hdr_encoded: Vec::new(),
            match_hdr_encoded: Vec::new(),
        }
    }

    /// merge_partials_equals_combined_histogram checks lossless HDR merging.
    /// It compares merged shard sketches against one histogram built from all
    /// raw samples.
    #[test]
    fn merge_partials_equals_combined_histogram() {
        let shard_a = vec![100u64, 200, 300, 400, 500];
        let shard_b = vec![600u64, 700, 800, 900, 1000];

        let merged = merge_partials(&[partial(5.0, 5, 1, &shard_a), partial(5.0, 5, 0, &shard_b)]);

        let mut all = new_hist();
        for &s in shard_a.iter().chain(shard_b.iter()) {
            let _ = all.record(s);
        }
        assert_eq!(merged.p50_ns, all.value_at_quantile(0.50));
        assert_eq!(merged.p99_ns, all.value_at_quantile(0.99));
        assert_eq!(merged.p999_ns, all.value_at_quantile(0.999));

        assert_eq!(merged.tps_1s, 10.0);
        assert!((merged.error_rate - 0.1).abs() < 1e-9);

        let mut acc = new_hist();
        decode_into(&mut acc, &merged.hdr_encoded);
        assert_eq!(acc.value_at_quantile(0.99), all.value_at_quantile(0.99));
        assert_eq!(acc.len(), 10);
    }

    /// merge_partials_handles_empty_and_zero_offered checks empty inputs.
    /// It ensures absent offers produce a zero error rate and empty histograms do
    /// not fail merging.
    #[test]
    fn merge_partials_handles_empty_and_zero_offered() {
        let merged = merge_partials(&[PartialRow::default(), partial(0.0, 0, 0, &[])]);
        assert_eq!(merged.error_rate, 0.0);
        assert_eq!(merged.tps_1s, 0.0);
        assert_eq!(merged.p99_ns, 0);
    }

    /// merge_partials_single_shard_is_identity checks the single-replica path.
    /// It ensures a one-shard rollup preserves percentile and counter values.
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

    /// blob_count returns the decoded sample count from a merged HDR blob.
    /// It acts as a compact proxy for how many orders a row covers.
    fn blob_count(blob: &[u8]) -> u64 {
        let mut h = new_hist();
        decode_into(&mut h, blob);
        h.len()
    }

    /// blob_max returns the maximum decoded sample in a merged HDR blob.
    /// It checks whether carried-forward shard tails survive rollup.
    fn blob_max(blob: &[u8]) -> u64 {
        let mut h = new_hist();
        decode_into(&mut h, blob);
        h.max()
    }

    /// roll_wave_buckets_carries_forward_a_shard_that_finished_earlier protects LOCF.
    /// It verifies the final wave bucket reconstructs all shards even when one
    /// shard stops flushing earlier than another.
    #[test]
    fn roll_wave_buckets_carries_forward_a_shard_that_finished_earlier() {
        let parts = vec![
            shard_partial("Y", 1, 2.0, 2, &[400, 500]),
            shard_partial("X", 1, 2.0, 2, &[100, 200]),
            shard_partial("X", 2, 1.0, 1, &[100, 200, 300]),
        ];
        let rows = roll_wave_buckets(&parts);
        assert_eq!(rows.len(), 2, "one row per bucket");

        let (b1, m1) = &rows[0];
        assert_eq!(*b1, 1);
        assert_eq!(blob_count(&m1.hdr_encoded), 4);

        let (b2, m2) = &rows[1];
        assert_eq!(*b2, 2);
        assert_eq!(
            blob_count(&m2.hdr_encoded),
            5,
            "absent shard Y must be carried forward"
        );
        assert_eq!(
            blob_max(&m2.hdr_encoded),
            500,
            "Y's tail must survive into the final bucket"
        );

        assert_eq!(m2.tps_1s, 1.0);
    }

    /// ref_quantile builds a reference histogram from raw slice samples.
    /// It lets delta tests assert exact value_at_quantile parity with the
    /// per-second slice the rollup should reconstruct via HDR subtraction.
    fn ref_quantile(samples: &[u64], q: f64) -> u64 {
        let mut h = new_hist();
        for &s in samples {
            let _ = h.record(s.max(1));
        }
        h.value_at_quantile(q)
    }

    /// roll_wave_buckets_scalars_are_per_second_deltas checks the core Phase-2
    /// contract: the p*/rt_p* scalars are quantiles of the 1s SLICE (cum(t) −
    /// cum(t−1)), NOT of the cumulative-within-wave histogram. A late spike must
    /// show sharply in its own bucket instead of being diluted by earlier volume.
    #[test]
    fn roll_wave_buckets_scalars_are_per_second_deltas() {
        // Single shard, cumulative within the wave: bucket 2 adds two 50us samples
        // on top of the two 10ns samples from bucket 1.
        let parts = vec![
            shard_partial("X", 1, 2.0, 2, &[10, 10]),
            shard_partial("X", 2, 2.0, 2, &[10, 10, 50_000, 50_000]),
        ];
        let rows = roll_wave_buckets(&parts);
        assert_eq!(rows.len(), 2);

        // Bucket 1 is the wave start: no prev → delta == cumulative == {10,10}.
        assert_eq!(rows[0].1.p99_ns, ref_quantile(&[10, 10], 0.99));

        // Bucket 2 slice = {50_000, 50_000} ONLY — the 10ns samples subtracted off.
        assert_eq!(rows[1].1.p50_ns, ref_quantile(&[50_000, 50_000], 0.50));
        assert_eq!(rows[1].1.p99_ns, ref_quantile(&[50_000, 50_000], 0.99));

        // Cumulative blob is unchanged (still whole-wave) for the aggregate chart.
        assert_eq!(blob_count(&rows[1].1.hdr_encoded), 4);
    }

    /// roll_wave_buckets_wave_start_delta_is_cumulative checks the boundary case.
    /// The first bucket of a wave has no predecessor, so its delta is the whole
    /// cumulative histogram (which is itself only ~1s of data at wave start).
    #[test]
    fn roll_wave_buckets_wave_start_delta_is_cumulative() {
        let parts = vec![shard_partial("X", 1, 1.0, 3, &[10, 20, 30])];
        let rows = roll_wave_buckets(&parts);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.p50_ns, ref_quantile(&[10, 20, 30], 0.50));
        assert_eq!(rows[0].1.p99_ns, ref_quantile(&[10, 20, 30], 0.99));
    }

    /// roll_wave_buckets_delta_merges_across_shards checks multi-replica deltas.
    /// Each shard's per-second slice is extracted independently, then merged, so
    /// the bucket scalar reflects the combined 1s distribution across shards.
    #[test]
    fn roll_wave_buckets_delta_merges_across_shards() {
        let parts = vec![
            shard_partial("X", 1, 1.0, 1, &[10]),
            shard_partial("Y", 1, 1.0, 1, &[20]),
            shard_partial("X", 2, 1.0, 1, &[10, 100]),
            shard_partial("Y", 2, 1.0, 1, &[20, 200]),
        ];
        let rows = roll_wave_buckets(&parts);
        assert_eq!(rows.len(), 2);
        // Bucket 2 merged slice = X's {100} ∪ Y's {200}.
        assert_eq!(rows[1].1.p50_ns, ref_quantile(&[100, 200], 0.50));
        assert_eq!(rows[1].1.p99_ns, ref_quantile(&[100, 200], 0.99));
    }

    /// roll_wave_buckets_spike_is_isolated_to_its_bucket checks that a one-second
    /// spike does not bleed into the next bucket. After the spiking second, the
    /// shard is quiet: cum(t) == cum(t−1) → empty delta → zero-valued scalars.
    #[test]
    fn roll_wave_buckets_spike_is_isolated_to_its_bucket() {
        let parts = vec![
            shard_partial("X", 1, 1.0, 1, &[10]),
            shard_partial("X", 2, 1.0, 1, &[10, 99_999]),
            shard_partial("X", 3, 0.0, 0, &[10, 99_999]), // quiet second, same cum
        ];
        let rows = roll_wave_buckets(&parts);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].1.p99_ns, ref_quantile(&[10], 0.99));
        assert_eq!(rows[1].1.p99_ns, ref_quantile(&[99_999], 0.99));
        // No new samples this second → empty slice → 0 (not the carried spike).
        assert_eq!(rows[2].1.p99_ns, 0);
        assert_eq!(rows[2].1.p50_ns, 0);
    }

    /// floor_to_second_snaps_down_to_second_boundary checks window alignment.
    /// It prevents sub-second partials from being split across neighboring
    /// rollup ticks.
    #[test]
    fn floor_to_second_snaps_down_to_second_boundary() {
        assert_eq!(floor_to_second(0), 0);
        assert_eq!(floor_to_second(NS_PER_SEC - 1), 0);
        assert_eq!(floor_to_second(NS_PER_SEC), NS_PER_SEC);
        let s = 56 * NS_PER_SEC;
        assert_eq!(floor_to_second(s + 106_774_000), s);
        assert_eq!(floor_to_second(s + 516_806_000), s);
    }

    /// unhex decodes compact hexadecimal fixtures into bytes.
    /// It supports ignored tests that compare production HDR blobs.
    fn unhex(s: &str) -> Vec<u8> {
        let s = s.trim();
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// pcts prints useful percentile diagnostics for ignored tail checks.
    /// It is only used during explicit debugging runs with production blobs.
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

    /// rt_tail_matches_shard_merge checks production response-time tail blobs.
    /// It is ignored by default and compares merged metrics against independent
    /// per-shard HDR merges supplied through environment variables.
    #[test]
    #[ignore]
    fn rt_tail_matches_shard_merge() {
        let merged_hex = std::env::var("HDR_MERGED_HEX").expect("HDR_MERGED_HEX");
        let shards_hex = std::env::var("HDR_SHARDS_HEX").expect("HDR_SHARDS_HEX");

        let mut from_metrics = new_hist();
        decode_into(&mut from_metrics, &unhex(&merged_hex));

        let mut from_shards = new_hist();
        for h in shards_hex.split(',') {
            decode_into(&mut from_shards, &unhex(h));
        }

        pcts("metrics(rollup) ", &from_metrics);
        pcts("shards(reference)", &from_shards);

        for q in [0.50, 0.90, 0.99, 0.999, 0.9999, 1.0] {
            assert_eq!(
                from_metrics.value_at_quantile(q),
                from_shards.value_at_quantile(q),
                "quantile {q} differs between rollup-stored blob and shard re-merge"
            );
        }
        assert_eq!(from_metrics.len(), from_shards.len(), "total count differs");
        assert!(
            from_metrics.value_at_quantile(0.99) <= from_metrics.value_at_quantile(0.999)
                && from_metrics.value_at_quantile(0.999) <= from_metrics.value_at_quantile(0.9999),
            "tail not monotonic — distribution flattened"
        );
        const HDR_CEIL_NS: u64 = 60_000_000_000;
        assert!(
            from_metrics.max() < HDR_CEIL_NS,
            "max {} == HDR ceiling {HDR_CEIL_NS}: tail is being clipped",
            from_metrics.max()
        );
    }
}
