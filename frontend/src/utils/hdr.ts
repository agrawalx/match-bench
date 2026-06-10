import * as hdr from 'hdr-histogram-js';
import type { RunDetail } from '@/types/run';

// One point on the Gil-Tene "Latency by Percentile Distribution" curve.
export interface PctPoint {
  percentile: number; // e.g. 99.9
  nines: number; // 1/(1-p) — the log x value (0%→1, 90%→10, 99%→100, ...)
  value_us: number; // service_time t7-t3 at that percentile, microseconds
}

export interface HdrSeries {
  scenario: string;
  total: number;
  p50_us: number;
  p99_us: number;
  points: PctPoint[];
}

// Standard percentile sample set for the curve. Capped at 99.99 because per-wave
// sample counts here are modest; deeper nines need more samples to be meaningful.
const PCTS = [0, 25, 50, 75, 90, 95, 99, 99.9, 99.99];

// The three latency metrics that carry a full HDR histogram. The gap between
// service_time (algo processing at the veth) and response_time (the bot's full
// round trip) is the coordinated-omission / queueing / back-pressure delay;
// schedule_slip is the purest back-pressure signal (how late the bot's own
// write fell behind its schedule).
// Two plotted curves keep the chart legible: service_time (algo processing at
// the veth, the scored metric) and response_time (the bot's full round trip).
// The gap between them is the non-algo overhead — coordinated omission + wire +
// kernel queueing. schedule_slip (t1−t0, the pure back-pressure signal) is still
// serialized in the data (slip_hdr_encoded) for offline analysis, but is not
// overlaid here — under load it tracks response_time closely and just clutters.
const METRICS: { key: 'hdr_encoded' | 'rt_hdr_encoded' | 'slip_hdr_encoded'; label: string }[] = [
  { key: 'hdr_encoded', label: 'service_time t7−t3 (scored)' },
  { key: 'rt_hdr_encoded', label: 'response_time r9−t0 (round trip)' },
];

// mergeLastPerWave decodes the chosen blob field for every session, takes the
// LAST blob per (session, wave) — the blobs are CUMULATIVE within a wave, so the
// last one holds the whole wave — and merges them (HDR histograms are additive).
// Never sum all rows: that double-counts the cumulative prefix.
function mergeLastPerWave(
  detail: RunDetail,
  field: 'hdr_encoded' | 'rt_hdr_encoded' | 'slip_hdr_encoded',
): hdr.Histogram | null {
  let merged: hdr.Histogram | null = null;
  for (const session of detail.sessions) {
    const lastByWave = new Map<number, string>();
    const lastTime = new Map<number, number>();
    for (const p of session.timeline ?? []) {
      const b64 = p[field];
      if (!b64) continue;
      const t = p.time_unix_ns ?? 0;
      if (!lastTime.has(p.wave_index) || t >= (lastTime.get(p.wave_index) as number)) {
        lastTime.set(p.wave_index, t);
        lastByWave.set(p.wave_index, b64);
      }
    }
    for (const b64 of lastByWave.values()) {
      try {
        const h = hdr.decodeFromCompressedBase64(b64.replace(/\s/g, ''));
        if (!merged) merged = h;
        else merged.add(h);
      } catch {
        // skip an unparseable blob rather than break the whole curve
      }
    }
  }
  return merged;
}

// deriveHdrSeries produces up to three latency-by-percentile curves for the run
// — service_time, response_time, schedule_slip — each merged across all
// sessions/waves. A metric with no samples (e.g. response_time when the engine
// never answered) is omitted.
export function deriveHdrSeries(detail: RunDetail | undefined): HdrSeries[] {
  if (!detail?.sessions) return [];
  const out: HdrSeries[] = [];
  for (const metric of METRICS) {
    const merged = mergeLastPerWave(detail, metric.key);
    if (!merged || merged.totalCount === 0) continue;
    const points = PCTS.map((pct) => ({
      percentile: pct,
      nines: pct < 100 ? 100 / (100 - pct) : 1e7,
      value_us: merged.getValueAtPercentile(pct) / 1000,
    }));
    out.push({
      scenario: metric.label,
      total: merged.totalCount,
      p50_us: merged.getValueAtPercentile(50) / 1000,
      p99_us: merged.getValueAtPercentile(99) / 1000,
      points,
    });
  }
  return out;
}
