/**
 * This file defines frontend behavior for hdr.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import * as hdr from "hdr-histogram-js";
import type { RunDetail } from "@/types/run";

/**
 * PctPoint describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface PctPoint {
  percentile: number; // e.g. 99.9
  nines: number; // 1/(1-p) — the log x value (0%→1, 90%→10, 99%→100, ...)
  value_us: number; // service_time t7-t3 at that percentile, microseconds
}

/**
 * HdrSeries describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface HdrSeries {
  scenario: string;
  total: number;
  p50_us: number;
  p99_us: number;
  points: PctPoint[];
}

const PCTS = [0, 25, 50, 75, 90, 95, 99, 99.9, 99.99];

const METRICS: {
  key: "hdr_encoded" | "rt_hdr_encoded" | "slip_hdr_encoded";
  label: string;
}[] = [
  { key: "hdr_encoded", label: "service_time t7−t3 (scored)" },
  { key: "rt_hdr_encoded", label: "response_time r9−t0 (round trip)" },
];

/**
 * mergeLastPerWave performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function mergeLastPerWave(
  detail: RunDetail,
  field: "hdr_encoded" | "rt_hdr_encoded" | "slip_hdr_encoded",
): hdr.Histogram | null {
  let merged: hdr.Histogram | null = null;
  for (const session of detail.sessions) {
    const lastByWave = new Map<number, string>();
    const lastTime = new Map<number, number>();
    for (const p of session.timeline ?? []) {
      const b64 = p[field];
      if (!b64) continue;
      const t = p.time_unix_ns ?? 0;
      if (
        !lastTime.has(p.wave_index) ||
        t >= (lastTime.get(p.wave_index) as number)
      ) {
        lastTime.set(p.wave_index, t);
        lastByWave.set(p.wave_index, b64);
      }
    }
    for (const b64 of lastByWave.values()) {
      try {
        const h = hdr.decodeFromCompressedBase64(b64.replace(/\s/g, ""));
        if (!merged) merged = h;
        else merged.add(h);
      } catch {}
    }
  }
  return merged;
}

/**
 * deriveHdrSeries performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
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
