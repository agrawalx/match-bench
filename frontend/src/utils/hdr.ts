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
 * HdrScenario groups the per-metric HDR percentile series for a SINGLE scenario
 * session (constant / spike / ramp), so each scenario renders its own chart.
 */
export interface HdrScenario {
  scenario: string; // scenario name: constant | spike | ramp
  sessionId: string;
  series: HdrSeries[]; // one per metric (service_time, round trip)
}

/**
 * histForSession merges the last HDR snapshot of each wave within ONE session
 * (waves are cumulative within a wave but distinct across waves, so we take the
 * latest per wave and add them together). Returns null if the session has no
 * encoded histogram for `field`.
 */
function histForSession(
  session: RunDetail["sessions"][number],
  field: "hdr_encoded" | "rt_hdr_encoded" | "slip_hdr_encoded" | "match_hdr_encoded",
): hdr.Histogram | null {
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
  let merged: hdr.Histogram | null = null;
  for (const b64 of lastByWave.values()) {
    try {
      const h = hdr.decodeFromCompressedBase64(b64.replace(/\s/g, ""));
      if (!merged) merged = h;
      else merged.add(h);
    } catch {}
  }
  return merged;
}

/**
 * seriesFromHistogram turns one decoded HDR histogram into a percentile curve.
 */
function seriesFromHistogram(label: string, h: hdr.Histogram): HdrSeries {
  const points = PCTS.map((pct) => ({
    percentile: pct,
    nines: pct < 100 ? 100 / (100 - pct) : 1e7,
    value_us: h.getValueAtPercentile(pct) / 1000,
  }));
  return {
    scenario: label,
    total: h.totalCount,
    p50_us: h.getValueAtPercentile(50) / 1000,
    p99_us: h.getValueAtPercentile(99) / 1000,
    points,
  };
}

/**
 * deriveHdrByScenario returns ONE entry per scenario session (constant / spike /
 * ramp), each carrying its own per-metric percentile series. The /run page renders
 * a separate HdrPercentileChart for each entry instead of merging all scenarios
 * into a single chart.
 */
export function deriveHdrByScenario(detail: RunDetail | undefined): HdrScenario[] {
  if (!detail?.sessions) return [];
  const out: HdrScenario[] = [];
  for (const session of detail.sessions) {
    const series: HdrSeries[] = [];
    for (const metric of METRICS) {
      const h = histForSession(session, metric.key);
      if (!h || h.totalCount === 0) continue;
      series.push(seriesFromHistogram(metric.label, h));
    }
    if (series.length > 0) {
      out.push({
        scenario: session.scenario || session.session_id,
        sessionId: session.session_id,
        series,
      });
    }
  }
  return out;
}

/**
 * deriveMatchByScenario builds the STANDALONE matching-latency percentile series
 * per scenario from match_hdr_encoded (t7−t3 for taker fills only, FIX 851=2). It
 * is intentionally separate from deriveHdrByScenario so matching latency renders in
 * its own chart rather than as another line on the service/response-time plot.
 */
export function deriveMatchByScenario(detail: RunDetail | undefined): HdrScenario[] {
  if (!detail?.sessions) return [];
  const out: HdrScenario[] = [];
  for (const session of detail.sessions) {
    const h = histForSession(session, "match_hdr_encoded");
    if (!h || h.totalCount === 0) continue;
    out.push({
      scenario: session.scenario || session.session_id,
      sessionId: session.session_id,
      series: [seriesFromHistogram("matching latency t7−t3 (taker fills)", h)],
    });
  }
  return out;
}
