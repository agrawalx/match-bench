/**
 * This file defines frontend behavior for LatencyTimeline.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { ChartCard, SeriesLegend } from "@/components/common/ChartCard";
import type { MetricPoint } from "@/types/run";
import { formatClock, formatLatencyUs } from "@/utils/format";
import { chart, series } from "@/utils/chartTheme";

interface TimelinePoint {
  timestamp: string;
  p50: number;
  p90: number;
  p99: number;
}

const META = {
  service: { title: "Service time", caption: "t7−t3 · µs over time" },
  response: { title: "Response time", caption: "r9−t0 · µs over time" },
} as const;

/**
 * readPercentiles pulls the three per-second percentiles for the requested metric
 * out of a MetricPoint (service = p*_ns engine time; response = rt_p*_ns round
 * trip). Explicit branch keeps field access type-safe.
 */
function readPercentiles(
  point: MetricPoint,
  metric: "service" | "response",
): { p50: number; p90: number; p99: number } {
  if (metric === "service") {
    return { p50: point.p50_ns, p90: point.p90_ns, p99: point.p99_ns };
  }
  return { p50: point.rt_p50_ns, p90: point.rt_p90_ns, p99: point.rt_p99_ns };
}

/**
 * LatencyTimeline plots p50/p90/p99 for one latency metric over wall-clock time
 * for a single scenario session. It keeps inputs, side effects, and returned
 * values within this module's contract.
 */
export function LatencyTimeline({
  points,
  metric,
}: {
  points: MetricPoint[];
  metric: "service" | "response";
}) {
  const rows: TimelinePoint[] = points.map((point) => {
    const q = readPercentiles(point, metric);
    return {
      timestamp: new Date(point.time_unix_ns / 1_000_000).toISOString(),
      p50: q.p50 / 1000, // ns -> us
      p90: q.p90 / 1000,
      p99: q.p99 / 1000,
    };
  });

  // Each snapshot writes one row per active (session, wave) window, so a wave
  // boundary yields two rows sharing a timestamp. Collapse to one point per
  // timestamp keeping the worst-case percentile so a spike is never masked.
  const byTime = new Map<string, TimelinePoint>();
  for (const p of rows) {
    const existing = byTime.get(p.timestamp);
    if (existing) {
      existing.p50 = Math.max(existing.p50, p.p50);
      existing.p90 = Math.max(existing.p90, p.p90);
      existing.p99 = Math.max(existing.p99, p.p99);
    } else {
      byTime.set(p.timestamp, { ...p });
    }
  }
  const sorted = Array.from(byTime.values()).sort((a, b) =>
    a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0,
  );

  // Drop a warm-up window at the start of the run: the first ~second is a cold
  // engine plus catch-up-pacing schedule slip at the barrier, a transient that
  // flattens the whole rest of the chart if left in the y-range.
  const WARMUP_MS = 2000;
  const data =
    sorted.length > 0
      ? sorted.filter(
          (d) =>
            Date.parse(d.timestamp) >= Date.parse(sorted[0].timestamp) + WARMUP_MS,
        )
      : sorted;

  const meta = META[metric];

  return (
    <ChartCard
      title={meta.title}
      caption={meta.caption}
      right={<SeriesLegend keys={["p50", "p90", "p99"]} />}
      empty={data.length === 0}
      height={220}
    >
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 4, right: 6, bottom: 0, left: 0 }}>
          <CartesianGrid stroke={chart.grid} vertical={false} />
          <XAxis
            dataKey="timestamp"
            tickFormatter={formatClock}
            tick={chart.axisTick}
            stroke={chart.axis}
            minTickGap={32}
          />
          <YAxis
            tickFormatter={formatLatencyUs}
            tick={chart.axisTick}
            stroke={chart.axis}
            width={72}
          />
          <Tooltip
            formatter={(value: number) => formatLatencyUs(value)}
            labelFormatter={formatClock}
            contentStyle={chart.tooltip}
          />
          <Line type="monotone" dataKey="p50" name="p50" stroke={series.p50} dot={false} strokeWidth={1.4} isAnimationActive={false} />
          <Line type="monotone" dataKey="p90" name="p90" stroke={series.p90} dot={false} strokeWidth={1.4} isAnimationActive={false} />
          <Line type="monotone" dataKey="p99" name="p99" stroke={series.p99} dot={false} strokeWidth={1.4} isAnimationActive={false} />
        </LineChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}
