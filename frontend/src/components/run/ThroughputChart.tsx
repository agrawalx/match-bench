/**
 * This file defines frontend behavior for ThroughputChart.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import {
  Area,
  ComposedChart,
  CartesianGrid,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { ChartCard, SeriesLegend } from "@/components/common/ChartCard";
import type { MetricPoint } from "@/types/run";
import { formatClock, formatNumber, formatPct } from "@/utils/format";
import { chart, series } from "@/utils/chartTheme";

interface Row {
  timestamp: string;
  tps: number;
  errors: number;
}

/**
 * ThroughputChart plots offered throughput (orders/s) with error-rate overlaid,
 * for a single scenario session. It keeps inputs, side effects, and returned
 * values within this module's contract.
 */
export function ThroughputChart({ points }: { points: MetricPoint[] }) {
  const rows: Row[] = points.map((point) => ({
    timestamp: new Date(point.time_unix_ns / 1_000_000).toISOString(),
    tps: point.tps_1s,
    errors: point.error_rate,
  }));

  // Wave boundaries emit two rows per second (one per active wave): tps is
  // additive (true instantaneous throughput is the sum), error_rate is a ratio
  // so surface the worst wave.
  const byTime = new Map<string, Row>();
  for (const p of rows) {
    const existing = byTime.get(p.timestamp);
    if (existing) {
      existing.tps += p.tps;
      existing.errors = Math.max(existing.errors, p.errors);
    } else {
      byTime.set(p.timestamp, { ...p });
    }
  }
  const data = Array.from(byTime.values()).sort((a, b) =>
    a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0,
  );

  return (
    <ChartCard
      title="Throughput"
      caption="orders/s · error rate"
      right={<SeriesLegend keys={["tps", "err"]} />}
      empty={data.length === 0}
      height={220}
    >
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={{ top: 4, right: 6, bottom: 0, left: 0 }}>
          <CartesianGrid stroke={chart.grid} vertical={false} />
          <XAxis
            dataKey="timestamp"
            tickFormatter={formatClock}
            tick={chart.axisTick}
            stroke={chart.axis}
            minTickGap={32}
          />
          <YAxis
            tickFormatter={(v: number) => formatNumber(v)}
            tick={chart.axisTick}
            stroke={chart.axis}
            width={72}
          />
          <Tooltip
            formatter={(value: number, name: string) =>
              name === "errors"
                ? [formatPct(value), "errors"]
                : [formatNumber(value), "tps"]
            }
            labelFormatter={formatClock}
            contentStyle={chart.tooltip}
          />
          <Area
            type="monotone"
            dataKey="tps"
            name="tps"
            stroke={series.tps}
            strokeWidth={1.4}
            fill={series.tps}
            fillOpacity={0.12}
            isAnimationActive={false}
          />
          <Line
            type="monotone"
            dataKey="errors"
            name="errors"
            stroke={series.err}
            dot={false}
            strokeWidth={1.4}
            isAnimationActive={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}
