/**
 * This file defines frontend behavior for HdrPercentileChart.
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
import { ChartCard } from "@/components/common/ChartCard";
import type { HdrSeries } from "@/utils/hdr";
import { formatLatencyUs } from "@/utils/format";
import { chart, series as palette } from "@/utils/chartTheme";

// Each LINE is a metric (service_time / response_time / match), not a percentile,
// so colors come from the palette in a fixed order rather than the p50/p90/p99 map.
const LINE_COLORS = [palette.p99, palette.p90, palette.p50, palette.err];
const TICKS = [1, 10, 100, 1000, 10000];

/**
 * ninesToLabel converts a log "nines" x value back to a percentile label.
 */
function ninesToLabel(v: number): string {
  if (v <= 1) return "p0";
  const p = 100 - 100 / v;
  return v >= 1000 ? `p${p.toFixed(2)}` : `p${p.toFixed(0)}`;
}

/**
 * HdrPercentileChart plots latency-by-percentile curves (x = percentile on a log
 * "nines" scale, y = µs). It keeps inputs, side effects, and returned values
 * within this module's contract.
 */
export function HdrPercentileChart({
  series,
  title,
  caption,
  height = 118,
}: {
  series: HdrSeries[];
  title: string;
  caption?: string;
  height?: number;
}) {
  if (!series.length) {
    return <ChartCard title={title} caption={caption} height={height} empty />;
  }
  const n = series[0].points.length;
  const data = Array.from({ length: n }, (_, i) => {
    const row: Record<string, number> = { nines: series[0].points[i].nines };
    series.forEach((s) => {
      row[s.scenario] = Number(s.points[i].value_us.toFixed(1));
    });
    return row;
  });

  return (
    <ChartCard title={title} caption={caption} height={height}>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 4, right: 6, bottom: 0, left: 0 }}>
          <CartesianGrid stroke={chart.grid} vertical={false} />
          <XAxis
            dataKey="nines"
            type="number"
            scale="log"
            domain={[1, "dataMax"]}
            ticks={TICKS}
            tickFormatter={ninesToLabel}
            tick={chart.axisTick}
            stroke={chart.axis}
          />
          <YAxis
            tickFormatter={formatLatencyUs}
            tick={chart.axisTick}
            stroke={chart.axis}
            width={72}
          />
          <Tooltip
            formatter={(v: number, name: string) => [formatLatencyUs(v), name]}
            labelFormatter={(v: number) => ninesToLabel(Number(v))}
            contentStyle={chart.tooltip}
          />
          {series.map((s, i) => (
            <Line
              key={s.scenario}
              dataKey={s.scenario}
              name={s.scenario}
              stroke={LINE_COLORS[i % LINE_COLORS.length]}
              strokeWidth={1.6}
              dot={false}
              type="monotone"
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}
