/**
 * This file defines frontend behavior for HdrPercentileChart.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { HdrSeries } from "@/utils/hdr";

const COLORS = ["#3b82f6", "#f59e0b", "#10b981", "#ef4444"];
const TICKS = [1, 10, 100, 1000, 10000];

/**
 * ninesToLabel performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function ninesToLabel(v: number): string {
  if (v <= 1) return "0%";
  const p = 100 - 100 / v;
  return v >= 1000 ? `${p.toFixed(2)}%` : `${p.toFixed(0)}%`;
}

/**
 * HdrPercentileChart performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function HdrPercentileChart({
  series,
  title,
}: {
  series: HdrSeries[];
  title?: string;
}) {
  if (!series.length) return null;
  const n = series[0].points.length;
  const data = Array.from({ length: n }, (_, i) => {
    const row: Record<string, number> = { nines: series[0].points[i].nines };
    series.forEach((s) => {
      row[s.scenario] = Number(s.points[i].value_us.toFixed(1));
    });
    return row;
  });
  return (
    <div>
      <h3>{title ?? "Latency by Percentile Distribution"}</h3>
      <p style={{ opacity: 0.7, fontSize: 13, marginTop: 0 }}>
        service_time = algo processing at the veth (scored). response_time = the
        bot&apos;s full round trip. The gap between them is the non-algo
        overhead — coordinated omission + network + kernel queueing.
      </p>
      <ResponsiveContainer width="100%" height={380}>
        <LineChart
          data={data}
          margin={{ top: 8, right: 28, bottom: 28, left: 16 }}
        >
          <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
          <XAxis
            dataKey="nines"
            type="number"
            scale="log"
            domain={[1, "dataMax"]}
            ticks={TICKS}
            tickFormatter={ninesToLabel}
            height={44}
            label={{ value: "Percentile", position: "insideBottom", offset: 2 }}
          />
          <YAxis
            width={68}
            tickFormatter={(v: number) => `${v}`}
            label={{
              value: "latency (µs)",
              angle: -90,
              position: "insideLeft",
              style: { textAnchor: "middle" },
            }}
          />
          <Tooltip
            formatter={(v: number, name: string) => [`${v} µs`, name]}
            labelFormatter={(v: number) => `p${ninesToLabel(Number(v))}`}
          />
          {}
          <Legend
            verticalAlign="top"
            align="center"
            height={30}
            wrapperStyle={{ paddingBottom: 10 }}
          />
          {series.map((s, i) => (
            <Line
              key={s.scenario}
              dataKey={s.scenario}
              name={`${s.scenario}  ·  p99 ${s.p99_us.toFixed(0)}µs  ·  n=${s.total}`}
              stroke={COLORS[i % COLORS.length]}
              strokeWidth={2}
              dot
              type="monotone"
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
