/**
 * This file defines frontend behavior for ThroughputChart.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { RunDetail, ThroughputWindow } from "@/types/run";
import { formatClock } from "@/utils/format";
import styles from "./Charts.module.css";

/**
 * ThroughputChart performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function ThroughputChart({
  throughput,
  run,
}: {
  throughput?: ThroughputWindow[];
  run: RunDetail;
}) {
  const points =
    throughput ??
    run.sessions.flatMap(
      (session) =>
        session.timeline.map((point) => ({
          timestamp: new Date(point.time_unix_ns / 1_000_000).toISOString(),
          tps: point.tps_1s,
          errors: point.error_rate,
          scenario: session.scenario,
        })) ?? [],
    );

  // Each snapshot writes one row per active (session, wave) window, so at a wave
  // boundary two or more rows share a timestamp. Plotted raw, the area line
  // whipsaws between the busy wave and the near-zero wave that is just opening or
  // draining. Collapse to one point per timestamp: tps is additive (true
  // instantaneous throughput is the sum across waves); error_rate is a ratio, so
  // surface the worst wave rather than summing.
  const byTime = new Map<string, (typeof points)[number]>();
  for (const p of points) {
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
  const hasSamples = data.length > 0;

  return (
    <section className={styles.card}>
      <h2>THROUGHPUT TIMELINE</h2>
      {hasSamples ? (
        <ResponsiveContainer width="100%" height={280}>
          <AreaChart data={data}>
            <CartesianGrid stroke="#1a1a1a" vertical={false} />
            <XAxis
              dataKey="timestamp"
              tickFormatter={formatClock}
              tick={{ fill: "#555", fontSize: 11 }}
              stroke="#222"
            />
            <YAxis tick={{ fill: "#555", fontSize: 11 }} stroke="#222" />
            <Tooltip
              contentStyle={{
                background: "#161616",
                border: "1px solid #222",
                borderRadius: 4,
                color: "#e8e8e8",
                fontFamily: "var(--font-mono)",
              }}
            />
            <Area
              type="monotone"
              dataKey="tps"
              stroke="#00e5ff"
              strokeWidth={1}
              fill="rgba(0,229,255,0.10)"
            />
            <Line
              type="monotone"
              dataKey="errors"
              stroke="#ff4d4d"
              dot={false}
              strokeWidth={1}
            />
          </AreaChart>
        </ResponsiveContainer>
      ) : (
        <div className={styles.emptyChart}>
          No throughput samples have been written for this run yet.
        </div>
      )}
    </section>
  );
}
