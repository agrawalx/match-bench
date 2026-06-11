/**
 * This file defines frontend behavior for RunHeader.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { Badge } from "@/components/common/Badge";
import { StatCard } from "@/components/common/StatCard";
import type { RunDetail } from "@/types/run";
import { formatLatencyNs, formatPct, formatNumber } from "@/utils/format";
import styles from "./RunHeader.module.css";

/**
 * RunHeader performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function RunHeader({ run }: { run: RunDetail }) {
  const p99Values = run.sessions.flatMap((session) =>
    session.timeline.map((point) => point.p99_ns),
  );
  const bestP99 = Math.min(...p99Values.filter(Number.isFinite));
  const timelineTps = run.sessions
    .flatMap((session) => session.timeline.map((point) => point.tps_1s))
    .filter(Number.isFinite);
  const observedPeakTps =
    timelineTps.length > 0 ? Math.max(...timelineTps) : Number.NaN;
  const correctness = run.score?.total_correctness;
  const status = run.score?.disqualified
    ? "disqualified"
    : run.sessions.some((session) => session.status === "failed")
      ? "failed"
      : run.sessions.every((session) => session.status === "completed")
        ? "scored"
        : "running";
  return (
    <header className={styles.header}>
      <div>
        <span className={styles.id}>RUN GROUP {run.run_group_id}</span>
        <h1>
          {run.score?.team_name ||
            run.score?.submission_id ||
            "Benchmark telemetry"}
        </h1>
        <Badge variant={status} />
        <p>
          {run.sessions.length} scenarios / {run.violations.length} correctness
          violations
        </p>
      </div>
      <div className={styles.stats}>
        <StatCard
          label="Observed Peak TPS"
          value={formatNumber(observedPeakTps)}
        />
        <StatCard
          label="Correctness"
          value={correctness === undefined ? "Pending" : formatPct(correctness)}
        />
        <StatCard label="Best P99" value={formatLatencyNs(bestP99)} />
      </div>
    </header>
  );
}
