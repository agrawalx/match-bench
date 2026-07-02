/**
 * This file defines frontend behavior for RunHeader.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { Badge } from "@/components/common/Badge";
import { StatCard } from "@/components/common/StatCard";
import type { RunDetail } from "@/types/run";
import { formatPct, formatNumber } from "@/utils/format";
import styles from "./RunHeader.module.css";

/**
 * RunHeader is the run-detail hero: identity + status on the left, headline KPIs
 * on the right. It keeps inputs, side effects, and returned values within this
 * module's contract.
 */
export function RunHeader({ run }: { run: RunDetail }) {
  const timelineTps = run.sessions
    .flatMap((session) => session.timeline.map((point) => point.tps_1s))
    .filter(Number.isFinite);
  const observedPeakTps =
    timelineTps.length > 0 ? Math.max(...timelineTps) : Number.NaN;
  const correctness = run.score?.total_correctness;
  const totalViolations = run.violation_counts.reduce(
    (sum, c) => sum + c.count,
    0,
  );
  const status = run.score?.disqualified
    ? "disqualified"
    : run.sessions.some((session) => session.status === "failed")
      ? "failed"
      : run.sessions.every((session) => session.status === "completed")
        ? "scored"
        : "running";

  return (
    <header className={styles.header}>
      <div className={styles.identity}>
        <div className={styles.titleRow}>
          <span className={styles.id}>{run.run_group_id}</span>
          <Badge variant={status} />
        </div>
        <div className={styles.sub}>
          {run.score?.team_name ? (
            <>
              <b>{run.score.team_name}</b> ·{" "}
            </>
          ) : null}
          {run.sessions.length} scenario{run.sessions.length === 1 ? "" : "s"} ·{" "}
          {formatNumber(totalViolations)} violation
          {totalViolations === 1 ? "" : "s"}
        </div>
      </div>
      <div className={styles.stats}>
        <StatCard
          label="Observed peak TPS"
          value={formatNumber(observedPeakTps)}
        />
        <StatCard
          label="Correctness"
          value={correctness === undefined ? "Pending" : formatPct(correctness)}
        />
      </div>
    </header>
  );
}
