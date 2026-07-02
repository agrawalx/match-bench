/**
 * This file defines frontend behavior for SessionCards.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { Badge } from "@/components/common/Badge";
import type { SessionDetail } from "@/types/run";
import { formatNumber } from "@/utils/format";
import styles from "./SessionCards.module.css";

/**
 * SessionCards summarizes each scenario session — throughput, correctness and
 * fills — without per-percentile latency (that lives in the timelines and HDR
 * charts). It keeps inputs, side effects, and returned values within this
 * module's contract.
 */
export function SessionCards({ sessions }: { sessions: SessionDetail[] }) {
  return (
    <div className={styles.grid}>
      {sessions.map((session) => (
        <article key={session.session_id} className={styles.card}>
          <header className={styles.head}>
            <h3>{session.scenario}</h3>
            <Badge
              variant={
                session.status === "completed" ? "scored" : session.status
              }
            />
          </header>
          <dl className={styles.rows}>
            <Item
              label="Peak TPS"
              value={formatNumber(
                max(session.timeline.map((point) => point.tps_1s)),
              )}
            />
            <Item
              label="Correctness"
              value={formatCorrectness(session.correctness_score)}
            />
            <Item label="Fills" value={formatFills(session)} />
            <Item
              label="Samples"
              value={formatNumber(session.timeline.length)}
            />
          </dl>
        </article>
      ))}
    </div>
  );
}

/**
 * formatCorrectness renders a per-scenario correctness score (0..1) as a
 * percentage, or "pending" before the validator has scored the session.
 */
function formatCorrectness(value?: number): string {
  return typeof value === "number" && Number.isFinite(value)
    ? `${(value * 100).toFixed(2)}%`
    : "pending";
}

/**
 * formatFills renders valid/total fills, or a dash when the validator has not
 * reported fill counts yet.
 */
function formatFills(session: SessionDetail): string {
  if (
    typeof session.valid_fills === "number" &&
    typeof session.total_fills === "number"
  ) {
    return `${formatNumber(session.valid_fills)} / ${formatNumber(session.total_fills)}`;
  }
  return "—";
}

/**
 * max performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function max(values: number[]): number {
  const finite = values.filter(Number.isFinite);
  return finite.length === 0 ? Number.NaN : Math.max(...finite);
}

/**
 * Item performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function Item({ label, value }: { label: string; value: string }) {
  return (
    <div className={styles.item}>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}
