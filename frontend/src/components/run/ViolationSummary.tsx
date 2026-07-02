/**
 * This file defines frontend behavior for ViolationSummary.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import type { ViolationCount } from "@/types/run";
import { formatNumber } from "@/utils/format";
import styles from "./ViolationSummary.module.css";

/**
 * ViolationSummary renders the per-category violation totals for one session.
 * The counts are already aggregated server-side (GROUP BY violation_type), so
 * this only scopes to the active session and sorts. It keeps inputs, side
 * effects, and returned values within this module's contract.
 */
export function ViolationSummary({
  counts,
  sessionId,
}: {
  counts: ViolationCount[];
  sessionId?: string;
}) {
  const scoped = (
    sessionId ? counts.filter((c) => c.session_id === sessionId) : counts
  )
    .slice()
    .sort((a, b) => b.count - a.count);
  const total = scoped.reduce((sum, c) => sum + c.count, 0);

  return (
    <section>
      <div className={styles.sectionLabel}>
        VIOLATIONS
        {total > 0 ? (
          <span className={styles.count}>{formatNumber(total)}</span>
        ) : null}
      </div>
      <div className={styles.card}>
        {scoped.length === 0 ? (
          <div className={styles.empty}>No violations recorded.</div>
        ) : (
          scoped.map((c) => (
            <div key={c.violation_type} className={styles.row}>
              <span className={styles.dot} aria-hidden="true" />
              <span className={styles.code}>{c.violation_type}</span>
              <span className={styles.n}>×{formatNumber(c.count)}</span>
            </div>
          ))
        )}
      </div>
    </section>
  );
}
