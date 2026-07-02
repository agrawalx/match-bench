/**
 * This file defines frontend behavior for ScoringRules.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import styles from "./ScoringRules.module.css";

/**
 * ScoringRules is the side panel describing how runs are ranked. The formula and
 * weights are being revised, so this is a placeholder for now. It keeps inputs,
 * side effects, and returned values within this module's contract.
 */
export function ScoringRules() {
  return (
    <aside className={styles.panel}>
      <div className={styles.title}>Scoring rules</div>
      <p className={styles.lead}>
        Runs are ranked on a composite of throughput, latency and correctness.
      </p>
      <div className={styles.placeholder}>
        <span className={styles.badge}>Draft</span>
        <p>
          The exact formula and per-metric weights are being revised and will be
          published here.
        </p>
      </div>
    </aside>
  );
}
