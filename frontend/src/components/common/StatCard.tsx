/**
 * This file defines frontend behavior for StatCard.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import styles from "./StatCard.module.css";

/**
 * StatCard renders one labelled metric. `unit` sits inline after the value;
 * `sub` is a secondary line beneath it. It keeps inputs, side effects, and
 * returned values within this module's contract.
 */
export function StatCard({
  label,
  value,
  unit,
  sub,
}: {
  label: string;
  value: string;
  unit?: string;
  sub?: string;
}) {
  return (
    <div className={styles.card}>
      <span className={styles.label}>{label}</span>
      <span className={styles.valueRow}>
        <strong className={styles.value}>{value}</strong>
        {unit ? <span className={styles.unit}>{unit}</span> : null}
      </span>
      {sub ? <span className={styles.sub}>{sub}</span> : null}
    </div>
  );
}
