/**
 * This file defines frontend behavior for Badge.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import styles from "./Badge.module.css";

/**
 * BadgeVariant describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export type BadgeVariant =
  | "scored"
  | "running"
  | "failed"
  | "disqualified"
  | "ready"
  | "requested"
  | "completed"
  | "building"
  | "queued"
  | "scanning"
  | "promoting";

/**
 * Badge performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function Badge({ variant }: { variant: BadgeVariant | string }) {
  const safeVariant = isBadgeVariant(variant) ? variant : "queued";
  return (
    <span
      className={`${styles.badge} ${styles[safeVariant]}`}
      role="status"
      aria-label={`Status: ${variant}`}
    >
      <span className={styles.dot} aria-hidden="true" />
      {variant}
    </span>
  );
}

/**
 * isBadgeVariant performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function isBadgeVariant(value: string): value is BadgeVariant {
  return [
    "scored",
    "running",
    "failed",
    "disqualified",
    "ready",
    "requested",
    "completed",
    "building",
    "queued",
    "scanning",
    "promoting",
  ].includes(value);
}
