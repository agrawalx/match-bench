/**
 * This file defines frontend behavior for LeaderboardControls.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import type { SortBy, SortOrder } from "./LeaderboardClient";
import { ArrowDownUp } from "lucide-react";
import styles from "./LeaderboardControls.module.css";

/**
 * Props describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
interface Props {
  sortBy: SortBy;
  sortOrder: SortOrder;
  live: boolean;
  onSortBy: (value: SortBy) => void;
  onSortOrder: () => void;
}

/**
 * LeaderboardControls performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function LeaderboardControls({
  sortBy,
  sortOrder,
  live,
  onSortBy,
  onSortOrder,
}: Props) {
  return (
    <div className={styles.controls}>
      <span className={`${styles.live} ${live ? styles.on : styles.off}`}>
        <span className={styles.liveDot} />
        {live ? "Live" : "Paused"}
      </span>
      <select
        value={sortBy}
        className={styles.select}
        aria-label="Sort by"
        onChange={(event) => onSortBy(event.target.value as SortBy)}
      >
        <option value="rank">Sort · Rank</option>
        <option value="peak_sustained_tps">Sort · Peak TPS</option>
        <option value="total_correctness">Sort · Correctness</option>
      </select>
      <button className={styles.button} type="button" onClick={onSortOrder}>
        <ArrowDownUp size={13} strokeWidth={1.8} aria-hidden="true" />
        {sortOrder === "asc" ? "Asc" : "Desc"}
      </button>
    </div>
  );
}
