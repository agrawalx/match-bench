import type { SortBy, SortOrder } from './LeaderboardClient';
import { ArrowDownUp, RadioTower } from 'lucide-react';
import styles from './LeaderboardControls.module.css';

interface Props {
  sortBy: SortBy;
  sortOrder: SortOrder;
  sessionId: string;
  live: boolean;
  onSortBy: (value: SortBy) => void;
  onSortOrder: () => void;
}

export function LeaderboardControls({ sortBy, sortOrder, sessionId, live, onSortBy, onSortOrder }: Props) {
  return (
    <div className={styles.controls}>
      <select value={sessionId} className={styles.select} aria-label="Session">
        <option value={sessionId}>{sessionId || 'global'}</option>
      </select>
      <select value={sortBy} className={styles.select} aria-label="Sort by" onChange={(event) => onSortBy(event.target.value as SortBy)}>
        <option value="rank">Rank</option>
        <option value="peak_sustained_tps">Peak TPS</option>
        <option value="p99_ns_at_peak_tps">P99 at Peak</option>
        <option value="spike_recovery_ns">Spike Recovery</option>
        <option value="total_correctness">Correctness</option>
      </select>
      <button className={styles.button} type="button" onClick={onSortOrder}>
        <ArrowDownUp size={14} strokeWidth={1.8} aria-hidden="true" />
        {sortOrder === 'asc' ? 'ASC' : 'DESC'}
      </button>
      <span className={`${styles.live} ${live ? styles.on : styles.off}`}>
        <RadioTower size={14} strokeWidth={1.8} aria-hidden="true" />
        {live ? 'LIVE' : 'PAUSED'}
      </span>
    </div>
  );
}
