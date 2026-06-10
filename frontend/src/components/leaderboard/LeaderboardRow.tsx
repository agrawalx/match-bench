import { Badge } from '@/components/common/Badge';
import { statusForEntry, type LeaderboardEntry } from '@/types/leaderboard';
import { formatLatencyNs, formatNumber, formatPct, shortId } from '@/utils/format';
import { rankTone } from '@/utils/score';
import styles from './LeaderboardRow.module.css';

interface Props {
  entry: LeaderboardEntry;
  isOwnRow: boolean;
  flash: boolean;
}

export function LeaderboardRow({ entry, isOwnRow, flash }: Props) {
  const correctness = Math.max(0, Math.min(100, entry.total_correctness <= 1 ? entry.total_correctness * 100 : entry.total_correctness));
  return (
    <tr className={`${styles.row} ${isOwnRow ? styles.own : ''} ${flash ? styles.flash : ''}`}>
      <td className={`${styles.rank} ${styles[rankTone(entry.rank)]}`}>{entry.rank}</td>
      <td>
        <div className={styles.name}>{entry.team_name || 'Untitled team'}</div>
        <div className={styles.id}>{shortId(entry.contestant_id)}</div>
      </td>
      <td className={`${styles.number} ${entry.rank <= 3 ? styles.scoreHot : ''}`}>{formatNumber(entry.peak_sustained_tps)}</td>
      <td className={styles.number}>{formatLatencyNs(entry.p99_ns_at_peak_tps)}</td>
      <td className={styles.number}>{formatLatencyNs(entry.spike_recovery_ns)}</td>
      <td aria-label={`Correctness: ${correctness.toFixed(1)}%`}>
        <div className={styles.track}>
          <div className={styles.fill} style={{ width: `${correctness}%` }} />
        </div>
        <div className={styles.correctLabel}>{formatPct(entry.total_correctness)}</div>
      </td>
      <td className={styles.number}>{entry.rank_delta > 0 ? `+${entry.rank_delta}` : entry.rank_delta}</td>
      <td className={styles.status}><Badge variant={statusForEntry(entry)} /></td>
    </tr>
  );
}
