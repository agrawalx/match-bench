import { SkeletonRows } from '@/components/common/Skeleton';
import type { LeaderboardEntry } from '@/types/leaderboard';
import type { SortBy, SortOrder } from './LeaderboardClient';
import { LeaderboardRow } from './LeaderboardRow';
import styles from './LeaderboardTable.module.css';

interface Props {
  rows: LeaderboardEntry[];
  loading: boolean;
  ownContestantId?: string;
  flashedRows: Set<string>;
  sortBy: SortBy;
  sortOrder: SortOrder;
}

const headers = [
  ['rank', '#'],
  ['team_name', 'TEAM'],
  ['peak_sustained_tps', 'PEAK TPS'],
  ['p99_ns_at_peak_tps', 'P99 AT PEAK'],
  ['spike_recovery_ns', 'RECOVERY'],
  ['total_correctness', 'CORRECT'],
  ['rank_delta', 'DELTA'],
  ['status', 'STATUS'],
] as const;

export function LeaderboardTable({ rows, loading, ownContestantId, flashedRows, sortBy, sortOrder }: Props) {
  return (
    <div className={styles.wrap}>
      <table className={styles.table} aria-label="Leaderboard">
        <colgroup>
          <col className={styles.rankCol} />
          <col />
          <col className={styles.scoreCol} />
          <col className={styles.latencyCol} />
          <col className={styles.latencyCol} />
          <col className={styles.correctCol} />
          <col className={styles.tpsCol} />
          <col className={styles.statusCol} />
        </colgroup>
        <thead>
          <tr>
            {headers.map(([key, label]) => (
              <th key={key} scope="col" aria-sort={key === sortBy ? (sortOrder === 'asc' ? 'ascending' : 'descending') : undefined}>
                {label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {loading ? (
            <SkeletonRows />
          ) : (
            rows.map((entry) => (
              <LeaderboardRow
                key={entry.contestant_id}
                entry={entry}
                isOwnRow={ownContestantId === entry.contestant_id}
                flash={flashedRows.has(entry.contestant_id)}
              />
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
