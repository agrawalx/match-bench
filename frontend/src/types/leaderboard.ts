export type LeaderboardStatus = 'scored' | 'running' | 'failed' | 'disqualified';

export interface LeaderboardEntry {
  rank: number;
  run_group_id: string;
  submission_id: string;
  contestant_id: string;
  team_name: string;
  peak_sustained_tps: number;
  p99_ns_at_peak_tps: number;
  spike_recovery_ns: number;
  total_correctness: number;
  disqualified: boolean;
  disqualification_code?: string;
  rank_delta: number;
  computed_at_ns: number;
}

export interface LeaderboardResponse {
  source: string;
  rows: LeaderboardEntry[];
  next_cursor?: string;
}

export type SSEEvent =
  | { type: 'leaderboard_update'; data: LeaderboardResponse }
  | { type: 'score_update'; data: { contestant_id: string; peak_sustained_tps: number; delta: number } }
  | { type: 'run_update'; data: { run_group_id: string; status: string } };

export function statusForEntry(entry: LeaderboardEntry): LeaderboardStatus {
  return entry.disqualified ? 'disqualified' : 'scored';
}
