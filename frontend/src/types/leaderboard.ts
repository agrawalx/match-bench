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

// Wire shape of the leaderboard-api SSE "update" event. Mirrors
// schemas/go/topics/topics.go LeaderboardUpdateEvent (snake_case JSON tags).
export interface LeaderboardUpdateEvent {
  run_group_id: string;
  submission_id: string;
  contestant_id: string;
  team_name: string;
  rank: number;
  rank_delta: number;
  peak_sustained_tps: number;
  p99_ns_at_peak_tps: number;
  spike_recovery_ns: number;
  total_correctness: number;
  disqualified: boolean;
  disqualification_code?: string;
  updated_at_ns: number;
}

// The broker sends two named SSE events: "snapshot" (the raw LeaderboardResponse
// rendered on connect) and "update" (one flat LeaderboardUpdateEvent per change).
export type SSEEvent =
  | { type: 'snapshot'; data: LeaderboardResponse }
  | { type: 'update'; data: LeaderboardUpdateEvent };

export function statusForEntry(entry: LeaderboardEntry): LeaderboardStatus {
  return entry.disqualified ? 'disqualified' : 'scored';
}
