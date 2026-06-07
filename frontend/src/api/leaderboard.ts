import { apiFetch } from './client';
import { platformConfig } from '@/config/platform';
import type { RunDetail } from '@/types/run';
import type { LeaderboardResponse } from '@/types/leaderboard';

export function getLeaderboard(
  params: { runGroupId?: string; limit?: number; cursor?: string },
  token?: string,
): Promise<LeaderboardResponse> {
  const query = new URLSearchParams();
  if (params.runGroupId) query.set('run_group_id', params.runGroupId);
  if (params.limit) query.set('limit', String(params.limit));
  if (params.cursor) query.set('cursor', params.cursor);
  return apiFetch<LeaderboardResponse>(`${platformConfig.endpoints.leaderboard.rows}?${query.toString()}`, { token });
}

export function getRunDetail(runGroupId: string, token: string): Promise<RunDetail> {
  return apiFetch<RunDetail>(platformConfig.endpoints.leaderboard.run(runGroupId), { token });
}
