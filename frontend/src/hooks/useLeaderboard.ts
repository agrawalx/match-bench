'use client';

import { useCallback, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getLeaderboard } from '@/api/leaderboard';
import { ApiError } from '@/api/client';
import { useAuth } from '@/auth/useAuth';
import { platformConfig } from '@/config/platform';
import type { LeaderboardEntry, LeaderboardResponse, LeaderboardUpdateEvent, SSEEvent } from '@/types/leaderboard';
import { useSSE, type SSEStatus } from './useSSE';

// Merges one SSE "update" event into the cached leaderboard: the row keyed by
// (run_group_id, contestant_id) is replaced, or appended if it is new. The
// component sorts client-side, so insertion order does not matter here.
export function applyLeaderboardUpdate(
  current: LeaderboardResponse,
  update: LeaderboardUpdateEvent,
): LeaderboardResponse {
  const entry: LeaderboardEntry = {
    rank: update.rank,
    run_group_id: update.run_group_id,
    submission_id: update.submission_id,
    contestant_id: update.contestant_id,
    team_name: update.team_name,
    peak_sustained_tps: update.peak_sustained_tps,
    p99_ns_at_peak_tps: update.p99_ns_at_peak_tps,
    spike_recovery_ns: update.spike_recovery_ns,
    total_correctness: update.total_correctness,
    disqualified: update.disqualified,
    disqualification_code: update.disqualification_code,
    rank_delta: update.rank_delta,
    computed_at_ns: update.updated_at_ns,
  };
  const matches = (row: LeaderboardEntry) =>
    row.run_group_id === update.run_group_id && row.contestant_id === update.contestant_id;
  const rows = current.rows.some(matches)
    ? current.rows.map((row) => (matches(row) ? entry : row))
    : [...current.rows, entry];
  return { ...current, rows };
}

export function useLeaderboard(sessionId?: string) {
  const { getToken } = useAuth();
  const token = getToken();
  const queryClient = useQueryClient();
  const [sseStatus, setSseStatus] = useState<SSEStatus>('closed');
  const [flashedRows, setFlashedRows] = useState<Set<string>>(new Set());

  const query = useQuery<LeaderboardResponse, ApiError>({
    queryKey: ['leaderboard', sessionId],
    queryFn: () => getLeaderboard({ runGroupId: sessionId, limit: platformConfig.leaderboardLimit }, token ?? undefined),
  });

  const flash = useCallback((contestantId: string) => {
    setFlashedRows((current) => new Set(current).add(contestantId));
    setTimeout(() => {
      setFlashedRows((current) => {
        const next = new Set(current);
        next.delete(contestantId);
        return next;
      });
    }, 400);
  }, []);

  const onMessage = useCallback(
    (event: SSEEvent) => {
      if (event.type === 'snapshot') {
        // Snapshot is the full LeaderboardResponse: replace the cache wholesale.
        queryClient.setQueryData(['leaderboard', sessionId], event.data);
      }
      if (event.type === 'update') {
        queryClient.setQueryData<LeaderboardResponse>(['leaderboard', sessionId], (current) => {
          if (!current) return current;
          return applyLeaderboardUpdate(current, event.data);
        });
        flash(event.data.contestant_id);
      }
    },
    [flash, queryClient, sessionId],
  );

  useSSE(platformConfig.endpoints.leaderboard.events, {
    enabled: Boolean(query.data && !query.error),
    onMessage,
    onStatusChange: setSseStatus,
  });

  return {
    data: query.data,
    isLoading: query.isLoading,
    error: query.error,
    sseStatus,
    flashedRows,
  };
}
