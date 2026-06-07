'use client';

import { useCallback, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getLeaderboard } from '@/api/leaderboard';
import { ApiError } from '@/api/client';
import { useAuth } from '@/auth/useAuth';
import { platformConfig } from '@/config/platform';
import type { LeaderboardResponse, SSEEvent } from '@/types/leaderboard';
import { useSSE, type SSEStatus } from './useSSE';

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
      if (event.type === 'leaderboard_update') {
        queryClient.setQueryData(['leaderboard', sessionId], event.data);
        event.data.rows.forEach((row) => flash(row.contestant_id));
      }
      if (event.type === 'score_update') {
        queryClient.setQueryData<LeaderboardResponse>(['leaderboard', sessionId], (current) => {
          if (!current) return current;
          return {
            ...current,
            rows: current.rows.map((row) =>
              row.contestant_id === event.data.contestant_id
                ? { ...row, peak_sustained_tps: event.data.peak_sustained_tps }
                : row,
            ),
          };
        });
        flash(event.data.contestant_id);
      }
    },
    [flash, queryClient, sessionId],
  );

  useSSE(platformConfig.endpoints.leaderboard.events, {
    token,
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
