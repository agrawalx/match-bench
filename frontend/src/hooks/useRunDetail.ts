/**
 * This file defines frontend behavior for useRunDetail.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { useQuery } from "@tanstack/react-query";
import { getRunDetail } from "@/api/leaderboard";
import type { RunDetail } from "@/types/run";
import {
  deriveHdrByScenario,
  deriveMatchByScenario,
  type HdrScenario,
} from "@/utils/hdr";

/**
 * useRunDetail performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function useRunDetail(runGroupId: string | null) {
  const query = useQuery<{
    detail: RunDetail;
    hdrByScenario?: HdrScenario[];
    matchByScenario?: HdrScenario[];
  }>({
    queryKey: ['run-detail', runGroupId],
    enabled: Boolean(runGroupId),
    queryFn: async () => {
      if (!runGroupId) throw new Error('missing_run');
      const detail = await getRunDetail(runGroupId);
      const hdrByScenario = deriveHdrByScenario(detail);
      const matchByScenario = deriveMatchByScenario(detail);
      return { detail, hdrByScenario, matchByScenario };
    },
    // Poll only while the run is still in progress. Once every session is terminal
    // (completed/failed) we stop — regardless of whether a score has been computed yet.
    // (Previously an unscored-but-finished run polled the full ~8 MB payload forever.)
    refetchInterval: (query) => {
      const detail = query.state.data?.detail;
      if (!detail) return 2500;
      const live = detail.sessions.some(
        (session) => !["completed", "failed"].includes(session.status),
      );
      return live ? 2500 : false;
    },
  });

  return {
    data: query.data?.detail,
    hdrByScenario: query.data?.hdrByScenario,
    matchByScenario: query.data?.matchByScenario,
    isLoading: query.isLoading,
    error: query.error,
  };
}
