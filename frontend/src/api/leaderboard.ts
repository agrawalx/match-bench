/**
 * This file defines frontend behavior for leaderboard.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { apiFetch } from "./client";
import { platformConfig } from "@/config/platform";
import type { RunDetail } from "@/types/run";
import type { LeaderboardResponse } from "@/types/leaderboard";

/**
 * getLeaderboard performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function getLeaderboard(
  params: { runGroupId?: string; limit?: number; cursor?: string },
  token?: string,
): Promise<LeaderboardResponse> {
  const query = new URLSearchParams();
  if (params.runGroupId) query.set("run_group_id", params.runGroupId);
  if (params.limit) query.set("limit", String(params.limit));
  if (params.cursor) query.set("cursor", params.cursor);
  return apiFetch<LeaderboardResponse>(
    `${platformConfig.endpoints.leaderboard.rows}?${query.toString()}`,
    { token },
  );
}

/**
 * getRunDetail performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function getRunDetail(
  runGroupId: string,
  token: string,
): Promise<RunDetail> {
  return apiFetch<RunDetail>(
    platformConfig.endpoints.leaderboard.run(runGroupId),
    { token },
  );
}
