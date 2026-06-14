/**
 * This file defines frontend behavior for platform.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
export const platformConfig = {
  sessionId: process.env.NEXT_PUBLIC_SESSION_ID ?? "global",
  uploadMaxBytes: Number(
    process.env.NEXT_PUBLIC_UPLOAD_MAX_BYTES ?? 100 * 1024 * 1024,
  ),
  pageSize: Number(process.env.NEXT_PUBLIC_LEADERBOARD_PAGE_SIZE ?? 50),
  leaderboardLimit: Number(process.env.NEXT_PUBLIC_LEADERBOARD_LIMIT ?? 100),
  endpoints: {
    auth: {
      token: "/api/auth/token",
      refresh: "/api/auth/refresh",
      logout: "/api/auth/logout",
    },
    leaderboard: {
      rows: "/api/leaderboard/v1/leaderboard",
      events: "/api/leaderboard/v1/events",
      run: (runGroupId: string) =>
        `/api/leaderboard/v1/runs/${encodeURIComponent(runGroupId)}`,
    },
    submission: {
      create: "/api/submission/v1/submissions",
      detail: (submissionId: string) =>
        `/api/submission/v1/submissions/${encodeURIComponent(submissionId)}`,
      benchmark: (submissionId: string) =>
        `/api/submission/v1/submissions/${encodeURIComponent(submissionId)}/benchmark`,
      runGroup: (runGroupId: string) =>
        `/api/submission/v1/run-groups/${encodeURIComponent(runGroupId)}`,
      runGroups: "/api/submission/v1/run-groups",
    },
  },
} as const;

/**
 * authDisabled reports whether authentication is turned off. Auth has been REMOVED
 * from this benchmark frontend: it is hardcoded off (NOT dependent on a build arg, so
 * it can't silently fall back to the OAuth path if the arg is omitted at build time).
 * The visitor is always the default contestant; the backend runs AUTH_REQUIRED=false.
 */
export function authDisabled(): boolean {
  return true;
}

/**
 * defaultContestantId is the identity used for every request (auth is off). It must
 * match the submission-api DEFAULT_CONTESTANT_ID so submission ownership lines up.
 */
export function defaultContestantId(): string {
  return process.env.NEXT_PUBLIC_DEFAULT_CONTESTANT_ID ?? "echo-contestant";
}

/**
 * googleClientId performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function googleClientId(): string {
  return process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID ?? "";
}

/**
 * googleRedirectUri performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function googleRedirectUri(): string {
  if (process.env.NEXT_PUBLIC_GOOGLE_REDIRECT_URI) {
    return process.env.NEXT_PUBLIC_GOOGLE_REDIRECT_URI;
  }
  if (typeof window === "undefined") return "";
  return `${window.location.origin}/auth/callback`;
}
