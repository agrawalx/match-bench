export const platformConfig = {
  sessionId: process.env.NEXT_PUBLIC_SESSION_ID ?? 'global',
  uploadMaxBytes: Number(process.env.NEXT_PUBLIC_UPLOAD_MAX_BYTES ?? 100 * 1024 * 1024),
  pageSize: Number(process.env.NEXT_PUBLIC_LEADERBOARD_PAGE_SIZE ?? 50),
  leaderboardLimit: Number(process.env.NEXT_PUBLIC_LEADERBOARD_LIMIT ?? 100),
  endpoints: {
    auth: {
      token: '/api/auth/token',
      refresh: '/api/auth/refresh',
      logout: '/api/auth/logout',
    },
    leaderboard: {
      rows: '/api/leaderboard/v1/leaderboard',
      events: '/api/leaderboard/v1/events',
      run: (runGroupId: string) => `/api/leaderboard/v1/runs/${encodeURIComponent(runGroupId)}`,
    },
    submission: {
      create: '/api/submission/v1/submissions',
      detail: (submissionId: string) => `/api/submission/v1/submissions/${encodeURIComponent(submissionId)}`,
      benchmark: (submissionId: string) =>
        `/api/submission/v1/submissions/${encodeURIComponent(submissionId)}/benchmark`,
      runGroup: (runGroupId: string) => `/api/submission/v1/run-groups/${encodeURIComponent(runGroupId)}`,
      runGroups: '/api/submission/v1/run-groups',
    },
  },
} as const;

export function googleClientId(): string {
  return process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID ?? '';
}

export function googleRedirectUri(): string {
  if (process.env.NEXT_PUBLIC_GOOGLE_REDIRECT_URI) {
    return process.env.NEXT_PUBLIC_GOOGLE_REDIRECT_URI;
  }
  if (typeof window === 'undefined') return '';
  return `${window.location.origin}/auth/callback`;
}
