// API client + types for the IICPC leaderboard-api. Field names match the Go
// JSON tags exactly (services/leaderboard-api/internal/read/store.go,
// internal/handler/handler.go).
//
// The dashboard always renders LIVE data from leaderboard-api — there is no mock
// or fixture data. In production the app is served same-origin and nginx proxies
// /api/* to leaderboard-api, so API_BASE is empty ("") by default. For local dev
// against a port-forwarded API, set VITE_API_BASE (e.g. http://localhost:8080).

export interface LeaderboardRow {
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
  rows: LeaderboardRow[];
  next_cursor?: string;
}

export interface MetricPoint {
  time_unix_ns: number;
  wave_index: number;
  p50_ns: number;
  p90_ns: number;
  p99_ns: number;
  // response_time = r9 - t0 (bot-side round trip incl. coordinated-omission delay)
  rt_p50_ns: number;
  rt_p90_ns: number;
  rt_p99_ns: number;
  tps_1s: number;
  error_rate: number;
  hdr_encoded?: string;
}

export interface SessionDetail {
  session_id: string;
  scenario: string;
  status: string;
  timeline: MetricPoint[];
}

export interface ViolationEntry {
  session_id: string;
  contestant_id: string;
  violation_type: string;
  order_id: string;
  detail: string;
  detected_at_ns: number;
}

export interface RunDetail {
  run_group_id: string;
  score: LeaderboardRow | null;
  sessions: SessionDetail[];
  violations: ViolationEntry[];
}

export interface HealthPanel {
  source: string;
  prometheus_url: string;
  panels: string[];
}

export interface ActiveSession {
  session_id: string;
  scenario: string;
  status: string;
}

export interface ActiveRun {
  run_group_id: string;
  team_name: string;
  sessions: ActiveSession[];
}

const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
  return (await res.json()) as T;
}

export const getLeaderboard = () => get<LeaderboardResponse>(`/api/leaderboard?limit=100`);
export const getRunDetail = (runGroupID: string) =>
  get<RunDetail>(`/api/runs/${encodeURIComponent(runGroupID)}`);
export const getHealthPanel = () => get<HealthPanel>(`/api/health-panel`);
export const getLive = () => get<{ runs: ActiveRun[] }>(`/api/live`);
export const getChart = (sessionID: string) =>
  get<{ session_id: string; points: MetricPoint[] }>(
    `/api/charts/${encodeURIComponent(sessionID)}`,
  );

// subscribeLeaderboard streams live leaderboard snapshots over SSE (the browser
// EventSource auto-reconnects). The broker pushes either a full snapshot
// ({rows}) or a single update; on any event we ensure a fresh full snapshot.
export function subscribeLeaderboard(
  onSnapshot: (resp: LeaderboardResponse) => void,
): () => void {
  const es = new EventSource(`${API_BASE}/api/events`);
  es.onmessage = (ev) => {
    try {
      const data = JSON.parse(ev.data);
      if (data && Array.isArray(data.rows)) onSnapshot(data as LeaderboardResponse);
      else getLeaderboard().then(onSnapshot).catch(() => {});
    } catch {
      /* ignore malformed frame */
    }
  };
  return () => es.close();
}

// ---- formatting helpers --------------------------------------------------

export const nsToMicros = (ns: number) => ns / 1_000;
export const fmtMicros = (ns: number) =>
  ns === 0 ? "—" : `${(ns / 1_000).toLocaleString(undefined, { maximumFractionDigits: 1 })} µs`;
export const fmtTPS = (tps: number) => tps.toLocaleString();
export const fmtPct = (frac: number) => `${(frac * 100).toFixed(2)}%`;
export const fmtMillis = (ns: number) =>
  ns === 0 ? "—" : `${(ns / 1_000_000).toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`;
