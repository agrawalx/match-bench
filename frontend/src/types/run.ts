/**
 * This file defines frontend behavior for run.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import type { LeaderboardEntry } from "./leaderboard";

/**
 * RunDetail describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface RunDetail {
  run_group_id: string;
  score?: LeaderboardEntry;
  sessions: SessionDetail[];
  violations: ViolationEntry[];
}

/**
 * SessionDetail describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface SessionDetail {
  session_id: string;
  scenario: string;
  status: string;
  timeline: MetricPoint[];
}

/**
 * MetricPoint describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface MetricPoint {
  time_unix_ns: number;
  wave_index: number;
  p50_ns: number;
  p90_ns: number;
  p99_ns: number;
  rt_p50_ns: number;
  rt_p90_ns: number;
  rt_p99_ns: number;
  tps_1s: number;
  error_rate: number;
  hdr_encoded?: string; // service_time t7-t3 (scored)
  rt_hdr_encoded?: string; // response_time r9-t0 (client round trip)
  slip_hdr_encoded?: string; // schedule_slip t1-t0 (back-pressure)
}

/**
 * ViolationEntry describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface ViolationEntry {
  session_id: string;
  contestant_id: string;
  violation_type: string;
  order_id: string;
  detail: string;
  detected_at_ns: number;
}

/**
 * LatencyHistogram describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface LatencyHistogram {
  buckets: { upper_bound_us: number; count: number }[];
  p50_us: number;
  p99_us: number;
  max_us: number;
}

/**
 * ThroughputWindow describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface ThroughputWindow {
  timestamp: string;
  tps: number;
  errors: number;
  scenario: string;
}

/**
 * RunGroupStatus describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface RunGroupStatus {
  run_group_id: string;
  submission_id: string;
  status: "requested" | "running" | "completed" | "failed" | string;
  created_at: string;
  runs: RunGroupChild[];
}

/**
 * RunGroupHistoryResponse describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface RunGroupHistoryResponse {
  run_groups: RunGroupStatus[];
}

/**
 * RunGroupChild describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface RunGroupChild {
  session_id: string;
  scenario_id: string;
  scenario_name: string;
  status: "requested" | "running" | "completed" | "failed" | string;
  message?: string;
  created_at: string;
  updated_at: string;
}
