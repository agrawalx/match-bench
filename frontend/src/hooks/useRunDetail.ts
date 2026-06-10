'use client';

import { useQuery } from '@tanstack/react-query';
import { getRunDetail } from '@/api/leaderboard';
import { useAuth } from '@/auth/useAuth';
import type { LatencyHistogram, RunDetail, ThroughputWindow } from '@/types/run';
import { deriveHdrSeries, type HdrSeries } from '@/utils/hdr';

export function useRunDetail(runGroupId: string | null) {
  const { getToken } = useAuth();
  const token = getToken();
  const query = useQuery<{
    detail: RunDetail;
    histogram?: LatencyHistogram;
    throughput?: ThroughputWindow[];
    hdrSeries?: HdrSeries[];
  }>({
    queryKey: ['run-detail', runGroupId],
    // Run detail + HDR charts are served by leaderboard-api with no auth, so the
    // view works signed-out (public run pages). A token, when present, is sent
    // but ignored server-side; we don't gate the query on it.
    enabled: Boolean(runGroupId),
    queryFn: async () => {
      if (!runGroupId) throw new Error('missing_run');
      const detail = await getRunDetail(runGroupId, token ?? '');
      const histogram = deriveLatencyHistogram(detail);
      const throughput = deriveThroughput(detail);
      const hdrSeries = deriveHdrSeries(detail);
      return { detail, histogram, throughput, hdrSeries };
    },
    refetchInterval: (query) => {
      const detail = query.state.data?.detail;
      if (!detail) return 2500;
      if (!detail.score) return 2500;
      return detail.sessions.some((session) => !['completed', 'failed'].includes(session.status)) ? 2500 : false;
    },
  });

  return {
    data: query.data?.detail,
    histogram: query.data?.histogram,
    throughput: query.data?.throughput,
    hdrSeries: query.data?.hdrSeries,
    isLoading: query.isLoading,
    error: query.error,
  };
}

function deriveLatencyHistogram(detail: RunDetail): LatencyHistogram {
  const values = detail.sessions.flatMap((session) =>
    session.timeline.map((point) => point.p99_ns / 1000).filter(Number.isFinite),
  );
  if (values.length === 0) {
    return { buckets: [{ upper_bound_us: 0, count: 0 }], p50_us: 0, p99_us: 0, max_us: 0 };
  }
  const sorted = [...values].sort((a, b) => a - b);
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  const width = Math.max((max - min) / 18, 1);
  const buckets = Array.from({ length: 18 }, (_, index) => {
    const lower = min + width * index;
    const upper = index === 17 ? max + 1 : lower + width;
    return {
      upper_bound_us: Math.round(upper),
      count: values.filter((value) => value >= lower && value < upper).length,
    };
  });
  return {
    buckets,
    p50_us: percentile(sorted, 0.5),
    p99_us: percentile(sorted, 0.99),
    max_us: max,
  };
}

function deriveThroughput(detail: RunDetail): ThroughputWindow[] {
  return detail.sessions.flatMap((session) =>
    session.timeline.map((point) => ({
      timestamp: new Date(point.time_unix_ns / 1_000_000).toISOString(),
      tps: point.tps_1s,
      errors: point.error_rate,
      scenario: session.scenario,
    })),
  );
}

function percentile(sorted: number[], pct: number): number {
  if (sorted.length === 0) return 0;
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * pct) - 1));
  return sorted[index];
}
