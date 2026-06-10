import { describe, expect, it } from 'vitest';
import type { LeaderboardEntry, LeaderboardResponse, LeaderboardUpdateEvent } from '@/types/leaderboard';
import { applyLeaderboardUpdate } from './useLeaderboard';

function entry(overrides: Partial<LeaderboardEntry> = {}): LeaderboardEntry {
  return {
    rank: 1,
    run_group_id: 'rg-1',
    submission_id: 'sub-1',
    contestant_id: 'c-1',
    team_name: 'Ada',
    peak_sustained_tps: 9001,
    p99_ns_at_peak_tps: 2000,
    spike_recovery_ns: 3000,
    total_correctness: 0.99,
    disqualified: false,
    rank_delta: 0,
    computed_at_ns: 1000,
    ...overrides,
  };
}

function update(overrides: Partial<LeaderboardUpdateEvent> = {}): LeaderboardUpdateEvent {
  return {
    run_group_id: 'rg-1',
    submission_id: 'sub-1',
    contestant_id: 'c-1',
    team_name: 'Ada',
    rank: 2,
    rank_delta: -1,
    peak_sustained_tps: 9500,
    p99_ns_at_peak_tps: 1900,
    spike_recovery_ns: 2800,
    total_correctness: 1,
    disqualified: false,
    updated_at_ns: 2000,
    ...overrides,
  };
}

const current: LeaderboardResponse = {
  source: 'store',
  rows: [entry(), entry({ rank: 2, run_group_id: 'rg-2', contestant_id: 'c-2', team_name: 'Bob' })],
  next_cursor: 'cursor-1',
};

describe('applyLeaderboardUpdate', () => {
  it('replaces the row matching run_group_id/contestant_id', () => {
    const next = applyLeaderboardUpdate(current, update());
    expect(next.rows).toHaveLength(2);
    const merged = next.rows.find((row) => row.run_group_id === 'rg-1');
    expect(merged).toMatchObject({
      rank: 2,
      rank_delta: -1,
      peak_sustained_tps: 9500,
      p99_ns_at_peak_tps: 1900,
      spike_recovery_ns: 2800,
      total_correctness: 1,
      computed_at_ns: 2000,
    });
    // The non-matching row is untouched.
    expect(next.rows.find((row) => row.run_group_id === 'rg-2')).toEqual(current.rows[1]);
  });

  it('appends a row when no run_group_id/contestant_id matches', () => {
    const next = applyLeaderboardUpdate(current, update({ run_group_id: 'rg-3', contestant_id: 'c-3', team_name: 'Eve' }));
    expect(next.rows).toHaveLength(3);
    expect(next.rows[2]).toMatchObject({ run_group_id: 'rg-3', contestant_id: 'c-3', team_name: 'Eve' });
  });

  it('does not merge across rows that share only one of the two keys', () => {
    const next = applyLeaderboardUpdate(current, update({ run_group_id: 'rg-2', contestant_id: 'c-1' }));
    expect(next.rows).toHaveLength(3);
  });

  it('preserves source and next_cursor', () => {
    const next = applyLeaderboardUpdate(current, update());
    expect(next.source).toBe('store');
    expect(next.next_cursor).toBe('cursor-1');
  });

  it('carries the disqualification fields through', () => {
    const next = applyLeaderboardUpdate(current, update({ disqualified: true, disqualification_code: 'DQ_SPOOF' }));
    const merged = next.rows.find((row) => row.run_group_id === 'rg-1');
    expect(merged?.disqualified).toBe(true);
    expect(merged?.disqualification_code).toBe('DQ_SPOOF');
  });
});
