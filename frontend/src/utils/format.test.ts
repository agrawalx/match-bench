import { describe, expect, it } from 'vitest';
import { formatLatencyNs } from './format';

describe('formatLatencyNs', () => {
  it('formats edge values', () => {
    expect(formatLatencyNs(0)).toBe('0 us');
    expect(formatLatencyNs(1_500_000_000)).toBe('1.50 s');
    expect(formatLatencyNs(Number.NaN)).toBe('-');
  });
});
