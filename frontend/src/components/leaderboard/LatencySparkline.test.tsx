import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { LatencySparkline } from './LatencySparkline';

describe('LatencySparkline', () => {
  it('renders null for too few points', () => {
    const { container: empty } = render(<LatencySparkline data={[]} />);
    expect(empty.firstChild).toBeNull();
    const { container: single } = render(<LatencySparkline data={[42]} />);
    expect(single.firstChild).toBeNull();
  });

  it('renders a polyline with one coordinate per point', () => {
    render(<LatencySparkline data={[10, 20, 30]} />);
    const polyline = document.querySelector('polyline');
    expect(polyline).not.toBeNull();
    expect(polyline?.getAttribute('points')?.split(' ')).toHaveLength(3);
  });
});
