export function formatLatencyNs(value: number): string {
  if (!Number.isFinite(value)) return '-';
  if (value === 0) return '0 us';
  return formatLatencyUs(value / 1000);
}

export function formatLatencyUs(value: number): string {
  if (!Number.isFinite(value)) return '-';
  if (value === 0) return '0 us';
  const microseconds = value;
  if (microseconds < 1000) return `${microseconds.toFixed(1)} us`;
  const milliseconds = microseconds / 1000;
  if (milliseconds < 1000) return `${milliseconds.toFixed(2)} ms`;
  return `${(milliseconds / 1000).toFixed(2)} s`;
}

export function formatNumber(value: number, digits = 0): string {
  if (!Number.isFinite(value)) return '-';
  return value.toLocaleString('en-US', {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

export function formatPct(value: number): string {
  if (!Number.isFinite(value)) return '-';
  const pct = value <= 1 ? value * 100 : value;
  return `${pct.toFixed(1)}%`;
}

export function shortId(value: string, length = 12): string {
  return value.length > length ? value.slice(0, length) : value;
}

export function formatClock(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '-';
  return date.toLocaleTimeString('en-US', { hour12: false });
}
