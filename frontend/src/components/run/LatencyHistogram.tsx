'use client';

import { Bar, BarChart, CartesianGrid, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { LatencyHistogram as Histogram, RunDetail } from '@/types/run';
import { formatLatencyUs } from '@/utils/format';
import styles from './Charts.module.css';

export function LatencyHistogram({ histogram, run }: { histogram?: Histogram; run: RunDetail }) {
  const derived = !histogram;
  const data =
    histogram?.buckets ??
    deriveBuckets(run).map((bucket) => ({ upper_bound_us: bucket.upper_bound_us, count: bucket.count }));
  const hasSamples = data.some((bucket) => bucket.count > 0);

  return (
    <section className={styles.card}>
      <h2>LATENCY HISTOGRAM</h2>
      {hasSamples ? (
        <>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={data}>
              <CartesianGrid stroke="#1a1a1a" vertical={false} />
              <XAxis
                dataKey="upper_bound_us"
                tick={{ fill: '#555', fontSize: 11 }}
                stroke="#222"
                tickFormatter={formatLatencyUs}
              />
              <YAxis tick={{ fill: '#555', fontSize: 11 }} stroke="#222" />
              <Tooltip
                formatter={(value) => [value, 'Samples']}
                labelFormatter={(value) => `<= ${formatLatencyUs(Number(value))}`}
                contentStyle={{
                  background: '#161616',
                  border: '1px solid #222',
                  borderRadius: 4,
                  color: '#e8e8e8',
                  fontFamily: 'var(--font-mono)',
                }}
              />
              <Bar dataKey="count" fill="#00e5ff" opacity={0.65} />
              {histogram && <ReferenceLine x={histogram.p50_us} stroke="#00c853" strokeDasharray="4 4" />}
              {histogram && <ReferenceLine x={histogram.p99_us} stroke="#ffd600" strokeDasharray="4 4" />}
            </BarChart>
          </ResponsiveContainer>
          {derived && <p className={styles.caption}>Derived from timeline data</p>}
        </>
      ) : (
        <div className={styles.emptyChart}>No latency samples have been written for this run yet.</div>
      )}
    </section>
  );
}

function deriveBuckets(run: RunDetail) {
  const values = run.sessions.flatMap((session) => session.timeline.map((point) => point.p99_ns / 1000));
  if (values.length === 0) return [{ upper_bound_us: 0, count: 0 }];
  const min = Math.min(...values);
  const max = Math.max(...values);
  const width = Math.max((max - min) / 20, 1);
  return Array.from({ length: 20 }, (_, index) => {
    const lower = min + width * index;
    const upper = lower + width;
    return {
      upper_bound_us: Math.round(upper),
      count: values.filter((value) => value >= lower && value < upper).length,
    };
  });
}
