import { Badge } from '@/components/common/Badge';
import type { SessionDetail } from '@/types/run';
import { formatLatencyNs, formatNumber } from '@/utils/format';
import styles from './SessionCards.module.css';

export function SessionCards({ sessions }: { sessions: SessionDetail[] }) {
  return (
    <div className={styles.grid}>
      {sessions.map((session) => (
        <article key={session.session_id} className={styles.card}>
          <header>
            <h3>{session.scenario}</h3>
            <Badge variant={session.status === 'completed' ? 'scored' : session.status} />
          </header>
          <dl>
            <Item label="Peak TPS" value={formatNumber(max(session.timeline.map((point) => point.tps_1s)))} />
            <Item label="P50 latency" value={formatLatencyNs(max(session.timeline.map((point) => point.p50_ns)))} />
            <Item label="P99 latency" value={formatLatencyNs(max(session.timeline.map((point) => point.p99_ns)))} />
            <Item label="RT P99" value={formatLatencyNs(max(session.timeline.map((point) => point.rt_p99_ns)))} />
            <Item label="Error rate" value={formatPercent(max(session.timeline.map((point) => point.error_rate * 100)))} />
            <Item label="Samples" value={formatNumber(session.timeline.length)} />
          </dl>
        </article>
      ))}
    </div>
  );
}

function formatPercent(value: number): string {
  return Number.isFinite(value) ? `${value.toFixed(2)}%` : '-';
}

function max(values: number[]): number {
  const finite = values.filter(Number.isFinite);
  return finite.length === 0 ? Number.NaN : Math.max(...finite);
}

function Item({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}
