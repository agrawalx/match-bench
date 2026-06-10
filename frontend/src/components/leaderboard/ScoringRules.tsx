import styles from './ScoringRules.module.css';

// ScoringRules states the contest scoring contract on the leaderboard so it is
// explicit to contestants and judges. The thresholds mirror the platform's
// scoring_config (correctness DQ 95%, p99 ≤ 1ms, error ≤ 1%, coverage ≥ 90%).
export function ScoringRules() {
  return (
    <details className={styles.rules}>
      <summary>Scoring rules</summary>
      <div className={styles.body}>
        <p>
          <strong>Ranking metric — Peak Sustained TPS.</strong> The highest ramp-wave load
          (orders/sec offered) a submission sustained while passing every gate below. Ties
          break by p99 service-time, then spike recovery, then correctness.
        </p>
        <p>
          <strong>Per-wave gates.</strong> p99 service time (t7−t3, stamped in-kernel by eBPF
          at the pod&apos;s veth before any contestant code runs) ≤ <strong>1&nbsp;ms</strong>;
          error rate ≤ <strong>1%</strong>; telemetry coverage ≥ <strong>90%</strong>.
        </p>
        <p>
          <strong>Correctness.</strong> <code>correctness = valid_fills / total_fills</code>. A
          reported fill is <em>valid</em> when it matches the platform&apos;s reference
          price-time-priority engine — best price first, FIFO within a price level, executed at
          the resting (maker) price — within the cross-flow / aggressive-fill timing tolerance.
          A submission is <strong>disqualified if correctness &lt; 95%</strong>.
        </p>
        <p className={styles.note}>
          Only <em>reported</em> fills are judged: under-reporting a fill is never penalized;
          reporting a wrong one is (phantom, overfill, price break, time-priority/queue jump,
          or self-trade). Cancels and replaces must keep the book consistent with the reference.
        </p>
      </div>
    </details>
  );
}
