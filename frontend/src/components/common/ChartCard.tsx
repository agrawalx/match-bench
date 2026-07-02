/**
 * This file defines frontend behavior for ChartCard.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import type { ReactNode } from "react";
import styles from "./ChartCard.module.css";

/**
 * ChartCard is the single wrapper every chart renders inside: title + caption on
 * the left, an optional legend/control node on the right, then a fixed-height
 * body or an empty state. Keeps chart chrome consistent across the app.
 */
export function ChartCard({
  title,
  caption,
  right,
  empty,
  emptyLabel = "No data for this window",
  height = 132,
  children,
}: {
  title: string;
  caption?: string;
  right?: ReactNode;
  empty?: boolean;
  emptyLabel?: string;
  height?: number;
  children?: ReactNode;
}) {
  return (
    <section className={styles.card}>
      <header className={styles.head}>
        <div className={styles.heading}>
          <h3 className={styles.title}>{title}</h3>
          {caption ? <p className={styles.caption}>{caption}</p> : null}
        </div>
        {right ? <div className={styles.right}>{right}</div> : null}
      </header>
      {empty ? (
        <div className={styles.empty} style={{ height }}>
          <svg
            width="20"
            height="20"
            viewBox="0 0 24 24"
            fill="none"
            stroke="var(--text-muted)"
            strokeWidth="1.6"
            aria-hidden="true"
          >
            <path d="M3 3v18h18" />
            <path d="M7 15l3-3 3 2 4-5" />
          </svg>
          <span>{emptyLabel}</span>
        </div>
      ) : (
        <div className={styles.body} style={{ height }}>
          {children}
        </div>
      )}
    </section>
  );
}

const SERIES_LABELS: Record<string, string> = {
  p50: "p50",
  p90: "p90",
  p99: "p99",
  tps: "tps",
  err: "errors",
};

/**
 * SeriesLegend renders the fixed color→label chips for a chart's series, so the
 * legend matches the semantic palette everywhere it appears.
 */
export function SeriesLegend({ keys }: { keys: Array<keyof typeof SERIES_LABELS> }) {
  return (
    <div className={styles.legend}>
      {keys.map((k) => (
        <span key={k} className={styles.legendItem}>
          <span
            className={styles.swatch}
            style={{ background: `var(--s-${k})` }}
            aria-hidden="true"
          />
          {SERIES_LABELS[k]}
        </span>
      ))}
    </div>
  );
}
