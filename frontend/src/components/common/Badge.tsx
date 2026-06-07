import styles from './Badge.module.css';

export type BadgeVariant =
  | 'scored'
  | 'running'
  | 'failed'
  | 'disqualified'
  | 'ready'
  | 'requested'
  | 'completed'
  | 'building'
  | 'queued'
  | 'scanning'
  | 'promoting';

export function Badge({ variant }: { variant: BadgeVariant | string }) {
  const safeVariant = isBadgeVariant(variant) ? variant : 'queued';
  return (
    <span className={`${styles.badge} ${styles[safeVariant]}`} role="status" aria-label={`Status: ${variant}`}>
      {(safeVariant === 'running' || safeVariant === 'building') && <span className={styles.dot}>●</span>}
      {variant.toUpperCase()}
    </span>
  );
}

function isBadgeVariant(value: string): value is BadgeVariant {
  return ['scored', 'running', 'failed', 'disqualified', 'ready', 'requested', 'completed', 'building', 'queued', 'scanning', 'promoting'].includes(value);
}
