import { Badge } from '@/components/common/Badge';
import type { SubmissionStatus } from '@/types/submission';
import { AnimatePresence, motion } from 'framer-motion';
import { AlertTriangle, CheckCircle2, XCircle } from 'lucide-react';
import styles from './BuildTimeline.module.css';

const steps = ['queued', 'building', 'scanning', 'promoting', 'ready'] as const;
const labels: Record<(typeof steps)[number] | 'failed', string> = {
  queued: 'Queued',
  building: 'Building image',
  scanning: 'Scanning image',
  promoting: 'Promoting artifact',
  ready: 'Ready',
  failed: 'Failed',
};

export function BuildTimeline({ status }: { status: SubmissionStatus | null }) {
  if (!status) return null;
  const activeIndex = Math.max(steps.indexOf(status.status === 'failed' ? 'ready' : status.status), 0);
  const progress = status.status === 'failed' ? activeIndex : activeIndex + 1;
  const isTerminal = status.status === 'ready' || status.status === 'failed';
  const logs = status.build_logs?.split('\n').slice(-20).join('\n');
  const timestamp = status.updated_at ?? status.created_at;
  const queuedForMs = Date.now() - Date.parse(timestamp);
  const showQueuedHint = status.status === 'queued' && queuedForMs > 90_000;

  return (
    <section className={styles.timeline}>
      <div className={styles.header}>
        <span>{status.submission_id}</span>
        <Badge variant={status.status} />
      </div>
      <AnimatePresence mode="wait">
        <motion.div
          key={status.status}
          className={styles.current}
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -8 }}
          transition={{ duration: 0.2, ease: 'easeOut' }}
        >
          <span className={`${styles.marker} ${status.status === 'ready' ? styles.done : ''} ${status.status === 'failed' ? styles.failed : styles.active}`}>
            {status.status === 'ready' && <CheckCircle2 size={24} strokeWidth={1.8} />}
            {status.status === 'failed' && <XCircle size={24} strokeWidth={1.8} />}
            {!isTerminal && <span className={styles.spinnerRing} aria-hidden="true" />}
          </span>
          <div className={styles.currentText}>
            <strong>{labels[status.status]}</strong>
            <span>
              Step {progress} of {steps.length}
            </span>
          </div>
          <time>{timestamp}</time>
        </motion.div>
      </AnimatePresence>
      <div className={styles.track} aria-hidden="true">
        <span style={{ width: `${(progress / steps.length) * 100}%` }} />
      </div>
      {showQueuedHint && (
        <div className={styles.hint} role="status">
          <AlertTriangle size={16} strokeWidth={2} aria-hidden="true" />
          <span>
            Still queued. Check that the local build-worker is running and connected to the same Kafka/Postgres stack.
          </span>
        </div>
      )}
      {status.status === 'failed' && logs && <pre className={styles.logs}>{logs}</pre>}
    </section>
  );
}
