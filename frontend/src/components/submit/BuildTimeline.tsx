/**
 * This file defines frontend behavior for BuildTimeline.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { Badge } from "@/components/common/Badge";
import type { SubmissionStatus } from "@/types/submission";
import { AlertTriangle, Check } from "lucide-react";
import styles from "./BuildTimeline.module.css";

const steps = ["queued", "building", "scanning", "promoting", "ready"] as const;
const labels: Record<(typeof steps)[number], string> = {
  queued: "Queued",
  building: "Building",
  scanning: "Scanning",
  promoting: "Promoting",
  ready: "Ready",
};

/**
 * BuildTimeline renders the build pipeline as a horizontal stepper: completed
 * steps checked, the current step marked, later steps pending. No motion — state
 * changes are conveyed by the marker styling. It keeps inputs, side effects, and
 * returned values within this module's contract.
 */
export function BuildTimeline({ status }: { status: SubmissionStatus | null }) {
  if (!status) return null;
  const failed = status.status === "failed";
  const activeIndex = Math.max(
    steps.indexOf(
      (failed ? "ready" : status.status) as (typeof steps)[number],
    ),
    0,
  );
  const logs = status.build_logs?.split("\n").slice(-20).join("\n");
  const timestamp = status.updated_at ?? status.created_at;
  const queuedForMs = Date.now() - Date.parse(timestamp);
  const showQueuedHint = status.status === "queued" && queuedForMs > 90_000;

  return (
    <section className={styles.timeline}>
      <div className={styles.header}>
        <span className={styles.id}>{status.submission_id}</span>
        <Badge variant={status.status} />
      </div>

      <div className={styles.stepper}>
        <div className={styles.rail} aria-hidden="true" />
        {steps.map((step, i) => {
          const done = i < activeIndex || status.status === "ready";
          const active = i === activeIndex && !done;
          const isFailedHere = failed && i === activeIndex;
          return (
            <div key={step} className={styles.step}>
              <span
                className={`${styles.marker} ${done ? styles.done : ""} ${active ? styles.active : ""} ${isFailedHere ? styles.failed : ""}`}
              >
                {done ? (
                  <Check size={14} strokeWidth={2.6} />
                ) : (
                  <span className={styles.dot} />
                )}
              </span>
              <span className={styles.stepLabel}>{labels[step]}</span>
            </div>
          );
        })}
      </div>

      {showQueuedHint && (
        <div className={styles.hint} role="status">
          <AlertTriangle size={15} strokeWidth={2} aria-hidden="true" />
          <span>
            Still queued. Check that the build-worker is running and connected to
            the same Kafka/Postgres stack.
          </span>
        </div>
      )}
      {failed && logs && <pre className={styles.logs}>{logs}</pre>}
    </section>
  );
}
