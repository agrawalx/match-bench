/**
 * This file defines frontend behavior for UploadProgress.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import styles from "./UploadProgress.module.css";

/**
 * UploadProgress performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function UploadProgress({ progress }: { progress: number }) {
  if (progress <= 0) return null;
  return (
    <div className={styles.wrap}>
      <span>{progress}%</span>
      <div className={styles.track}>
        <div className={styles.fill} style={{ width: `${progress}%` }} />
      </div>
    </div>
  );
}
