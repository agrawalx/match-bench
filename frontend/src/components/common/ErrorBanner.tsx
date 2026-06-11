/**
 * This file defines frontend behavior for ErrorBanner.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import styles from "./ErrorBanner.module.css";

/**
 * ErrorBanner performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function ErrorBanner({ message }: { message: string }) {
  return <div className={styles.banner}>{message}</div>;
}
