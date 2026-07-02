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
  return (
    <div className={styles.banner} role="alert">
      <svg
        width="17"
        height="17"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.9"
        className={styles.icon}
        aria-hidden="true"
      >
        <circle cx="12" cy="12" r="9" />
        <path d="M12 8v5M12 16.5v.01" />
      </svg>
      <span>{message}</span>
    </div>
  );
}
