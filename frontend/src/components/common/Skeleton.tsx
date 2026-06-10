/**
 * This file defines frontend behavior for Skeleton.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import styles from "./Skeleton.module.css";

/**
 * SkeletonRows performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function SkeletonRows({
  rows = 12,
  columns = 10,
}: {
  rows?: number;
  columns?: number;
}) {
  return (
    <>
      {Array.from({ length: rows }, (_, row) => (
        <tr key={row} className={styles.row}>
          {Array.from({ length: columns }, (_, column) => (
            <td key={column}>
              <div className={styles.cell} />
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}
