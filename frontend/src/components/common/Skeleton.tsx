import styles from './Skeleton.module.css';

export function SkeletonRows({ rows = 12, columns = 10 }: { rows?: number; columns?: number }) {
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
