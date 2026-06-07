import styles from './UploadProgress.module.css';

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
